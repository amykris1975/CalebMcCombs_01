"""
soil_sensors.py
Project Harmony — Soil Sensor Telemetry Ingestion Pipeline

Ingests NPK (Nitrogen, Phosphorus, Potassium) and pH readings from
field-deployed IoT soil sensors. Supports LoRaWAN, NB-IoT, and cellular
gateways. Performs real-time validation against agronomic thresholds,
detects sensor drift, and streams validated telemetry to BigQuery for
the Digital Twin soil health layer.

All telemetry adheres to TDA traceability and UN SDG 13 efficiency standards.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import paho.mqtt.client as mqtt
import requests
from google.cloud import bigquery

from src.utils.auth import CredentialManager
from src.utils.db_connect import BigQueryConnector

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("harmony.soil_sensors")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SOIL_READINGS_TABLE = "project_harmony.soil_readings"

# MQTT topic patterns
MQTT_TOPIC_NPK = "harmony/field/+/soil/npk"
MQTT_TOPIC_PH = "harmony/field/+/soil/ph"
MQTT_TOPIC_RAW = "harmony/field/+/soil/raw"

# Agronomic thresholds for hemp cultivation (validated against state/federal guidelines)
AGRonomic_THRESHOLDS = {
    "nitrogen_mg_kg": {"min": 10.0, "max": 200.0, "optimal_low": 50.0, "optimal_high": 150.0},
    "phosphorus_mg_kg": {"min": 5.0, "max": 100.0, "optimal_low": 15.0, "optimal_high": 60.0},
    "potassium_mg_kg": {"min": 20.0, "max": 400.0, "optimal_low": 80.0, "optimal_high": 250.0},
    "ph": {"min": 5.5, "max": 8.5, "optimal_low": 6.0, "optimal_high": 7.5},
    "soil_moisture_percent": {"min": 0.0, "max": 100.0, "optimal_low": 20.0, "optimal_high": 60.0},
    "soil_temp_c": {"min": -10.0, "max": 60.0, "optimal_low": 10.0, "optimal_high": 35.0},
    "ec_ms_m": {"min": 0.0, "max": 10.0, "optimal_low": 0.5, "optimal_high": 3.0},
}

# Sensor drift detection parameters
DRIFT_WINDOW_SIZE = 50
DRIFT_Z_THRESHOLD = 3.0


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class SoilSensorConfig:
    """Configuration for a soil sensor deployment."""

    sensor_type: str  # 'sentek', 'teros', 'decagon', 'custom_lora'
    connectivity: str  # 'lora', 'nbiot', 'cellular', 'wifi'
    gateway_id: str = ""
    sampling_interval_sec: int = 3600  # 1 hour default
    depth_cm: float = 20.0
    calibration_date: Optional[datetime] = None
    location_wkt: str = ""  # POINT(lon lat)


@dataclass
class SoilReading:
    """A single validated soil sensor reading."""

    reading_id: str
    sensor_id: str
    field_id: str
    gateway_id: str
    timestamp_utc: datetime
    sensor_type: str
    depth_cm: float
    nitrogen_mg_kg: Optional[float] = None
    phosphorus_mg_kg: Optional[float] = None
    potassium_mg_kg: Optional[float] = None
    ph: Optional[float] = None
    soil_moisture_percent: Optional[float] = None
    soil_temp_c: Optional[float] = None
    ec_ms_m: Optional[float] = None
    location_wkt: str = ""
    quality_flags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    ingestion_timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    traceability_hash: str = ""

    def __post_init__(self) -> None:
        if not self.traceability_hash:
            self.traceability_hash = self._compute_hash()

    def _compute_hash(self) -> str:
        payload = (
            f"{self.reading_id}|{self.sensor_id}|{self.field_id}|"
            f"{self.timestamp_utc.isoformat()}|"
            f"N:{self.nitrogen_mg_kg}|P:{self.phosphorus_mg_kg}|"
            f"K:{self.potassium_mg_kg}|pH:{self.ph}"
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    def to_bigquery_row(self) -> Dict[str, Any]:
        return {
            "reading_id": self.reading_id,
            "sensor_id": self.sensor_id,
            "field_id": self.field_id,
            "gateway_id": self.gateway_id,
            "timestamp_utc": self.timestamp_utc.isoformat(),
            "sensor_type": self.sensor_type,
            "depth_cm": self.depth_cm,
            "nitrogen_mg_kg": self.nitrogen_mg_kg,
            "phosphorus_mg_kg": self.phosphorus_mg_kg,
            "potassium_mg_kg": self.potassium_mg_kg,
            "ph": self.ph,
            "soil_moisture_percent": self.soil_moisture_percent,
            "soil_temp_c": self.soil_temp_c,
            "ec_ms_m": self.ec_ms_m,
            "location_wkt": self.location_wkt,
            "quality_flags": self.quality_flags,
            "metadata_json": json.dumps(self.metadata),
            "ingestion_timestamp": self.ingestion_timestamp.isoformat(),
            "traceability_hash": self.traceability_hash,
        }


# ---------------------------------------------------------------------------
# Core ingestion class
# ---------------------------------------------------------------------------
class SoilSensorIngestor:
    """
    Real-time soil sensor telemetry ingestor.

    Supports multiple ingestion modes:
      - MQTT subscriber for live LoRaWAN/NB-IoT streams
      - REST API polling for cellular/wifi gateways
      - Batch file ingestion for historical data loads

    Each reading passes through a validation pipeline:
      1. Schema validation (required fields present)
      2. Range validation (values within agronomic thresholds)
      3. Drift detection (statistical anomaly detection)
      4. Hot Crop compliance check (hemp-specific regulatory bounds)
      5. Traceability hashing
    """

    def __init__(
        self,
        config: Optional[SoilSensorConfig] = None,
        bq_connector: Optional[BigQueryConnector] = None,
        cred_manager: Optional[CredentialManager] = None,
    ) -> None:
        self.config = config or SoilSensorConfig(sensor_type="sentek", connectivity="lora")
        self.bq = bq_connector or BigQueryConnector()
        self.creds = cred_manager or CredentialManager()
        self._mqtt_client: Optional[mqtt.Client] = None
        self._drift_history: Dict[str, List[float]] = {}

    # ------------------------------------------------------------------
    # MQTT ingestion (real-time)
    # ------------------------------------------------------------------
    def start_mqtt_listener(self, broker_host: str, broker_port: int = 1883) -> None:
        """Start blocking MQTT listener for live sensor telemetry."""
        username, password = self.creds.get_mqtt_credentials()

        self._mqtt_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"harmony_soil_{int(time.time())}",
        )
        self._mqtt_client.username_pw_set(username, password)
        self._mqtt_client.on_connect = self._on_mqtt_connect
        self._mqtt_client.on_message = self._on_mqtt_message

        logger.info("Connecting to MQTT broker | %s:%d", broker_host, broker_port)
        self._mqtt_client.connect(broker_host, broker_port, keepalive=60)
        self._mqtt_client.loop_forever()

    def _on_mqtt_connect(self, client, userdata, flags, rc, properties=None):  # noqa: ANN001
        logger.info("MQTT connected | rc=%s", rc)
        client.subscribe(MQTT_TOPIC_NPK)
        client.subscribe(MQTT_TOPIC_PH)
        client.subscribe(MQTT_TOPIC_RAW)

    def _on_mqtt_message(self, client, userdata, msg):  # noqa: ANN001
        try:
            payload = json.loads(msg.payload.decode())
            readings = self._parse_payload(payload, topic=msg.topic)
            for reading in readings:
                if self._validate_reading(reading):
                    self._persist_reading(reading)
                else:
                    logger.warning("Validation failed | reading_id=%s", reading.reading_id)
        except Exception as exc:
            logger.error("MQTT message processing failed: %s", exc)

    # ------------------------------------------------------------------
    # REST API polling
    # ------------------------------------------------------------------
    def poll_gateway_api(self, gateway_url: str, field_id: str) -> List[SoilReading]:
        """Poll a sensor gateway REST API for new readings."""
        api_key = self.creds.get_sensor_api_key()
        headers = {"Authorization": f"Bearer {api_key}"}

        resp = requests.get(
            f"{gateway_url}/readings",
            headers=headers,
            params={"field_id": field_id, "limit": 100},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        readings: List[SoilReading] = []
        for item in data.get("readings", []):
            parsed = self._parse_api_item(item, field_id)
            if parsed and self._validate_reading(parsed):
                readings.append(parsed)

        if readings:
            self._persist_readings(readings)
        return readings

    # ------------------------------------------------------------------
    # Batch ingestion
    # ------------------------------------------------------------------
    def ingest_batch_file(self, file_path: str, field_id: str) -> List[SoilReading]:
        """Ingest a batch JSON/CSV file of historical soil readings."""
        path = os.path.expanduser(file_path)
        readings: List[SoilReading] = []

        if path.endswith(".json"):
            with open(path) as fh:
                data = json.load(fh)
            for item in data:
                parsed = self._parse_api_item(item, field_id)
                if parsed and self._validate_reading(parsed):
                    readings.append(parsed)

        elif path.endswith(".csv"):
            import csv
            with open(path, newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    parsed = self._parse_csv_row(row, field_id)
                    if parsed and self._validate_reading(parsed):
                        readings.append(parsed)

        if readings:
            self._persist_readings(readings)
            logger.info("Batch ingested | file=%s | readings=%d", file_path, len(readings))
        return readings

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------
    def _parse_payload(
        self, payload: Dict[str, Any], topic: str = ""
    ) -> List[SoilReading]:
        """Parse an incoming MQTT payload into SoilReading objects."""
        readings: List[SoilReading] = []

        # Handle both single-reading and batch payloads
        items = payload if isinstance(payload, list) else [payload]

        for item in items:
            sensor_id = item.get("sensor_id", item.get("dev_eui", "unknown"))
            field_id = item.get("field_id", self._extract_field_from_topic(topic))
            gateway_id = item.get("gateway_id", self.config.gateway_id)

            ts = self._parse_timestamp(item.get("timestamp", item.get("ts", "")))

            reading = SoilReading(
                reading_id=f"SOIL-{sensor_id}-{ts.strftime('%Y%m%d%H%M%S')}",
                sensor_id=sensor_id,
                field_id=field_id,
                gateway_id=gateway_id,
                timestamp_utc=ts,
                sensor_type=item.get("sensor_type", self.config.sensor_type),
                depth_cm=item.get("depth_cm", self.config.depth_cm),
                nitrogen_mg_kg=self._safe_float(item.get("n", item.get("nitrogen"))),
                phosphorus_mg_kg=self._safe_float(item.get("p", item.get("phosphorus"))),
                potassium_mg_kg=self._safe_float(item.get("k", item.get("potassium"))),
                ph=self._safe_float(item.get("ph")),
                soil_moisture_percent=self._safe_float(item.get("moisture", item.get("vwc"))),
                soil_temp_c=self._safe_float(item.get("soil_temp", item.get("temp"))),
                ec_ms_m=self._safe_float(item.get("ec", item.get("electrical_conductivity"))),
                location_wkt=item.get("location_wkt", self.config.location_wkt),
                metadata={"raw_payload": item, "topic": topic},
            )
            readings.append(reading)

        return readings

    def _parse_api_item(self, item: Dict[str, Any], field_id: str) -> Optional[SoilReading]:
        """Parse a single item from a gateway REST API response."""
        try:
            ts = self._parse_timestamp(item.get("timestamp", item.get("recorded_at", "")))
            sensor_id = item.get("sensor_id", item.get("device_id", "unknown"))

            return SoilReading(
                reading_id=f"SOIL-{sensor_id}-{ts.strftime('%Y%m%d%H%M%S')}",
                sensor_id=sensor_id,
                field_id=item.get("field_id", field_id),
                gateway_id=item.get("gateway_id", ""),
                timestamp_utc=ts,
                sensor_type=item.get("sensor_type", "unknown"),
                depth_cm=self._safe_float(item.get("depth_cm", 20.0)),
                nitrogen_mg_kg=self._safe_float(item.get("nitrogen_mg_kg", item.get("n"))),
                phosphorus_mg_kg=self._safe_float(item.get("phosphorus_mg_kg", item.get("p"))),
                potassium_mg_kg=self._safe_float(item.get("potassium_mg_kg", item.get("k"))),
                ph=self._safe_float(item.get("ph")),
                soil_moisture_percent=self._safe_float(item.get("soil_moisture_percent")),
                soil_temp_c=self._safe_float(item.get("soil_temp_c")),
                ec_ms_m=self._safe_float(item.get("ec_ms_m")),
                location_wkt=item.get("location", ""),
                metadata={"source": "rest_api", "raw": item},
            )
        except Exception as exc:
            logger.error("API item parse failed: %s", exc)
            return None

    def _parse_csv_row(self, row: Dict[str, str], field_id: str) -> Optional[SoilReading]:
        """Parse a CSV row into a SoilReading."""
        try:
            ts = self._parse_timestamp(row.get("timestamp", row.get("datetime", "")))
            sensor_id = row.get("sensor_id", row.get("device_id", "unknown"))

            return SoilReading(
                reading_id=f"SOIL-{sensor_id}-{ts.strftime('%Y%m%d%H%M%S')}",
                sensor_id=sensor_id,
                field_id=row.get("field_id", field_id),
                gateway_id=row.get("gateway_id", ""),
                timestamp_utc=ts,
                sensor_type=row.get("sensor_type", "unknown"),
                depth_cm=float(row.get("depth_cm", 20)),
                nitrogen_mg_kg=self._safe_float(row.get("nitrogen_mg_kg", row.get("n"))),
                phosphorus_mg_kg=self._safe_float(row.get("phosphorus_mg_kg", row.get("p"))),
                potassium_mg_kg=self._safe_float(row.get("potassium_mg_kg", row.get("k"))),
                ph=self._safe_float(row.get("ph")),
                soil_moisture_percent=self._safe_float(row.get("soil_moisture_percent")),
                soil_temp_c=self._safe_float(row.get("soil_temp_c")),
                ec_ms_m=self._safe_float(row.get("ec_ms_m")),
                location_wkt=row.get("location_wkt", ""),
            )
        except Exception as exc:
            logger.error("CSV row parse failed: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Validation pipeline
    # ------------------------------------------------------------------
    def _validate_reading(self, reading: SoilReading) -> bool:
        """
        Full validation pipeline for a soil reading.

        Returns True only if the reading passes all checks.
        Quality flags are populated for informational warnings.
        """
        flags: List[str] = []

        # 1. Schema check — at least NPK or pH must be present
        has_npk = any(
            v is not None
            for v in [reading.nitrogen_mg_kg, reading.phosphorus_mg_kg, reading.potassium_mg_kg]
        )
        has_ph = reading.ph is not None
        if not (has_npk or has_ph):
            logger.warning("No NPK or pH data | reading=%s", reading.reading_id)
            return False

        # 2. Range validation
        fields_to_check = {
            "nitrogen_mg_kg": reading.nitrogen_mg_kg,
            "phosphorus_mg_kg": reading.phosphorus_mg_kg,
            "potassium_mg_kg": reading.potassium_mg_kg,
            "ph": reading.ph,
            "soil_moisture_percent": reading.soil_moisture_percent,
            "soil_temp_c": reading.soil_temp_c,
            "ec_ms_m": reading.ec_ms_m,
        }

        for field_name, value in fields_to_check.items():
            if value is None:
                continue
            thresholds = AGRonomic_THRESHOLDS.get(field_name)
            if not thresholds:
                continue
            if not (thresholds["min"] <= value <= thresholds["max"]):
                logger.warning(
                    "Out of range | %s=%.2f (allowed %.1f-%.1f) | reading=%s",
                    field_name,
                    value,
                    thresholds["min"],
                    thresholds["max"],
                    reading.reading_id,
                )
                return False
            if not (thresholds["optimal_low"] <= value <= thresholds["optimal_high"]):
                flags.append(f"{field_name}_suboptimal")

        # 3. Drift detection
        for field_name, value in fields_to_check.items():
            if value is None:
                continue
            if self._detect_drift(reading.sensor_id, field_name, value):
                flags.append(f"{field_name}_drift_detected")

        # 4. Hot Crop compliance (hemp-specific)
        if reading.ph is not None and not (6.0 <= reading.ph <= 7.5):
            flags.append("ph_hemp_nonoptimal")

        reading.quality_flags = flags
        return True

    def _detect_drift(self, sensor_id: str, field: str, value: float) -> bool:
        """
        Simple z-score drift detection over a rolling window.

        Maintains per-sensor, per-field rolling history and flags
        readings that exceed DRIFT_Z_THRESHOLD standard deviations.
        """
        key = f"{sensor_id}:{field}"
        history = self._drift_history.setdefault(key, [])
        history.append(value)

        if len(history) > DRIFT_WINDOW_SIZE:
            history.pop(0)

        if len(history) < 10:
            return False

        mean = sum(history) / len(history)
        variance = sum((x - mean) ** 2 for x in history) / len(history)
        std = variance ** 0.5

        if std == 0:
            return False

        z_score = abs(value - mean) / std
        return z_score > DRIFT_Z_THRESHOLD

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _persist_reading(self, reading: SoilReading) -> None:
        self.bq.insert_rows(SOIL_READINGS_TABLE, [reading.to_bigquery_row()])

    def _persist_readings(self, readings: List[SoilReading]) -> None:
        rows = [r.to_bigquery_row() for r in readings]
        self.bq.insert_rows(SOIL_READINGS_TABLE, rows)
        logger.info("Persisted %d soil readings", len(rows))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        """Safely convert a value to float, returning None on failure."""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_timestamp(ts_raw: str) -> datetime:
        """Parse various timestamp formats."""
        if not ts_raw:
            return datetime.now(timezone.utc)
        ts_str = str(ts_raw).replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(ts_str)
        except ValueError:
            # Try common formats
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y %H:%M:%S"):
                try:
                    return datetime.strptime(ts_str, fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
            return datetime.now(timezone.utc)

    @staticmethod
    def _extract_field_from_topic(topic: str) -> str:
        """Extract field_id from MQTT topic like harmony/field/F123/soil/npk."""
        match = re.search(r"/field/([^/]+)/", topic)
        return match.group(1) if match else "unknown"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Project Harmony — Soil Sensor Ingestion")
    subparsers = parser.add_subparsers(dest="command")

    # MQTT listener
    mqtt_parser = subparsers.add_parser("mqtt", help="Start MQTT listener")
    mqtt_parser.add_argument("--broker", default="localhost")
    mqtt_parser.add_argument("--port", type=int, default=1883)

    # REST poll
    poll_parser = subparsers.add_parser("poll", help="Poll gateway API")
    poll_parser.add_argument("--gateway-url", required=True)
    poll_parser.add_argument("--field-id", required=True)

    # Batch ingest
    batch_parser = subparsers.add_parser("batch", help="Ingest batch file")
    batch_parser.add_argument("--file", required=True)
    batch_parser.add_argument("--field-id", required=True)

    args = parser.parse_args()

    ingestor = SoilSensorIngestor()

    if args.command == "mqtt":
        ingestor.start_mqtt_listener(args.broker, args.port)
    elif args.command == "poll":
        readings = ingestor.poll_gateway_api(args.gateway_url, args.field_id)
        print(json.dumps([r.to_bigquery_row() for r in readings], indent=2, default=str))
    elif args.command == "batch":
        readings = ingestor.ingest_batch_file(args.file, args.field_id)
        print(json.dumps([r.to_bigquery_row() for r in readings], indent=2, default=str))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
