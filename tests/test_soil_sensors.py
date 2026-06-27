"""
test_soil_sensors.py
Unit and integration tests for the soil sensor telemetry ingestion pipeline.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.soil_sensors import (
    SoilReading,
    SoilSensorConfig,
    SoilSensorIngestor,
)
from src.utils.auth import CredentialManager
from src.utils.db_connect import BigQueryConnector


# =============================================================================
# Fixtures
# =============================================================================
@pytest.fixture
def sensor_config() -> SoilSensorConfig:
    return SoilSensorConfig(
        sensor_type="sentek",
        connectivity="lora",
        gateway_id="GW-001",
        depth_cm=20.0,
        location_wkt="POINT(-105.0 40.0)",
    )


@pytest.fixture
def mock_bq() -> MagicMock:
    return MagicMock(spec=BigQueryConnector)


@pytest.fixture
def mock_creds() -> MagicMock:
    cm = MagicMock(spec=CredentialManager)
    cm.get_mqtt_credentials.return_value = ("mqtt_user", "mqtt_pass")
    cm.get_sensor_api_key.return_value = "test_api_key"
    return cm


@pytest.fixture
def valid_payload() -> dict:
    return {
        "sensor_id": "SENTEK-042",
        "field_id": "FIELD-A",
        "gateway_id": "GW-001",
        "timestamp": "2024-06-15T14:30:00Z",
        "sensor_type": "sentek",
        "depth_cm": 20.0,
        "n": 85.0,
        "p": 25.0,
        "k": 120.0,
        "ph": 6.8,
        "moisture": 35.0,
        "temp": 22.5,
        "ec": 1.2,
    }


# =============================================================================
# SoilReading Tests
# =============================================================================
class TestSoilReading:
    def test_traceability_hash(self) -> None:
        reading = SoilReading(
            reading_id="SOIL-001",
            sensor_id="SENTEK-042",
            field_id="FIELD-A",
            gateway_id="GW-001",
            timestamp_utc=datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc),
            sensor_type="sentek",
            depth_cm=20.0,
            nitrogen_mg_kg=85.0,
            ph=6.8,
        )
        assert reading.traceability_hash
        assert len(reading.traceability_hash) == 64

    def test_to_bigquery_row(self) -> None:
        reading = SoilReading(
            reading_id="SOIL-001",
            sensor_id="SENTEK-042",
            field_id="FIELD-A",
            gateway_id="GW-001",
            timestamp_utc=datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc),
            sensor_type="sentek",
            depth_cm=20.0,
            nitrogen_mg_kg=85.0,
            phosphorus_mg_kg=25.0,
            potassium_mg_kg=120.0,
            ph=6.8,
            soil_moisture_percent=35.0,
            soil_temp_c=22.5,
            ec_ms_m=1.2,
            quality_flags=["ph_optimal"],
        )
        row = reading.to_bigquery_row()
        assert row["reading_id"] == "SOIL-001"
        assert row["nitrogen_mg_kg"] == 85.0
        assert row["ph"] == 6.8
        assert row["quality_flags"] == ["ph_optimal"]
        assert row["traceability_hash"] == reading.traceability_hash

    def test_none_values_allowed(self) -> None:
        reading = SoilReading(
            reading_id="SOIL-002",
            sensor_id="SENTEK-043",
            field_id="FIELD-B",
            gateway_id="GW-001",
            timestamp_utc=datetime.now(timezone.utc),
            sensor_type="sentek",
            depth_cm=20.0,
            nitrogen_mg_kg=None,
            ph=7.0,
        )
        row = reading.to_bigquery_row()
        assert row["nitrogen_mg_kg"] is None
        assert row["ph"] == 7.0


# =============================================================================
# Parsing Tests
# =============================================================================
class TestParsing:
    def test_parse_mqtt_payload_single(self, sensor_config: SoilSensorConfig) -> None:
        ingestor = SoilSensorIngestor(config=sensor_config)
        payload = {
            "sensor_id": "SENTEK-042",
            "field_id": "FIELD-A",
            "timestamp": "2024-06-15T14:30:00Z",
            "n": 85.0,
            "p": 25.0,
            "k": 120.0,
            "ph": 6.8,
            "moisture": 35.0,
            "temp": 22.5,
            "ec": 1.2,
        }
        readings = ingestor._parse_payload(payload)
        assert len(readings) == 1
        r = readings[0]
        assert r.sensor_id == "SENTEK-042"
        assert r.nitrogen_mg_kg == 85.0
        assert r.phosphorus_mg_kg == 25.0
        assert r.potassium_mg_kg == 120.0
        assert r.ph == 6.8

    def test_parse_mqtt_payload_batch(self, sensor_config: SoilSensorConfig) -> None:
        ingestor = SoilSensorIngestor(config=sensor_config)
        payloads = [
            {"sensor_id": "S1", "timestamp": "2024-06-15T14:00:00Z", "ph": 6.5},
            {"sensor_id": "S2", "timestamp": "2024-06-15T14:00:00Z", "ph": 7.0},
        ]
        readings = ingestor._parse_payload(payloads)
        assert len(readings) == 2
        assert readings[0].sensor_id == "S1"
        assert readings[1].sensor_id == "S2"

    def test_parse_timestamp_iso(self, sensor_config: SoilSensorConfig) -> None:
        ingestor = SoilSensorIngestor(config=sensor_config)
        ts = ingestor._parse_timestamp("2024-06-15T14:30:00Z")
        assert ts.year == 2024
        assert ts.month == 6
        assert ts.day == 15
        assert ts.hour == 14

    def test_parse_timestamp_empty(self, sensor_config: SoilSensorConfig) -> None:
        ingestor = SoilSensorIngestor(config=sensor_config)
        ts = ingestor._parse_timestamp("")
        assert isinstance(ts, datetime)

    def test_extract_field_from_topic(self, sensor_config: SoilSensorConfig) -> None:
        ingestor = SoilSensorIngestor(config=sensor_config)
        field = ingestor._extract_field_from_topic("harmony/field/F123/soil/npk")
        assert field == "F123"


# =============================================================================
# Validation Tests
# =============================================================================
class TestValidation:
    def test_valid_npk_reading_passes(self, sensor_config: SoilSensorConfig) -> None:
        ingestor = SoilSensorIngestor(config=sensor_config)
        reading = SoilReading(
            reading_id="SOIL-001",
            sensor_id="SENTEK-042",
            field_id="FIELD-A",
            gateway_id="GW-001",
            timestamp_utc=datetime.now(timezone.utc),
            sensor_type="sentek",
            depth_cm=20.0,
            nitrogen_mg_kg=85.0,
            phosphorus_mg_kg=25.0,
            potassium_mg_kg=120.0,
            ph=6.8,
        )
        assert ingestor._validate_reading(reading) is True

    def test_ph_only_reading_passes(self, sensor_config: SoilSensorConfig) -> None:
        ingestor = SoilSensorIngestor(config=sensor_config)
        reading = SoilReading(
            reading_id="SOIL-002",
            sensor_id="SENTEK-043",
            field_id="FIELD-A",
            gateway_id="GW-001",
            timestamp_utc=datetime.now(timezone.utc),
            sensor_type="sentek",
            depth_cm=20.0,
            ph=6.8,
        )
        assert ingestor._validate_reading(reading) is True

    def test_no_npk_or_ph_fails(self, sensor_config: SoilSensorConfig) -> None:
        ingestor = SoilSensorIngestor(config=sensor_config)
        reading = SoilReading(
            reading_id="SOIL-003",
            sensor_id="SENTEK-044",
            field_id="FIELD-A",
            gateway_id="GW-001",
            timestamp_utc=datetime.now(timezone.utc),
            sensor_type="sentek",
            depth_cm=20.0,
            soil_moisture_percent=35.0,
        )
        assert ingestor._validate_reading(reading) is False

    def test_nitrogen_out_of_range_fails(self, sensor_config: SoilSensorConfig) -> None:
        ingestor = SoilSensorIngestor(config=sensor_config)
        reading = SoilReading(
            reading_id="SOIL-004",
            sensor_id="SENTEK-045",
            field_id="FIELD-A",
            gateway_id="GW-001",
            timestamp_utc=datetime.now(timezone.utc),
            sensor_type="sentek",
            depth_cm=20.0,
            nitrogen_mg_kg=500.0,  # Exceeds max of 200
        )
        assert ingestor._validate_reading(reading) is False

    def test_ph_out_of_range_fails(self, sensor_config: SoilSensorConfig) -> None:
        ingestor = SoilSensorIngestor(config=sensor_config)
        reading = SoilReading(
            reading_id="SOIL-005",
            sensor_id="SENTEK-046",
            field_id="FIELD-A",
            gateway_id="GW-001",
            timestamp_utc=datetime.now(timezone.utc),
            sensor_type="sentek",
            depth_cm=20.0,
            ph=9.0,  # Exceeds max of 8.5
        )
        assert ingestor._validate_reading(reading) is False

    def test_suboptimal_ph_flag(self, sensor_config: SoilSensorConfig) -> None:
        ingestor = SoilSensorIngestor(config=sensor_config)
        reading = SoilReading(
            reading_id="SOIL-006",
            sensor_id="SENTEK-047",
            field_id="FIELD-A",
            gateway_id="GW-001",
            timestamp_utc=datetime.now(timezone.utc),
            sensor_type="sentek",
            depth_cm=20.0,
            ph=5.8,  # Within range but suboptimal for hemp (< 6.0)
            nitrogen_mg_kg=50.0,
        )
        assert ingestor._validate_reading(reading) is True
        assert "ph_hemp_nonoptimal" in reading.quality_flags


# =============================================================================
# Drift Detection Tests
# =============================================================================
class TestDriftDetection:
    def test_no_drift_with_few_samples(self, sensor_config: SoilSensorConfig) -> None:
        ingestor = SoilSensorIngestor(config=sensor_config)
        assert ingestor._detect_drift("S1", "ph", 7.0) is False
        assert ingestor._detect_drift("S1", "ph", 7.1) is False

    def test_drift_detected_with_outlier(self, sensor_config: SoilSensorConfig) -> None:
        ingestor = SoilSensorIngestor(config=sensor_config)
        # Build history of consistent readings
        for _ in range(15):
            ingestor._detect_drift("S1", "ph", 7.0)
        # Extreme outlier should trigger drift
        result = ingestor._detect_drift("S1", "ph", 12.0)
        assert result is True

    def test_no_drift_with_normal_variation(self, sensor_config: SoilSensorConfig) -> None:
        ingestor = SoilSensorIngestor(config=sensor_config)
        for val in [6.8, 7.0, 7.2, 6.9, 7.1, 6.8, 7.0, 7.2, 6.9, 7.1, 6.8, 7.0]:
            ingestor._detect_drift("S2", "ph", val)
        result = ingestor._detect_drift("S2", "ph", 7.5)
        assert result is False


# =============================================================================
# Helper Tests
# =============================================================================
class TestHelpers:
    def test_safe_float_valid(self, sensor_config: SoilSensorConfig) -> None:
        ingestor = SoilSensorIngestor(config=sensor_config)
        assert ingestor._safe_float("42.5") == 42.5
        assert ingestor._safe_float(42.5) == 42.5
        assert ingestor._safe_float(42) == 42.0

    def test_safe_float_none(self, sensor_config: SoilSensorConfig) -> None:
        ingestor = SoilSensorIngestor(config=sensor_config)
        assert ingestor._safe_float(None) is None

    def test_safe_float_invalid(self, sensor_config: SoilSensorConfig) -> None:
        ingestor = SoilSensorIngestor(config=sensor_config)
        assert ingestor._safe_float("not_a_number") is None


# =============================================================================
# Batch Ingestion Tests
# =============================================================================
class TestBatchIngestion:
    def test_ingest_batch_json(
        self,
        sensor_config: SoilSensorConfig,
        mock_bq: MagicMock,
        tmp_path,
    ) -> None:
        ingestor = SoilSensorIngestor(config=sensor_config, bq_connector=mock_bq)

        data = [
            {"sensor_id": "S1", "timestamp": "2024-06-15T14:00:00Z", "ph": 6.5},
            {"sensor_id": "S2", "timestamp": "2024-06-15T14:00:00Z", "ph": 7.0},
        ]
        json_file = tmp_path / "batch.json"
        json_file.write_text(json.dumps(data))

        readings = ingestor.ingest_batch_file(str(json_file), "FIELD-A")
        assert len(readings) == 2
        mock_bq.insert_rows.assert_called_once()

    def test_ingest_batch_csv(
        self,
        sensor_config: SoilSensorConfig,
        mock_bq: MagicMock,
        tmp_path,
    ) -> None:
        ingestor = SoilSensorIngestor(config=sensor_config, bq_connector=mock_bq)

        csv_content = "sensor_id,timestamp,ph,nitrogen_mg_kg\nS1,2024-06-15T14:00:00Z,6.5,50\nS2,2024-06-15T14:00:00Z,7.0,60"
        csv_file = tmp_path / "batch.csv"
        csv_file.write_text(csv_content)

        readings = ingestor.ingest_batch_file(str(csv_file), "FIELD-A")
        assert len(readings) == 2
        mock_bq.insert_rows.assert_called_once()
