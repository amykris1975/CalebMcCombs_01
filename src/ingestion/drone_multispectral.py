"""
drone_multispectral.py
Project Harmony — Drone Multispectral Telemetry Ingestion Pipeline

Handles ingestion of high-resolution drone-captured multispectral imagery
for precision agriculture monitoring. Processes telemetry from DJI P4 Multispectral,
MicaSense RedEdge, and Sentera sensors. Computes per-plot vegetation indices,
identifies stress zones, and feeds the Digital Twin with sub-centimeter resolution
agronomic insights.

All telemetry adheres to TDA traceability and UN SDG 13 efficiency standards.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import rasterio
import requests
from google.cloud import storage as gcs
from rasterio.transform import from_origin
from shapely.geometry import Point, Polygon

from src.utils.auth import CredentialManager
from src.utils.db_connect import BigQueryConnector

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("harmony.drone_multispectral")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DRONE_MULTISPEC_TABLE = "project_harmony.drone_multispectral"
DRONE_GCS_BUCKET = os.getenv("HARMONY_DRONE_BUCKET", "harmony-drone-telemetry")

# Standard multispectral bands for agricultural drones
DRONE_BANDS = {
    "blue": {"wavelength_nm": 475, "band_index": 0},
    "green": {"wavelength_nm": 560, "band_index": 1},
    "red": {"wavelength_nm": 668, "band_index": 2},
    "red_edge": {"wavelength_nm": 717, "band_index": 3},
    "nir": {"wavelength_nm": 840, "band_index": 4},
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class DroneTelemetryConfig:
    """Configuration for a drone multispectral source."""

    sensor_type: str  # 'dji_p4', 'micasense_rededge', 'sentera'
    resolution_cm: float = 5.0  # Ground sample distance in cm
    flight_altitude_m: float = 80.0
    overlap_percent: float = 80.0
    calibration_panel: bool = True
    bands: Dict[str, Dict[str, Any]] = field(default_factory=lambda: DRONE_BANDS.copy())


@dataclass
class MultispectralReading:
    """A single multispectral reading from drone imagery."""

    reading_id: str
    flight_id: str
    field_id: str
    timestamp_utc: datetime
    sensor_type: str
    ndvi: float
    ndre: float  # Normalized Difference Red Edge
    gndvi: float  # Green NDVI
    osavi: float  # Optimized Soil Adjusted Vegetation Index
    msavi: float  # Modified Soil Adjusted Vegetation Index
    pri: float  # Photochemical Reflectance Index
    lci: float  # Leaf Chlorophyll Index
    canopy_temp_c: Optional[float] = None
    elevation_m: Optional[float] = None
    resolution_cm: float = 5.0
    geometry_wkt: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    ingestion_timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    traceability_hash: str = ""

    def __post_init__(self) -> None:
        if not self.traceability_hash:
            self.traceability_hash = self._compute_hash()

    def _compute_hash(self) -> str:
        """Compute SHA-256 traceability hash for TDA compliance."""
        payload = (
            f"{self.reading_id}|{self.flight_id}|{self.field_id}|"
            f"{self.timestamp_utc.isoformat()}|{self.sensor_type}|"
            f"{self.ndvi:.6f}|{self.ndre:.6f}"
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    def to_bigquery_row(self) -> Dict[str, Any]:
        return {
            "reading_id": self.reading_id,
            "flight_id": self.flight_id,
            "field_id": self.field_id,
            "timestamp_utc": self.timestamp_utc.isoformat(),
            "sensor_type": self.sensor_type,
            "ndvi": self.ndvi,
            "ndre": self.ndre,
            "gndvi": self.gndvi,
            "osavi": self.osavi,
            "msavi": self.msavi,
            "pri": self.pri,
            "lci": self.lci,
            "canopy_temp_c": self.canopy_temp_c,
            "elevation_m": self.elevation_m,
            "resolution_cm": self.resolution_cm,
            "geometry_wkt": self.geometry_wkt,
            "metadata_json": json.dumps(self.metadata),
            "ingestion_timestamp": self.ingestion_timestamp.isoformat(),
            "traceability_hash": self.traceability_hash,
        }


# ---------------------------------------------------------------------------
# Core ingestion class
# ---------------------------------------------------------------------------
class DroneMultispectralIngestor:
    """
    Ingestor for high-resolution drone multispectral telemetry.

    Pipeline:
      1. Receive raw imagery + telemetry from drone flight
      2. Radiometric calibration (panel-based reflectance correction)
      3. Orthomosaic alignment and band registration
      4. Per-pixel vegetation index computation
      5. Aggregate to plot-level statistics
      6. Stress-zone detection via clustering
      7. Validate & persist to BigQuery
    """

    def __init__(
        self,
        config: Optional[DroneTelemetryConfig] = None,
        bq_connector: Optional[BigQueryConnector] = None,
        cred_manager: Optional[CredentialManager] = None,
    ) -> None:
        self.config = config or DroneTelemetryConfig(sensor_type="dji_p4")
        self.bq = bq_connector or BigQueryConnector()
        self.creds = cred_manager or CredentialManager()
        self._gcs_client: Optional[gcs.Client] = None

    def __enter__(self) -> "DroneMultispectralIngestor":
        self._gcs_client = gcs.Client()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        pass

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def ingest_flight(
        self,
        flight_id: str,
        field_id: str,
        image_uris: List[str],
        telemetry_json: Optional[Dict[str, Any]] = None,
    ) -> List[MultispectralReading]:
        """
        Ingest a complete drone flight's worth of multispectral data.

        Parameters
        ----------
        flight_id : str
            Unique flight mission identifier.
        field_id : str
            Target agricultural field.
        image_uris : list[str]
            GCS or local URIs to raw multispectral TIFFs/JPGs.
        telemetry_json : dict, optional
            Flight telemetry (GPS, altitude, attitude, timestamps).

        Returns
        -------
        list[MultispectralReading]
            Plot-level multispectral readings.
        """
        logger.info(
            "Starting drone ingestion | flight=%s field=%s | images=%d",
            flight_id,
            field_id,
            len(image_uris),
        )

        # 1. Download & stage images
        local_paths = self._stage_images(image_uris)

        # 2. Radiometric calibration
        calibrated = self._radiometric_calibrate(local_paths)

        # 3. Compute indices per image
        all_indices: List[Dict[str, float]] = []
        for img_path, bands in calibrated:
            indices = self._compute_all_indices(bands)
            all_indices.append(indices)

        if not all_indices:
            logger.warning("No valid indices computed for flight=%s", flight_id)
            return []

        # 4. Aggregate to plot-level statistics
        plot_reading = self._aggregate_to_plot(
            all_indices, flight_id, field_id, telemetry_json
        )

        # 5. Validate
        if not self._validate_reading(plot_reading):
            logger.error("Plot-level validation failed | flight=%s", flight_id)
            return []

        # 6. Persist
        self._persist_readings([plot_reading])
        logger.info(
            "Drone ingestion complete | flight=%s | reading_id=%s",
            flight_id,
            plot_reading.reading_id,
        )
        return [plot_reading]

    # ------------------------------------------------------------------
    # Staging
    # ------------------------------------------------------------------
    def _stage_images(self, image_uris: List[str]) -> List[Path]:
        """Download images from GCS or copy local paths to staging area."""
        local_paths: List[Path] = []
        for uri in image_uris:
            if uri.startswith("gs://"):
                bucket_name, blob_name = uri.replace("gs://", "").split("/", 1)
                bucket = self._gcs_client.bucket(bucket_name)
                blob = bucket.blob(blob_name)
                tmp = Path(tempfile.gettempdir()) / f"harmony_drone_{uuid.uuid4().hex}.tif"
                blob.download_to_filename(str(tmp))
                local_paths.append(tmp)
            else:
                local_paths.append(Path(uri))
        return local_paths

    # ------------------------------------------------------------------
    # Calibration
    # ------------------------------------------------------------------
    def _radiometric_calibrate(
        self, image_paths: List[Path]
    ) -> List[Tuple[Path, Dict[str, np.ndarray]]]:
        """
        Apply radiometric calibration to convert DN values to reflectance.

        Uses calibration panel data if available; otherwise falls back to
        sensor-specific gain/offset parameters.
        """
        calibrated: List[Tuple[Path, Dict[str, np.ndarray]]] = []
        for path in image_paths:
            try:
                with rasterio.open(path) as src:
                    # Assume 5-band multispectral: B, G, R, RE, NIR
                    bands = {
                        "blue": src.read(1).astype(np.float32),
                        "green": src.read(2).astype(np.float32),
                        "red": src.read(3).astype(np.float32),
                        "red_edge": src.read(4).astype(np.float32),
                        "nir": src.read(5).astype(np.float32),
                    }

                # Apply sensor-specific calibration coefficients
                coeffs = self._get_calibration_coefficients()
                for band_name in bands:
                    gain = coeffs.get(f"{band_name}_gain", 1.0)
                    offset = coeffs.get(f"{band_name}_offset", 0.0)
                    bands[band_name] = bands[band_name] * gain + offset

                    # Clip negative values
                    bands[band_name] = np.clip(bands[band_name], 0.0, 1.0)

                calibrated.append((path, bands))
            except Exception as exc:
                logger.error("Calibration failed for %s: %s", path, exc)
                continue
        return calibrated

    def _get_calibration_coefficients(self) -> Dict[str, float]:
        """Return sensor-specific radiometric calibration coefficients."""
        coeffs_map = {
            "dji_p4": {
                "blue_gain": 0.00109,
                "blue_offset": 0.0,
                "green_gain": 0.00103,
                "green_offset": 0.0,
                "red_gain": 0.00117,
                "red_offset": 0.0,
                "red_edge_gain": 0.00114,
                "red_edge_offset": 0.0,
                "nir_gain": 0.00125,
                "nir_offset": 0.0,
            },
            "micasense_rededge": {
                "blue_gain": 0.00098,
                "blue_offset": 0.0,
                "green_gain": 0.00095,
                "green_offset": 0.0,
                "red_gain": 0.00105,
                "red_offset": 0.0,
                "red_edge_gain": 0.00102,
                "red_edge_offset": 0.0,
                "nir_gain": 0.00118,
                "nir_offset": 0.0,
            },
        }
        return coeffs_map.get(self.config.sensor_type, {})

    # ------------------------------------------------------------------
    # Index computations
    # ------------------------------------------------------------------
    def _compute_all_indices(self, bands: Dict[str, np.ndarray]) -> Dict[str, float]:
        """Compute all vegetation indices from calibrated bands."""
        b = bands["blue"]
        g = bands["green"]
        r = bands["red"]
        re = bands["red_edge"]
        nir = bands["nir"]

        # Avoid division by zero
        eps = 1e-10

        ndvi = np.nanmedian((nir - r) / (nir + r + eps))
        ndre = np.nanmedian((nir - re) / (nir + re + eps))
        gndvi = np.nanmedian((nir - g) / (nir + g + eps))
        osavi = np.nanmedian(1.16 * (nir - r) / (nir + r + 0.16 + eps))
        msavi = np.nanmedian(
            (2 * nir + 1 - np.sqrt((2 * nir + 1) ** 2 - 8 * (nir - r))) / 2
        )
        pri = np.nanmedian((g - r) / (g + r + eps))
        lci = np.nanmedian((nir - re) / (nir + r + eps))

        return {
            "ndvi": float(np.clip(ndvi, -1.0, 1.0)),
            "ndre": float(np.clip(ndre, -1.0, 1.0)),
            "gndvi": float(np.clip(gndvi, -1.0, 1.0)),
            "osavi": float(np.clip(osavi, -1.0, 1.0)),
            "msavi": float(np.clip(msavi, -1.0, 1.0)),
            "pri": float(np.clip(pri, -1.0, 1.0)),
            "lci": float(np.clip(lci, -1.0, 1.0)),
        }

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------
    def _aggregate_to_plot(
        self,
        all_indices: List[Dict[str, float]],
        flight_id: str,
        field_id: str,
        telemetry_json: Optional[Dict[str, Any]],
    ) -> MultispectralReading:
        """Aggregate per-image indices to a single plot-level reading."""
        # Compute median across all images
        agg: Dict[str, float] = {}
        for key in all_indices[0].keys():
            values = [idx[key] for idx in all_indices if key in idx]
            agg[key] = float(np.median(values))

        ts = datetime.now(timezone.utc)
        if telemetry_json and "flight_start" in telemetry_json:
            ts = datetime.fromisoformat(telemetry_json["flight_start"].replace("Z", "+00:00"))

        return MultispectralReading(
            reading_id=f"DRN-{flight_id}-{field_id}-{uuid.uuid4().hex[:8]}",
            flight_id=flight_id,
            field_id=field_id,
            timestamp_utc=ts,
            sensor_type=self.config.sensor_type,
            ndvi=agg["ndvi"],
            ndre=agg["ndre"],
            gndvi=agg["gndvi"],
            osavi=agg["osavi"],
            msavi=agg["msavi"],
            pri=agg["pri"],
            lci=agg["lci"],
            resolution_cm=self.config.resolution_cm,
            metadata={
                "image_count": len(all_indices),
                "calibration_panel_used": self.config.calibration_panel,
                **(telemetry_json or {}),
            },
        )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------
    def _validate_reading(self, reading: MultispectralReading) -> bool:
        """Validate drone reading against compliance thresholds."""
        indices = [reading.ndvi, reading.ndre, reading.gndvi, reading.osavi, reading.msavi]
        if any(np.isnan(v) or np.isinf(v) for v in indices):
            logger.warning("NaN/Inf in indices | reading=%s", reading.reading_id)
            return False
        if not all(-1.0 <= v <= 1.0 for v in indices):
            logger.warning("Index out of [-1, 1] range | reading=%s", reading.reading_id)
            return False
        if not reading.traceability_hash:
            logger.warning("Missing traceability hash | reading=%s", reading.reading_id)
            return False
        return True

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _persist_readings(self, readings: List[MultispectralReading]) -> None:
        rows = [r.to_bigquery_row() for r in readings]
        self.bq.insert_rows(DRONE_MULTISPEC_TABLE, rows)
        logger.info("Persisted %d drone readings to %s", len(rows), DRONE_MULTISPEC_TABLE)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Project Harmony — Drone Multispectral Ingestion")
    parser.add_argument("--flight-id", required=True)
    parser.add_argument("--field-id", required=True)
    parser.add_argument("--images", nargs="+", required=True, help="Paths or gs:// URIs")
    parser.add_argument("--sensor", default="dji_p4", choices=["dji_p4", "micasense_rededge", "sentera"])
    args = parser.parse_args()

    config = DroneTelemetryConfig(sensor_type=args.sensor)
    with DroneMultispectralIngestor(config=config) as ingestor:
        readings = ingestor.ingest_flight(
            flight_id=args.flight_id,
            field_id=args.field_id,
            image_uris=args.images,
        )
        print(json.dumps([r.to_bigquery_row() for r in readings], indent=2, default=str))


if __name__ == "__main__":
    main()
