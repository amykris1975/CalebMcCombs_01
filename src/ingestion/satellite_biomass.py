"""
satellite_biomass.py
Project Harmony — Satellite Biomass Density Ingestion Pipeline

Fetches and processes satellite imagery data for agricultural biomass density mapping.
Integrates with Sentinel-2, Landsat-8, and commercial satellite APIs to retrieve
multispectral imagery, compute NDVI/EVI indices, and estimate biomass density
for hemp cultivation monitoring.

All telemetry adheres to TDA traceability and UN SDG 13 efficiency standards.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import rasterio
import requests
from google.cloud import bigquery
from google.cloud import storage as gcs
from rasterio.mask import mask
from rasterio.transform import from_bounds
from shapely.geometry import box, mapping

from src.utils.auth import CredentialManager
from src.utils.db_connect import BigQueryConnector

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("harmony.satellite_biomass")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SENTINEL_HUB_URL = "https://services.sentinel-hub.com"
LANDSAT_API_URL = "https://m2m.cr.usgs.gov/api/api/json/stable"
SATELLITE_CONFIG_TABLE = "project_harmony.satellite_configs"
BIODENSITY_TABLE = "project_harmony.biomass_density"

# Default bounding box for hemp pilot fields (adjust per deployment)
DEFAULT_BBOX = {
    "min_lon": -105.0,
    "min_lat": 40.0,
    "max_lon": -104.9,
    "max_lat": 40.1,
}

# Spectral bands for biomass estimation (Sentinel-2 10m/20m resolution)
REQUIRED_BANDS = ["B04", "B08", "B11", "B12"]  # Red, NIR, SWIR1, SWIR2


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class SatelliteConfig:
    """Configuration for a satellite data source."""

    source: str  # 'sentinel2', 'landsat8', 'planet'
    api_endpoint: str
    collection_id: Optional[str] = None
    resolution_m: float = 10.0
    max_cloud_cover: float = 15.0  # percent
    bands: List[str] = field(default_factory=lambda: REQUIRED_BANDS)
    credentials_key: str = "satellite_api"


@dataclass
class BiomassReading:
    """A single biomass density reading derived from satellite imagery."""

    reading_id: str
    field_id: str
    timestamp_utc: datetime
    source_satellite: str
    ndvi: float
    evi: float
    biomass_density_kg_ha: float
    canopy_cover_percent: float
    cloud_cover_percent: float
    resolution_m: float
    geometry_wkt: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    ingestion_timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    traceability_hash: str = ""

    def __post_init__(self) -> None:
        if not self.traceability_hash:
            self.traceability_hash = self._compute_hash()

    def _compute_hash(self) -> str:
        """Compute SHA-256 traceability hash for TDA compliance."""
        payload = (
            f"{self.reading_id}|{self.field_id}|{self.timestamp_utc.isoformat()}|"
            f"{self.source_satellite}|{self.ndvi:.6f}|{self.evi:.6f}|"
            f"{self.biomass_density_kg_ha:.2f}"
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    def to_bigquery_row(self) -> Dict[str, Any]:
        """Convert to a BigQuery-compatible dictionary."""
        return {
            "reading_id": self.reading_id,
            "field_id": self.field_id,
            "timestamp_utc": self.timestamp_utc.isoformat(),
            "source_satellite": self.source_satellite,
            "ndvi": self.ndvi,
            "evi": self.evi,
            "biomass_density_kg_ha": self.biomass_density_kg_ha,
            "canopy_cover_percent": self.canopy_cover_percent,
            "cloud_cover_percent": self.cloud_cover_percent,
            "resolution_m": self.resolution_m,
            "geometry_wkt": self.geometry_wkt,
            "metadata_json": json.dumps(self.metadata),
            "ingestion_timestamp": self.ingestion_timestamp.isoformat(),
            "traceability_hash": self.traceability_hash,
        }


# ---------------------------------------------------------------------------
# Core ingestion class
# ---------------------------------------------------------------------------
class SatelliteBiomassIngestor:
    """
    Main ingestion orchestrator for satellite-derived biomass density data.

    Responsibilities:
      - Authenticate with satellite imagery providers
      - Query available imagery for field boundaries & date ranges
      - Download and cache raw scenes
      - Compute vegetation indices (NDVI, EVI)
      - Estimate biomass density via calibrated regression models
      - Validate outputs against compliance thresholds
      - Stream results to BigQuery with full traceability
    """

    def __init__(
        self,
        config: Optional[SatelliteConfig] = None,
        bq_connector: Optional[BigQueryConnector] = None,
        cred_manager: Optional[CredentialManager] = None,
    ) -> None:
        self.config = config or SatelliteConfig(
            source="sentinel2",
            api_endpoint=SENTINEL_HUB_URL,
        )
        self.bq = bq_connector or BigQueryConnector()
        self.creds = cred_manager or CredentialManager()
        self._session: Optional[requests.Session] = None
        self._gcs_client: Optional[gcs.Client] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def __enter__(self) -> "SatelliteBiomassIngestor":
        self._session = requests.Session()
        self._gcs_client = gcs.Client()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        if self._session:
            self._session.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run_ingestion(
        self,
        field_id: str,
        bbox: Optional[Dict[str, float]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[BiomassReading]:
        """
        Execute the full ingestion pipeline for a given field.

        Parameters
        ----------
        field_id : str
            Unique identifier for the agricultural field/polygon.
        bbox : dict, optional
            Bounding box as {"min_lon", "min_lat", "max_lon", "max_lat"}.
        start_date : datetime, optional
            Start of the temporal window (defaults to 7 days ago).
        end_date : datetime, optional
            End of the temporal window (defaults to now).

        Returns
        -------
        list[BiomassReading]
            Validated biomass density readings ready for downstream analytics.
        """
        bbox = bbox or DEFAULT_BBOX
        end_date = end_date or datetime.now(timezone.utc)
        start_date = start_date or (end_date - timedelta(days=7))

        logger.info(
            "Starting satellite biomass ingestion | field=%s source=%s | %s → %s",
            field_id,
            self.config.source,
            start_date.date(),
            end_date.date(),
        )

        # 1. Discover available scenes
        scenes = self._discover_scenes(bbox, start_date, end_date)
        if not scenes:
            logger.warning("No satellite scenes found for the given criteria.")
            return []

        # 2. Download & process each scene
        readings: List[BiomassReading] = []
        for scene in scenes:
            try:
                reading = self._process_scene(scene, field_id, bbox)
                if reading and self._validate_reading(reading):
                    readings.append(reading)
            except Exception as exc:
                logger.error(
                    "Failed to process scene %s: %s", scene.get("id", "unknown"), exc
                )
                continue

        # 3. Persist to BigQuery
        if readings:
            self._persist_readings(readings)
            logger.info(
                "Ingestion complete | field=%s | readings=%d", field_id, len(readings)
            )
        else:
            logger.warning("No valid readings produced for field=%s", field_id)

        return readings

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------
    def _discover_scenes(
        self,
        bbox: Dict[str, float],
        start_date: datetime,
        end_date: datetime,
    ) -> List[Dict[str, Any]]:
        """Query the satellite catalog for scenes intersecting the bbox & time range."""
        if self.config.source == "sentinel2":
            return self._discover_sentinel(bbox, start_date, end_date)
        if self.config.source == "landsat8":
            return self._discover_landsat(bbox, start_date, end_date)
        return []

    def _discover_sentinel(
        self,
        bbox: Dict[str, float],
        start_date: datetime,
        end_date: datetime,
    ) -> List[Dict[str, Any]]:
        """Search Sentinel-2 L2A products via Sentinel Hub Catalog API."""
        token = self._get_sentinel_token()
        headers = {"Authorization": f"Bearer {token}"}

        catalog_url = f"{SENTINEL_HUB_URL}/api/v1/catalog/search"
        payload = {
            "bbox": [
                bbox["min_lon"],
                bbox["min_lat"],
                bbox["max_lon"],
                bbox["max_lat"],
            ],
            "datetime": f"{start_date.isoformat()}/{end_date.isoformat()}",
            "collections": ["sentinel-2-l2a"],
            "limit": 20,
            "fields": {
                "include": ["id", "datetime", "eo:cloud_cover", "sara:revisit_frequency"],
                "exclude": [],
            },
        }

        resp = self._session.post(catalog_url, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        features = data.get("features", [])

        # Filter by cloud cover
        filtered = [
            f
            for f in features
            if f.get("properties", {}).get("eo:cloud_cover", 100) <= self.config.max_cloud_cover
        ]
        logger.info("Sentinel-2 catalog | total=%d | cloud-filtered=%d", len(features), len(filtered))
        return filtered

    def _discover_landsat(
        self,
        bbox: Dict[str, float],
        start_date: datetime,
        end_date: datetime,
    ) -> List[Dict[str, Any]]:
        """Search Landsat-8/9 Collection 2 Level-2 via USGS M2M API."""
        # Simplified placeholder — production would implement USGS M2M login + scene search
        logger.info("Landsat discovery placeholder — implement USGS M2M API flow")
        return []

    # ------------------------------------------------------------------
    # Processing
    # ------------------------------------------------------------------
    def _process_scene(
        self,
        scene: Dict[str, Any],
        field_id: str,
        bbox: Dict[str, float],
    ) -> Optional[BiomassReading]:
        """Download a single scene, compute indices, and derive biomass."""
        scene_id = scene["id"]
        timestamp = datetime.fromisoformat(scene["properties"]["datetime"].replace("Z", "+00:00"))
        cloud_cover = scene["properties"].get("eo:cloud_cover", 0.0)

        logger.debug("Processing scene %s", scene_id)

        # Download multispectral bands
        band_arrays = self._download_sentinel_bands(scene_id, bbox)
        if not band_arrays:
            return None

        # Compute vegetation indices
        red = band_arrays["B04"].astype(np.float32)
        nir = band_arrays["B08"].astype(np.float32)
        swir1 = band_arrays["B11"].astype(np.float32)

        ndvi = self._compute_ndvi(nir, red)
        evi = self._compute_evi(nir, red, swir1)

        # Calibrated biomass regression (kg/ha)
        # Coefficients derived from hemp-specific ground-truth campaigns
        biomass_density = self._estimate_biomass(ndvi, evi)

        # Geometry
        geom_wkt = box(bbox["min_lon"], bbox["min_lat"], bbox["max_lon"], bbox["max_lat"]).wkt

        reading = BiomassReading(
            reading_id=f"SAT-{scene_id}-{field_id}",
            field_id=field_id,
            timestamp_utc=timestamp,
            source_satellite=self.config.source,
            ndvi=float(np.nanmedian(ndvi)),
            evi=float(np.nanmedian(evi)),
            biomass_density_kg_ha=float(np.nanmedian(biomass_density)),
            canopy_cover_percent=float(np.nanmean(ndvi > 0.3)) * 100.0,
            cloud_cover_percent=cloud_cover,
            resolution_m=self.config.resolution_m,
            geometry_wkt=geom_wkt,
            metadata={"scene_id": scene_id, "processing_level": "L2A"},
        )
        return reading

    def _download_sentinel_bands(
        self,
        scene_id: str,
        bbox: Dict[str, float],
    ) -> Optional[Dict[str, np.ndarray]]:
        """Download specified Sentinel-2 bands via Sentinel Hub Process API."""
        token = self._get_sentinel_token()
        headers = {"Authorization": f"Bearer {token}"}

        process_url = f"{SENTINEL_HUB_URL}/api/v1/process"
        evalscript = """
        //VERSION=3
        function setup() {
            return {
                input: ["B04", "B08", "B11", "B12"],
                output: { bands: 4, sampleType: "FLOAT32" }
            };
        }
        function evaluatePixel(sample) {
            return [sample.B04, sample.B08, sample.B11, sample.B12];
        }
        """
        payload = {
            "input": {
                "bounds": {
                    "bbox": [
                        bbox["min_lon"],
                        bbox["min_lat"],
                        bbox["max_lon"],
                        bbox["max_lat"],
                    ],
                    "properties": {"crs": "http://www.opengis.net/def/crs/EPSG/0/4326"},
                },
                "data": [
                    {
                        "type": "sentinel-2-l2a",
                        "dataFilter": {"ids": [scene_id]},
                    }
                ],
            },
            "output": {
                "responses": [
                    {
                        "identifier": "default",
                        "format": {"type": "image/tiff"},
                    }
                ]
            },
        }

        resp = self._session.post(
            process_url,
            headers={**headers, "Content-Type": "application/json", "Accept": "image/tiff"},
            json=payload,
            timeout=120,
        )
        if resp.status_code != 200:
            logger.error("Band download failed | scene=%s | status=%d", scene_id, resp.status_code)
            return None

        # Parse GeoTIFF into numpy arrays
        with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
            tmp.write(resp.content)
            tmp_path = tmp.name

        try:
            with rasterio.open(tmp_path) as src:
                bands = {
                    "B04": src.read(1),
                    "B08": src.read(2),
                    "B11": src.read(3),
                    "B12": src.read(4),
                }
        finally:
            Path(tmp_path).unlink(missing_ok=True)

        return bands

    # ------------------------------------------------------------------
    # Index computations
    # ------------------------------------------------------------------
    @staticmethod
    def _compute_ndvi(nir: np.ndarray, red: np.ndarray) -> np.ndarray:
        """Compute Normalized Difference Vegetation Index."""
        denominator = nir + red
        return np.divide(nir - red, denominator, out=np.zeros_like(nir), where=denominator != 0)

    @staticmethod
    def _compute_evi(nir: np.ndarray, red: np.ndarray, blue: np.ndarray) -> np.ndarray:
        """Compute Enhanced Vegetation Index (2.5 * (NIR - Red) / (NIR + 6*Red - 7.5*Blue + 1))."""
        denominator = nir + 6.0 * red - 7.5 * blue + 1.0
        return 2.5 * np.divide(nir - red, denominator, out=np.zeros_like(nir), where=denominator != 0)

    @staticmethod
    def _estimate_biomass(ndvi: np.ndarray, evi: np.ndarray) -> np.ndarray:
        """
        Estimate biomass density (kg/ha) from vegetation indices.

        Uses a hemp-calibrated linear model:
            biomass = 1420 * NDVI + 680 * EVI + 350

        Coefficients should be updated annually via ground-truth validation.
        """
        return 1420.0 * ndvi + 680.0 * evi + 350.0

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------
    def _validate_reading(self, reading: BiomassReading) -> bool:
        """
        Validate a biomass reading against Project Harmony compliance thresholds.

        Checks:
          - NDVI range [-1, 1]
          - Biomass density non-negative
          - Cloud cover below threshold
          - Traceability hash present
        """
        if not (-1.0 <= reading.ndvi <= 1.0):
            logger.warning("NDVI out of range | reading=%s | ndvi=%.3f", reading.reading_id, reading.ndvi)
            return False
        if reading.biomass_density_kg_ha < 0:
            logger.warning("Negative biomass | reading=%s", reading.reading_id)
            return False
        if reading.cloud_cover_percent > self.config.max_cloud_cover:
            logger.warning(
                "Excessive cloud cover | reading=%s | %.1f%%",
                reading.reading_id,
                reading.cloud_cover_percent,
            )
            return False
        if not reading.traceability_hash:
            logger.warning("Missing traceability hash | reading=%s", reading.reading_id)
            return False
        return True

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _persist_readings(self, readings: List[BiomassReading]) -> None:
        """Insert validated readings into BigQuery with batching."""
        rows = [r.to_bigquery_row() for r in readings]
        self.bq.insert_rows(BIODENSITY_TABLE, rows)
        logger.info("Persisted %d readings to %s", len(rows), BIODENSITY_TABLE)

    # ------------------------------------------------------------------
    # Auth helpers
    # ------------------------------------------------------------------
    def _get_sentinel_token(self) -> str:
        """Obtain (or refresh) a Sentinel Hub OAuth2 token."""
        client_id, client_secret = self.creds.get_satellite_credentials()
        resp = self._session.post(
            f"{SENTINEL_HUB_URL}/oauth/token",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["access_token"]


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main() -> None:
    """CLI wrapper for standalone execution."""
    import argparse

    parser = argparse.ArgumentParser(description="Project Harmony — Satellite Biomass Ingestion")
    parser.add_argument("--field-id", required=True, help="Field identifier")
    parser.add_argument("--min-lon", type=float, default=DEFAULT_BBOX["min_lon"])
    parser.add_argument("--min-lat", type=float, default=DEFAULT_BBOX["min_lat"])
    parser.add_argument("--max-lon", type=float, default=DEFAULT_BBOX["max_lon"])
    parser.add_argument("--max-lat", type=float, default=DEFAULT_BBOX["max_lat"])
    parser.add_argument("--days-back", type=int, default=7, help="Look-back window in days")
    parser.add_argument("--source", default="sentinel2", choices=["sentinel2", "landsat8"])
    args = parser.parse_args()

    bbox = {
        "min_lon": args.min_lon,
        "min_lat": args.min_lat,
        "max_lon": args.max_lon,
        "max_lat": args.max_lat,
    }
    start = datetime.now(timezone.utc) - timedelta(days=args.days_back)

    config = SatelliteConfig(source=args.source, api_endpoint=SENTINEL_HUB_URL)
    with SatelliteBiomassIngestor(config=config) as ingestor:
        readings = ingestor.run_ingestion(
            field_id=args.field_id, bbox=bbox, start_date=start
        )
        print(json.dumps([r.to_bigquery_row() for r in readings], indent=2, default=str))


if __name__ == "__main__":
    main()
