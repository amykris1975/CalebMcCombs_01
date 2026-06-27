"""
test_satellite_biomass.py
Unit and integration tests for the satellite biomass ingestion pipeline.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import responses

from src.ingestion.satellite_biomass import (
    BiomassReading,
    SatelliteBiomassIngestor,
    SatelliteConfig,
)
from src.utils.auth import CredentialManager
from src.utils.db_connect import BigQueryConnector


# =============================================================================
# Fixtures
# =============================================================================
@pytest.fixture
def satellite_config() -> SatelliteConfig:
    return SatelliteConfig(
        source="sentinel2",
        api_endpoint="https://services.sentinel-hub.com",
        resolution_m=10.0,
        max_cloud_cover=15.0,
    )


@pytest.fixture
def mock_bq_connector() -> MagicMock:
    return MagicMock(spec=BigQueryConnector)


@pytest.fixture
def mock_cred_manager() -> MagicMock:
    cm = MagicMock(spec=CredentialManager)
    cm.get_satellite_credentials.return_value = ("test_client_id", "test_client_secret")
    cm.get_sensor_api_key.return_value = "test_api_key"
    return cm


@pytest.fixture
def sample_bbox() -> dict:
    return {"min_lon": -105.0, "min_lat": 40.0, "max_lon": -104.9, "max_lat": 40.1}


# =============================================================================
# BiomassReading Tests
# =============================================================================
class TestBiomassReading:
    def test_traceability_hash_computed(self) -> None:
        reading = BiomassReading(
            reading_id="SAT-001",
            field_id="FIELD-A",
            timestamp_utc=datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
            source_satellite="sentinel2",
            ndvi=0.75,
            evi=0.65,
            biomass_density_kg_ha=2500.0,
            canopy_cover_percent=85.0,
            cloud_cover_percent=5.0,
            resolution_m=10.0,
            geometry_wkt="POLYGON((-105 40, -104.9 40, -104.9 40.1, -105 40.1, -105 40))",
        )
        assert reading.traceability_hash
        assert len(reading.traceability_hash) == 64  # SHA-256 hex

    def test_traceability_hash_deterministic(self) -> None:
        kwargs = {
            "reading_id": "SAT-001",
            "field_id": "FIELD-A",
            "timestamp_utc": datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
            "source_satellite": "sentinel2",
            "ndvi": 0.75,
            "evi": 0.65,
            "biomass_density_kg_ha": 2500.0,
            "canopy_cover_percent": 85.0,
            "cloud_cover_percent": 5.0,
            "resolution_m": 10.0,
            "geometry_wkt": "POLYGON((-105 40, -104.9 40, -104.9 40.1, -105 40.1, -105 40))",
        }
        r1 = BiomassReading(**kwargs)
        r2 = BiomassReading(**kwargs)
        assert r1.traceability_hash == r2.traceability_hash

    def test_to_bigquery_row(self) -> None:
        reading = BiomassReading(
            reading_id="SAT-001",
            field_id="FIELD-A",
            timestamp_utc=datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
            source_satellite="sentinel2",
            ndvi=0.75,
            evi=0.65,
            biomass_density_kg_ha=2500.0,
            canopy_cover_percent=85.0,
            cloud_cover_percent=5.0,
            resolution_m=10.0,
            geometry_wkt="POLYGON(...)",
            metadata={"scene_id": "S2A_20240601"},
        )
        row = reading.to_bigquery_row()
        assert row["reading_id"] == "SAT-001"
        assert row["field_id"] == "FIELD-A"
        assert row["ndvi"] == 0.75
        assert row["biomass_density_kg_ha"] == 2500.0
        assert json.loads(row["metadata_json"]) == {"scene_id": "S2A_20240601"}
        assert row["traceability_hash"] == reading.traceability_hash


# =============================================================================
# Index Computation Tests
# =============================================================================
class TestIndexComputations:
    def test_ndvi_computation(self) -> None:
        red = np.array([[0.1, 0.2], [0.3, 0.4]])
        nir = np.array([[0.5, 0.6], [0.7, 0.8]])
        ndvi = SatelliteBiomassIngestor._compute_ndvi(nir, red)
        expected = (nir - red) / (nir + red)
        np.testing.assert_array_almost_equal(ndvi, expected)

    def test_ndvi_zero_division(self) -> None:
        red = np.zeros((2, 2))
        nir = np.zeros((2, 2))
        ndvi = SatelliteBiomassIngestor._compute_ndvi(nir, red)
        np.testing.assert_array_equal(ndvi, np.zeros((2, 2)))

    def test_evi_computation(self) -> None:
        red = np.array([0.1, 0.2])
        nir = np.array([0.5, 0.6])
        blue = np.array([0.05, 0.1])
        evi = SatelliteBiomassIngestor._compute_evi(nir, red, blue)
        expected = 2.5 * (nir - red) / (nir + 6.0 * red - 7.5 * blue + 1.0)
        np.testing.assert_array_almost_equal(evi, expected)

    def test_biomass_estimation(self) -> None:
        ndvi = np.array([0.5, 0.6, 0.7])
        evi = np.array([0.4, 0.5, 0.6])
        biomass = SatelliteBiomassIngestor._estimate_biomass(ndvi, evi)
        expected = 1420.0 * ndvi + 680.0 * evi + 350.0
        np.testing.assert_array_almost_equal(biomass, expected)


# =============================================================================
# Validation Tests
# =============================================================================
class TestValidation:
    def test_valid_reading_passes(self, satellite_config: SatelliteConfig) -> None:
        ingestor = SatelliteBiomassIngestor(config=satellite_config)
        reading = BiomassReading(
            reading_id="SAT-001",
            field_id="FIELD-A",
            timestamp_utc=datetime.now(timezone.utc),
            source_satellite="sentinel2",
            ndvi=0.75,
            evi=0.65,
            biomass_density_kg_ha=2500.0,
            canopy_cover_percent=85.0,
            cloud_cover_percent=5.0,
            resolution_m=10.0,
            geometry_wkt="POLYGON(...)",
        )
        assert ingestor._validate_reading(reading) is True

    def test_ndvi_out_of_range_fails(self, satellite_config: SatelliteConfig) -> None:
        ingestor = SatelliteBiomassIngestor(config=satellite_config)
        reading = BiomassReading(
            reading_id="SAT-001",
            field_id="FIELD-A",
            timestamp_utc=datetime.now(timezone.utc),
            source_satellite="sentinel2",
            ndvi=1.5,  # Invalid
            evi=0.65,
            biomass_density_kg_ha=2500.0,
            canopy_cover_percent=85.0,
            cloud_cover_percent=5.0,
            resolution_m=10.0,
            geometry_wkt="POLYGON(...)",
        )
        assert ingestor._validate_reading(reading) is False

    def test_negative_biomass_fails(self, satellite_config: SatelliteConfig) -> None:
        ingestor = SatelliteBiomassIngestor(config=satellite_config)
        reading = BiomassReading(
            reading_id="SAT-001",
            field_id="FIELD-A",
            timestamp_utc=datetime.now(timezone.utc),
            source_satellite="sentinel2",
            ndvi=0.75,
            evi=0.65,
            biomass_density_kg_ha=-100.0,  # Invalid
            canopy_cover_percent=85.0,
            cloud_cover_percent=5.0,
            resolution_m=10.0,
            geometry_wkt="POLYGON(...)",
        )
        assert ingestor._validate_reading(reading) is False

    def test_excessive_cloud_cover_fails(self, satellite_config: SatelliteConfig) -> None:
        ingestor = SatelliteBiomassIngestor(config=satellite_config)
        reading = BiomassReading(
            reading_id="SAT-001",
            field_id="FIELD-A",
            timestamp_utc=datetime.now(timezone.utc),
            source_satellite="sentinel2",
            ndvi=0.75,
            evi=0.65,
            biomass_density_kg_ha=2500.0,
            canopy_cover_percent=85.0,
            cloud_cover_percent=50.0,  # Exceeds 15% threshold
            resolution_m=10.0,
            geometry_wkt="POLYGON(...)",
        )
        assert ingestor._validate_reading(reading) is False


# =============================================================================
# Sentinel Hub API Tests
# =============================================================================
class TestSentinelHubAPI:
    @responses.activate
    def test_oauth_token_acquisition(
        self,
        satellite_config: SatelliteConfig,
        mock_bq_connector: MagicMock,
        mock_cred_manager: MagicMock,
    ) -> None:
        # Mock OAuth token endpoint
        responses.post(
            "https://services.sentinel-hub.com/oauth/token",
            json={"access_token": "test_token_123", "expires_in": 3600},
            status=200,
        )

        ingestor = SatelliteBiomassIngestor(
            config=satellite_config,
            bq_connector=mock_bq_connector,
            cred_manager=mock_cred_manager,
        )

        with ingestor:
            token = ingestor._get_sentinel_token()
            assert token == "test_token_123"

    @responses.activate
    def test_scene_discovery(
        self,
        satellite_config: SatelliteConfig,
        mock_bq_connector: MagicMock,
        mock_cred_manager: MagicMock,
        sample_bbox: dict,
    ) -> None:
        # Mock OAuth
        responses.post(
            "https://services.sentinel-hub.com/oauth/token",
            json={"access_token": "test_token", "expires_in": 3600},
            status=200,
        )

        # Mock catalog search
        responses.post(
            "https://services.sentinel-hub.com/api/v1/catalog/search",
            json={
                "features": [
                    {
                        "id": "S2A_T13TDE_20240601",
                        "properties": {
                            "datetime": "2024-06-01T12:00:00Z",
                            "eo:cloud_cover": 5.0,
                        },
                    },
                    {
                        "id": "S2A_T13TDE_20240602",
                        "properties": {
                            "datetime": "2024-06-02T12:00:00Z",
                            "eo:cloud_cover": 20.0,  # Above threshold
                        },
                    },
                ]
            },
            status=200,
        )

        ingestor = SatelliteBiomassIngestor(
            config=satellite_config,
            bq_connector=mock_bq_connector,
            cred_manager=mock_cred_manager,
        )

        with ingestor:
            scenes = ingestor._discover_sentinel(
                sample_bbox,
                datetime(2024, 6, 1, tzinfo=timezone.utc),
                datetime(2024, 6, 7, tzinfo=timezone.utc),
            )

        assert len(scenes) == 1
        assert scenes[0]["id"] == "S2A_T13TDE_20240601"


# =============================================================================
# Integration Tests
# =============================================================================
class TestIntegration:
    @pytest.mark.integration
    def test_end_to_end_mocked(
        self,
        satellite_config: SatelliteConfig,
        mock_bq_connector: MagicMock,
        mock_cred_manager: MagicMock,
        sample_bbox: dict,
    ) -> None:
        """Test the full ingestion pipeline with mocked dependencies."""
        ingestor = SatelliteBiomassIngestor(
            config=satellite_config,
            bq_connector=mock_bq_connector,
            cred_manager=mock_cred_manager,
        )

        # Mock all external calls
        with patch.object(ingestor, "_discover_scenes") as mock_discover, \
             patch.object(ingestor, "_process_scene") as mock_process, \
             patch.object(ingestor, "_persist_readings") as mock_persist:

            mock_discover.return_value = [{"id": "scene1"}, {"id": "scene2"}]
            mock_process.side_effect = [
                BiomassReading(
                    reading_id="SAT-scene1-FIELD1",
                    field_id="FIELD1",
                    timestamp_utc=datetime.now(timezone.utc),
                    source_satellite="sentinel2",
                    ndvi=0.75,
                    evi=0.65,
                    biomass_density_kg_ha=2500.0,
                    canopy_cover_percent=80.0,
                    cloud_cover_percent=5.0,
                    resolution_m=10.0,
                    geometry_wkt="POLYGON(...)",
                ),
                BiomassReading(
                    reading_id="SAT-scene2-FIELD1",
                    field_id="FIELD1",
                    timestamp_utc=datetime.now(timezone.utc),
                    source_satellite="sentinel2",
                    ndvi=0.72,
                    evi=0.60,
                    biomass_density_kg_ha=2300.0,
                    canopy_cover_percent=78.0,
                    cloud_cover_percent=8.0,
                    resolution_m=10.0,
                    geometry_wkt="POLYGON(...)",
                ),
            ]

            with ingestor:
                readings = ingestor.run_ingestion(
                    field_id="FIELD1",
                    bbox=sample_bbox,
                    start_date=datetime(2024, 6, 1, tzinfo=timezone.utc),
                    end_date=datetime(2024, 6, 7, tzinfo=timezone.utc),
                )

            assert len(readings) == 2
            assert all(r.traceability_hash for r in readings)
            mock_persist.assert_called_once()
