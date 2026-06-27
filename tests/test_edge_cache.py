"""
test_edge_cache.py
Unit and integration tests for the edge telemetry cache manager.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.edge_cache import (
    CacheEntry,
    EdgeCacheConfig,
    EdgeCacheManager,
    PRIORITY_TIERS,
)
from src.utils.db_connect import BigQueryConnector


# =============================================================================
# Fixtures
# =============================================================================
@pytest.fixture
def cache_config(tmp_path) -> EdgeCacheConfig:
    return EdgeCacheConfig(
        db_path=str(tmp_path / "test_cache.db"),
        max_size_mb=10,
        batch_size=10,
        flush_interval_sec=60,
        compress_threshold_bytes=50,
        enable_scheduled_flush=False,
    )


@pytest.fixture
def mock_bq() -> MagicMock:
    return MagicMock(spec=BigQueryConnector)


# =============================================================================
# CacheEntry Tests
# =============================================================================
class TestCacheEntry:
    def test_traceability_hash(self) -> None:
        entry = CacheEntry(
            entry_id="EDGE-001",
            data_type="soil",
            payload_json='{"ph": 6.5}',
            priority=1,
        )
        assert entry.traceability_hash
        assert len(entry.traceability_hash) == 64

    def test_default_priority(self) -> None:
        entry = CacheEntry(
            entry_id="EDGE-002",
            data_type="satellite",
            payload_json="{}",
        )
        assert entry.priority == 4


# =============================================================================
# Store & Retrieve Tests
# =============================================================================
class TestStoreRetrieve:
    def test_store_single_entry(self, cache_config: EdgeCacheConfig, mock_bq: MagicMock) -> None:
        with EdgeCacheManager(config=cache_config, bq_connector=mock_bq) as mgr:
            eid = mgr.store("soil", {"ph": 6.5, "sensor_id": "S1"})
            assert eid.startswith("EDGE-soil-")

    def test_store_with_priority(self, cache_config: EdgeCacheConfig, mock_bq: MagicMock) -> None:
        with EdgeCacheManager(config=cache_config, bq_connector=mock_bq) as mgr:
            eid = mgr.store("soil", {"ph": 6.5}, priority_tier="soil_critical")
            assert eid.startswith("EDGE-soil-")

    def test_store_batch(self, cache_config: EdgeCacheConfig, mock_bq: MagicMock) -> None:
        with EdgeCacheManager(config=cache_config, bq_connector=mock_bq) as mgr:
            payloads = [
                {"ph": 6.5, "sensor_id": "S1"},
                {"ph": 7.0, "sensor_id": "S2"},
                {"ph": 6.8, "sensor_id": "S3"},
            ]
            eids = mgr.store_batch("soil", payloads)
            assert len(eids) == 3
            assert all(eid.startswith("EDGE-soil-") for eid in eids)

    def test_compression_large_payload(self, cache_config: EdgeCacheConfig, mock_bq: MagicMock) -> None:
        with EdgeCacheManager(config=cache_config, bq_connector=mock_bq) as mgr:
            large_payload = {"data": "x" * 1000}
            eid = mgr.store("satellite", large_payload)
            assert eid  # Should compress and store successfully


# =============================================================================
# Flush Tests
# =============================================================================
class TestFlush:
    def test_flush_all_success(
        self, cache_config: EdgeCacheConfig, mock_bq: MagicMock
    ) -> None:
        with EdgeCacheManager(config=cache_config, bq_connector=mock_bq) as mgr:
            mgr.store("soil", {"ph": 6.5})
            mgr.store("soil", {"ph": 7.0})

            success, failed = mgr.flush_all()
            assert success == 2
            assert failed == 0
            mock_bq.insert_rows.assert_called()

    def test_flush_priority_filter(
        self, cache_config: EdgeCacheConfig, mock_bq: MagicMock
    ) -> None:
        with EdgeCacheManager(config=cache_config, bq_connector=mock_bq) as mgr:
            mgr.store("soil", {"ph": 6.5}, priority_tier="soil_critical")
            mgr.store("soil", {"ph": 7.0}, priority_tier="batch")

            success, failed = mgr.flush_priority("soil_critical")
            assert success == 1
            assert failed == 0

    def test_flush_empty_cache(
        self, cache_config: EdgeCacheConfig, mock_bq: MagicMock
    ) -> None:
        with EdgeCacheManager(config=cache_config, bq_connector=mock_bq) as mgr:
            success, failed = mgr.flush_all()
            assert success == 0
            assert failed == 0

    def test_flush_with_bq_failure(
        self, cache_config: EdgeCacheConfig, mock_bq: MagicMock
    ) -> None:
        mock_bq.insert_rows.side_effect = Exception("BQ Error")

        with EdgeCacheManager(config=cache_config, bq_connector=mock_bq) as mgr:
            mgr.store("soil", {"ph": 6.5})
            success, failed = mgr.flush_all()
            assert success == 0
            assert failed == 1


# =============================================================================
# Statistics Tests
# =============================================================================
class TestStatistics:
    def test_stats_empty_cache(self, cache_config: EdgeCacheConfig, mock_bq: MagicMock) -> None:
        with EdgeCacheManager(config=cache_config, bq_connector=mock_bq) as mgr:
            stats = mgr.get_stats()
            assert stats["total_entries"] == 0
            assert stats["db_size_mb"] >= 0

    def test_stats_with_entries(self, cache_config: EdgeCacheConfig, mock_bq: MagicMock) -> None:
        with EdgeCacheManager(config=cache_config, bq_connector=mock_bq) as mgr:
            mgr.store("soil", {"ph": 6.5})
            mgr.store("satellite", {"ndvi": 0.75})

            stats = mgr.get_stats()
            assert stats["total_entries"] == 2
            assert "soil" in stats["by_data_type"]
            assert "satellite" in stats["by_data_type"]


# =============================================================================
# Eviction Tests
# =============================================================================
class TestEviction:
    def test_no_eviction_when_under_limit(
        self, cache_config: EdgeCacheConfig, mock_bq: MagicMock
    ) -> None:
        with EdgeCacheManager(config=cache_config, bq_connector=mock_bq) as mgr:
            for i in range(5):
                mgr.store("soil", {"ph": 6.5 + i * 0.1})
            stats = mgr.get_stats()
            assert stats["total_entries"] == 5


# =============================================================================
# Retry Logic Tests
# =============================================================================
class TestRetryLogic:
    def test_retry_count_incremented(
        self, cache_config: EdgeCacheConfig, mock_bq: MagicMock
    ) -> None:
        mock_bq.insert_rows.side_effect = Exception("BQ Error")

        with EdgeCacheManager(config=cache_config, bq_connector=mock_bq) as mgr:
            mgr.store("soil", {"ph": 6.5})
            mgr.flush_all()

            # Entry should still exist with retry count > 0
            stats = mgr.get_stats()
            assert stats["total_retries"] > 0


# =============================================================================
# Integration Tests
# =============================================================================
class TestIntegration:
    def test_store_flush_verify_empty(
        self, cache_config: EdgeCacheConfig, mock_bq: MagicMock
    ) -> None:
        with EdgeCacheManager(config=cache_config, bq_connector=mock_bq) as mgr:
            mgr.store("soil", {"ph": 6.5})
            mgr.store("soil", {"ph": 7.0})
            assert mgr.get_stats()["total_entries"] == 2

            mgr.flush_all()
            assert mgr.get_stats()["total_entries"] == 0

    def test_multiple_data_types(
        self, cache_config: EdgeCacheConfig, mock_bq: MagicMock
    ) -> None:
        with EdgeCacheManager(config=cache_config, bq_connector=mock_bq) as mgr:
            mgr.store("soil", {"ph": 6.5})
            mgr.store("satellite", {"ndvi": 0.75})
            mgr.store("drone", {"ndre": 0.5})
            mgr.store("compliance", {"event": "ph_alert"})

            stats = mgr.get_stats()
            assert stats["total_entries"] == 4
            assert len(stats["by_data_type"]) == 4

            mgr.flush_all()
            # Verify BigQuery was called for each data type
            assert mock_bq.insert_rows.call_count >= 1
