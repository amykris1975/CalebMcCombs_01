"""
test_db_connect.py
Unit and integration tests for the BigQuery connection manager.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from google.cloud import bigquery

from src.utils.db_connect import (
    BigQueryConfig,
    BigQueryConnector,
    BigQueryInsertError,
)


# =============================================================================
# Fixtures
# =============================================================================
@pytest.fixture
def bq_config() -> BigQueryConfig:
    return BigQueryConfig(
        project_id="test-project",
        dataset="test_dataset",
        location="US",
        use_streaming_inserts=True,
        max_batch_size=100,
    )


@pytest.fixture
def mock_bigquery_client() -> MagicMock:
    return MagicMock(spec=bigquery.Client)


# =============================================================================
# Configuration Tests
# =============================================================================
class TestConfiguration:
    def test_default_config(self) -> None:
        config = BigQueryConfig()
        assert config.project_id  # Should read from env or have default
        assert config.dataset == "project_harmony"
        assert config.location == "US"
        assert config.use_streaming_inserts is True

    def test_custom_config(self) -> None:
        config = BigQueryConfig(
            project_id="my-project",
            dataset="my_dataset",
            location="EU",
            max_batch_size=500,
        )
        assert config.project_id == "my-project"
        assert config.dataset == "my_dataset"
        assert config.location == "EU"
        assert config.max_batch_size == 500


# =============================================================================
# Schema Inference Tests
# =============================================================================
class TestSchemaInference:
    def test_infer_schema_from_dict(self) -> None:
        sample = {
            "reading_id": "SAT-001",
            "field_id": "FIELD-A",
            "ndvi": 0.75,
            "count": 42,
            "active": True,
            "timestamp_utc": datetime.now(timezone.utc),
            "metadata_json": {"key": "value"},
        }
        schema = BigQueryConnector._infer_schema(sample)
        schema_dict = {field.name: field.field_type for field in schema}

        assert schema_dict["reading_id"] == "STRING"
        assert schema_dict["field_id"] == "STRING"
        assert schema_dict["ndvi"] == "FLOAT64"
        assert schema_dict["count"] == "INT64"
        assert schema_dict["active"] == "BOOL"
        assert schema_dict["timestamp_utc"] == "TIMESTAMP"
        assert schema_dict["metadata_json"] == "JSON"

    def test_infer_schema_empty_dict(self) -> None:
        schema = BigQueryConnector._infer_schema({})
        assert len(schema) == 0


# =============================================================================
# Parameter Type Inference Tests
# =============================================================================
class TestParamTypeInference:
    def test_infer_int(self) -> None:
        assert BigQueryConnector._infer_param_type(42) == "INT64"

    def test_infer_float(self) -> None:
        assert BigQueryConnector._infer_param_type(3.14) == "FLOAT64"

    def test_infer_bool(self) -> None:
        assert BigQueryConnector._infer_param_type(True) == "BOOL"

    def test_infer_string(self) -> None:
        assert BigQueryConnector._infer_param_type("hello") == "STRING"

    def test_infer_datetime(self) -> None:
        dt = datetime.now(timezone.utc)
        assert BigQueryConnector._infer_param_type(dt) == "TIMESTAMP"

    def test_infer_unknown_defaults_to_string(self) -> None:
        assert BigQueryConnector._infer_param_type([1, 2, 3]) == "STRING"


# =============================================================================
# Insert Tests (Mocked)
# =============================================================================
class TestInsertOperations:
    @patch("src.utils.db_connect.bigquery.Client")
    def test_insert_rows_success(self, mock_client_class) -> None:
        mock_client = MagicMock()
        mock_client.insert_rows_json.return_value = []
        mock_client_class.return_value = mock_client

        connector = BigQueryConnector()
        rows = [
            {"reading_id": "R1", "field_id": "F1"},
            {"reading_id": "R2", "field_id": "F1"},
        ]
        connector.insert_rows("test-project.test_dataset.test_table", rows)
        mock_client.insert_rows_json.assert_called_once()

    @patch("src.utils.db_connect.bigquery.Client")
    def test_insert_rows_with_errors(self, mock_client_class) -> None:
        mock_client = MagicMock()
        mock_client.insert_rows_json.return_value = [
            {"index": 0, "errors": [{"message": "Invalid value"}]}
        ]
        mock_client_class.return_value = mock_client

        connector = BigQueryConnector()
        rows = [{"reading_id": "R1"}]

        with pytest.raises(BigQueryInsertError):
            connector.insert_rows("test-project.test_dataset.test_table", rows)

    @patch("src.utils.db_connect.bigquery.Client")
    def test_insert_empty_rows(self, mock_client_class) -> None:
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        connector = BigQueryConnector()
        connector.insert_rows("test-project.test_dataset.test_table", [])
        mock_client.insert_rows_json.assert_not_called()

    @patch("src.utils.db_connect.bigquery.Client")
    def test_insert_large_batch_uses_load_job(self, mock_client_class, tmp_path) -> None:
        mock_client = MagicMock()
        mock_job = MagicMock()
        mock_client.load_table_from_file.return_value = mock_job
        mock_client_class.return_value = mock_client

        connector = BigQueryConnector()
        rows = [{"reading_id": f"R{i}", "field_id": "F1"} for i in range(1500)]
        connector.insert_rows("test-project.test_dataset.test_table", rows)
        mock_client.load_table_from_file.assert_called_once()


# =============================================================================
# Table Operations Tests
# =============================================================================
class TestTableOperations:
    @patch("src.utils.db_connect.bigquery.Client")
    def test_table_exists_true(self, mock_client_class) -> None:
        mock_client = MagicMock()
        mock_client.get_table.return_value = MagicMock()
        mock_client_class.return_value = mock_client

        connector = BigQueryConnector()
        assert connector.table_exists("test-project.test_dataset.test_table") is True

    @patch("src.utils.db_connect.bigquery.Client")
    def test_table_exists_false(self, mock_client_class) -> None:
        mock_client = MagicMock()
        mock_client.get_table.side_effect = Exception("Not found")
        mock_client_class.return_value = mock_client

        connector = BigQueryConnector()
        assert connector.table_exists("test-project.test_dataset.test_table") is False

    @patch("src.utils.db_connect.bigquery.Client")
    def test_get_table_schema(self, mock_client_class) -> None:
        mock_schema = [
            bigquery.SchemaField("reading_id", "STRING"),
            bigquery.SchemaField("ndvi", "FLOAT64"),
        ]
        mock_table = MagicMock()
        mock_table.schema = mock_schema

        mock_client = MagicMock()
        mock_client.get_table.return_value = mock_table
        mock_client_class.return_value = mock_client

        connector = BigQueryConnector()
        schema = connector.get_table_schema("test-project.test_dataset.test_table")
        assert len(schema) == 2
        assert schema[0].name == "reading_id"


# =============================================================================
# Query Tests
# =============================================================================
class TestQueryOperations:
    @patch("src.utils.db_connect.bigquery.Client")
    def test_simple_query(self, mock_client_class) -> None:
        mock_result = MagicMock()
        mock_result.__iter__.return_value = [
            {"cnt": 42},
        ]

        mock_job = MagicMock()
        mock_job.result.return_value = mock_result

        mock_client = MagicMock()
        mock_client.query.return_value = mock_job
        mock_client_class.return_value = mock_client

        connector = BigQueryConnector()
        result = connector.query("SELECT COUNT(*) as cnt FROM `test.table`")
        rows = list(result)
        assert len(rows) == 1
        assert rows[0]["cnt"] == 42

    @patch("src.utils.db_connect.bigquery.Client")
    def test_parameterized_query(self, mock_client_class) -> None:
        mock_result = MagicMock()
        mock_result.__iter__.return_value = []

        mock_job = MagicMock()
        mock_job.result.return_value = mock_result

        mock_client = MagicMock()
        mock_client.query.return_value = mock_job
        mock_client_class.return_value = mock_client

        connector = BigQueryConnector()
        connector.query(
            "SELECT * FROM `test.table` WHERE field_id = ?",
            params=["FIELD-A"],
        )
        mock_client.query.assert_called_once()


# =============================================================================
# Singleton Tests
# =============================================================================
class TestSingleton:
    @patch("src.utils.db_connect.bigquery.Client")
    def test_singleton_instance(self, mock_client_class) -> None:
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        c1 = BigQueryConnector()
        c2 = BigQueryConnector()
        assert c1 is c2
