"""
db_connect.py
Project Harmony — BigQuery Connection Manager

Centralized database connectivity for the Project Harmony ingestion pipeline.
Manages BigQuery client lifecycle, connection pooling, retry logic, and
schema-aware batch inserts. Supports streaming inserts for real-time telemetry
and load jobs for bulk historical data.

All connections use ADC (Application Default Credentials) or service account
JSON with automatic token refresh.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from google.api_core import retry as google_retry
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, SourceFormat, WriteDisposition
from google.oauth2 import service_account

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("harmony.db_connect")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "project-harmony-prod")
DEFAULT_DATASET = "project_harmony"
DEFAULT_LOCATION = "US"

# Retry configuration for transient BigQuery errors
BQ_RETRY_CONFIG = google_retry.Retry(
    predicate=google_retry.if_transient_error,
    initial=1.0,
    maximum=60.0,
    multiplier=2.0,
    deadline=300.0,
)

# Batch insert configuration
MAX_BATCH_SIZE = 500  # Max rows per insert
STREAMING_DELAY_SEC = 0.1  # Rate limiting between streaming inserts


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class BigQueryConfig:
    """Configuration for BigQuery connectivity."""

    project_id: str = DEFAULT_PROJECT_ID
    dataset: str = DEFAULT_DATASET
    location: str = DEFAULT_LOCATION
    service_account_json: Optional[str] = None  # Path to SA key file
    use_streaming_inserts: bool = True
    max_batch_size: int = MAX_BATCH_SIZE
    create_tables_if_missing: bool = False


# ---------------------------------------------------------------------------
# Connection manager
# ---------------------------------------------------------------------------
class BigQueryConnector:
    """
    Managed BigQuery client with connection pooling and retry semantics.

    Responsibilities:
      - Authenticate with BigQuery (ADC or service account)
      - Maintain a singleton client instance
      - Provide schema-aware table operations
      - Batch insert rows with automatic retry
      - Bulk load from GCS or local files
      - Execute parameterized queries for analytics
    """

    _instance: Optional["BigQueryConnector"] = None
    _lock = False

    def __new__(cls, *args, **kwargs):  # noqa: ANN003
        # Singleton pattern — reuse client across the pipeline
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._lock = True
        return cls._instance

    def __init__(self, config: Optional[BigQueryConfig] = None) -> None:
        if not self._lock:
            return  # Already initialized

        self.config = config or BigQueryConfig()
        self._client: Optional[bigquery.Client] = None
        self._table_schemas: Dict[str, List[bigquery.SchemaField]] = {}
        self._init_client()
        self._lock = False

    # ------------------------------------------------------------------
    # Client lifecycle
    # ------------------------------------------------------------------
    def _init_client(self) -> None:
        """Initialize the BigQuery client with appropriate credentials."""
        if self.config.service_account_json and os.path.exists(self.config.service_account_json):
            credentials = service_account.Credentials.from_service_account_file(
                self.config.service_account_json,
                scopes=["https://www.googleapis.com/auth/bigquery"],
            )
            self._client = bigquery.Client(
                project=self.config.project_id,
                credentials=credentials,
                location=self.config.location,
            )
            logger.info(
                "BigQuery client initialized (service account) | project=%s",
                self.config.project_id,
            )
        else:
            # Use Application Default Credentials
            self._client = bigquery.Client(
                project=self.config.project_id,
                location=self.config.location,
            )
            logger.info(
                "BigQuery client initialized (ADC) | project=%s",
                self.config.project_id,
            )

    @property
    def client(self) -> bigquery.Client:
        """Return the underlying BigQuery client, reinitializing if needed."""
        if self._client is None:
            self._init_client()
        return self._client  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Table operations
    # ------------------------------------------------------------------
    def table_exists(self, table_id: str) -> bool:
        """Check if a fully-qualified table exists."""
        try:
            self.client.get_table(table_id)
            return True
        except Exception:
            return False

    def get_table_schema(self, table_id: str) -> List[bigquery.SchemaField]:
        """Fetch and cache the schema for a table."""
        if table_id not in self._table_schemas:
            table = self.client.get_table(table_id)
            self._table_schemas[table_id] = list(table.schema)
        return self._table_schemas[table_id]

    def create_table(self, table_id: str, schema: List[bigquery.SchemaField]) -> None:
        """Create a new table with the given schema."""
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="timestamp_utc",
        )
        self.client.create_table(table, exists_ok=True)
        logger.info("Created table | %s", table_id)

    # ------------------------------------------------------------------
    # Insert operations
    # ------------------------------------------------------------------
    def insert_rows(
        self,
        table_id: str,
        rows: List[Dict[str, Any]],
    ) -> None:
        """
        Insert rows into BigQuery with automatic batching and retry.

        Uses streaming inserts for real-time data, falling back to
        load jobs for very large batches.

        Parameters
        ----------
        table_id : str
            Fully-qualified table ID (project.dataset.table).
        rows : list[dict]
            Row dictionaries matching the table schema.
        """
        if not rows:
            return

        # Create table if configured and missing
        if self.config.create_tables_if_missing and not self.table_exists(table_id):
            logger.info("Table %s does not exist — creating...", table_id)
            self._infer_and_create_table(table_id, rows[0])

        # Use load job for large batches (>1000 rows)
        if len(rows) > 1000:
            self._insert_via_load_job(table_id, rows)
            return

        # Batch into chunks
        for i in range(0, len(rows), self.config.max_batch_size):
            chunk = rows[i : i + self.config.max_batch_size]
            self._insert_batch(table_id, chunk)

    def _insert_batch(
        self,
        table_id: str,
        rows: List[Dict[str, Any]],
    ) -> None:
        """Insert a single batch of rows with retry."""
        errors = self.client.insert_rows_json(
            table_id,
            rows,
            retry=BQ_RETRY_CONFIG,
        )

        if errors:
            failed_count = len(errors)
            logger.error(
                "Insert errors | table=%s | failed=%d | sample=%s",
                table_id,
                failed_count,
                errors[:3],
            )
            raise BigQueryInsertError(f"Failed to insert {failed_count} rows into {table_id}")

        logger.debug("Inserted %d rows into %s", len(rows), table_id)

    def _insert_via_load_job(
        self,
        table_id: str,
        rows: List[Dict[str, Any]],
    ) -> None:
        """Insert a large batch via a temporary JSON load job."""
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            for row in rows:
                tmp.write(json.dumps(row, default=str) + "\n")
            tmp_path = tmp.name

        try:
            job_config = LoadJobConfig(
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=WriteDisposition.WRITE_APPEND,
                autodetect=True,
            )
            with open(tmp_path, "rb") as fh:
                job = self.client.load_table_from_file(
                    fh, table_id, job_config=job_config, retry=BQ_RETRY_CONFIG
                )
            job.result(timeout=300)
            logger.info("Load job complete | table=%s | rows=%d", table_id, len(rows))
        finally:
            os.unlink(tmp_path)

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------
    def query(
        self,
        sql: str,
        params: Optional[List[Any]] = None,
        timeout_sec: int = 300,
    ) -> bigquery.QueryJob:
        """
        Execute a parameterized BigQuery query.

        Parameters
        ----------
        sql : str
            SQL query with positional parameters as ?.
        params : list, optional
            Parameter values.
        timeout_sec : int
            Query timeout in seconds.

        Returns
        -------
        google.cloud.bigquery.QueryJob
        """
        job_config = None
        if params:
            query_params = [
                bigquery.ScalarQueryParameter(None, self._infer_param_type(p), p)
                for p in params
            ]
            job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        query_job = self.client.query(sql, job_config=job_config, retry=BQ_RETRY_CONFIG)
        return query_job.result(timeout=timeout_sec)

    @staticmethod
    def _infer_param_type(value: Any) -> str:
        """Infer BigQuery parameter type from Python value."""
        type_map = {
            int: "INT64",
            float: "FLOAT64",
            bool: "BOOL",
            str: "STRING",
            datetime: "TIMESTAMP",
        }
        return type_map.get(type(value), "STRING")

    # ------------------------------------------------------------------
    # Schema inference
    # ------------------------------------------------------------------
    def _infer_and_create_table(
        self,
        table_id: str,
        sample_row: Dict[str, Any],
    ) -> None:
        """Infer schema from a sample row and create the table."""
        schema = self._infer_schema(sample_row)
        self.create_table(table_id, schema)

    @staticmethod
    def _infer_schema(sample_row: Dict[str, Any]) -> List[bigquery.SchemaField]:
        """Infer BigQuery schema from a dictionary."""
        type_mapping = {
            str: "STRING",
            int: "INT64",
            float: "FLOAT64",
            bool: "BOOL",
            list: "STRING",  # Store lists as JSON strings
            dict: "STRING",  # Store dicts as JSON strings
        }

        schema = []
        for key, value in sample_row.items():
            bq_type = type_mapping.get(type(value), "STRING")
            mode = "NULLABLE"

            # Special handling for timestamp fields
            if key in ("timestamp_utc", "ingestion_timestamp"):
                bq_type = "TIMESTAMP"
            elif key in ("metadata_json",):
                bq_type = "JSON"

            schema.append(bigquery.SchemaField(key, bq_type, mode=mode))

        return schema

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------
    def get_row_count(self, table_id: str) -> int:
        """Return approximate row count for a table."""
        query = f"SELECT COUNT(*) as cnt FROM `{table_id}`"
        result = self.query(query)
        for row in result:
            return row.cnt
        return 0

    def get_recent_rows(
        self,
        table_id: str,
        limit: int = 10,
        timestamp_column: str = "ingestion_timestamp",
    ) -> List[Dict[str, Any]]:
        """Fetch the most recent rows from a table."""
        query = f"""
            SELECT * FROM `{table_id}`
            ORDER BY {timestamp_column} DESC
            LIMIT {limit}
        """
        result = self.query(query)
        return [dict(row) for row in result]


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------
class BigQueryInsertError(Exception):
    """Raised when BigQuery row insertion fails."""
    pass


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Project Harmony — BigQuery Connector")
    subparsers = parser.add_subparsers(dest="command")

    # Query
    query_parser = subparsers.add_parser("query", help="Execute a query")
    query_parser.add_argument("--sql", required=True)

    # Row count
    count_parser = subparsers.add_parser("count", help="Get row count")
    count_parser.add_argument("--table", required=True)

    # Recent rows
    recent_parser = subparsers.add_parser("recent", help="Get recent rows")
    recent_parser.add_argument("--table", required=True)
    recent_parser.add_argument("--limit", type=int, default=10)

    args = parser.parse_args()

    connector = BigQueryConnector()

    if args.command == "query":
        result = connector.query(args.sql)
        for row in result:
            print(dict(row))
    elif args.command == "count":
        count = connector.get_row_count(args.table)
        print(f"Row count: {count}")
    elif args.command == "recent":
        rows = connector.get_recent_rows(args.table, args.limit)
        print(json.dumps(rows, indent=2, default=str))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
