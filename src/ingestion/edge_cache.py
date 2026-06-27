"""
edge_cache.py
Project Harmony — Edge Telemetry Cache & Buffer Manager

Manages local caching and buffering of telemetry data before upstream
transmission to BigQuery. Provides store-and-forward reliability for
field deployments with intermittent connectivity. Implements LRU eviction,
compression, batch aggregation, and automatic retry with exponential backoff.

All telemetry adheres to TDA traceability and UN SDG 13 efficiency standards.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import sqlite3
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import schedule

from src.utils.auth import CredentialManager
from src.utils.db_connect import BigQueryConnector

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("harmony.edge_cache")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_DB_PATH = os.path.join(os.path.expanduser("~"), ".harmony", "edge_cache.db")
MAX_CACHE_SIZE_MB = 500  # Maximum on-disk cache size
BATCH_SIZE = 100  # Records per upstream flush
FLUSH_INTERVAL_SEC = 300  # 5 minutes
MAX_RETRY_ATTEMPTS = 5
RETRY_BACKOFF_BASE = 2  # seconds

# Priority tiers for telemetry types
PRIORITY_TIERS = {
    "soil_critical": 1,  # Critical soil alerts (e.g., pH crash)
    "compliance": 2,     # Hot Crop regulatory data
    "realtime": 3,       # Live telemetry streams
    "batch": 4,          # Bulk historical uploads
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class CacheEntry:
    """A single cached telemetry record."""

    entry_id: str
    data_type: str  # 'soil', 'satellite', 'drone', 'compliance'
    payload_json: str
    priority: int = 4
    timestamp_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    retry_count: int = 0
    last_retry_utc: Optional[datetime] = None
    traceability_hash: str = ""
    compressed: bool = False

    def __post_init__(self) -> None:
        if not self.traceability_hash:
            self.traceability_hash = hashlib.sha256(
                f"{self.entry_id}|{self.data_type}|{self.timestamp_utc.isoformat()}".encode()
            ).hexdigest()


@dataclass
class EdgeCacheConfig:
    """Configuration for the edge cache."""

    db_path: str = DEFAULT_DB_PATH
    max_size_mb: int = MAX_CACHE_SIZE_MB
    batch_size: int = BATCH_SIZE
    flush_interval_sec: int = FLUSH_INTERVAL_SEC
    max_retries: int = MAX_RETRY_ATTEMPTS
    compress_threshold_bytes: int = 1024  # Compress payloads > 1KB
    enable_scheduled_flush: bool = True


# ---------------------------------------------------------------------------
# Core cache manager
# ---------------------------------------------------------------------------
class EdgeCacheManager:
    """
    SQLite-backed edge cache for reliable telemetry buffering.

    Features:
      - Persistent local storage with WAL mode for high concurrency
      - Priority-based flushing (critical alerts first)
      - Automatic gzip compression for large payloads
      - Size-based LRU eviction when cache exceeds max_size_mb
      - Exponential backoff retry for failed upstream transmissions
      - Thread-safe operations for concurrent sensor ingestors
      - Automatic scheduled flushing via background thread
    """

    def __init__(
        self,
        config: Optional[EdgeCacheConfig] = None,
        bq_connector: Optional[BigQueryConnector] = None,
    ) -> None:
        self.config = config or EdgeCacheConfig()
        self.bq = bq_connector or BigQueryConnector()
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._flush_thread: Optional[threading.Thread] = None

        # Ensure cache directory exists
        Path(self.config.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def __enter__(self) -> "EdgeCacheManager":
        if self.config.enable_scheduled_flush:
            self.start_scheduler()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        self.shutdown()

    def start_scheduler(self) -> None:
        """Start the background flush scheduler thread."""
        self._stop_event.clear()
        self._flush_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._flush_thread.start()
        logger.info("Edge cache scheduler started | interval=%ds", self.config.flush_interval_sec)

    def shutdown(self) -> None:
        """Graceful shutdown — flush remaining entries and stop scheduler."""
        self._stop_event.set()
        logger.info("Edge cache shutting down — flushing remaining entries...")
        self.flush_all()
        if self._flush_thread and self._flush_thread.is_alive():
            self._flush_thread.join(timeout=10)
        logger.info("Edge cache shutdown complete")

    # ------------------------------------------------------------------
    # Database
    # ------------------------------------------------------------------
    def _init_db(self) -> None:
        """Initialize SQLite schema with WAL journal mode."""
        with sqlite3.connect(self.config.db_path, timeout=30.0) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache_entries (
                    entry_id TEXT PRIMARY KEY,
                    data_type TEXT NOT NULL,
                    payload_json BLOB NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 4,
                    timestamp_utc TEXT NOT NULL,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    last_retry_utc TEXT,
                    traceability_hash TEXT NOT NULL,
                    compressed INTEGER NOT NULL DEFAULT 0
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_priority_time
                ON cache_entries(priority, timestamp_utc)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_data_type
                ON cache_entries(data_type)
            """)
            conn.commit()

    def _get_connection(self) -> sqlite3.Connection:
        return sqlite3.connect(self.config.db_path, timeout=30.0)

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------
    def store(
        self,
        data_type: str,
        payload: Dict[str, Any],
        priority_tier: Optional[str] = None,
    ) -> str:
        """
        Store a telemetry payload in the local edge cache.

        Parameters
        ----------
        data_type : str
            Telemetry category (soil, satellite, drone, compliance).
        payload : dict
            The telemetry payload to cache.
        priority_tier : str, optional
            Priority key from PRIORITY_TIERS. Higher-priority entries
            are flushed first.

        Returns
        -------
        str
            The generated entry_id for traceability.
        """
        entry_id = f"EDGE-{data_type}-{int(time.time() * 1000)}-{os.urandom(4).hex()}"
        priority = PRIORITY_TIERS.get(priority_tier or "batch", 4)

        payload_json = json.dumps(payload, default=str)
        compressed = False

        # Compress large payloads
        payload_bytes = payload_json.encode()
        if len(payload_bytes) > self.config.compress_threshold_bytes:
            payload_bytes = gzip.compress(payload_bytes)
            compressed = True

        entry = CacheEntry(
            entry_id=entry_id,
            data_type=data_type,
            payload_json=payload_bytes.hex() if compressed else payload_json,
            priority=priority,
            compressed=compressed,
        )

        with self._lock, self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO cache_entries
                (entry_id, data_type, payload_json, priority, timestamp_utc,
                 retry_count, last_retry_utc, traceability_hash, compressed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    entry.entry_id,
                    entry.data_type,
                    entry.payload_json,
                    entry.priority,
                    entry.timestamp_utc.isoformat(),
                    entry.retry_count,
                    entry.last_retry_utc.isoformat() if entry.last_retry_utc else None,
                    entry.traceability_hash,
                    int(entry.compressed),
                ),
            )
            conn.commit()

        # Check if eviction is needed
        self._maybe_evict()

        logger.debug("Stored entry | id=%s | type=%s | priority=%d", entry_id, data_type, priority)
        return entry_id

    def store_batch(
        self, data_type: str, payloads: List[Dict[str, Any]], priority_tier: Optional[str] = None
    ) -> List[str]:
        """Store multiple payloads in a single transaction."""
        entry_ids: List[str] = []
        priority = PRIORITY_TIERS.get(priority_tier or "batch", 4)

        with self._lock, self._get_connection() as conn:
            for payload in payloads:
                entry_id = f"EDGE-{data_type}-{int(time.time() * 1000)}-{os.urandom(4).hex()}"
                payload_json = json.dumps(payload, default=str)
                payload_bytes = payload_json.encode()
                compressed = False

                if len(payload_bytes) > self.config.compress_threshold_bytes:
                    payload_bytes = gzip.compress(payload_bytes)
                    compressed = True

                ts = datetime.now(timezone.utc)
                trace_hash = hashlib.sha256(
                    f"{entry_id}|{data_type}|{ts.isoformat()}".encode()
                ).hexdigest()

                conn.execute(
                    """
                    INSERT INTO cache_entries
                    (entry_id, data_type, payload_json, priority, timestamp_utc,
                     retry_count, last_retry_utc, traceability_hash, compressed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entry_id,
                        data_type,
                        payload_bytes.hex() if compressed else payload_json,
                        priority,
                        ts.isoformat(),
                        0,
                        None,
                        trace_hash,
                        int(compressed),
                    ),
                )
                entry_ids.append(entry_id)
            conn.commit()

        self._maybe_evict()
        logger.info("Batch stored | type=%s | count=%d", data_type, len(payloads))
        return entry_ids

    # ------------------------------------------------------------------
    # Read / flush path
    # ------------------------------------------------------------------
    def flush_all(self) -> Tuple[int, int]:
        """
        Flush all pending entries to BigQuery.

        Returns
        -------
        tuple[int, int]
            (success_count, failure_count)
        """
        return self._flush_with_limit(limit=None)

    def flush_priority(self, tier: str) -> Tuple[int, int]:
        """Flush all entries of a given priority tier."""
        priority = PRIORITY_TIERS.get(tier, 4)
        return self._flush_with_limit(limit=None, priority_filter=priority)

    def _flush_with_limit(
        self,
        limit: Optional[int] = None,
        priority_filter: Optional[int] = None,
    ) -> Tuple[int, int]:
        """Internal flush with optional limit and priority filter."""
        success_count = 0
        failure_count = 0

        with self._lock:
            entries = self._fetch_pending(limit, priority_filter)

            if not entries:
                return 0, 0

            # Group by data_type for targeted BigQuery tables
            grouped: Dict[str, List[Dict[str, Any]]] = {}
            entry_ids: List[str] = []

            for entry in entries:
                payload = self._decode_payload(entry)
                table_name = self._resolve_table(entry.data_type)
                grouped.setdefault(table_name, []).append(payload)
                entry_ids.append(entry.entry_id)

            # Attempt BigQuery inserts
            for table_name, rows in grouped.items():
                try:
                    self.bq.insert_rows(table_name, rows)
                    success_count += len(rows)
                except Exception as exc:
                    logger.error("BigQuery insert failed | table=%s: %s", table_name, exc)
                    failure_count += len(rows)
                    # Mark entries for retry
                    self._mark_retry(entry_ids)
                    continue

            # Remove successfully flushed entries
            if success_count > 0:
                self._remove_entries(entry_ids)

        logger.info(
            "Flush complete | success=%d | failed=%d", success_count, failure_count
        )
        return success_count, failure_count

    def _fetch_pending(
        self,
        limit: Optional[int],
        priority_filter: Optional[int],
    ) -> List[Tuple]:
        """Fetch pending entries ordered by priority and timestamp."""
        query = "SELECT * FROM cache_entries"
        params: List[Any] = []

        if priority_filter is not None:
            query += " WHERE priority = ?"
            params.append(priority_filter)

        query += " ORDER BY priority ASC, timestamp_utc ASC"

        if limit is not None:
            query += f" LIMIT {limit}"

        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            return cursor.fetchall()

    # ------------------------------------------------------------------
    # Eviction
    # ------------------------------------------------------------------
    def _maybe_evict(self) -> None:
        """LRU eviction if cache exceeds max_size_mb."""
        db_size_mb = Path(self.config.db_path).stat().st_size / (1024 * 1024)
        if db_size_mb <= self.config.max_size_mb:
            return

        logger.warning("Cache size %.1fMB exceeds limit %dMB — evicting...", db_size_mb, self.config.max_size_mb)

        with self._lock, self._get_connection() as conn:
            # Delete oldest low-priority entries first
            conn.execute("""
                DELETE FROM cache_entries
                WHERE entry_id IN (
                    SELECT entry_id FROM cache_entries
                    ORDER BY priority DESC, timestamp_utc ASC
                    LIMIT 1000
                )
            """)
            conn.commit()

        # Vacuum to reclaim space
        with self._get_connection() as conn:
            conn.execute("VACUUM")

    # ------------------------------------------------------------------
    # Retry logic
    # ------------------------------------------------------------------
    def _mark_retry(self, entry_ids: List[str]) -> None:
        """Increment retry count and set last_retry timestamp."""
        now = datetime.now(timezone.utc).isoformat()
        with self._get_connection() as conn:
            for eid in entry_ids:
                conn.execute(
                    """
                    UPDATE cache_entries
                    SET retry_count = retry_count + 1, last_retry_utc = ?
                    WHERE entry_id = ? AND retry_count < ?
                    """,
                    (now, eid, self.config.max_retries),
                )
            conn.commit()

        # Remove entries that have exceeded max retries
        with self._get_connection() as conn:
            conn.execute(
                "DELETE FROM cache_entries WHERE retry_count >= ?",
                (self.config.max_retries,),
            )
            conn.commit()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _decode_payload(row: sqlite3.Row) -> Dict[str, Any]:
        """Decompress and decode a cached payload."""
        payload_data = row["payload_json"]
        if row["compressed"]:
            payload_bytes = bytes.fromhex(payload_data)
            payload_json = gzip.decompress(payload_bytes).decode()
        else:
            payload_json = payload_data
        return json.loads(payload_json)

    @staticmethod
    def _resolve_table(data_type: str) -> str:
        """Map data_type to BigQuery table name."""
        table_map = {
            "soil": "project_harmony.soil_readings",
            "satellite": "project_harmony.biomass_density",
            "drone": "project_harmony.drone_multispectral",
            "compliance": "project_harmony.compliance_events",
        }
        return table_map.get(data_type, "project_harmony.raw_telemetry")

    # ------------------------------------------------------------------
    # Scheduler loop
    # ------------------------------------------------------------------
    def _scheduler_loop(self) -> None:
        """Background thread that periodically flushes the cache."""
        schedule.every(self.config.flush_interval_sec).seconds.do(self.flush_all)

        while not self._stop_event.is_set():
            schedule.run_pending()
            time.sleep(1)

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------
    def get_stats(self) -> Dict[str, Any]:
        """Return cache statistics."""
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            total = conn.execute("SELECT COUNT(*) as cnt FROM cache_entries").fetchone()["cnt"]
            by_type = conn.execute(
                "SELECT data_type, COUNT(*) as cnt FROM cache_entries GROUP BY data_type"
            ).fetchall()
            by_priority = conn.execute(
                "SELECT priority, COUNT(*) as cnt FROM cache_entries GROUP BY priority"
            ).fetchall()
            retries = conn.execute(
                "SELECT SUM(retry_count) as total FROM cache_entries"
            ).fetchone()["total"] or 0

        db_size_mb = Path(self.config.db_path).stat().st_size / (1024 * 1024)

        return {
            "total_entries": total,
            "db_size_mb": round(db_size_mb, 2),
            "by_data_type": {row["data_type"]: row["cnt"] for row in by_type},
            "by_priority": {row["priority"]: row["cnt"] for row in by_priority},
            "total_retries": retries,
            "max_size_mb": self.config.max_size_mb,
        }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Project Harmony — Edge Cache Manager")
    subparsers = parser.add_subparsers(dest="command")

    # Store
    store_parser = subparsers.add_parser("store", help="Store a payload")
    store_parser.add_argument("--type", required=True, choices=["soil", "satellite", "drone", "compliance"])
    store_parser.add_argument("--payload", required=True, help="JSON payload string")
    store_parser.add_argument("--priority", default="batch", choices=list(PRIORITY_TIERS.keys()))

    # Flush
    subparsers.add_parser("flush", help="Flush all pending entries")

    # Stats
    subparsers.add_parser("stats", help="Show cache statistics")

    # Scheduler (blocking)
    sched_parser = subparsers.add_parser("scheduler", help="Run flush scheduler")

    args = parser.parse_args()

    config = EdgeCacheConfig()

    if args.command == "store":
        with EdgeCacheManager(config=config) as mgr:
            payload = json.loads(args.payload)
            eid = mgr.store(args.type, payload, priority_tier=args.priority)
            print(f"Stored entry: {eid}")

    elif args.command == "flush":
        with EdgeCacheManager(config=config) as mgr:
            success, failed = mgr.flush_all()
            print(f"Flushed: {success} success, {failed} failed")

    elif args.command == "stats":
        with EdgeCacheManager(config=config) as mgr:
            print(json.dumps(mgr.get_stats(), indent=2))

    elif args.command == "scheduler":
        with EdgeCacheManager(config=config) as mgr:
            mgr.start_scheduler()
            print(f"Scheduler running. Press Ctrl+C to stop.")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                mgr.shutdown()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
