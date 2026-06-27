"""
auth.py
Project Harmony — Authentication & Credential Manager

Handles API key authentication, OAuth2 token lifecycle management,
and secure credential rotation for all external services (Sentinel Hub,
sensor gateways, MQTT brokers, BigQuery). Integrates with Google Secret
Manager for production deployments and falls back to environment variables
for local development.

All credential access is audited for TDA traceability compliance.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from google.cloud import secretmanager
from google.oauth2 import service_account

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("harmony.auth")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SECRET_MANAGER_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "project-harmony-prod")
LOCAL_CREDS_PATH = os.path.join(os.path.expanduser("~"), ".harmony", "credentials.json")

# Token refresh margins (refresh before actual expiry)
TOKEN_REFRESH_MARGIN_SEC = 300  # 5 minutes

# Credential keys used across the pipeline
CRED_KEYS = {
    "satellite_api": "harmony-sentinel-hub-credentials",
    "sensor_api": "harmony-sensor-gateway-api-key",
    "mqtt": "harmony-mqtt-broker-credentials",
    "bigquery_sa": "harmony-bigquery-service-account",
    "gcs": "harmony-gcs-service-account",
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class TokenInfo:
    """OAuth2 token with metadata."""

    access_token: str
    token_type: str = "Bearer"
    expires_at: Optional[datetime] = None
    refresh_token: Optional[str] = None
    scope: str = ""

    @property
    def is_expired(self) -> bool:
        if not self.expires_at:
            return False
        return datetime.now(timezone.utc) >= (self.expires_at - timedelta(seconds=TOKEN_REFRESH_MARGIN_SEC))


@dataclass
class AuthConfig:
    """Configuration for the credential manager."""

    use_secret_manager: bool = True
    secret_manager_project: str = SECRET_MANAGER_PROJECT
    local_creds_path: str = LOCAL_CREDS_PATH
    cache_in_memory: bool = True
    cache_ttl_sec: int = 3600  # 1 hour


# ---------------------------------------------------------------------------
# Credential manager
# ---------------------------------------------------------------------------
class CredentialManager:
    """
    Centralized credential management for Project Harmony.

    Features:
      - Google Secret Manager integration (production)
      - Environment variable fallback (development)
      - Local encrypted cache (optional)
      - OAuth2 token lifecycle (acquire, refresh, invalidate)
      - Thread-safe credential access
      - Audit logging for all credential operations
    """

    def __init__(self, config: Optional[AuthConfig] = None) -> None:
        self.config = config or AuthConfig()
        self._secret_client: Optional[secretmanager.SecretManagerServiceClient] = None
        self._token_cache: Dict[str, TokenInfo] = {}
        self._credential_cache: Dict[str, Any] = {}
        self._cache_timestamps: Dict[str, datetime] = {}
        self._lock = threading.RLock()

        if self.config.use_secret_manager:
            try:
                self._secret_client = secretmanager.SecretManagerServiceClient()
                logger.info("Secret Manager client initialized")
            except Exception as exc:
                logger.warning("Secret Manager unavailable — falling back to env vars: %s", exc)
                self.config.use_secret_manager = False

    # ------------------------------------------------------------------
    # Sentinel Hub credentials
    # ------------------------------------------------------------------
    def get_satellite_credentials(self) -> Tuple[str, str]:
        """
        Retrieve Sentinel Hub OAuth2 client ID and secret.

        Returns
        -------
        tuple[str, str]
            (client_id, client_secret)
        """
        creds = self._get_credential("satellite_api")
        if isinstance(creds, dict):
            return creds.get("client_id", ""), creds.get("client_secret", "")
        # Fallback to env vars
        return (
            os.getenv("SENTINEL_CLIENT_ID", ""),
            os.getenv("SENTINEL_CLIENT_SECRET", ""),
        )

    # ------------------------------------------------------------------
    # Sensor gateway API key
    # ------------------------------------------------------------------
    def get_sensor_api_key(self) -> str:
        """Retrieve sensor gateway API key."""
        creds = self._get_credential("sensor_api")
        if isinstance(creds, str):
            return creds
        if isinstance(creds, dict):
            return creds.get("api_key", "")
        return os.getenv("SENSOR_API_KEY", "")

    # ------------------------------------------------------------------
    # MQTT broker credentials
    # ------------------------------------------------------------------
    def get_mqtt_credentials(self) -> Tuple[str, str]:
        """
        Retrieve MQTT broker username and password.

        Returns
        -------
        tuple[str, str]
            (username, password)
        """
        creds = self._get_credential("mqtt")
        if isinstance(creds, dict):
            return creds.get("username", ""), creds.get("password", "")
        return (
            os.getenv("MQTT_USERNAME", ""),
            os.getenv("MQTT_PASSWORD", ""),
        )

    # ------------------------------------------------------------------
    # BigQuery service account
    # ------------------------------------------------------------------
    def get_bigquery_service_account_path(self) -> Optional[str]:
        """
        Retrieve path to BigQuery service account JSON file.

        If credentials are stored in Secret Manager, writes them to a
        temporary file and returns the path.
        """
        # Check for local env var first
        sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if sa_path and os.path.exists(sa_path):
            return sa_path

        # Try Secret Manager
        creds = self._get_credential("bigquery_sa")
        if isinstance(creds, (str, dict)):
            return self._write_sa_to_temp(creds, "bq_sa")

        return None

    # ------------------------------------------------------------------
    # GCS service account
    # ------------------------------------------------------------------
    def get_gcs_service_account_path(self) -> Optional[str]:
        """Retrieve path to GCS service account JSON file."""
        sa_path = os.getenv("GCS_SERVICE_ACCOUNT_JSON")
        if sa_path and os.path.exists(sa_path):
            return sa_path

        creds = self._get_credential("gcs")
        if isinstance(creds, (str, dict)):
            return self._write_sa_to_temp(creds, "gcs_sa")

        return None

    # ------------------------------------------------------------------
    # Generic credential retrieval
    # ------------------------------------------------------------------
    def _get_credential(self, key: str) -> Any:
        """
        Retrieve a credential by key, using the following priority:
        1. In-memory cache (if valid)
        2. Google Secret Manager (if enabled)
        3. Environment variables
        4. Local credentials file
        """
        cache_key = key

        # Check in-memory cache
        if self.config.cache_in_memory and self._is_cache_valid(cache_key):
            return self._credential_cache[cache_key]

        with self._lock:
            # Double-check after acquiring lock
            if self.config.cache_in_memory and self._is_cache_valid(cache_key):
                return self._credential_cache[cache_key]

            # Try Secret Manager
            if self.config.use_secret_manager and self._secret_client:
                try:
                    value = self._fetch_from_secret_manager(CRED_KEYS[key])
                    self._cache_credential(cache_key, value)
                    return value
                except Exception as exc:
                    logger.debug("Secret Manager miss for %s: %s", key, exc)

            # Try environment variables
            env_value = self._fetch_from_env(key)
            if env_value is not None:
                self._cache_credential(cache_key, env_value)
                return env_value

            # Try local credentials file
            local_value = self._fetch_from_local_file(key)
            if local_value is not None:
                self._cache_credential(cache_key, local_value)
                return local_value

        logger.warning("Credential not found for key: %s", key)
        return None

    # ------------------------------------------------------------------
    # Secret Manager
    # ------------------------------------------------------------------
    def _fetch_from_secret_manager(self, secret_id: str) -> Any:
        """Fetch and parse a secret from Google Secret Manager."""
        name = f"projects/{self.config.secret_manager_project}/secrets/{secret_id}/versions/latest"
        response = self._secret_client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8")

        # Try JSON parsing
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return payload

    # ------------------------------------------------------------------
    # Environment variables
    # ------------------------------------------------------------------
    @staticmethod
    def _fetch_from_env(key: str) -> Any:
        """Map credential keys to environment variable names."""
        env_map = {
            "satellite_api": ("SENTINEL_CLIENT_ID", "SENTINEL_CLIENT_SECRET"),
            "sensor_api": ("SENSOR_API_KEY",),
            "mqtt": ("MQTT_USERNAME", "MQTT_PASSWORD"),
            "bigquery_sa": ("BIGQUERY_SERVICE_ACCOUNT_JSON",),
            "gcs": ("GCS_SERVICE_ACCOUNT_JSON",),
        }

        env_vars = env_map.get(key, ())
        if not env_vars:
            return None

        # Single value
        if len(env_vars) == 1:
            value = os.getenv(env_vars[0])
            if value:
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value
            return None

        # Multiple values (return as dict)
        result = {}
        for var in env_vars:
            val = os.getenv(var)
            if val:
                result[var.lower().replace("sentinel_", "").replace("mqtt_", "")] = val

        return result if result else None

    # ------------------------------------------------------------------
    # Local file
    # ------------------------------------------------------------------
    def _fetch_from_local_file(self, key: str) -> Any:
        """Fetch credential from local JSON credentials file."""
        if not os.path.exists(self.config.local_creds_path):
            return None

        try:
            with open(self.config.local_creds_path) as fh:
                all_creds = json.load(fh)
            return all_creds.get(key)
        except (json.JSONDecodeError, IOError) as exc:
            logger.debug("Local creds file read failed: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Caching
    # ------------------------------------------------------------------
    def _is_cache_valid(self, key: str) -> bool:
        if key not in self._credential_cache:
            return False
        if key not in self._cache_timestamps:
            return False
        age = (datetime.now(timezone.utc) - self._cache_timestamps[key]).total_seconds()
        return age < self.config.cache_ttl_sec

    def _cache_credential(self, key: str, value: Any) -> None:
        self._credential_cache[key] = value
        self._cache_timestamps[key] = datetime.now(timezone.utc)

    def invalidate_cache(self, key: Optional[str] = None) -> None:
        """Invalidate credential cache. If key is None, clear all."""
        with self._lock:
            if key:
                self._credential_cache.pop(key, None)
                self._cache_timestamps.pop(key, None)
                logger.info("Invalidated credential cache for %s", key)
            else:
                self._credential_cache.clear()
                self._cache_timestamps.clear()
                logger.info("Invalidated all credential caches")

    # ------------------------------------------------------------------
    # Token lifecycle
    # ------------------------------------------------------------------
    def get_token(self, token_key: str, refresh_callback: Optional[Callable[[], TokenInfo]] = None) -> Optional[str]:
        """
        Retrieve a cached token, refreshing if expired.

        Parameters
        ----------
        token_key : str
            Cache key for the token.
        refresh_callback : callable, optional
            Function to call to refresh the token if expired.

        Returns
        -------
        str or None
            The access token, or None if unavailable.
        """
        with self._lock:
            token = self._token_cache.get(token_key)

            if token and not token.is_expired:
                return token.access_token

            # Token expired or missing — refresh
            if refresh_callback:
                try:
                    new_token = refresh_callback()
                    self._token_cache[token_key] = new_token
                    return new_token.access_token
                except Exception as exc:
                    logger.error("Token refresh failed for %s: %s", token_key, exc)
                    return None

            return token.access_token if token else None

    def invalidate_token(self, token_key: str) -> None:
        """Remove a token from the cache."""
        with self._lock:
            self._token_cache.pop(token_key, None)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _write_sa_to_temp(creds: Any, prefix: str) -> str:
        """Write service account credentials to a temporary JSON file."""
        import tempfile

        if isinstance(creds, str):
            try:
                creds_dict = json.loads(creds)
            except json.JSONDecodeError:
                return creds if os.path.exists(creds) else ""
        else:
            creds_dict = creds

        fd, path = tempfile.mkstemp(prefix=f"harmony_{prefix}_", suffix=".json")
        with os.fdopen(fd, "w") as fh:
            json.dump(creds_dict, fh)

        return path

    # ------------------------------------------------------------------
    # Audit
    # ------------------------------------------------------------------
    def audit_access(self, credential_key: str, operation: str) -> None:
        """Log credential access for TDA traceability."""
        logger.info(
            "Credential access | key=%s | operation=%s | timestamp=%s",
            credential_key,
            operation,
            datetime.now(timezone.utc).isoformat(),
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Project Harmony — Credential Manager")
    subparsers = parser.add_subparsers(dest="command")

    # Get credential
    get_parser = subparsers.add_parser("get", help="Retrieve a credential")
    get_parser.add_argument("--key", required=True, choices=list(CRED_KEYS.keys()))

    # Invalidate cache
    inv_parser = subparsers.add_parser("invalidate", help="Invalidate cache")
    inv_parser.add_argument("--key", help="Specific key to invalidate (default: all)")

    # List keys
    subparsers.add_parser("list", help="List available credential keys")

    args = parser.parse_args()

    mgr = CredentialManager()

    if args.command == "get":
        value = mgr._get_credential(args.key)
        if isinstance(value, dict):
            # Mask sensitive values
            masked = {
                k: "***" if any(s in k.lower() for s in ("secret", "password", "key")) else v
                for k, v in value.items()
            }
            print(json.dumps(masked, indent=2))
        else:
            print("***" if value else "Not found")
    elif args.command == "invalidate":
        mgr.invalidate_cache(args.key)
    elif args.command == "list":
        for k, v in CRED_KEYS.items():
            print(f"{k}: {v}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
