"""
test_auth.py
Unit and integration tests for the authentication and credential manager.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

from src.utils.auth import (
    AuthConfig,
    CredentialManager,
    TokenInfo,
)


# =============================================================================
# Fixtures
# =============================================================================
@pytest.fixture
def auth_config(tmp_path) -> AuthConfig:
    return AuthConfig(
        use_secret_manager=False,
        local_creds_path=str(tmp_path / "credentials.json"),
        cache_in_memory=True,
        cache_ttl_sec=3600,
    )


@pytest.fixture
def mock_secret_client() -> MagicMock:
    return MagicMock()


# =============================================================================
# TokenInfo Tests
# =============================================================================
class TestTokenInfo:
    def test_token_not_expired(self) -> None:
        token = TokenInfo(
            access_token="test_token",
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )
        assert token.is_expired is False

    def test_token_expired(self) -> None:
        token = TokenInfo(
            access_token="test_token",
            expires_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        )
        assert token.is_expired is True

    def test_token_near_expiry_with_margin(self) -> None:
        # Within refresh margin (5 minutes)
        token = TokenInfo(
            access_token="test_token",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=3),
        )
        assert token.is_expired is True

    def test_token_no_expiry(self) -> None:
        token = TokenInfo(access_token="test_token")
        assert token.is_expired is False


# =============================================================================
# Credential Retrieval Tests
# =============================================================================
class TestCredentialRetrieval:
    def test_satellite_credentials_from_local_file(self, auth_config: AuthConfig, tmp_path) -> None:
        creds = {
            "satellite_api": {
                "client_id": "test_id",
                "client_secret": "test_secret",
            }
        }
        creds_file = tmp_path / "credentials.json"
        creds_file.write_text(json.dumps(creds))
        auth_config.local_creds_path = str(creds_file)

        mgr = CredentialManager(config=auth_config)
        client_id, client_secret = mgr.get_satellite_credentials()
        assert client_id == "test_id"
        assert client_secret == "test_secret"

    def test_sensor_api_key_from_local_file(self, auth_config: AuthConfig, tmp_path) -> None:
        creds = {
            "sensor_api": {"api_key": "test_api_key_123"}
        }
        creds_file = tmp_path / "credentials.json"
        creds_file.write_text(json.dumps(creds))
        auth_config.local_creds_path = str(creds_file)

        mgr = CredentialManager(config=auth_config)
        api_key = mgr.get_sensor_api_key()
        assert api_key == "test_api_key_123"

    def test_mqtt_credentials_from_local_file(self, auth_config: AuthConfig, tmp_path) -> None:
        creds = {
            "mqtt": {
                "username": "mqtt_user",
                "password": "mqtt_pass",
            }
        }
        creds_file = tmp_path / "credentials.json"
        creds_file.write_text(json.dumps(creds))
        auth_config.local_creds_path = str(creds_file)

        mgr = CredentialManager(config=auth_config)
        username, password = mgr.get_mqtt_credentials()
        assert username == "mqtt_user"
        assert password == "mqtt_pass"

    def test_fallback_to_env_vars(self, auth_config: AuthConfig, monkeypatch) -> None:
        monkeypatch.setenv("SENTINEL_CLIENT_ID", "env_id")
        monkeypatch.setenv("SENTINEL_CLIENT_SECRET", "env_secret")

        mgr = CredentialManager(config=auth_config)
        client_id, client_secret = mgr.get_satellite_credentials()
        assert client_id == "env_id"
        assert client_secret == "env_secret"

    def test_credential_not_found(self, auth_config: AuthConfig) -> None:
        mgr = CredentialManager(config=auth_config)
        api_key = mgr.get_sensor_api_key()
        assert api_key is None


# =============================================================================
# Cache Tests
# =============================================================================
class TestCache:
    def test_cache_hit(self, auth_config: AuthConfig, tmp_path) -> None:
        creds = {"satellite_api": {"client_id": "cached_id", "client_secret": "cached_secret"}}
        creds_file = tmp_path / "credentials.json"
        creds_file.write_text(json.dumps(creds))
        auth_config.local_creds_path = str(creds_file)

        mgr = CredentialManager(config=auth_config)
        # First call should cache
        mgr.get_satellite_credentials()
        # Second call should hit cache
        client_id, client_secret = mgr.get_satellite_credentials()
        assert client_id == "cached_id"

    def test_cache_invalidation(self, auth_config: AuthConfig, tmp_path) -> None:
        creds = {"satellite_api": {"client_id": "old_id", "client_secret": "old_secret"}}
        creds_file = tmp_path / "credentials.json"
        creds_file.write_text(json.dumps(creds))
        auth_config.local_creds_path = str(creds_file)

        mgr = CredentialManager(config=auth_config)
        mgr.get_satellite_credentials()
        mgr.invalidate_cache("satellite_api")

        # After invalidation, should re-read from file
        new_creds = {"satellite_api": {"client_id": "new_id", "client_secret": "new_secret"}}
        creds_file.write_text(json.dumps(new_creds))
        client_id, _ = mgr.get_satellite_credentials()
        assert client_id == "new_id"

    def test_cache_ttl_expiry(self, auth_config: AuthConfig, tmp_path) -> None:
        auth_config.cache_ttl_sec = 0  # Immediate expiry
        creds = {"satellite_api": {"client_id": "ttl_id", "client_secret": "ttl_secret"}}
        creds_file = tmp_path / "credentials.json"
        creds_file.write_text(json.dumps(creds))
        auth_config.local_creds_path = str(creds_file)

        mgr = CredentialManager(config=auth_config)
        mgr.get_satellite_credentials()
        # Due to 0 TTL, cache should be considered invalid
        assert not mgr._is_cache_valid("satellite_api")


# =============================================================================
# Token Lifecycle Tests
# =============================================================================
class TestTokenLifecycle:
    def test_get_valid_token(self, auth_config: AuthConfig) -> None:
        mgr = CredentialManager(config=auth_config)
        token = TokenInfo(
            access_token="valid_token",
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )
        mgr._token_cache["test_key"] = token

        result = mgr.get_token("test_key")
        assert result == "valid_token"

    def test_get_expired_token_with_refresh(self, auth_config: AuthConfig) -> None:
        mgr = CredentialManager(config=auth_config)
        old_token = TokenInfo(
            access_token="old_token",
            expires_at=datetime.now(timezone.utc) - timedelta(hours=1),
        )
        mgr._token_cache["test_key"] = old_token

        def refresh_callback() -> TokenInfo:
            return TokenInfo(
                access_token="new_token",
                expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            )

        result = mgr.get_token("test_key", refresh_callback=refresh_callback)
        assert result == "new_token"

    def test_token_invalidation(self, auth_config: AuthConfig) -> None:
        mgr = CredentialManager(config=auth_config)
        mgr._token_cache["test_key"] = TokenInfo(access_token="token_123")
        mgr.invalidate_token("test_key")
        assert "test_key" not in mgr._token_cache


# =============================================================================
# Audit Tests
# =============================================================================
class TestAudit:
    def test_audit_access_logs(self, auth_config: AuthConfig, caplog) -> None:
        import logging
        caplog.set_level(logging.INFO)

        mgr = CredentialManager(config=auth_config)
        mgr.audit_access("satellite_api", "retrieve")

        assert "satellite_api" in caplog.text
        assert "retrieve" in caplog.text


# =============================================================================
# Service Account Helper Tests
# =============================================================================
class TestServiceAccountHelpers:
    def test_write_sa_to_temp_with_dict(self, auth_config: AuthConfig) -> None:
        mgr = CredentialManager(config=auth_config)
        creds_dict = {"type": "service_account", "project_id": "test"}
        path = mgr._write_sa_to_temp(creds_dict, "test_sa")
        assert os.path.exists(path)
        with open(path) as f:
            loaded = json.load(f)
        assert loaded["project_id"] == "test"
        os.unlink(path)

    def test_write_sa_to_temp_with_json_string(self, auth_config: AuthConfig) -> None:
        mgr = CredentialManager(config=auth_config)
        creds_json = json.dumps({"type": "service_account", "project_id": "test2"})
        path = mgr._write_sa_to_temp(creds_json, "test_sa2")
        assert os.path.exists(path)
        os.unlink(path)

    def test_bigquery_sa_path_from_env(self, auth_config: AuthConfig, tmp_path, monkeypatch) -> None:
        sa_file = tmp_path / "bq_sa.json"
        sa_file.write_text(json.dumps({"type": "service_account"}))
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(sa_file))

        mgr = CredentialManager(config=auth_config)
        path = mgr.get_bigquery_service_account_path()
        assert path == str(sa_file)
