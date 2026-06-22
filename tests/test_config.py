from __future__ import annotations

import json
from functools import lru_cache
from unittest.mock import MagicMock, patch

import pytest

from worker.core.config import Settings, _load_secrets, get_settings


def _make_settings(**overrides) -> Settings:
    defaults = {
        "ENV": "prod",
        "SECRETS_ARN": "arn:aws:secretsmanager:ap-northeast-2:123456789012:secret:myblog/worker",
        "DATABASE_URL": "",
        "SPOTIFY_CLIENT_ID": "",
        "SPOTIFY_CLIENT_SECRET": "",
    }
    return Settings(**{**defaults, **overrides})


class TestLoadSecrets:
    # CHORE-secrets-ssm-migration: _load_secrets(param, arn) — SSM-preferred, SM fallback.
    def test_ssm_preferred_when_param_set(self):
        payload = {"DATABASE_URL": "postgresql://host/db"}
        ssm_mock = MagicMock()
        ssm_mock.get_parameter.return_value = {"Parameter": {"Value": json.dumps(payload)}}

        def client(name, **kw):
            return ssm_mock if name == "ssm" else MagicMock()

        with patch("boto3.client", side_effect=client):
            assert _load_secrets("/myblog/worker", "arn:fake") == payload

    def test_falls_back_to_secrets_manager_on_ssm_error(self):
        payload = {"DATABASE_URL": "sm-db"}
        sm_mock = MagicMock()
        sm_mock.get_secret_value.return_value = {"SecretString": json.dumps(payload)}

        def client(name, **kw):
            if name == "ssm":
                m = MagicMock()
                m.get_parameter.side_effect = Exception("AccessDenied")
                return m
            return sm_mock

        with patch("boto3.client", side_effect=client):
            assert _load_secrets("/myblog/worker", "arn:fake") == payload

    def test_returns_parsed_json_on_success(self):
        payload = {"DATABASE_URL": "postgresql://host/db", "SPOTIFY_CLIENT_ID": "cid"}
        sm_mock = MagicMock()
        sm_mock.get_secret_value.return_value = {"SecretString": json.dumps(payload)}
        with patch("boto3.client", return_value=sm_mock):
            result = _load_secrets("", "arn:fake")
        assert result == payload

    def test_returns_empty_dict_on_boto_error(self):
        with patch("boto3.client", side_effect=Exception("no network")):
            result = _load_secrets("", "arn:fake")
        assert result == {}


class TestGetSettings:
    def _call(self, secrets_return: dict, arn: str = "arn:fake") -> Settings:
        """Call get_settings() with a patched _load_secrets and cleared lru_cache."""
        get_settings.cache_clear()
        with (
            patch("worker.core.config._load_secrets", return_value=secrets_return),
            patch.dict(
                "os.environ",
                {
                    "SECRETS_ARN": arn,
                    "DATABASE_URL": "",
                    "SPOTIFY_CLIENT_ID": "",
                    "SPOTIFY_CLIENT_SECRET": "",
                },
                clear=False,
            ),
        ):
            return get_settings()

    def test_raises_when_database_url_missing_after_secrets_load(self):
        secrets = {
            "SPOTIFY_CLIENT_ID": "cid",
            "SPOTIFY_CLIENT_SECRET": "csecret",
            # DATABASE_URL intentionally absent
        }
        with pytest.raises(ValueError, match="DATABASE_URL"):
            self._call(secrets)

    def test_raises_when_spotify_creds_missing_after_secrets_load(self):
        secrets = {
            "DATABASE_URL": "postgresql://host/db",
            # Spotify creds intentionally absent
        }
        with pytest.raises(ValueError, match="SPOTIFY_CLIENT"):
            self._call(secrets)

    def test_raises_when_secrets_manager_fails_entirely(self):
        with pytest.raises(ValueError):
            self._call({})

    def test_succeeds_with_all_required_secrets(self):
        secrets = {
            "DATABASE_URL": "postgresql://host/db",
            "SPOTIFY_CLIENT_ID": "cid",
            "SPOTIFY_CLIENT_SECRET": "csecret",
        }
        get_settings.cache_clear()
        with (
            patch("worker.core.config._load_secrets", return_value=secrets),
            patch.dict(
                "os.environ",
                {"SECRETS_ARN": "arn:fake", "DATABASE_URL": "", "SPOTIFY_CLIENT_ID": "", "SPOTIFY_CLIENT_SECRET": ""},
                clear=False,
            ),
        ):
            s = get_settings()
        assert s.DATABASE_URL == "postgresql://host/db"
        assert s.SPOTIFY_CLIENT_ID == "cid"

    def test_skips_validation_when_secrets_arn_not_set(self):
        """Local dev: SECRETS_ARN empty → no validation, boot succeeds."""
        get_settings.cache_clear()
        with patch.dict("os.environ", {"SECRETS_ARN": ""}, clear=False):
            s = get_settings()
        assert s.SECRETS_ARN == ""
