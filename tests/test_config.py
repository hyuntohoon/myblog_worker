from __future__ import annotations

import json
from functools import lru_cache
from unittest.mock import MagicMock, patch

import pytest

from worker.core.config import Settings, _load_secrets, get_settings


def _make_settings(**overrides) -> Settings:
    defaults = {
        "ENV": "prod",
        "SECRETS_PARAM": "/myblog/worker",
        "DATABASE_URL": "",
        "SPOTIFY_CLIENT_ID": "",
        "SPOTIFY_CLIENT_SECRET": "",
    }
    return Settings(**{**defaults, **overrides})


class TestLoadSecrets:
    # CHORE-secrets-ssm-migration (final leg): _load_secrets(param) is SSM-only.
    # AWS Secrets Manager holds zero secrets in this account and no Lambda sets
    # SECRETS_ARN, so the old fallback branch was unreachable in production and
    # could only turn an SSM failure into a silent empty load.
    def test_returns_parsed_json_from_ssm(self):
        payload = {"DATABASE_URL": "postgresql://host/db", "SPOTIFY_CLIENT_ID": "cid"}
        ssm_mock = MagicMock()
        ssm_mock.get_parameter.return_value = {"Parameter": {"Value": json.dumps(payload)}}
        with patch("boto3.client", return_value=ssm_mock) as mk:
            assert _load_secrets("/myblog/worker") == payload
        assert mk.call_args.args[0] == "ssm"
        assert ssm_mock.get_parameter.call_args.kwargs == {
            "Name": "/myblog/worker",
            "WithDecryption": True,
        }

    def test_raises_on_ssm_error_instead_of_returning_empty(self):
        """The behaviour change: an SSM failure must be loud.

        It used to log 'falling back to Secrets Manager', find no ARN, and return
        {} — which surfaced one layer later as a ValueError naming the wrong
        subsystem. Now the boto3 error propagates with the parameter name logged.
        """
        ssm_mock = MagicMock()
        ssm_mock.get_parameter.side_effect = Exception("AccessDenied")
        with patch("boto3.client", return_value=ssm_mock):
            with pytest.raises(Exception, match="AccessDenied"):
                _load_secrets("/myblog/worker")

    def test_raises_when_boto_client_cannot_be_constructed(self):
        with patch("boto3.client", side_effect=Exception("no network")):
            with pytest.raises(Exception, match="no network"):
                _load_secrets("/myblog/worker")

    def test_never_constructs_a_secretsmanager_client(self):
        payload = {"DATABASE_URL": "postgresql://host/db"}
        ssm_mock = MagicMock()
        ssm_mock.get_parameter.return_value = {"Parameter": {"Value": json.dumps(payload)}}
        seen: list[str] = []

        def client(name, **kw):
            seen.append(name)
            return ssm_mock

        with patch("boto3.client", side_effect=client):
            _load_secrets("/myblog/worker")
        assert seen == ["ssm"]


class TestGetSettings:
    def _call(self, secrets_return: dict, param: str = "/myblog/worker") -> Settings:
        """Call get_settings() with a patched _load_secrets and cleared lru_cache."""
        get_settings.cache_clear()
        with (
            patch("worker.core.config._load_secrets", return_value=secrets_return),
            patch.dict(
                "os.environ",
                {
                    "SECRETS_PARAM": param,
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

    def test_raises_when_the_secret_payload_is_empty(self):
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
                {"SECRETS_PARAM": "/myblog/worker", "DATABASE_URL": "", "SPOTIFY_CLIENT_ID": "", "SPOTIFY_CLIENT_SECRET": ""},
                clear=False,
            ),
        ):
            s = get_settings()
        assert s.DATABASE_URL == "postgresql://host/db"
        assert s.SPOTIFY_CLIENT_ID == "cid"

    def test_skips_validation_when_secrets_param_not_set(self):
        """Local dev: SECRETS_PARAM empty → no SSM call, no validation, boot succeeds."""
        get_settings.cache_clear()
        with (
            patch("worker.core.config._load_secrets") as loader,
            patch.dict("os.environ", {"SECRETS_PARAM": ""}, clear=False),
        ):
            s = get_settings()
        loader.assert_not_called()
        assert s.SECRETS_PARAM == ""


class TestReleaseCalendarStep5Defaults:
    def test_oq5_ingest_floor_aligned_with_watchlist_floor(self):
        """OQ5 (owner-decided 2026-07-12): ARTIST_POP_MIN 60 → 50 == the
        calendar watchlist floor, so announced rows of every artist can flip."""
        s = _make_settings(SECRETS_PARAM="")
        assert s.ARTIST_POP_MIN == 50
        assert s.ARTIST_POP_MIN == s.RELEASE_POLL_POP_MIN

    def test_confirm_window_defaults(self):
        s = _make_settings(SECRETS_PARAM="")
        assert s.RELEASE_CONFIRM_DATE_PROXIMITY_DAYS == 7
        assert s.RELEASE_CONFIRM_LOOKBACK_DAYS == 90
