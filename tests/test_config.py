from __future__ import annotations

import json
import logging
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


class TestRequiredSecretContract:
    """The payload-based required-key contract (settings-loader sweep).

    `TestGetSettings` above already covers "a required key is absent". These
    cover the conditions the previous formulation — copy anything truthy across,
    then check the RESOLVED settings with `if not v` — could not see. Each was
    reproduced against that code, not inferred: `{"DATABASE_URL": "   "}` booted
    with a whitespace DSN, and `{"SPOTIFY_CLIENT_ID": 12345}` booted with an
    `int` sitting in a `str` field because this model does not set
    `validate_assignment`.

    The required SET is unchanged by this sweep — LASTFM_API_KEY and
    GENIUS_ACCESS_TOKEN stay optional, and `test_optional_keys_*` is what keeps
    them that way.
    """

    _DB = "postgresql+psycopg://u:pw@db.example/worker"
    _CID = "spotify-client-id"
    _CSECRET = "spotify-client-secret"

    @property
    def _full(self) -> dict:
        return {
            "DATABASE_URL": self._DB,
            "SPOTIFY_CLIENT_ID": self._CID,
            "SPOTIFY_CLIENT_SECRET": self._CSECRET,
        }

    def _call(self, secrets, *, param: str = "/myblog/worker", env: dict | None = None):
        """get_settings() with the SSM load stubbed and the lru_cache cleared.

        The required names are blanked in the environment so an exported shell
        value cannot stand in for a key the payload is missing — the
        substitution `test_env_var_cannot_mask_a_missing_key` pins. The optional
        names are blanked for the same reason.
        """
        get_settings.cache_clear()
        base = {
            "SECRETS_PARAM": param,
            "DATABASE_URL": "",
            "SPOTIFY_CLIENT_ID": "",
            "SPOTIFY_CLIENT_SECRET": "",
            "LASTFM_API_KEY": "",
            "GENIUS_ACCESS_TOKEN": "",
        }
        try:
            with (
                patch("worker.core.config._load_secrets", return_value=secrets),
                patch.dict("os.environ", {**base, **(env or {})}, clear=False),
            ):
                return get_settings()
        finally:
            get_settings.cache_clear()

    def test_env_var_cannot_mask_a_missing_key(self):
        """A stale Lambda env var must not satisfy the contract.

        Dormant today — `blogWorkerLambda` carries no such variable — which is
        exactly why only a test keeps it that way.
        """
        with pytest.raises(ValueError, match="SPOTIFY_CLIENT_SECRET"):
            self._call(
                {"DATABASE_URL": self._DB, "SPOTIFY_CLIENT_ID": self._CID},
                env={"SPOTIFY_CLIENT_SECRET": "from-the-env"},
            )

    @pytest.mark.parametrize("bad", ["", "   ", "\t\n", None, 123, 0, [], {}, True])
    def test_unusable_required_value_counts_as_missing(self, bad):
        """`null`, empty, whitespace-only and non-strings are all "absent".

        The parameter is hand-edited JSON; each shape has a plausible way of
        being written. `"   "` and `123` are the two the previous truthiness
        check let through.
        """
        with pytest.raises(ValueError, match="SPOTIFY_CLIENT_ID"):
            self._call({**self._full, "SPOTIFY_CLIENT_ID": bad})

    def test_payload_that_is_valid_json_but_not_an_object_fails(self):
        """`json.loads` succeeds on `"abc"` and `[1,2]`; `.get` would explode.

        The failure must name the parameter, not surface as an AttributeError.
        """
        for payload in ("just-a-string", [1, 2], 7, None):
            with pytest.raises(ValueError, match="/myblog/worker"):
                self._call(payload)

    def test_required_values_are_assigned_verbatim_and_never_stripped(self):
        """Whitespace decides presence but must NOT be trimmed off the value.

        "Normalize the config values" is a natural tidy-up and it breaks the
        Spotify token mint: the client secret is sent byte-for-byte and a
        trimmed one 400s the exchange, taking ingest, enrich and album-sync with
        it. Without this, `secrets[k].strip()` passes every other test here.
        """
        padded = {
            "DATABASE_URL": " postgresql+psycopg://u:pw@h/worker ",
            "SPOTIFY_CLIENT_ID": "  cid-with-padding  ",
            "SPOTIFY_CLIENT_SECRET": "\tcsecret-with-padding\t",
        }
        s = self._call(padded)
        assert s.DATABASE_URL == padded["DATABASE_URL"]
        assert s.SPOTIFY_CLIENT_ID == padded["SPOTIFY_CLIENT_ID"]
        assert s.SPOTIFY_CLIENT_SECRET == padded["SPOTIFY_CLIENT_SECRET"]

    @pytest.mark.parametrize("key", ["LASTFM_API_KEY", "GENIUS_ACCESS_TOKEN"])
    def test_optional_keys_are_applied_when_present(self, key):
        s = self._call({**self._full, key: "opt-value"})
        assert getattr(s, key) == "opt-value"

    @pytest.mark.parametrize("key", ["LASTFM_API_KEY", "GENIUS_ACCESS_TOKEN"])
    @pytest.mark.parametrize("absent", [None, "", "   "])
    def test_optional_keys_never_block_boot_and_are_warned_about(self, key, absent, caplog):
        """This Lambda hosts many independent jobs.

        Promoting either of these to required would take album ingest, artist
        enrich, the release sweep and the SQS album-sync consumer down at import
        time because one degradable integration is unconfigured. The WARNING is
        the only signal the operator gets — the gated job otherwise no-ops in
        silence.
        """
        payload = dict(self._full)
        if absent is not None:
            payload[key] = absent
        with caplog.at_level(logging.WARNING, logger="worker.core.config"):
            s = self._call(payload)
        assert s.DATABASE_URL == self._DB, "a missing optional key must not stop boot"
        assert getattr(s, key) == "", "an unusable optional value must not be stored"
        assert key in caplog.text
        assert "/myblog/worker" in caplog.text

    def test_secret_values_never_reach_the_exception_or_the_logs(self, caplog):
        """The error names the parameter and the KEYS. Never a value.

        A config error is read off CloudWatch by whoever is paged; a value
        pasted into it is a secret published to a log group with a different
        audience than the parameter it came from.
        """
        leaked = "SUPERSECRET-DB-PASSWORD"
        with caplog.at_level(logging.DEBUG):
            with pytest.raises(ValueError) as exc:
                self._call({"DATABASE_URL": f"postgresql://u:{leaked}@h/worker"})
        assert leaked not in str(exc.value)
        assert leaked not in caplog.text
        assert "SPOTIFY_CLIENT_ID" in str(exc.value)
        assert "/myblog/worker" in str(exc.value)

    def test_successful_boot_does_not_log_the_secret_values(self, caplog):
        with caplog.at_level(logging.DEBUG):
            self._call(self._full)
        assert self._CSECRET not in caplog.text
        assert "pw@db.example" not in caplog.text
