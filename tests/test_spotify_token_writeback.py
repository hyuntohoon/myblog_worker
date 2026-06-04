# Unit tests for the D30 Spotify token write-back: rotated refresh_token persistence,
# last_successful_refresh_at recording, and the needs_reauth marker on invalid_grant.
#
# Pure-mock — httpx and boto3 Secrets Manager are faked. The write-back is a Secrets
# Manager side-effect that touches no DB transaction boundary, so (unlike the
# listening-sync upsert/prune path) it needs no live-engine integration test
# (cf. feedback-sa-session-lifecycle-mock-blind: mocks are blind to conn-pool / SQL
# semantics, not to a stateless read-merge-write of a JSON blob).
from __future__ import annotations

import json
from unittest.mock import MagicMock

import httpx
import pytest

from worker.clients import spotify_user_client as suc
from worker.clients.spotify_user_client import (
    SpotifyUserClient,
    _is_invalid_grant,
    _persist_token_state,
)
from worker.core.config import settings

_CREDS = {"client_id": "c", "client_secret": "s", "refresh_token": "OLD"}


class _Resp:
    def __init__(self, status_code=200, body=None):
        self.status_code = status_code
        self._body = body or {}

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError("err", request=MagicMock(), response=MagicMock())


class _FakeSM:
    """A fake Secrets Manager client; records every put and serves a mutable payload."""

    def __init__(self, stored):
        self.stored = dict(stored)
        self.puts: list[dict] = []  # payloads written, in order

    def get_secret_value(self, SecretId):
        return {"SecretString": json.dumps(self.stored)}

    def put_secret_value(self, SecretId, SecretString):
        self.stored = json.loads(SecretString)
        self.puts.append(self.stored)


@pytest.fixture
def sm(monkeypatch):
    """Point settings + boto3 at a fake Secrets Manager holding the spotify secret."""
    fake = _FakeSM(dict(_CREDS))
    monkeypatch.setattr(settings, "SPOTIFY_SECRETS_ARN", "arn:aws:secretsmanager:::spotify")
    monkeypatch.setattr("boto3.client", lambda *a, **k: fake)
    return fake


def _post(body, status=200):
    return lambda *a, **k: _Resp(status, body)


# ── invalid_grant detection ──────────────────────────────────────────────────────

@pytest.mark.unit
def test_is_invalid_grant_only_for_spotify_invalid_grant():
    assert _is_invalid_grant(_Resp(400, {"error": "invalid_grant"})) is True
    assert _is_invalid_grant(_Resp(400, {"error": "invalid_client"})) is False

    class _Unparseable:
        def json(self):
            raise ValueError("not json")

    assert _is_invalid_grant(_Unparseable()) is False


# ── successful refresh write-back ─────────────────────────────────────────────────

@pytest.mark.unit
def test_refresh_success_stamps_timestamp_and_clears_reauth(sm, monkeypatch):
    sm.stored["needs_reauth"] = True  # a prior invalid_grant had flagged it
    monkeypatch.setattr(suc.httpx, "request", _post({"access_token": "AT", "expires_in": 3600}))

    client = SpotifyUserClient(creds=dict(_CREDS))
    assert client._get_access_token() == "AT"

    written = sm.puts[-1]
    assert "last_successful_refresh_at" in written
    assert "needs_reauth" not in written       # recovered → marker cleared
    assert written["refresh_token"] == "OLD"   # no rotation → token unchanged


@pytest.mark.unit
def test_refresh_rotated_token_persisted_and_cached_in_memory(sm, monkeypatch):
    monkeypatch.setattr(
        suc.httpx, "request",
        _post({"access_token": "AT", "expires_in": 3600, "refresh_token": "NEW"}),
    )
    creds = dict(_CREDS)
    client = SpotifyUserClient(creds=creds)
    client._get_access_token()

    assert creds["refresh_token"] == "NEW"          # warm-Lambda in-memory copy updated
    assert sm.puts[-1]["refresh_token"] == "NEW"    # and persisted to the secret
    assert "last_successful_refresh_at" in sm.puts[-1]


# ── invalid_grant → re-auth needed ────────────────────────────────────────────────

@pytest.mark.unit
def test_invalid_grant_flags_needs_reauth_and_raises(sm, monkeypatch):
    monkeypatch.setattr(suc.httpx, "request", _post({"error": "invalid_grant"}, status=400))

    client = SpotifyUserClient(creds=dict(_CREDS))
    with pytest.raises(RuntimeError, match="invalid_grant"):
        client._get_access_token()

    written = sm.puts[-1]
    assert written["needs_reauth"] is True
    assert written["refresh_token"] == "OLD"            # preserved for re-bootstrap
    assert "last_successful_refresh_at" not in written  # refresh did not succeed


@pytest.mark.unit
def test_transient_5xx_does_not_flip_reauth(sm, monkeypatch):
    monkeypatch.setattr(suc.httpx, "request", _post({"error": "server_error"}, status=503))
    monkeypatch.setattr(suc.time, "sleep", lambda s: None)  # retry helper backs off 3×

    client = SpotifyUserClient(creds=dict(_CREDS))
    with pytest.raises(httpx.HTTPStatusError):
        client._get_access_token()

    assert sm.puts == []  # a transient failure must never trip "재인증 필요"


# ── _persist_token_state edge cases ───────────────────────────────────────────────

@pytest.mark.unit
def test_persist_is_noop_without_arn(monkeypatch):
    monkeypatch.setattr(settings, "SPOTIFY_SECRETS_ARN", "")
    client_factory = MagicMock()
    monkeypatch.setattr("boto3.client", client_factory)

    _persist_token_state(rotated_refresh_token="NEW")
    client_factory.assert_not_called()  # local/dev: nothing to write back


@pytest.mark.unit
def test_persist_needs_reauth_is_idempotent(sm):
    sm.stored["needs_reauth"] = True
    _persist_token_state(needs_reauth=True)
    assert sm.puts == []  # already flagged → don't churn a new secret version each tick


@pytest.mark.unit
def test_write_back_failure_is_non_fatal(sm, monkeypatch):
    def _boom(**kwargs):
        raise RuntimeError("secretsmanager throttled")

    sm.put_secret_value = _boom
    monkeypatch.setattr(suc.httpx, "request", _post({"access_token": "AT", "expires_in": 3600}))

    client = SpotifyUserClient(creds=dict(_CREDS))
    # the access token is already in hand; a failed write-back must not break sync
    assert client._get_access_token() == "AT"
