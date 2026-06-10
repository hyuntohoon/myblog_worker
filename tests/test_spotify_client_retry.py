# FEAT-album-catalog-ingest Step 1 — the client-credentials catalog client
# (worker/clients/spotify_client.py) now routes token POST + catalog GETs through
# the shared `_request_with_retry` helper. Before this, a single Spotify 429 during
# batch album sync propagated out of sync_albums_batch, so the SQS message redelivered
# 3x and landed in the DLQ. The helper itself is unit-tested in test_listening_sync.py;
# these tests pin the *wiring* — each call site actually retries and the failure
# contract (raise_for_status after exhaustion) is preserved.
import time

import httpx
import pytest

import worker.clients.spotify_user_client as suc
from worker.clients.spotify_client import SpotifyClient


def _resp(status, headers=None, json_body=None):
    """Real httpx.Response so raise_for_status / .json() behave like production."""
    return httpx.Response(
        status,
        headers=headers or {},
        json=json_body,
        request=httpx.Request("GET", "https://api.spotify.com/v1/x"),
    )


def _patch_requests(monkeypatch, responses):
    """Feed the shared retry helper a fixed response sequence; record calls + sleeps.
    Patch targets live on the suc module because that's where the helper is defined."""
    calls: list = []
    sleeps: list = []

    def fake_request(method, url, **kwargs):
        idx = len(calls)
        calls.append((method, url))
        item = responses[idx]
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(suc.httpx, "request", fake_request)
    monkeypatch.setattr(suc.time, "sleep", lambda s: sleeps.append(s))
    return calls, sleeps


def _client_with_token() -> SpotifyClient:
    """Client with a pre-seeded bearer so tests hit only the catalog endpoint."""
    c = SpotifyClient()
    c._token = "test-token"
    c._exp = time.time() + 3600
    return c


@pytest.mark.unit
def test_get_albums_retries_429_then_succeeds(monkeypatch):
    calls, sleeps = _patch_requests(monkeypatch, [
        _resp(429, headers={"Retry-After": "1"}),
        _resp(200, json_body={"albums": [{"id": "a1"}, {"id": "a2"}]}),
    ])
    out = _client_with_token().get_albums(["a1", "a2"])
    assert [a["id"] for a in out] == ["a1", "a2"]
    assert len(calls) == 2
    assert sleeps == [1.0]  # honoured Retry-After, no raise


@pytest.mark.unit
def test_get_artists_retries_5xx_then_succeeds(monkeypatch):
    calls, sleeps = _patch_requests(monkeypatch, [
        _resp(503),
        _resp(200, json_body={"artists": [{"id": "ar1"}]}),
    ])
    out = _client_with_token().get_artists(["ar1"])
    assert [a["id"] for a in out] == ["ar1"]
    assert len(calls) == 2
    assert sleeps == [0.5]  # BASE_BACKOFF, first attempt


@pytest.mark.unit
def test_get_albums_exhausted_429_still_raises(monkeypatch):
    # failure contract preserved: after max tries the last response surfaces via
    # raise_for_status, so the SQS path still redelivers/DLQs a persistent outage
    calls, _ = _patch_requests(monkeypatch, [_resp(429), _resp(429), _resp(429)])
    with pytest.raises(httpx.HTTPStatusError):
        _client_with_token().get_albums(["a1"])
    assert len(calls) == 3


@pytest.mark.unit
def test_token_request_retries_transient_5xx(monkeypatch):
    calls, sleeps = _patch_requests(monkeypatch, [
        _resp(503),
        _resp(200, json_body={"access_token": "tok", "expires_in": 3600}),
        _resp(200, json_body={"albums": [{"id": "a1"}]}),
    ])
    c = SpotifyClient()  # no token seeded → first hits the token endpoint
    out = c.get_albums(["a1"])
    assert [a["id"] for a in out] == ["a1"]
    assert len(calls) == 3  # token 503 → token 200 → albums 200
    assert sleeps == [0.5]
