"""FEAT-genre-artist-distribution Step 2 unit tests — the saved-tracks client
pagination / incremental early-stop logic (pure, no DB) + sync mode validation.

The real ON CONFLICT / prune / CAST / catalog-resolve SQL is exercised against a
live engine in tests/integration/test_saved_tracks_sync_db.py
([[feedback-sa-session-lifecycle-mock-blind]] — fakes can't see real SQL)."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

import worker.clients.spotify_user_client as suc
from worker.clients.spotify_user_client import SpotifyUserClient
from worker.service.saved_tracks_sync_service import run_saved_tracks_sync


class _Resp:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def _item(tid, added_at, name="Song", artist="Artist", album_sid="alb", album_name="Album"):
    return {
        "added_at": added_at,
        "track": {
            "id": tid,
            "name": name,
            "artists": [{"name": artist}],
            "album": {"id": album_sid, "name": album_name},
        },
    }


def _client(monkeypatch, pages):
    """A SpotifyUserClient whose HTTP layer returns `pages` in call order and whose
    auth header is stubbed (so no token mint / Secrets read)."""
    calls = {"n": 0}

    def fake_request(method, url, headers=None, params=None, timeout=None):
        i = calls["n"]
        calls["n"] += 1
        return _Resp(pages[i] if i < len(pages) else {"items": [], "next": None})

    monkeypatch.setattr(suc, "_request_with_retry", fake_request)
    monkeypatch.setattr(SpotifyUserClient, "_headers", lambda self: {})
    client = SpotifyUserClient.__new__(SpotifyUserClient)  # bypass __init__ (no secrets)
    return client, calls


def test_get_saved_tracks_paginates_full_library(monkeypatch):
    page0 = {"items": [_item("t1", "2024-03-01T00:00:00Z"), _item("t2", "2024-02-01T00:00:00Z")], "next": "u", "total": 3}
    page1 = {"items": [_item("t3", "2024-01-01T00:00:00Z")], "next": None, "total": 3}
    client, calls = _client(monkeypatch, [page0, page1])

    rows = client.get_saved_tracks(since=None)

    assert [r["spotify_track_id"] for r in rows] == ["t1", "t2", "t3"]
    assert calls["n"] == 2


def test_get_saved_tracks_incremental_stops_at_since(monkeypatch):
    # items are added_at-desc; since == t2's added_at → t2 (<=) and everything older
    # are excluded, and paging stops before page 1 is fetched.
    since = datetime(2024, 2, 1, tzinfo=timezone.utc)
    page0 = {
        "items": [
            _item("t1", "2024-03-01T00:00:00Z"),
            _item("t2", "2024-02-01T00:00:00Z"),
            _item("t3", "2024-01-15T00:00:00Z"),
        ],
        "next": "u",
        "total": 5,
    }
    page1 = {"items": [_item("t9", "2023-01-01T00:00:00Z")], "next": None}
    client, calls = _client(monkeypatch, [page0, page1])

    rows = client.get_saved_tracks(since=since)

    assert [r["spotify_track_id"] for r in rows] == ["t1"]
    assert calls["n"] == 1  # stopped — never fetched page 1


def test_get_saved_tracks_normalizes_and_joins_artists(monkeypatch):
    item = {
        "added_at": "2024-03-01T00:00:00Z",
        "track": {
            "id": "t1",
            "name": "Song",
            "artists": [{"name": "A"}, {"name": "B"}],
            "album": {"id": "alb1", "name": "Alb"},
        },
    }
    client, _ = _client(monkeypatch, [{"items": [item], "next": None}])

    (row,) = client.get_saved_tracks()

    assert row == {
        "spotify_track_id": "t1",
        "track_name": "Song",
        "artist_name": "A, B",
        "album_name": "Alb",
        "album_sid": "alb1",
        "added_at": "2024-03-01T00:00:00Z",
    }


def test_get_saved_tracks_skips_items_without_track_id(monkeypatch):
    items = [{"added_at": "2024-03-01T00:00:00Z", "track": {"id": None}}, _item("t2", "2024-02-01T00:00:00Z")]
    client, _ = _client(monkeypatch, [{"items": items, "next": None}])

    rows = client.get_saved_tracks()

    assert [r["spotify_track_id"] for r in rows] == ["t2"]


def test_run_saved_tracks_sync_rejects_unknown_mode():
    # validated before session_factory/spotify_user are touched, so None is safe
    with pytest.raises(ValueError):
        run_saved_tracks_sync(None, None, mode="bogus")
