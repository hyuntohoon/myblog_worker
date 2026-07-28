"""Regression tests for BUG-artist-image-backfill Steps 1+2."""
import json
from unittest.mock import patch

import pytest

import worker.service.artist_enrich_service as artist_enrich
import worker.service.sync_service as sync_service
from worker.handler import lambda_handler


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _EnrichSession:
    """Write-only session double for `enrich_artists` (one per chunk)."""

    def __init__(self, factory):
        self.factory = factory
        self.closed = False
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.closed = True
        return False

    def execute(self, statement, params=None):
        self.factory.write_rows.append([dict(row) for row in params])
        return _Result([])

    def commit(self):
        self.commits += 1


class _EnrichSessionFactory:
    def __init__(self):
        self.sessions = []
        self.write_rows = []

    def __call__(self):
        session = _EnrichSession(self)
        self.sessions.append(session)
        return session


def _artist_detail(sid: str, *, has_image: bool = True) -> dict:
    return {
        "id": sid,
        "images": [{"url": f"https://example.com/{sid}.jpg"}] if has_image else [],
        "genres": ["test genre"],
        "followers": {"total": 42},
        "popularity": 7,
    }


@pytest.mark.unit
def test_enrich_artists_writes_empty_photo_sentinel(monkeypatch):
    factory = _EnrichSessionFactory()
    monkeypatch.setattr(
        artist_enrich.spotify,
        "get_artists_batch",
        lambda ids: [_artist_detail(ids[0], has_image=False)],
    )

    written = artist_enrich.enrich_artists(factory, ["artist-1"])

    assert written == 1
    rows = factory.write_rows[-1]
    assert rows[0]["photo"] == ""
    assert rows[0]["photo"] is not None
    # FIX-worker-txn-across-http: the write owns a fresh short session that is
    # committed and closed — it is not the caller's open transaction.
    assert [session.commits for session in factory.sessions] == [1]
    assert all(session.closed for session in factory.sessions)


@pytest.mark.unit
def test_sync_enrich_select_excludes_empty_photo_sentinel():
    """'' is the durable "Spotify has no image" sentinel — only NULL rows stay
    eligible, so the enrich SELECT must filter on IS NULL, never on = ''.

    Asserted against the statement itself; the end-to-end behaviour (a ''-photo
    artist is not re-fetched) is covered on a real engine in
    tests/test_sync_txn_boundary.py.
    """
    select_sql = str(sync_service._MISSING_PHOTO_SQL)

    assert "photo_url IS NULL" in select_sql
    assert "photo_url = ''" not in select_sql


class _Session:
    def __init__(self, factory, index):
        self.factory = factory
        self.index = index
        self.closed = False
        self.commits = 0
        self.executions = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.closed = True
        return False

    def execute(self, statement, params=None):
        self.executions.append((str(statement), params))
        if self.index == 0:
            return _Result([(sid,) for sid in self.factory.selected_ids])
        self.factory.write_rows.append([dict(row) for row in params])
        return _Result([])

    def commit(self):
        self.commits += 1


class _SessionFactory:
    def __init__(self, selected_ids):
        self.selected_ids = selected_ids
        self.sessions = []
        self.write_rows = []

    def __call__(self):
        session = _Session(self, len(self.sessions))
        self.sessions.append(session)
        return session


@pytest.mark.unit
def test_backfill_closes_select_session_and_uses_fresh_sorted_write_sessions(monkeypatch):
    selected_ids = [f"artist-{i:03d}" for i in range(101)]
    factory = _SessionFactory(selected_ids)
    spotify_calls = []

    def get_artists(ids):
        assert factory.sessions[0].closed
        assert all(session.closed for session in factory.sessions)
        spotify_calls.append(list(ids))
        return [
            _artist_detail(sid, has_image=(offset != 0))
            for offset, sid in enumerate(reversed(ids))
        ]

    monkeypatch.setattr(artist_enrich.spotify, "get_artists_batch", get_artists)

    metrics = artist_enrich.run_artist_photo_backfill(factory, limit=101)

    assert metrics == {
        "selected": 101,
        "enriched": 101,
        "sentinel_written": 3,
        "errors": 0,
    }
    assert len(spotify_calls) == 3
    assert len(factory.sessions) == 4
    assert all(session.closed for session in factory.sessions)
    assert all(session.commits == 1 for session in factory.sessions)
    assert len(factory.write_rows) == 3
    assert all(
        [row["sid"] for row in rows] == sorted(row["sid"] for row in rows)
        for rows in factory.write_rows
    )
    select_sql, select_params = factory.sessions[0].executions[0]
    assert "WHERE photo_url IS NULL" in select_sql
    assert "ORDER BY spotify_id" in select_sql
    assert "LIMIT :limit" in select_sql
    assert select_params == {"limit": 101}


@pytest.mark.unit
def test_backfill_isolates_failed_chunk_and_processes_later_chunks(monkeypatch):
    selected_ids = [f"artist-{i:03d}" for i in range(101)]
    factory = _SessionFactory(selected_ids)
    spotify_calls = []

    def get_artists(ids):
        spotify_calls.append(list(ids))
        if len(spotify_calls) == 2:
            raise RuntimeError("Spotify chunk failed")
        return [_artist_detail(sid, has_image=False) for sid in reversed(ids)]

    monkeypatch.setattr(artist_enrich.spotify, "get_artists_batch", get_artists)

    metrics = artist_enrich.run_artist_photo_backfill(factory)

    assert len(spotify_calls) == 3
    assert spotify_calls[-1] == ["artist-100"]
    assert metrics == {
        "selected": 101,
        "enriched": 51,
        "sentinel_written": 51,
        "errors": 1,
    }
    assert len(factory.sessions) == 3
    assert [row["sid"] for row in factory.write_rows[-1]] == ["artist-100"]


@pytest.mark.unit
@patch("worker.handler._run_artist_photo_backfill")
def test_eventbridge_artist_photo_backfill_routes_before_alias(mock_run):
    result = lambda_handler({"job": "artist_photo_backfill", "limit": 25}, None)

    mock_run.assert_called_once_with(limit=25)
    assert result == {}


@pytest.mark.unit
@patch("worker.handler._run_artist_photo_backfill")
def test_sqs_artist_photo_backfill_routes_from_record_body(mock_run):
    event = {
        "Records": [{
            "body": json.dumps({"job": "artist_photo_backfill", "limit": 10}),
        }],
    }

    result = lambda_handler(event, None)

    mock_run.assert_called_once_with(limit=10)
    assert result == {"batchItemFailures": []}
