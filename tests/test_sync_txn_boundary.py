"""FIX-worker-txn-across-http — transaction-boundary regression tests.

Why a REAL engine ([[feedback-sa-session-lifecycle-mock-blind]]): the existing
unit coverage for this path drives `AlbumSyncService` with a hand-rolled
`_RecordingConnection` double. A fake connection has no transaction semantics,
so it happily "passed" while the service held an open transaction — with
`ON CONFLICT DO UPDATE` row locks on artists/albums/tracks — across the
Spotify enrich loop. Mocks cannot see the bug this file exists to prevent.

`tests/test_sync_service.py` does use a real engine, but only via the Neon test
branch (`TEST_DB_URL`), so it is skipped on any checkout without that env var.
This file uses an in-memory SQLite engine so the boundary assertion runs
EVERYWHERE — locally and in CI — with no external dependency.

What is asserted: at the moment each outbound Spotify call is made, the engine
has **zero** open transactions. The tracker hooks SQLAlchemy's own
begin/commit/rollback events, so it observes real transaction state rather than
anything the service reports about itself.
"""
from __future__ import annotations

import json

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import worker.service.artist_enrich_service as artist_enrich
import worker.service.sync_service as sync_service
from worker.service.sync_service import AlbumSyncService

SCHEMA = [
    """CREATE TABLE artists (
        id INTEGER PRIMARY KEY, spotify_id TEXT UNIQUE, name TEXT,
        photo_url TEXT, genres TEXT, followers INTEGER, popularity INTEGER
    )""",
    """CREATE TABLE albums (
        id INTEGER PRIMARY KEY, spotify_id TEXT UNIQUE, title TEXT,
        release_date TEXT, cover_url TEXT, album_type TEXT, total_tracks INTEGER,
        label TEXT, popularity INTEGER, ext_refs TEXT
    )""",
    """CREATE TABLE tracks (
        id INTEGER PRIMARY KEY, spotify_id TEXT UNIQUE, album_id INTEGER,
        title TEXT, track_no INTEGER, duration_sec INTEGER
    )""",
    "CREATE TABLE album_artists (album_id INTEGER, artist_id INTEGER, PRIMARY KEY (album_id, artist_id))",
    "CREATE TABLE track_artists (track_id INTEGER, artist_id INTEGER, PRIMARY KEY (track_id, artist_id))",
    "CREATE TABLE genres (id INTEGER PRIMARY KEY, slug TEXT UNIQUE)",
    """CREATE TABLE album_genres (
        album_id INTEGER, genre_id INTEGER, source TEXT, confidence TEXT,
        PRIMARY KEY (album_id, genre_id)
    )""",
]


def _install_jsonb_shims(dbapi_conn, _record):
    """SQLite lacks Postgres' jsonb builders used by the albums upsert.

    Test-harness only — the production statement is unchanged and still runs as
    real jsonb on Neon (covered by tests/test_sync_service.py in CI).
    """
    def jsonb_build_object(*args):
        return json.dumps({args[i]: args[i + 1] for i in range(0, len(args), 2)})

    def jsonb_strip_nulls(doc):
        return json.dumps({k: v for k, v in json.loads(doc).items() if v is not None})

    dbapi_conn.create_function("jsonb_build_object", -1, jsonb_build_object)
    dbapi_conn.create_function("jsonb_strip_nulls", 1, jsonb_strip_nulls)


class _TxnTracker:
    """Counts open transactions on a real engine via SQLAlchemy's own events."""

    def __init__(self, engine):
        self.depth = 0
        self.observed_at_http = []
        event.listen(engine, "begin", self._begin)
        event.listen(engine, "commit", self._end)
        event.listen(engine, "rollback", self._end)

    def _begin(self, _conn):
        self.depth += 1

    def _end(self, _conn):
        self.depth -= 1

    def record_http_call(self, label):
        self.observed_at_http.append((label, self.depth))


@pytest.fixture
def engine():
    eng = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    event.listen(eng, "connect", _install_jsonb_shims)
    with eng.connect() as conn:
        for ddl in SCHEMA:
            conn.execute(text(ddl))
        conn.commit()
    yield eng
    eng.dispose()


@pytest.fixture
def session_factory(engine):
    return sessionmaker(bind=engine, autoflush=False, future=True)


@pytest.fixture
def tracker(engine):
    return _TxnTracker(engine)


def _album_payload():
    return {
        "id": "album-1",
        "name": "Boundary",
        "artists": [{"id": "artist-b", "name": "B"}, {"id": "artist-a", "name": "A"}],
        "images": [{"url": "https://example.com/cover.jpg"}],
        "release_date": "2026-01-01",
        "album_type": "album",
        "total_tracks": 2,
        "label": "Label",
        "popularity": 10,
        "external_urls": {"spotify": "https://open.spotify.com/album/album-1"},
        "external_ids": {"upc": "00000000"},
        "tracks": {
            "items": [
                {
                    "id": "track-2", "name": "Second", "track_number": 2,
                    "duration_ms": 200000,
                    "artists": [{"id": "artist-b", "name": "B"}],
                },
                {
                    "id": "track-1", "name": "First", "track_number": 1,
                    "duration_ms": 100000,
                    "artists": [{"id": "artist-a", "name": "A"}],
                },
            ]
        },
    }


def _artist_detail(sid: str):
    return {
        "id": sid,
        "images": [{"url": f"https://example.com/{sid}.jpg"}],
        "genres": ["pop"],
        "followers": {"total": 5},
        "popularity": 3,
    }


@pytest.mark.unit
def test_no_transaction_is_open_during_any_spotify_call(
    monkeypatch, engine, session_factory, tracker
):
    """The regression guard: every outbound Spotify call must see depth 0.

    Before the fix the handler opened `session.begin()` and passed
    `session.connection()` in, so both calls below ran at depth 1 — a Neon
    connection parked idle-in-transaction, holding row locks, across HTTP.
    """
    monkeypatch.setattr(
        sync_service.spotify,
        "get_albums",
        lambda ids, market: (tracker.record_http_call("get_albums"), [_album_payload()])[1],
    )
    monkeypatch.setattr(
        artist_enrich.spotify,
        "get_artists_batch",
        lambda ids: (
            tracker.record_http_call("get_artists_batch"),
            [_artist_detail(sid) for sid in ids],
        )[1],
    )

    AlbumSyncService(session_factory).sync_albums_batch(["album-1"], "KR")

    labels = [label for label, _ in tracker.observed_at_http]
    assert labels == ["get_albums", "get_artists_batch"], (
        "both Spotify calls should have fired"
    )
    assert all(depth == 0 for _, depth in tracker.observed_at_http), (
        f"a DB transaction was open during a Spotify call: {tracker.observed_at_http}"
    )
    # The batch really did commit — otherwise depth 0 would be trivially true.
    assert tracker.depth == 0, "no transaction may be left open after the batch"
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM albums")).scalar() == 1
        assert conn.execute(text("SELECT count(*) FROM tracks")).scalar() == 2
        photos = conn.execute(
            text("SELECT spotify_id, photo_url FROM artists ORDER BY spotify_id")
        ).fetchall()
    assert [r[0] for r in photos] == ["artist-a", "artist-b"]
    assert all(r[1] for r in photos), "enrich should have written photo_url for both"


@pytest.mark.unit
def test_bulk_inserts_are_sorted_by_conflict_key(monkeypatch, session_factory, tracker):
    """audit §9 C-9: every bulk insert takes row locks in a stable order.

    The album payload deliberately lists artists and tracks out of order, so an
    unsorted parameter list would surface here.
    """
    seen: dict[str, list] = {}

    def _capture(conn, cursor, statement, parameters, context, executemany):
        if not executemany:
            return
        for table in ("artists", "albums ", "album_artists", "tracks ", "track_artists"):
            if f"INSERT INTO {table}" in statement:
                seen[table.strip()] = list(parameters)

    event.listen(session_factory.kw["bind"], "before_cursor_execute", _capture)

    monkeypatch.setattr(
        sync_service.spotify, "get_albums", lambda ids, market: [_album_payload()]
    )
    monkeypatch.setattr(
        artist_enrich.spotify,
        "get_artists_batch",
        lambda ids: [_artist_detail(sid) for sid in ids],
    )

    AlbumSyncService(session_factory).sync_albums_batch(["album-1"], "KR")

    def _assert_sorted(table, key):
        rows = seen.get(table)
        assert rows, f"expected an executemany INSERT for {table}"
        keys = [key(r) for r in rows]
        assert keys == sorted(keys), f"{table} inserted unsorted: {keys}"

    _assert_sorted("artists", lambda r: r[0])
    _assert_sorted("album_artists", lambda r: (r[0], r[1]))
    _assert_sorted("track_artists", lambda r: (r[0], r[1]))


@pytest.mark.unit
def test_empty_photo_sentinel_is_not_re_enriched(monkeypatch, engine, session_factory):
    """'' means "Spotify has no image for this artist" and is durable: only NULL
    rows stay eligible. Real-engine counterpart to the SQL-level assertion in
    tests/test_artist_enrich.py."""
    with engine.connect() as conn:
        conn.execute(
            text("INSERT INTO artists (spotify_id, name, photo_url) VALUES ('artist-a', 'A', '')")
        )
        conn.commit()

    asked_for = []

    def _get_artists(ids):
        asked_for.extend(ids)
        return [_artist_detail(sid) for sid in ids]

    monkeypatch.setattr(
        sync_service.spotify, "get_albums", lambda ids, market: [_album_payload()]
    )
    monkeypatch.setattr(artist_enrich.spotify, "get_artists_batch", _get_artists)

    AlbumSyncService(session_factory).sync_albums_batch(["album-1"], "KR")

    assert asked_for == ["artist-b"], (
        "the ''-sentinel artist must not be re-fetched from Spotify"
    )
    with engine.connect() as conn:
        photo = conn.execute(
            text("SELECT photo_url FROM artists WHERE spotify_id = 'artist-a'")
        ).scalar()
    assert photo == "", "the sentinel must survive the sync"


@pytest.mark.unit
def test_enrich_failure_does_not_roll_back_the_album_catalog(
    monkeypatch, engine, session_factory
):
    """Failure isolation: the catalog is committed before enrich runs, so a
    Spotify artist-image outage leaves the albums/tracks in place. The rows keep
    photo_url NULL, so the weekly backfill sweep still picks them up."""
    monkeypatch.setattr(
        sync_service.spotify, "get_albums", lambda ids, market: [_album_payload()]
    )

    def _boom(ids):
        raise RuntimeError("Spotify 403 during enrich")

    monkeypatch.setattr(artist_enrich.spotify, "get_artists_batch", _boom)

    AlbumSyncService(session_factory).sync_albums_batch(["album-1"], "KR")

    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM albums")).scalar() == 1
        assert conn.execute(text("SELECT count(*) FROM tracks")).scalar() == 2
        nulls = conn.execute(
            text("SELECT count(*) FROM artists WHERE photo_url IS NULL")
        ).scalar()
    assert nulls == 2, "un-enriched artists must stay eligible for the backfill sweep"
