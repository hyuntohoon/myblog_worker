"""FEAT-member-dashboard Step 3 integration test — exercises the recently-played
upsert/prune against a real SQLAlchemy engine.

Why this exists ([[feedback-sa-session-lifecycle-mock-blind]]): the unit tests in
tests/test_listening_sync.py use a fake session and never touch real SQL, so they
can't catch a broken `ON CONFLICT`, a wrong `= ANY(:keep)` array binding, or a
`CAST(... AS timestamptz)` failure. This runs the real service on a live engine.

Guarded by TEST_DB_URL (Neon test branch); skipped when unset
([[feedback-local-db-smoke-fallback]]). Also skipped if the V9 tables aren't on the
test branch yet (schema-drift guard, mirroring test_alias_fill_session_lifecycle).
"""
from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from worker.service.listening_sync_service import (
    run_listening_sync,
    sync_now_playing,
    sync_recent_albums,
)

_TEST_DB_URL = os.environ.get("TEST_DB_URL")

pytestmark = pytest.mark.skipif(
    not _TEST_DB_URL,
    reason="integration test requires TEST_DB_URL env var (Neon test branch)",
)


@pytest.fixture(scope="module")
def factory():
    eng = create_engine(_TEST_DB_URL, pool_pre_ping=True, future=True)
    with eng.connect() as conn:
        present = conn.execute(
            text(
                """
                SELECT 1 FROM information_schema.tables
                 WHERE table_schema = 'public' AND table_name = 'spotify_recent_albums'
                """
            )
        ).first()
    if not present:
        eng.dispose()
        pytest.skip("spotify_recent_albums not on test branch — apply migration V9 first")
    yield sessionmaker(bind=eng, autoflush=False, autocommit=False, future=True)
    eng.dispose()


class _Client:
    def __init__(self, recent, now=None):
        self._recent = recent
        self._now = now

    def get_recently_played(self, limit=50):
        return self._recent

    def get_currently_playing(self):
        return self._now


def _seed_album(session, spotify_id) -> uuid.UUID:
    row = session.execute(
        text(
            "INSERT INTO albums (title, spotify_id) VALUES (:t, :s) RETURNING id"
        ),
        {"t": "ITEST listening album", "s": spotify_id},
    ).first()
    return row.id


def test_recent_upsert_then_prune_to_window(factory):
    sid = f"itest_recent_{uuid.uuid4().hex[:8]}"
    # seed one catalog album in its own committed tx
    with factory() as s:
        with s.begin():
            album_id = _seed_album(s, sid)
    try:
        client = _Client(recent=[{"track": {"album": {"id": sid}}, "played_at": "2026-06-04T10:00:00Z"}])
        res = sync_recent_albums(factory, client)
        assert res["known"] == 1

        with factory() as s:
            row = s.execute(
                text("SELECT last_played_at, source FROM spotify_recent_albums WHERE album_id = :a"),
                {"a": album_id},
            ).first()
        assert row is not None
        assert row.source == "spotify"

        # next window no longer contains our album → it must be pruned out
        sync_recent_albums(factory, _Client(recent=[]))  # empty window leaves cache as-is
        # a window with only an unknown album prunes everything (incl. ours)
        sync_recent_albums(factory, _Client(recent=[{"track": {"album": {"id": "itest_unknown_xyz"}}, "played_at": "2026-06-04T11:00:00Z"}]))
        with factory() as s:
            gone = s.execute(
                text("SELECT 1 FROM spotify_recent_albums WHERE album_id = :a"), {"a": album_id}
            ).first()
        assert gone is None
    finally:
        with factory() as s:
            with s.begin():
                s.execute(text("DELETE FROM spotify_recent_albums WHERE album_id = :a"), {"a": album_id})
                s.execute(text("DELETE FROM albums WHERE id = :a"), {"a": album_id})


def test_now_playing_singleton_upsert(factory):
    sync_now_playing(factory, _Client(recent=[], now=None))
    with factory() as s:
        rows = s.execute(text("SELECT id, is_playing FROM spotify_now_playing")).fetchall()
    assert len(rows) == 1
    assert rows[0].id == 1


class _BoomClient:
    """Any Spotify read is a failure — proves the debounce returned before syncing."""

    def get_recently_played(self, limit=50):
        raise AssertionError("Spotify read despite debounce")

    def get_currently_playing(self):
        raise AssertionError("Spotify read despite debounce")


def test_manual_refresh_debounced_when_cache_fresh(factory):
    """D31: the debounce age query (GREATEST + EXTRACT EPOCH over both cache tables)
    runs on real Postgres, and a fresh cache short-circuits a manual refresh before
    any Spotify read."""
    sid = f"itest_debounce_{uuid.uuid4().hex[:8]}"
    with factory() as s:
        with s.begin():
            album_id = _seed_album(s, sid)
            s.execute(
                text(
                    "INSERT INTO spotify_recent_albums (album_id, last_played_at, source, synced_at) "
                    "VALUES (:a, now(), 'spotify', now())"  # fresh
                ),
                {"a": album_id},
            )
    try:
        res = run_listening_sync(factory, _BoomClient(), is_manual_refresh=True)
        assert res == {"skipped": "debounced"}
    finally:
        with factory() as s:
            with s.begin():
                s.execute(text("DELETE FROM spotify_recent_albums WHERE album_id = :a"), {"a": album_id})
                s.execute(text("DELETE FROM albums WHERE id = :a"), {"a": album_id})


def test_manual_refresh_allowed_when_caches_stale(factory):
    """D31: when every cache write is older than the window, a manual refresh runs.
    Both tables are forced stale because the debounce spans GREATEST(recent, now-playing).
    Empty window → the sync is a no-op that still returns a normal (non-skipped) result."""
    sid = f"itest_stale_{uuid.uuid4().hex[:8]}"
    with factory() as s:
        with s.begin():
            album_id = _seed_album(s, sid)
            s.execute(
                text(
                    "INSERT INTO spotify_recent_albums (album_id, last_played_at, source, synced_at) "
                    "VALUES (:a, now(), 'spotify', now() - interval '5 minutes')"
                ),
                {"a": album_id},
            )
            s.execute(
                text(
                    "INSERT INTO spotify_now_playing (id, is_playing, updated_at) "
                    "VALUES (1, false, now() - interval '5 minutes') "
                    "ON CONFLICT (id) DO UPDATE SET updated_at = now() - interval '5 minutes'"
                )
            )
    try:
        res = run_listening_sync(factory, _Client(recent=[], now=None), is_manual_refresh=True)
        assert "skipped" not in res
        assert res["recent"] == {"known": 0, "unknown": 0, "pruned": 0}
    finally:
        with factory() as s:
            with s.begin():
                s.execute(text("DELETE FROM spotify_recent_albums WHERE album_id = :a"), {"a": album_id})
                s.execute(text("DELETE FROM albums WHERE id = :a"), {"a": album_id})
