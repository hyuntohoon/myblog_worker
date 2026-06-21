"""FEAT-genre-artist-distribution Step 2 integration test — exercises the
saved-tracks upsert / full-reconcile prune / catalog-resolve against a REAL
SQLAlchemy engine (Neon test branch).

Why this exists ([[feedback-sa-session-lifecycle-mock-blind]]): the unit tests use
fakes and never touch real SQL, so they can't catch a broken ON CONFLICT, a wrong
``= ANY(:keep)`` array binding, or a ``CAST(... AS timestamptz)`` failure. This runs
the real service on a live engine.

spotify_saved_tracks on the test branch is an exclusive sandbox for this test (the
worker saved-tracks sync only ever runs against prod), so the full-mode prune
deleting non-test rows is harmless here; each test still cleans up its own rows.

Guarded by TEST_DB_URL; skipped when unset. Also skipped if the V24 table isn't on
the test branch yet (schema-drift guard, mirroring test_listening_sync_db.py).
"""
from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from worker.service.saved_tracks_sync_service import run_saved_tracks_sync

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
                 WHERE table_schema = 'public' AND table_name = 'spotify_saved_tracks'
                """
            )
        ).first()
        has_duration = present and conn.execute(
            text(
                """
                SELECT 1 FROM information_schema.columns
                 WHERE table_name = 'spotify_saved_tracks' AND column_name = 'duration_ms'
                """
            )
        ).first()
    if not present:
        eng.dispose()
        pytest.skip("spotify_saved_tracks not on test branch — apply migration V24 first")
    if not has_duration:
        eng.dispose()
        pytest.skip("spotify_saved_tracks.duration_ms missing — apply V26 to the test branch first")
    yield sessionmaker(bind=eng, autoflush=False, autocommit=False, future=True)
    eng.dispose()


class _Client:
    """Fake spotify_user exposing only get_saved_tracks; returns a fixed set,
    ignoring `since` (the service's mode flag drives prune, not the fake)."""

    def __init__(self, rows):
        self._rows = rows

    def get_saved_tracks(self, since=None):
        return list(self._rows)


def _row(tid, added_at, album_sid=None, name="ITEST saved song", artist="ITEST artist", album_name="ITEST album", duration_ms=None):
    return {
        "spotify_track_id": tid,
        "track_name": name,
        "artist_name": artist,
        "album_name": album_name,
        "album_sid": album_sid,
        "duration_ms": duration_ms,
        "added_at": added_at,
    }


def _seed_album(session, spotify_id) -> uuid.UUID:
    return session.execute(
        text("INSERT INTO albums (title, spotify_id) VALUES (:t, :s) RETURNING id"),
        {"t": "ITEST saved album", "s": spotify_id},
    ).first().id


def _seed_track(session, spotify_id, album_id) -> uuid.UUID:
    return session.execute(
        text("INSERT INTO tracks (album_id, title, spotify_id) VALUES (:a, :t, :s) RETURNING id"),
        {"a": album_id, "t": "ITEST saved track", "s": spotify_id},
    ).first().id


def _present(factory, tids):
    with factory() as s:
        return {
            r.spotify_track_id
            for r in s.execute(
                text("SELECT spotify_track_id FROM spotify_saved_tracks WHERE spotify_track_id = ANY(:ids)"),
                {"ids": tids},
            ).fetchall()
        }


def _cleanup(factory, tids, track_id=None, album_id=None):
    with factory() as s:
        with s.begin():
            s.execute(
                text("DELETE FROM spotify_saved_tracks WHERE spotify_track_id = ANY(:ids)"),
                {"ids": tids},
            )
            if track_id is not None:
                s.execute(text("DELETE FROM tracks WHERE id = :t"), {"t": track_id})
            if album_id is not None:
                s.execute(text("DELETE FROM albums WHERE id = :a"), {"a": album_id})


def test_full_sync_upserts_and_resolves_catalog(factory):
    run = uuid.uuid4().hex[:8]
    alb_sid = f"itest_st_alb_{run}"
    tid_known = f"itest_st_k_{run}"
    tid_unknown = f"itest_st_u_{run}"
    with factory() as s:
        with s.begin():
            album_id = _seed_album(s, alb_sid)
            track_id = _seed_track(s, tid_known, album_id)
    try:
        client = _Client([
            _row(tid_known, "2026-06-01T10:00:00Z", album_sid=alb_sid, duration_ms=234000),
            _row(tid_unknown, "2026-06-01T09:00:00Z", album_sid="itest_st_noncatalog"),
        ])
        res = run_saved_tracks_sync(factory, client, mode="full")
        assert res["upserted"] == 2

        with factory() as s:
            rows = s.execute(
                text(
                    "SELECT spotify_track_id, track_id, album_id, artist_name, added_at, duration_ms "
                    "FROM spotify_saved_tracks WHERE spotify_track_id = ANY(:ids)"
                ),
                {"ids": [tid_known, tid_unknown]},
            ).fetchall()
        by = {r.spotify_track_id: r for r in rows}
        assert by[tid_known].track_id == track_id   # catalog track → resolved
        assert by[tid_known].album_id == album_id    # catalog album → resolved
        assert by[tid_known].duration_ms == 234000   # length written from /me/tracks
        assert by[tid_unknown].track_id is None       # not in catalog → NULL, still cached
        assert by[tid_unknown].album_id is None
        assert by[tid_unknown].duration_ms is None    # row without duration → NULL
        assert by[tid_known].added_at is not None     # CAST(... AS timestamptz) succeeded
    finally:
        _cleanup(factory, [tid_known, tid_unknown], track_id, album_id)


def test_full_reconcile_prunes_unliked(factory):
    run = uuid.uuid4().hex[:8]
    tid_keep = f"itest_st_keep_{run}"
    tid_drop = f"itest_st_drop_{run}"
    try:
        run_saved_tracks_sync(factory, _Client([
            _row(tid_keep, "2026-06-01T10:00:00Z"),
            _row(tid_drop, "2026-06-01T09:00:00Z"),
        ]), mode="full")
        assert _present(factory, [tid_keep, tid_drop]) == {tid_keep, tid_drop}

        # next full set drops tid_drop → prune removes it
        run_saved_tracks_sync(factory, _Client([_row(tid_keep, "2026-06-01T10:00:00Z")]), mode="full")
        present = _present(factory, [tid_keep, tid_drop])
        assert tid_keep in present
        assert tid_drop not in present
    finally:
        _cleanup(factory, [tid_keep, tid_drop])


def test_incremental_upserts_without_pruning(factory):
    run = uuid.uuid4().hex[:8]
    tid_old = f"itest_st_old_{run}"
    tid_new = f"itest_st_new_{run}"
    try:
        run_saved_tracks_sync(factory, _Client([_row(tid_old, "2026-06-01T10:00:00Z")]), mode="full")
        # incremental returns only a newer item; the older row must SURVIVE (no prune)
        run_saved_tracks_sync(factory, _Client([_row(tid_new, "2026-06-02T10:00:00Z")]), mode="incremental")
        present = _present(factory, [tid_old, tid_new])
        assert tid_old in present   # incremental never prunes
        assert tid_new in present
    finally:
        _cleanup(factory, [tid_old, tid_new])
