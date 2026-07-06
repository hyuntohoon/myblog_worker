"""FEAT-lyrics-corpus Step 1b integration test — exercises ISRC backfill against
a REAL SQLAlchemy engine (Neon test branch).

Why this exists ([[feedback-sa-session-lifecycle-mock-blind]]): the unit tests
use mocks for Spotify API responses and never touch real SQL, so they can't catch
a wrong UUID CAST, a broken batch update, or a sentinel handling error. This runs
the real service on a live engine.

Guarded by TEST_DB_URL; skipped when unset. Also skipped if the V34 table
(Track.isrc column) isn't on the test branch yet (schema-drift guard).

`test_isrc_backfill_txn_boundary_and_batch_isolation` is the FIX-bug-audit-2026-07
WS-C H2 regression: it drives the service exactly as the handler now does (a session,
no outer ``session.begin()``) and proves (a) a completed run does NOT raise
``InvalidRequestError`` from a desynced transaction, and (b) a failing batch is rolled
back so a LATER batch still commits (no ``InFailedSqlTransaction`` cascade). A mock
unit can't see either — both are pool/transaction semantics.
"""
from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from unittest.mock import Mock, patch

from worker.service.isrc_backfill_service import IsrcBackfillService

_TEST_DB_URL = os.environ.get("TEST_DB_URL")

pytestmark = pytest.mark.skipif(
    not _TEST_DB_URL,
    reason="integration test requires TEST_DB_URL env var (Neon test branch)",
)


@pytest.fixture(scope="module")
def factory():
    eng = create_engine(_TEST_DB_URL, pool_pre_ping=True, future=True)
    with eng.connect() as conn:
        # Check if Track.isrc column exists (V34)
        has_isrc = conn.execute(
            text(
                """
                SELECT 1 FROM information_schema.columns
                 WHERE table_schema = 'public' AND table_name = 'tracks'
                   AND column_name = 'isrc'
                """
            )
        ).first()
        if not has_isrc:
            pytest.skip("V34 (Track.isrc) not deployed to test branch yet")
    return sessionmaker(bind=eng)


@pytest.fixture
def session_factory(factory):
    """Session factory for test."""
    return factory


def test_isrc_backfill_fetches_tracks_without_isrc(session_factory):
    """Service fetches only tracks with NULL isrc."""
    Session = session_factory
    session = Session()
    try:
        # Create a test album and two test tracks
        album_id = str(uuid.uuid4())
        track1_id = str(uuid.uuid4())
        track2_id = str(uuid.uuid4())
        track1_spotify_id = f"test_track_1_{uuid.uuid4()}"
        track2_spotify_id = f"test_track_2_{uuid.uuid4()}"

        with session.begin():
            # Insert album
            session.execute(
                text(
                    """
                    INSERT INTO albums (id, spotify_id, title)
                    VALUES (:id, :sid, 'Test Album')
                    ON CONFLICT (spotify_id) DO NOTHING
                    """
                ),
                {"id": album_id, "sid": f"test_album_{uuid.uuid4()}"},
            )

            # Insert track 1 without ISRC
            session.execute(
                text(
                    """
                    INSERT INTO tracks (id, album_id, spotify_id, title)
                    VALUES (:id, :alb_id, :sid, 'Test Track 1')
                    ON CONFLICT (spotify_id) DO NOTHING
                    """
                ),
                {
                    "id": track1_id,
                    "alb_id": album_id,
                    "sid": track1_spotify_id,
                },
            )

            # Insert track 2 WITH ISRC (should be skipped)
            session.execute(
                text(
                    """
                    INSERT INTO tracks (id, album_id, spotify_id, title, isrc)
                    VALUES (:id, :alb_id, :sid, 'Test Track 2', 'USRC12345678')
                    ON CONFLICT (spotify_id) DO NOTHING
                    """
                ),
                {
                    "id": track2_id,
                    "alb_id": album_id,
                    "sid": track2_spotify_id,
                },
            )

        # Fetch tracks without ISRC (service now owns the session, not a raw conn)
        svc = IsrcBackfillService(session)
        tracks = svc._fetch_tracks_without_isrc(limit=100)

        # Should get only track1 (track2 has ISRC already)
        track_ids = [t["id"] for t in tracks]
        assert track1_id in [t["id"] for t in tracks], f"Expected {track1_id} in {tracks}"

    finally:
        # Cleanup
        with session.begin():
            session.execute(
                text("DELETE FROM tracks WHERE spotify_id LIKE :pat"),
                {"pat": "test_track_%"},
            )
            session.execute(
                text("DELETE FROM albums WHERE spotify_id LIKE :pat"),
                {"pat": "test_album_%"},
            )
        session.close()


@patch("worker.service.isrc_backfill_service.spotify")
def test_isrc_backfill_writes_isrc(mock_spotify, session_factory):
    """Service writes ISRC from Spotify response."""
    Session = session_factory
    session = Session()
    try:
        # Create test track
        album_id = str(uuid.uuid4())
        track_id = str(uuid.uuid4())
        track_spotify_id = f"test_track_{uuid.uuid4()}"

        with session.begin():
            session.execute(
                text(
                    """
                    INSERT INTO albums (id, spotify_id, title)
                    VALUES (:id, :sid, 'Test Album')
                    ON CONFLICT (spotify_id) DO NOTHING
                    """
                ),
                {"id": album_id, "sid": f"test_album_{uuid.uuid4()}"},
            )
            session.execute(
                text(
                    """
                    INSERT INTO tracks (id, album_id, spotify_id, title)
                    VALUES (:id, :alb_id, :sid, 'Test Track')
                    ON CONFLICT (spotify_id) DO NOTHING
                    """
                ),
                {
                    "id": track_id,
                    "alb_id": album_id,
                    "sid": track_spotify_id,
                },
            )

        # Mock Spotify response
        mock_spotify.get_tracks.return_value = [
            {
                "id": track_spotify_id,
                "name": "Test Track",
                "external_ids": {"isrc": "USRC12345678"},
            }
        ]

        # Run backfill (service commits per batch via the session — no outer begin)
        svc = IsrcBackfillService(session)
        metrics = svc.backfill_isrc(limit=100)

        # Verify ISRC was written
        with session.begin():
            result = session.execute(
                text("SELECT isrc FROM tracks WHERE id = CAST(:id AS UUID)"),
                {"id": track_id},
            ).first()
            assert result[0] == "USRC12345678"

        assert metrics["matched"] >= 1

    finally:
        # Cleanup
        with session.begin():
            session.execute(
                text("DELETE FROM tracks WHERE spotify_id LIKE :pat"),
                {"pat": "test_track_%"},
            )
            session.execute(
                text("DELETE FROM albums WHERE spotify_id LIKE :pat"),
                {"pat": "test_album_%"},
            )
        session.close()


@patch("worker.service.isrc_backfill_service.spotify")
def test_isrc_backfill_writes_sentinel_on_miss(mock_spotify, session_factory):
    """Service writes sentinel when Spotify returns no ISRC."""
    Session = session_factory
    session = Session()
    try:
        # Create test track
        album_id = str(uuid.uuid4())
        track_id = str(uuid.uuid4())
        track_spotify_id = f"test_track_{uuid.uuid4()}"

        with session.begin():
            session.execute(
                text(
                    """
                    INSERT INTO albums (id, spotify_id, title)
                    VALUES (:id, :sid, 'Test Album')
                    ON CONFLICT (spotify_id) DO NOTHING
                    """
                ),
                {"id": album_id, "sid": f"test_album_{uuid.uuid4()}"},
            )
            session.execute(
                text(
                    """
                    INSERT INTO tracks (id, album_id, spotify_id, title)
                    VALUES (:id, :alb_id, :sid, 'Test Track')
                    ON CONFLICT (spotify_id) DO NOTHING
                    """
                ),
                {
                    "id": track_id,
                    "alb_id": album_id,
                    "sid": track_spotify_id,
                },
            )

        # Mock Spotify response with no ISRC
        mock_spotify.get_tracks.return_value = [
            {
                "id": track_spotify_id,
                "name": "Test Track",
                "external_ids": {},  # No ISRC
            }
        ]

        # Run backfill (service commits per batch via the session — no outer begin)
        svc = IsrcBackfillService(session)
        metrics = svc.backfill_isrc(limit=100)

        # Verify sentinel was written
        with session.begin():
            result = session.execute(
                text("SELECT isrc FROM tracks WHERE id = CAST(:id AS UUID)"),
                {"id": track_id},
            ).first()
            assert result[0] == "no_isrc"

        assert metrics["sentinel_written"] >= 1

    finally:
        # Cleanup
        with session.begin():
            session.execute(
                text("DELETE FROM tracks WHERE spotify_id LIKE :pat"),
                {"pat": "test_track_%"},
            )
            session.execute(
                text("DELETE FROM albums WHERE spotify_id LIKE :pat"),
                {"pat": "test_album_%"},
            )
        session.close()


def test_isrc_backfill_txn_boundary_and_batch_isolation(session_factory):
    """H2 regression — txn boundary + per-batch failure isolation on a real engine.

    Seeds 60 NULL-isrc tracks and drives ``backfill_isrc`` through TWO 50-row batches
    (``_fetch_tracks_without_isrc`` is stubbed to return exactly the seeds so the run is
    isolated from other NULL rows on the shared branch). Spotify raises on the FIRST
    batch and returns ISRCs on the SECOND. Expected:

      - the run returns metrics WITHOUT raising ``InvalidRequestError`` (the old
        ``conn.commit()``-inside-``session.begin()`` desync),
      - batch 1 is rolled back (``errors == 1``) so batch 2 still commits
        (``matched == 10``) instead of failing with ``InFailedSqlTransaction``,
      - a FRESH session sees exactly the 10 committed ISRCs (proves the commit landed).
    """
    Session = session_factory
    prefix = f"wsc_txn_{uuid.uuid4().hex[:8]}_"
    album_sid = f"test_album_{uuid.uuid4()}"

    seeded = [(str(uuid.uuid4()), f"{prefix}{n:03d}") for n in range(60)]

    session = Session()
    try:
        with session.begin():
            session.execute(
                text(
                    """
                    INSERT INTO albums (id, spotify_id, title)
                    VALUES (:id, :sid, 'Test Album')
                    ON CONFLICT (spotify_id) DO NOTHING
                    """
                ),
                {"id": str(uuid.uuid4()), "sid": album_sid},
            )
            alb_id = session.execute(
                text("SELECT id FROM albums WHERE spotify_id = :sid"),
                {"sid": album_sid},
            ).scalar()
            session.execute(
                text(
                    """
                    INSERT INTO tracks (id, album_id, spotify_id, title)
                    VALUES (CAST(:id AS UUID), :alb_id, :sid, 'Test Track')
                    ON CONFLICT (spotify_id) DO NOTHING
                    """
                ),
                [{"id": tid, "alb_id": alb_id, "sid": sid} for tid, sid in seeded],
            )
        session.close()

        # Spotify: outage on batch 1, ISRCs on batch 2.
        calls = {"n": 0}

        def get_tracks_side_effect(ids, market=None):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("simulated Spotify outage on batch 1")
            return [
                {"id": sid, "external_ids": {"isrc": f"US{sid[-8:].upper()}"}}
                for sid in ids
            ]

        # Drive the service exactly like worker.handler._run_isrc_backfill does now:
        # a session, NO outer session.begin(). Stub the selection so the run only
        # touches our 60 seeds (deterministic 50 + 10 split).
        run_session = Session()
        with patch("worker.service.isrc_backfill_service.spotify") as mock_spotify:
            mock_spotify.get_tracks.side_effect = get_tracks_side_effect
            svc = IsrcBackfillService(run_session)
            svc._fetch_tracks_without_isrc = lambda limit: [
                {"id": tid, "spotify_id": sid} for tid, sid in seeded
            ]
            metrics = svc.backfill_isrc(limit=1000)  # must NOT raise
        run_session.close()

        assert calls["n"] == 2, "both batches should have been attempted"
        assert metrics["errors"] == 1, "batch 1 (outage) should be counted as an error"
        assert metrics["matched"] == 10, "batch 2 (10 tracks) should have committed"

        # Fresh session read-back: exactly the 10 batch-2 tracks carry an ISRC; the
        # 50 rolled-back batch-1 tracks are still NULL.
        with Session() as rb:
            non_null = rb.execute(
                text(
                    "SELECT count(*) FROM tracks "
                    "WHERE spotify_id LIKE :pat AND isrc IS NOT NULL"
                ),
                {"pat": f"{prefix}%"},
            ).scalar()
        assert non_null == 10, f"expected 10 committed ISRCs, got {non_null}"

    finally:
        with Session() as cleanup, cleanup.begin():
            cleanup.execute(
                text("DELETE FROM tracks WHERE spotify_id LIKE :pat"),
                {"pat": f"{prefix}%"},
            )
            cleanup.execute(
                text("DELETE FROM albums WHERE spotify_id = :sid"),
                {"sid": album_sid},
            )
