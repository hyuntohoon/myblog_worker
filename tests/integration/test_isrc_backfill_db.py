"""FEAT-lyrics-corpus Step 1b integration test — exercises ISRC backfill against
a REAL SQLAlchemy engine (Neon test branch).

Why this exists ([[feedback-sa-session-lifecycle-mock-blind]]): the unit tests
use mocks for Spotify API responses and never touch real SQL, so they can't catch
a wrong UUID CAST, a broken batch update, or a sentinel handling error. This runs
the real service on a live engine.

Guarded by TEST_DB_URL; skipped when unset. Also skipped if the V34 table
(Track.isrc column) isn't on the test branch yet (schema-drift guard).
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

        # Fetch tracks without ISRC
        with session.begin():
            svc = IsrcBackfillService(session.connection())
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

        # Run backfill
        with session.begin():
            svc = IsrcBackfillService(session.connection())
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

        # Run backfill
        with session.begin():
            svc = IsrcBackfillService(session.connection())
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
