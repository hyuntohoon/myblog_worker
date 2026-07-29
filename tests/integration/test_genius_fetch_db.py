"""FEAT-lyrics-annotations — Genius fetch eligibility against a REAL engine.

Why this exists ([[feedback-sa-session-lifecycle-mock-blind]] + the isrc lesson):
the unit tests assert the SQL *text* (no `track_lyrics`, `album_research` present)
but never run it, so a broken bind or a bad `CAST(:album_id AS uuid)` would pass
CI and die on the first prod invocation. This runs both selection queries on the
Neon test branch:

  * research demand is the eligibility — a track with NO lyrics row is selected
    once its album has an `album_research` row, and an album without one is not
  * already-fetched tracks (`track_genius_songs` row) stay out of the pool
  * the album-scoped nudge path binds and CASTs its uuid through a real driver

Guarded by TEST_DB_URL; skipped when unset. Also skipped if V49
(`track_genius_songs`) is not on the test branch (schema-drift guard).
"""
from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from unittest.mock import MagicMock

from worker.service.genius_fetch_service import GeniusFetchService

_TEST_DB_URL = os.environ.get("TEST_DB_URL")

pytestmark = pytest.mark.skipif(
    not _TEST_DB_URL,
    reason="integration test requires TEST_DB_URL env var (Neon test branch)",
)


@pytest.fixture(scope="module")
def factory():
    eng = create_engine(_TEST_DB_URL, pool_pre_ping=True, future=True)
    with eng.connect() as conn:
        has_v49 = conn.execute(
            text(
                """
                SELECT 1 FROM information_schema.tables
                 WHERE table_schema = 'public' AND table_name = 'track_genius_songs'
                """
            )
        ).first()
        if not has_v49:
            pytest.skip("V49 (track_genius_songs) not deployed to test branch yet")
    return sessionmaker(bind=eng)


@pytest.fixture
def seeded(factory):
    """Two albums: one research-requested (one fetched + one unfetched track,
    neither with any track_lyrics row), one with no research row at all."""
    ids = {
        "album_demanded": str(uuid.uuid4()),
        "album_undemanded": str(uuid.uuid4()),
        "track_unfetched": str(uuid.uuid4()),
        "track_fetched": str(uuid.uuid4()),
        "track_undemanded": str(uuid.uuid4()),
    }
    session = factory()
    try:
        with session.begin():
            for album_key in ("album_demanded", "album_undemanded"):
                session.execute(
                    text(
                        """
                        INSERT INTO albums (id, spotify_id, title)
                        VALUES (:id, :sid, 'Genius Eligibility Test Album')
                        ON CONFLICT (spotify_id) DO NOTHING
                        """
                    ),
                    {"id": ids[album_key], "sid": f"test_album_{uuid.uuid4()}"},
                )
            for track_key, album_key in (
                ("track_unfetched", "album_demanded"),
                ("track_fetched", "album_demanded"),
                ("track_undemanded", "album_undemanded"),
            ):
                session.execute(
                    text(
                        """
                        INSERT INTO tracks (id, album_id, spotify_id, title)
                        VALUES (:id, :alb_id, :sid, 'Genius Eligibility Test Track')
                        ON CONFLICT (spotify_id) DO NOTHING
                        """
                    ),
                    {
                        "id": ids[track_key],
                        "alb_id": ids[album_key],
                        "sid": f"test_track_{uuid.uuid4()}",
                    },
                )
            session.execute(
                text(
                    """
                    INSERT INTO album_research (album_id, prompt_version, status)
                    VALUES (:alb_id, 'v2', 'queued')
                    ON CONFLICT ON CONSTRAINT uq_album_research_album_prompt DO NOTHING
                    """
                ),
                {"alb_id": ids["album_demanded"]},
            )
            session.execute(
                text(
                    """
                    INSERT INTO track_genius_songs (track_id, genius_song_id, match_status)
                    VALUES (:tid, 0, 'not_found')
                    ON CONFLICT (track_id) DO NOTHING
                    """
                ),
                {"tid": ids["track_fetched"]},
            )
        yield ids
    finally:
        session.rollback()
        with session.begin():
            # albums cascade to tracks, album_research and track_genius_songs
            session.execute(
                text("DELETE FROM albums WHERE id IN (:a, :b)"),
                {"a": ids["album_demanded"], "b": ids["album_undemanded"]},
            )
        session.close()


def test_selection_is_research_scoped_and_lyrics_blind(factory, seeded):
    # The demand pool orders active-request albums first, so the seeded queued
    # album is at the head — but assert by membership, not position, so other
    # active seeds on the branch cannot fail this spuriously.
    work = GeniusFetchService(factory, MagicMock())._claim_work(1_000_000)
    got = {w["track_id"] for w in work}
    assert seeded["track_unfetched"] in got, (
        "a track with no track_lyrics row must be eligible once its album is "
        "research-requested — lyrics status must not gate collection"
    )
    assert seeded["track_fetched"] not in got, "already-fetched tracks leave the pool"
    assert seeded["track_undemanded"] not in got, (
        "an album nobody requested research for must not be collected"
    )


def test_album_scoped_claim_casts_the_uuid_bind(factory, seeded):
    work = GeniusFetchService(factory, MagicMock())._claim_work(
        1_000_000, album_id=seeded["album_demanded"]
    )
    got = {w["track_id"] for w in work}
    assert got == {seeded["track_unfetched"]}, (
        "album scope must select exactly that album's unfetched tracks"
    )
