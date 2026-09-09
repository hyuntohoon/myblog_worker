"""Real-Postgres regression coverage for lyrics collector transaction boundaries.

The provider probe inspects ``pg_stat_activity`` from a second connection while
the external call is in flight. A mocked ``session.commit`` cannot observe the
idle-in-transaction failure this guards against.
"""
from __future__ import annotations

import os
import threading
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from worker.clients.lrclib_client import LrclibTransientError
from worker.service.lyrics_incremental_service import LyricsIncrementalService
from worker.service.lyrics_matcher import Candidate
from worker.service.lyrics_reassessment_service import LyricsReassessmentService

_TEST_DB_URL = os.environ.get("TEST_DB_URL")

pytestmark = pytest.mark.skipif(
    not _TEST_DB_URL,
    reason="integration test requires TEST_DB_URL env var (Postgres test database)",
)


class _ProbeClient:
    """Provider fake that checks the service connection during its call."""

    def __init__(self, probe):
        self._probe = probe

    def search_candidates(self, title, artist, **kwargs):
        self._probe()
        # Skip the writer: this test isolates the read-transaction boundary.
        raise LrclibTransientError("test provider unavailable")

    def close(self):
        pass


@pytest.fixture(scope="module")
def factory():
    engine = create_engine(_TEST_DB_URL, pool_pre_ping=True, future=True)
    yield sessionmaker(bind=engine, future=True)
    engine.dispose()


def _track():
    return {
        "id": uuid.uuid4(),
        "title": "Transaction Boundary",
        "duration_sec": 180,
        "artist_names": ["Test Artist"],
        "aliases": [],
        "existing_status": "not_found",
        "existing_basis": None,
    }


@pytest.mark.parametrize("entrypoint", ["incremental", "reassess", "album"])
def test_collector_selection_transaction_is_closed_before_provider_call(factory, entrypoint):
    """Every ``run_eval_batch`` caller releases its selection transaction first."""
    states: list[str] = []

    with factory() as session:
        pid = session.execute(text("SELECT pg_backend_pid()")).scalar_one()
        session.commit()

        def probe():
            with factory() as other:
                states.append(other.execute(text(
                    "SELECT state FROM pg_stat_activity WHERE pid = :pid"),
                    {"pid": pid},
                ).scalar() or "gone")

        client = _ProbeClient(probe)
        if entrypoint == "incremental":
            service = LyricsIncrementalService(session, client=client, time_budget_sec=30)

            def fetch(limit):
                session.execute(text("SELECT 1"))
                return [_track()]

            service._fetch_uncorpused_tracks = fetch
            service.collect(limit=1)
        else:
            service = LyricsReassessmentService(session, client=client, time_budget_sec=30)

            def fetch(*args):
                session.execute(text("SELECT 1"))
                return [_track()]

            if entrypoint == "reassess":
                # The selection transaction, rather than exclusion policy, is under test.
                service._fetch_unresolved_tracks = fetch
                service.reassess(limit=1)
            else:
                service._fetch_album_tracks = fetch
                service.reassess_album(str(uuid.uuid4()), limit=1, cooldown_sec=0)

    assert states, "provider probe did not run"
    assert "idle in transaction" not in states, states


def test_incremental_lost_race_closes_guard_transaction_before_next_provider_call(factory):
    """A lost first-row race cannot leave its guard SELECT open for row two's HTTP call."""
    album_id = str(uuid.uuid4())
    first_id, second_id = str(uuid.uuid4()), str(uuid.uuid4())
    spotify_prefix = f"lyr_txn_{uuid.uuid4().hex[:8]}"
    released_second = threading.Event()
    peer_timestamps = []
    states: list[str] = []

    try:
        with factory() as setup:
            setup.execute(text(
                "INSERT INTO albums (id, spotify_id, title, total_tracks) "
                "VALUES (:id, :spotify_id, 'Transaction Album', 2)"),
                {"id": album_id, "spotify_id": spotify_prefix})
            for number, track_id in enumerate((first_id, second_id), start=1):
                setup.execute(text(
                    "INSERT INTO tracks (id, album_id, spotify_id, title, duration_sec, track_no) "
                    "VALUES (:id, :album_id, :spotify_id, :title, 180, :track_no)"),
                    {"id": track_id, "album_id": album_id,
                     "spotify_id": f"{spotify_prefix}_{number}",
                     "title": f"Transaction {number}", "track_no": number})
            setup.commit()

        with factory() as session:
            pid = session.execute(text("SELECT pg_backend_pid()")).scalar_one()
            session.commit()

            rows = [
                {"id": first_id, "title": "Transaction 1", "duration_sec": 180,
                 "artist_names": ["Test Artist"], "aliases": []},
                {"id": second_id, "title": "Transaction 2", "duration_sec": 180,
                 "artist_names": ["Test Artist"], "aliases": []},
            ]

            class _RaceClient:
                def search_candidates(self, title, artist, **kwargs):
                    if title == "Transaction 1":
                        # A peer completes the first track after selection but before
                        # this invocation's guard re-check.
                        with factory() as peer:
                            peer.execute(text(
                                "INSERT INTO track_lyrics (track_id, match_status, lyric_plain) "
                                "VALUES (:track_id, 'matched', 'peer result')"),
                                {"track_id": first_id})
                            peer.commit()
                            peer_timestamps.append(peer.execute(text(
                                "SELECT updated_at FROM track_lyrics WHERE track_id = :track_id"),
                                {"track_id": first_id},
                            ).scalar_one())
                        return [Candidate(
                            id=1, title=title, artist=artist, album=None,
                            duration_sec=180, instrumental=False,
                            plain_lyrics="test", synced_lyrics=None,
                        )]

                    if not released_second.wait(timeout=5):
                        raise RuntimeError("first-row guard did not complete")
                    with factory() as probe_session:
                        states.append(probe_session.execute(text(
                            "SELECT state FROM pg_stat_activity WHERE pid = :pid"),
                            {"pid": pid},
                        ).scalar() or "gone")
                    raise LrclibTransientError("test provider unavailable")

                def close(self):
                    pass

            service = LyricsIncrementalService(
                session, client=_RaceClient(), concurrency=1, time_budget_sec=30,
            )

            def fetch(limit):
                session.execute(text("SELECT 1"))
                return rows

            original_guard = service._still_uncorpused

            def guard(row, outcome):
                keep = original_guard(row, outcome)
                if row["id"] == first_id:
                    released_second.set()
                return keep

            service._fetch_uncorpused_tracks = fetch
            service._still_uncorpused = guard
            metrics = service.collect(limit=2)

        assert metrics["guard_kept"] == 1
        assert states, "second provider call did not run"
        assert "idle in transaction" not in states, states
        with factory() as verify:
            assert verify.execute(text(
                "SELECT updated_at FROM track_lyrics WHERE track_id = :track_id"),
                {"track_id": first_id},
            ).scalar_one() == peer_timestamps[0]
    finally:
        with factory() as cleanup:
            cleanup.execute(text(
                "DELETE FROM track_lyrics WHERE track_id IN (:first_id, :second_id)"),
                {"first_id": first_id, "second_id": second_id})
            cleanup.execute(text("DELETE FROM tracks WHERE id IN (:first_id, :second_id)"),
                            {"first_id": first_id, "second_id": second_id})
            cleanup.execute(text("DELETE FROM albums WHERE id = :id"), {"id": album_id})
            cleanup.commit()
