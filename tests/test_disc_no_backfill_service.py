"""Unit coverage for DiscNoBackfillService's write-path branching.

No database needed: these capture the SQL + params the service hands to a fake
session. DB-bound end-to-end coverage (real album row → real Spotify mock →
real disc_no column) belongs in a Neon-test-branch integration test if this
ever becomes a recurring job; it currently isn't one (DATA-multidisc-track-order
Step 2b is a one-off backfill over a fixed, already-measured population).

Invariants pinned here:
  1. a track Spotify returns that has no local row in this album is skipped,
     NEVER inserted,
  2. only tracks belonging to the album being processed are eligible for a
     match (a spotify_id collision with another album's track must not leak),
  3. one album's failure rolls back and does not block the next album,
  4. writes are sorted by track_id (bulk-write lock-ordering rule),
  5. a market-relinked track (`linked_from.id` differs from `id`) still matches
     the LOCAL id — a live prod gap found running this backfill: 2 of the 78
     albums (Queen's "Sheer Heart Attack (Deluxe Remastered Version)" and "The
     Game (Deluxe Remastered Version)") returned market=KR ids that differ from
     what's stored locally, with the matching id only reachable via
     `linked_from.id`.
"""
from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import patch

from worker.service.disc_no_backfill_service import DiscNoBackfillService


class FakeSession:
    """Routes SELECT tracks-by-album to a caller-supplied table; records UPDATEs."""

    def __init__(self, tracks_by_album: Dict[Any, List[tuple]]) -> None:
        self.tracks_by_album = tracks_by_album
        self.calls: List[tuple] = []
        self.commits = 0
        self.rollbacks = 0

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.calls.append((sql, params))
        if "SELECT spotify_id FROM tracks WHERE album_id" in sql:
            rows = self.tracks_by_album.get(params["album_id"], [])
            return _FakeResult(rows)
        return None

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def updates(self) -> List[tuple]:
        return [(sql, p) for sql, p in self.calls if "UPDATE tracks" in sql]


class _FakeResult:
    def __init__(self, rows: List[tuple]) -> None:
        self._rows = rows

    def fetchall(self):
        return self._rows


def _svc(session, albums: List[Dict[str, Any]]) -> DiscNoBackfillService:
    svc = DiscNoBackfillService(session)
    svc._fetch_colliding_albums = lambda limit=None: list(albums)  # type: ignore[method-assign]
    return svc


def test_track_with_no_local_row_is_skipped_not_inserted():
    session = FakeSession(tracks_by_album={"alb-1": [("sp-track-1",)]})
    svc = _svc(session, [{"id": "alb-1", "spotify_id": "alb-sp-1"}])

    with patch("worker.service.disc_no_backfill_service.spotify") as mock_spotify:
        mock_spotify.get_album_tracks.return_value = [
            {"id": "sp-track-1", "disc_number": 1},
            {"id": "sp-track-ghost", "disc_number": 2},  # no local row
        ]
        metrics = svc.backfill_disc_no()

    assert metrics["tracks_matched"] == 1
    assert metrics["tracks_skipped_no_local_row"] == 1
    updates = session.updates()
    assert len(updates) == 1
    written_ids = {row["track_id"] for row in updates[0][1]}
    assert written_ids == {"sp-track-1"}


def test_spotify_id_belonging_to_another_album_is_not_updated():
    """A track Spotify returns must only match a local row from the SAME album —
    the SELECT is scoped by album_id, so a same-spotify_id row living under a
    different local album never enters the candidate set."""
    session = FakeSession(tracks_by_album={
        "alb-1": [("sp-shared",)],
        "alb-2": [],  # sp-shared does NOT live under alb-2 locally
    })
    svc = _svc(session, [{"id": "alb-2", "spotify_id": "alb-sp-2"}])

    with patch("worker.service.disc_no_backfill_service.spotify") as mock_spotify:
        mock_spotify.get_album_tracks.return_value = [
            {"id": "sp-shared", "disc_number": 1},
        ]
        metrics = svc.backfill_disc_no()

    assert metrics["tracks_matched"] == 0
    assert metrics["tracks_skipped_no_local_row"] == 1
    assert session.updates() == []


def test_writes_are_sorted_by_track_id():
    session = FakeSession(tracks_by_album={
        "alb-1": [("sp-b",), ("sp-a",)],
    })
    svc = _svc(session, [{"id": "alb-1", "spotify_id": "alb-sp-1"}])

    with patch("worker.service.disc_no_backfill_service.spotify") as mock_spotify:
        mock_spotify.get_album_tracks.return_value = [
            {"id": "sp-b", "disc_number": 2},
            {"id": "sp-a", "disc_number": 1},
        ]
        svc.backfill_disc_no()

    updates = session.updates()
    ids = [row["track_id"] for row in updates[0][1]]
    assert ids == sorted(ids)


def test_one_album_failure_does_not_block_the_next_album():
    session = FakeSession(tracks_by_album={
        "alb-1": [("sp-1",)],
        "alb-2": [("sp-2",)],
    })
    svc = _svc(session, [
        {"id": "alb-1", "spotify_id": "alb-sp-1"},
        {"id": "alb-2", "spotify_id": "alb-sp-2"},
    ])

    def _get_album_tracks(album_sid, market=None):
        if album_sid == "alb-sp-1":
            raise RuntimeError("Spotify 500")
        return [{"id": "sp-2", "disc_number": 1}]

    with patch("worker.service.disc_no_backfill_service.spotify") as mock_spotify:
        mock_spotify.get_album_tracks.side_effect = _get_album_tracks
        metrics = svc.backfill_disc_no()

    assert metrics["errors"] == 1
    assert metrics["albums_processed"] == 1
    assert metrics["tracks_matched"] == 1
    assert session.rollbacks == 1
    assert session.commits == 2  # the initial read-close commit + album-2's write commit


def test_market_relinked_track_matches_via_linked_from_id():
    """Spotify returns a market-scoped id (`it["id"]`) that differs from the id
    stored locally; the ORIGINAL id only shows up under `linked_from.id`. Matching
    on `it["id"]` alone (the pre-fix behavior) silently skips this track forever."""
    session = FakeSession(tracks_by_album={"alb-1": [("sp-local-original",)]})
    svc = _svc(session, [{"id": "alb-1", "spotify_id": "alb-sp-1"}])

    with patch("worker.service.disc_no_backfill_service.spotify") as mock_spotify:
        mock_spotify.get_album_tracks.return_value = [
            {
                "id": "sp-market-relinked",
                "disc_number": 1,
                "linked_from": {"id": "sp-local-original"},
            },
        ]
        metrics = svc.backfill_disc_no()

    assert metrics["tracks_matched"] == 1
    assert metrics["tracks_skipped_no_local_row"] == 0
    updates = session.updates()
    written_ids = {row["track_id"] for row in updates[0][1]}
    assert written_ids == {"sp-local-original"}


def test_track_with_null_disc_number_from_spotify_is_dropped_before_matching():
    """Spotify returning disc_number: null for some malformed item must not
    overwrite a track with NULL — it is simply excluded from the candidate map."""
    session = FakeSession(tracks_by_album={"alb-1": [("sp-1",)]})
    svc = _svc(session, [{"id": "alb-1", "spotify_id": "alb-sp-1"}])

    with patch("worker.service.disc_no_backfill_service.spotify") as mock_spotify:
        mock_spotify.get_album_tracks.return_value = [
            {"id": "sp-1", "disc_number": None},
        ]
        metrics = svc.backfill_disc_no()

    assert metrics["tracks_matched"] == 0
    assert session.updates() == []
