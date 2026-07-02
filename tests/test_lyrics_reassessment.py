"""Unit tests for periodic reassessment + replacement guard (FEAT-lyrics-corpus Step 4).

Two units, both DB-free:

  * ``should_replace`` — the replacement guard, tested directly: unresolved rows are always
    rewritable; a resolved ``matched`` / ``no_lyrics`` row is never downgraded and is replaced
    only by a strictly-stronger-evidence ``matched`` outcome.
  * ``LyricsReassessmentService.reassess`` — the orchestration over the shared eval core with a
    fake client + stubbed selection + mock session: an unresolved row promotes/refreshes and is
    written; a (defensively passed) matched row is protected (``guard_kept``, not written).

The eval loop + budget guard are covered by ``test_lyrics_incremental.py`` (same core).
"""
from __future__ import annotations

import uuid
from unittest.mock import MagicMock

from worker.service.lyrics_matcher import (
    STATUS_MATCHED,
    Candidate,
    MatchOutcome,
)
from worker.service.lyrics_reassessment_service import (
    LyricsReassessmentService,
    should_replace,
)


# --------------------------------------------------------------------------
# should_replace — the replacement guard
# --------------------------------------------------------------------------
def _outcome(status, basis=None):
    return MatchOutcome(track_id=uuid.uuid4(), match_status=status, evidence={}, match_basis=basis)


class TestShouldReplace:
    def test_unresolved_existing_is_always_rewritable(self):
        for st in ("not_found", "ambiguous", "review_required"):
            assert should_replace({"existing_status": st}, _outcome("matched", "exact-title")) is True
            assert should_replace({"existing_status": st}, _outcome("not_found")) is True

    def test_matched_existing_never_downgraded(self):
        row = {"existing_status": "matched", "existing_basis": "exact-title"}
        assert should_replace(row, _outcome("not_found")) is False
        assert should_replace(row, _outcome("ambiguous")) is False
        assert should_replace(row, _outcome("no_lyrics")) is False

    def test_matched_existing_not_replaced_by_equal_strength(self):
        # the current matcher only ever emits 'exact-title' for matched -> a matched row is
        # never replaced in practice (2 is not strictly greater than 2).
        row = {"existing_status": "matched", "existing_basis": "exact-title"}
        assert should_replace(row, _outcome("matched", "exact-title")) is False
        assert should_replace(row, _outcome("matched", "fuzzy-title")) is False

    def test_matched_existing_replaced_only_by_strictly_stronger(self):
        row = {"existing_status": "matched", "existing_basis": "exact-title"}
        assert should_replace(row, _outcome("matched", "mb-recording")) is True
        assert should_replace(row, _outcome("matched", "isrc")) is True

    def test_no_lyrics_existing_is_protected(self):
        row = {"existing_status": "no_lyrics", "existing_basis": None}
        assert should_replace(row, _outcome("not_found")) is False
        # a stronger matched outcome may flip an instrumental sentinel that now has lyrics
        assert should_replace(row, _outcome("matched", "isrc")) is True


# --------------------------------------------------------------------------
# LyricsReassessmentService.reassess — orchestration
# --------------------------------------------------------------------------
def _matching_candidate():
    return Candidate(
        id=1, title="Hello", artist="Adele", album="25", duration_sec=295.0,
        instrumental=False, plain_lyrics="Hello, it's me", synced_lyrics=None,
    )


def _unresolved_row(status="not_found"):
    return {
        "id": uuid.uuid4(), "title": "Hello", "duration_sec": 295,
        "artist_names": ["Adele"], "aliases": [],
        "existing_status": status, "existing_basis": None,
    }


class _FakeClient:
    def __init__(self, result=None):
        self.result = result if result is not None else []
        self.closed = False

    def search_candidates(self, title, artist, **kw):
        return list(self.result)

    def close(self):
        self.closed = True


def _service(client, tracks):
    session = MagicMock()
    svc = LyricsReassessmentService(session, client=client)
    svc._fetch_unresolved_tracks = MagicMock(return_value=tracks)
    return svc, session


def test_unresolved_promotes_to_matched_and_writes():
    client = _FakeClient(result=[_matching_candidate()])
    svc, session = _service(client, [_unresolved_row("not_found")])
    metrics = svc.reassess()
    assert metrics[STATUS_MATCHED] == 1          # promotion
    assert metrics["evaluated"] == 1
    assert session.execute.call_count == 1       # row rewritten (promoted)
    assert metrics["guard_kept"] == 0


def test_unresolved_still_unresolved_is_refreshed():
    client = _FakeClient(result=[])              # LRCLIB still finds nothing
    svc, session = _service(client, [_unresolved_row("not_found")])
    metrics = svc.reassess()
    assert metrics["not_found"] == 1
    assert metrics["evaluated"] == 1
    assert session.execute.call_count == 1       # refreshed (updated_at bumps -> queue rotates)


def test_matched_row_is_protected_by_guard():
    # Defensive: even if a matched row were selected, the guard must keep it (not written).
    client = _FakeClient(result=[_matching_candidate()])
    row = {"id": uuid.uuid4(), "title": "Hello", "duration_sec": 295,
           "artist_names": ["Adele"], "aliases": [],
           "existing_status": "matched", "existing_basis": "exact-title"}
    svc, session = _service(client, [row])
    metrics = svc.reassess()
    assert metrics["guard_kept"] == 1
    assert metrics["evaluated"] == 0
    session.execute.assert_not_called()          # good match untouched
