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


class TestShouldReplaceBestOf:
    """FEAT-lyrics-best-of-promotion Step 2: best-of-* on the replacement ladder
    (above fuzzy-title, below exact-title; both best-of bases share one rung)."""

    def _best_of_row(self, basis="best-of-ambiguous"):
        return {"existing_status": "matched", "existing_basis": basis}

    def test_best_of_superseded_by_exact_title(self):
        # the promised upgrade path: LRCLIB grows -> a clean exact match replaces best-of
        assert should_replace(self._best_of_row(), _outcome("matched", "exact-title")) is True
        assert should_replace(self._best_of_row("best-of-review"),
                              _outcome("matched", "exact-title")) is True

    def test_best_of_superseded_by_stronger_future_evidence(self):
        assert should_replace(self._best_of_row(), _outcome("matched", "mb-recording")) is True
        assert should_replace(self._best_of_row(), _outcome("matched", "isrc")) is True

    def test_lateral_best_of_swap_refused(self):
        # no churn: a best-of row is never re-promoted to a DIFFERENT best-of candidate
        assert should_replace(self._best_of_row(), _outcome("matched", "best-of-ambiguous")) is False
        assert should_replace(self._best_of_row(), _outcome("matched", "best-of-review")) is False
        assert should_replace(self._best_of_row("best-of-review"),
                              _outcome("matched", "best-of-ambiguous")) is False

    def test_best_of_never_downgraded(self):
        # a usable best-of lyric is never dropped back to unresolved / fuzzy / no_lyrics
        assert should_replace(self._best_of_row(), _outcome("not_found")) is False
        assert should_replace(self._best_of_row(), _outcome("ambiguous")) is False
        assert should_replace(self._best_of_row(), _outcome("no_lyrics")) is False
        assert should_replace(self._best_of_row(), _outcome("matched", "fuzzy-title")) is False

    def test_exact_title_never_displaced_by_best_of(self):
        # the predecessor's exact-title guarantee is fenced off from the best-of channel
        row = {"existing_status": "matched", "existing_basis": "exact-title"}
        assert should_replace(row, _outcome("matched", "best-of-ambiguous")) is False
        assert should_replace(row, _outcome("matched", "best-of-review")) is False


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


def test_best_of_row_superseded_by_fresh_exact_title():
    # A best-of matched row IS re-selected (widened pool); a fresh re-check that now
    # yields a clean exact-title match supersedes it (the promised upgrade path).
    client = _FakeClient(result=[_matching_candidate()])
    row = {"id": uuid.uuid4(), "title": "Hello", "duration_sec": 295,
           "artist_names": ["Adele"], "aliases": [],
           "existing_status": "matched", "existing_basis": "best-of-ambiguous"}
    svc, session = _service(client, [row])
    metrics = svc.reassess()
    # the fresh decide_match yields exact-title matched (strictly stronger than best-of)
    # -> supersession happens: the row IS rewritten
    assert metrics["evaluated"] == 1
    assert metrics[STATUS_MATCHED] == 1
    assert metrics["guard_kept"] == 0


def test_best_of_row_not_downgraded_when_recheck_fails():
    # Re-check finds nothing (LRCLIB regression) -> the usable best-of lyric is kept.
    client = _FakeClient(result=[])
    row = {"id": uuid.uuid4(), "title": "Hello", "duration_sec": 295,
           "artist_names": ["Adele"], "aliases": [],
           "existing_status": "matched", "existing_basis": "best-of-ambiguous"}
    svc, session = _service(client, [row])
    metrics = svc.reassess()
    assert metrics["guard_kept"] == 1
    assert metrics["evaluated"] == 0
    session.execute.assert_not_called()


# --------------------------------------------------------------------------
# Widened selection SQL (Step 2): best-of rows re-selected, unresolved keeps priority
# --------------------------------------------------------------------------
def test_selection_sql_reselects_best_of_with_unresolved_priority():
    """The selection is otherwise DB-only (stubbed in the tests above); assert the SQL
    carries the two Step-2 arms: the best-of re-select and the unresolved-first ORDER BY.
    (A live-DB integration needs TEST_DB_URL — CI-only.)"""
    session = MagicMock()
    session.execute.return_value.fetchall.return_value = []
    svc = LyricsReassessmentService(session, client=_FakeClient())
    svc._fetch_unresolved_tracks(10)
    sql = str(session.execute.call_args[0][0])
    # best-of matched rows are back in the pool…
    assert "tl.evidence ->> 'match_basis' LIKE 'best-of-%'" in sql
    assert "tl.match_status = 'matched'" in sql
    # …but unresolved rows keep rotation priority (CASE tier before updated_at)
    assert "CASE WHEN tl.match_status IN ('not_found', 'ambiguous', 'review_required')" in sql
    assert sql.index("CASE WHEN") < sql.index("tl.updated_at ASC")
