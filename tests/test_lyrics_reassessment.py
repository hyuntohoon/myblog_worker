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

import pytest

from worker.core.config import settings
from worker.service.lyrics_matcher import (
    STATUS_MATCHED,
    Candidate,
    MatchOutcome,
)
from worker.service.lyrics_reassessment_service import (
    LyricsReassessmentService,
    should_replace,
)


@pytest.fixture(autouse=True)
def _no_exclusion_sync(monkeypatch):
    """Neutralise the label-yield exclusion sync for the guard tests.

    `reassess()` recomputes exclusions before selecting, which costs two extra
    `session.execute` calls. These tests assert the replacement guard writes the row
    EXACTLY once, so counting total executes would either break or, worse, have to be
    bumped to 3 — turning a meaningful assertion into a number nobody can read. The
    exclusion rule has its own coverage; here it is noise.

    Patched at the point of USE, not at its definition: `lyrics_reassessment_service`
    imported the symbol, so rebinding it in `lyrics_label_yield` would leave this
    module still holding the real function.
    """
    monkeypatch.setattr(
        "worker.service.lyrics_reassessment_service.sync_exclusions",
        lambda session: {"marked": 0, "unmarked": 0, "rule": "test-noop"},
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


def _sole_statement(session):
    """The one SQL statement a guard-kept row produces (its rotation touch)."""
    assert session.execute.call_count == 1
    return " ".join(str(session.execute.call_args[0][0]).split())


def test_matched_row_is_protected_by_guard():
    # Defensive: even if a matched row were selected, the guard must keep its CONTENT.
    client = _FakeClient(result=[_matching_candidate()])
    row = {"id": uuid.uuid4(), "title": "Hello", "duration_sec": 295,
           "artist_names": ["Adele"], "aliases": [],
           "existing_status": "matched", "existing_basis": "exact-title"}
    svc, session = _service(client, [row])
    metrics = svc.reassess()
    assert metrics["guard_kept"] == 1
    assert metrics["evaluated"] == 0
    # The row is re-checked, so its rotation cursor advances — but nothing else may move.
    assert _sole_statement(session) == (
        "UPDATE track_lyrics SET updated_at = NOW() WHERE track_id = :track_id"
    )


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
    assert metrics["bestof_superseded"] == 0
    assert "UPDATE track_lyrics SET updated_at" in _sole_statement(session)


def test_unsupersedable_best_of_row_still_rotates():
    """The failure mode Step 3b's queue inversion would otherwise create.

    A best-of row whose re-check reproduces the same best-of basis is refused by the
    guard (a lateral swap) and therefore never rewritten. With best-of rows now ordered
    AHEAD of the unresolved pool and sorted stalest-first, a row whose `updated_at` never
    moves would be re-selected on every single run for the rest of time — the ~16% that
    cannot be superseded would permanently own all 150 slots and unresolved recovery
    would stop dead. The touch is what prevents that, so it is asserted on its own.
    """
    client = _FakeClient(result=[_matching_candidate()])
    row = {"id": uuid.uuid4(), "title": "Hello", "duration_sec": 295,
           "artist_names": ["Adele"], "aliases": [],
           "existing_status": "matched", "existing_basis": "best-of-ambiguous"}
    svc, session = _service(client, [row])
    # decide_match returns exact-title here, so force the lateral-swap case explicitly.
    import worker.service.lyrics_eval_core as core
    original = core.decide_match
    try:
        core.decide_match = lambda **kw: MatchOutcome(
            track_id=kw["track_id"], match_status=STATUS_MATCHED,
            match_basis="best-of-ambiguous", version_agrees=True,
            evidence={"promotion": {"from_status": "ambiguous"}},
            lyric_plain="x", lyric_synced=None, matcher_version="test",
        )
        metrics = svc.reassess()
    finally:
        core.decide_match = original
    assert metrics["guard_kept"] == 1          # lateral best-of -> best-of refused
    assert metrics["evaluated"] == 0           # content untouched
    assert "UPDATE track_lyrics SET updated_at" in _sole_statement(session)


def test_best_of_supersession_is_counted():
    """`bestof_superseded` is how prod reports whether the Step 3b flip is working —
    LOG_LEVEL=WARNING hides the per-run info log, so the metric is the only signal."""
    client = _FakeClient(result=[_matching_candidate()])
    row = {"id": uuid.uuid4(), "title": "Hello", "duration_sec": 295,
           "artist_names": ["Adele"], "aliases": [],
           "existing_status": "matched", "existing_basis": "best-of-ambiguous"}
    svc, _ = _service(client, [row])
    metrics = svc.reassess()
    assert metrics["bestof_superseded"] == 1

    # An ordinary unresolved promotion is NOT a supersession.
    svc2, _ = _service(_FakeClient(result=[_matching_candidate()]), [_unresolved_row("not_found")])
    assert svc2.reassess()["bestof_superseded"] == 0


# --------------------------------------------------------------------------
# Widened selection SQL (Step 2): best-of rows re-selected, unresolved keeps priority
# --------------------------------------------------------------------------
def _album_service(client, tracks):
    session = MagicMock()
    svc = LyricsReassessmentService(session, client=client)
    svc._fetch_album_tracks = MagicMock(return_value=tracks)
    return svc, session


class TestReassessAlbum:
    """DATA-catalog-noise Step 4 — album-scoped expedite (orchestration)."""

    def test_promotes_and_writes_under_the_same_guard(self):
        client = _FakeClient(result=[_matching_candidate()])
        svc, session = _album_service(client, [_unresolved_row("not_found")])
        metrics = svc.reassess_album("album-1")
        assert metrics[STATUS_MATCHED] == 1
        assert metrics["evaluated"] == 1
        assert metrics["album_id"] == "album-1"
        assert session.execute.call_count == 1

    def test_good_match_still_protected(self):
        # The expedite bypasses the exclusion, NOT the replacement guard.
        client = _FakeClient(result=[_matching_candidate()])
        row = {"id": uuid.uuid4(), "title": "Hello", "duration_sec": 295,
               "artist_names": ["Adele"], "aliases": [],
               "existing_status": "matched", "existing_basis": "exact-title"}
        svc, session = _album_service(client, [row])
        metrics = svc.reassess_album("album-1")
        assert metrics["guard_kept"] == 1
        assert "UPDATE track_lyrics SET updated_at" in _sole_statement(session)

    def test_does_not_run_pool_wide_exclusion_sync(self, monkeypatch):
        """`reassess` recomputes exclusions before selecting; the expedite must not. It is a
        single-album request that ignores the marks anyway, so syncing them would only add
        two writes and a lock to an interactive path."""
        calls = []
        monkeypatch.setattr(
            "worker.service.lyrics_reassessment_service.sync_exclusions",
            lambda session: calls.append(1) or {},
        )
        svc, _ = _album_service(_FakeClient(), [])
        svc.reassess_album("album-1")
        assert calls == []

    def test_defaults_come_from_settings_but_zero_cooldown_is_honoured(self):
        """`0` means "no cooldown, re-run now" and must not collapse into the default the way
        `cooldown_sec or SETTING` would."""
        svc, _ = _album_service(_FakeClient(), [])
        svc.reassess_album("album-1")
        assert svc._fetch_album_tracks.call_args[0][2] == settings.LYRICS_EXPEDITE_COOLDOWN_SEC

        svc.reassess_album("album-1", limit=5, cooldown_sec=0)
        album_id, limit, cooldown = svc._fetch_album_tracks.call_args[0]
        assert (limit, cooldown) == (5, 0)


class TestAlbumSelectionSQL:
    """The expedite's selection differs from the scheduled one in exactly two ways."""

    def _sql_and_params(self, cooldown_sec=600.0):
        session = MagicMock()
        session.execute.return_value.fetchall.return_value = []
        svc = LyricsReassessmentService(session, client=_FakeClient())
        svc._fetch_album_tracks("album-1", 150, cooldown_sec)
        return str(session.execute.call_args[0][0]), session.execute.call_args[0][1]

    def test_scoped_to_the_album_with_a_cooldown(self):
        sql, params = self._sql_and_params()
        assert "t.album_id = :album_id" in sql
        assert "tl.updated_at < NOW() - make_interval(secs => :cooldown_sec)" in sql
        assert params == {"album_id": "album-1", "cooldown_sec": 600.0, "limit": 150}

    def test_label_yield_exclusion_is_bypassed(self):
        """The whole point of the expedite: an explicitly requested album is re-checked even
        if pool hygiene had excluded its rows. If this assertion ever starts failing, the
        expedite has silently become a no-op for exactly the albums it exists to rescue."""
        sql, _ = self._sql_and_params()
        assert "excluded_by" not in sql

    def test_otherwise_selects_what_the_scheduled_pass_selects(self):
        sql, _ = self._sql_and_params()
        assert "tl.match_status IN ('not_found', 'ambiguous', 'review_required')" in sql
        assert "tl.evidence ->> 'match_basis' LIKE 'best-of-%'" in sql


class TestScheduledSelectionSQL:
    """DATA-catalog-noise Step 3b — the scheduled queue drains the best-of backlog first.

    The selection is DB-only (stubbed everywhere else in this file), so these assert the
    SQL text. A live-DB integration needs TEST_DB_URL — CI-only.
    """

    def _sql_and_params(self, limit=10):
        session = MagicMock()
        session.execute.return_value.fetchall.return_value = []
        svc = LyricsReassessmentService(session, client=_FakeClient())
        svc._fetch_unresolved_tracks(limit)
        return str(session.execute.call_args[0][0]), session.execute.call_args[0][1]

    def test_best_of_rows_are_still_reselected(self):
        """Step 2's widened pool: without this arm a best-of row leaves the corpus forever
        and the promised supersession can never happen."""
        sql, _ = self._sql_and_params()
        assert "tl.evidence ->> 'match_basis' LIKE 'best-of-%'" in sql
        assert "tl.match_status = 'matched'" in sql

    def test_best_of_backlog_is_ordered_AHEAD_of_unresolved(self):
        """The Step 3b inversion, and the whole reason this arm executes at all.

        Measured 2026-08-03: 11,033 unresolved rows sat ahead of 2,765 best-of rows under
        a LIMIT of 150, so the supersession path was unreachable, not merely slow. If this
        assertion flips back, the arm is dead code in prod again — silently, because a
        dead arm still passes every other test in this file.
        """
        sql, _ = self._sql_and_params()
        assert "CASE WHEN tl.match_status = 'matched' THEN 0 ELSE 1 END" in sql
        assert sql.index("CASE WHEN") < sql.index("tl.updated_at ASC")

    def test_best_of_arm_rests_between_re_checks(self):
        """What makes the inversion terminate. ~16% of best-of rows cannot be superseded
        (the guard refuses a lateral swap), so they are re-checked and never rewritten;
        the rest interval is what stops them owning the queue head forever."""
        sql, params = self._sql_and_params()
        assert "make_interval(days => :bestof_rest_days)" in sql
        assert params["bestof_rest_days"] == settings.LYRICS_BESTOF_RECHECK_INTERVAL_DAYS

    def test_rest_interval_applies_only_to_the_best_of_arm(self):
        """An unresolved row must never be gated by the best-of rest interval — that would
        cut the pool's own rotation from daily to monthly."""
        sql, _ = self._sql_and_params()
        best_of_arm = sql[sql.index("tl.match_status = 'matched'"):sql.index("GROUP BY")]
        assert "make_interval(days => :bestof_rest_days)" in best_of_arm
        assert sql.count("make_interval(days => :bestof_rest_days)") == 1
