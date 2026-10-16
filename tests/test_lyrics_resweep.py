"""FEAT-lyrics-match-correction Step 2: the re-sweep of rows an older matcher wrote.

DB-free. Three units:

  * the lateral replacement rule (``should_replace`` / ``assess_lateral``) and the verdict
    the re-sweep stamps (``replacement_verdict``), on the real Bad Habit LRCLIB response
    (``tests/fixtures/lyrics_consensus.json``) and on small synthetic bodies;
  * ``LyricsReassessmentService`` orchestration of the sweep arm (``reassess``,
    ``reassess_track``) over a fake client and a mock session;
  * the selection / write SQL text (sweep arm, track scope, body attach, evidence patch).

The real-Postgres round trip of the SQL lives in tests/integration/test_lyrics_resweep_db.py.
RFC: ``docs/rfcs/FEAT-lyrics-match-correction.md`` (Step 2).
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from worker.core.config import settings
from worker.service.lyrics_matcher import (
    MATCHER_VERSION,
    STATUS_MATCHED,
    STATUS_NOT_FOUND,
    Candidate,
    MatchOutcome,
    TrackLyricsWriter,
    decide_match,
)
from worker.service.lyrics_reassessment_service import (
    LyricsReassessmentService,
    assess_lateral,
    produced_by_other_matcher,
    replacement_verdict,
    should_replace,
)

_FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "lyrics_consensus.json").read_text()
)
_BAD_HABIT = _FIXTURE["bad_habit"]
_WRONG_ID = str(_BAD_HABIT["wrong_id"])
_OLD = "step2-v2"


@pytest.fixture(autouse=True)
def _real_pass(monkeypatch):
    """Most tests describe the real pass; TestShadowMode switches shadow back on."""
    monkeypatch.setattr(settings, "LYRICS_RESWEEP_SHADOW", False)


@pytest.fixture(autouse=True)
def _no_exclusion_sync(monkeypatch):
    """As in test_lyrics_reassessment.py: the exclusion sync is not under test here."""
    monkeypatch.setattr(
        "worker.service.lyrics_reassessment_service.sync_exclusions",
        lambda session: {"marked": 0, "unmarked": 0, "rule": "test-noop"},
    )


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
def _bh_cands(drop_wrong: bool = False):
    cands = [Candidate.from_api(r) for r in _BAD_HABIT["candidates"]]
    return [c for c in cands if str(c.id) != _WRONG_ID] if drop_wrong else cands


def _bh_wrong():
    return next(c for c in _bh_cands() if str(c.id) == _WRONG_ID)


def _bh_decide(cands):
    t = _BAD_HABIT["track"]
    return decide_match(uuid.uuid4(), t["title"], t["artists"], None, t["duration_sec"], cands)


def _bh_row(stored: Candidate, **over):
    """The Bad Habit row as the sweep selection projects it, storing ``stored``'s body."""
    t = _BAD_HABIT["track"]
    row = {
        "id": uuid.uuid4(), "title": t["title"], "duration_sec": t["duration_sec"],
        "artist_names": list(t["artists"]), "aliases": [],
        "existing_status": STATUS_MATCHED, "existing_basis": "exact-title",
        "existing_matcher_version": _OLD, "existing_locked": False,
        "existing_lrclib_id": str(stored.id),
        "existing_lyric_plain": stored.plain_lyrics,
        "existing_lyric_synced": stored.synced_lyrics,
    }
    row.update(over)
    return row


def _c(cid, body, *, title="Song", artist="Artist", duration=200.0):
    return Candidate(
        id=cid, title=title, artist=artist, album=None, duration_sec=duration,
        instrumental=False, plain_lyrics=body, synced_lyrics=f"[00:00.00]{body}",
    )


_BODY_A = "the river runs down to the sea and carries every stone along with it tonight"
_BODY_B = "neon signs burn bright over empty streets while the city keeps on humming low"


def _synthetic_row(stored_body, stored_id, **over):
    row = {
        "id": uuid.uuid4(), "title": "Song", "duration_sec": 200,
        "artist_names": ["Artist"], "aliases": [],
        "existing_status": STATUS_MATCHED, "existing_basis": "exact-title",
        "existing_matcher_version": _OLD, "existing_locked": False,
        "existing_lrclib_id": stored_id,
        "existing_lyric_plain": stored_body,
        "existing_lyric_synced": f"[00:00.00]{stored_body}" if stored_body else None,
    }
    row.update(over)
    return row


def _picked(body, cid, cluster):
    return MatchOutcome(
        track_id=uuid.uuid4(), match_status=STATUS_MATCHED, match_basis="exact-title",
        evidence={"lrclib_id": cid, "content_cluster_size": cluster},
        lyric_plain=body, lyric_synced=f"[00:00.00]{body}", version_agrees=True,
    )


# --------------------------------------------------------------------------
# The lateral rule on the real Bad Habit response
# --------------------------------------------------------------------------
class TestBadHabitLateral:
    """The row the RFC names: stored with Ed Sheeran's body (8996695) by an older matcher
    (prod's row is ``step3-v1``; any version other than the running one behaves the same)."""

    def test_wrong_stored_body_is_replaced_by_the_majority(self):
        cands = _bh_cands()
        outcome = _bh_decide(cands)
        row = _bh_row(_bh_wrong())
        assert outcome.match_status == STATUS_MATCHED
        assert outcome.match_basis == "exact-title" == row["existing_basis"]  # same rung
        lateral = assess_lateral(row, outcome, cands)
        assert lateral.same_content is False
        assert lateral.existing_cluster_size == 1  # the wrong upload agrees only with itself
        assert lateral.new_cluster_size > lateral.existing_cluster_size
        assert should_replace(row, outcome, cands) is True

    def test_replaced_even_after_the_wrong_upload_left_the_search(self):
        """/api/search caps at 20; the stored id can drop out. The stored row still gets
        its own vote (cluster 1), so the comparison neither crashes nor flips."""
        cands = _bh_cands(drop_wrong=True)
        outcome = _bh_decide(cands)
        row = _bh_row(_bh_wrong())
        lateral = assess_lateral(row, outcome, cands)
        assert lateral.existing_cluster_size == 1
        assert should_replace(row, outcome, cands) is True

    def test_verdict_stamps_replaced_with_the_ids(self):
        cands = _bh_cands()
        outcome = _bh_decide(cands)
        v = replacement_verdict(_bh_row(_bh_wrong()), outcome, cands)
        assert v.write is True and bool(v) is True
        assert v.counter == "resweep_replaced"
        block = v.evidence["resweep"]
        assert block["verdict"] == "replaced"
        assert block["matcher_version"] == MATCHER_VERSION
        assert block["previous_matcher_version"] == _OLD
        assert block["replaced_lrclib_id"] == _WRONG_ID
        assert str(block["outcome_lrclib_id"]) != _WRONG_ID
        assert block["same_content"] is False

    def test_majority_body_already_stored_is_unchanged(self):
        cands = _bh_cands()
        outcome = _bh_decide(cands)
        good = next(c for c in cands if str(c.id) == str(outcome.evidence["lrclib_id"]))
        row = _bh_row(good)
        assert should_replace(row, outcome, cands) is False
        v = replacement_verdict(row, outcome, cands)
        assert (v.write, v.counter) == (False, "resweep_unchanged")
        assert v.evidence["resweep"]["same_content"] is True
        assert "replaced_lrclib_id" not in v.evidence["resweep"]

    def test_hand_fixed_row_is_locked_even_with_the_wrong_body(self):
        """The prod Bad Habit row carries manual_fix. Nothing automatic moves it — not the
        lateral rule, not a stronger basis."""
        cands = _bh_cands()
        outcome = _bh_decide(cands)
        row = _bh_row(_bh_wrong(), existing_locked=True)
        assert should_replace(row, outcome, cands) is False
        stronger = MatchOutcome(
            track_id=uuid.uuid4(), match_status=STATUS_MATCHED, match_basis="isrc",
            evidence={"content_cluster_size": 9},
        )
        assert should_replace(row, stronger, cands) is False
        v = replacement_verdict(row, outcome, cands)
        assert (v.write, v.counter) == (False, "resweep_locked")


# --------------------------------------------------------------------------
# Who is eligible for the lateral rule
# --------------------------------------------------------------------------
class TestEligibility:
    def test_same_version_row_keeps_the_no_churn_rule(self):
        cands = _bh_cands()
        outcome = _bh_decide(cands)
        row = _bh_row(_bh_wrong(), existing_matcher_version=MATCHER_VERSION)
        assert should_replace(row, outcome, cands) is False
        # and it is not a re-sweep row at all: no stamp, no counter
        v = replacement_verdict(row, outcome, cands)
        assert (v.write, v.counter, v.evidence) == (False, None, None)

    def test_promotion_suffix_does_not_make_a_row_stale(self):
        row = {"existing_matcher_version": f"{MATCHER_VERSION}+best-of-v1"}
        assert produced_by_other_matcher(row) is False
        assert produced_by_other_matcher({"existing_matcher_version": f"{_OLD}+best-of-v1"})

    def test_a_null_version_is_another_matcher(self):
        assert produced_by_other_matcher({"existing_matcher_version": None}) is True

    def test_rows_selected_without_version_info_fail_closed(self):
        """The demand-source job and the workspace backfill tool do not project the
        version; for them the lateral rule must not exist."""
        cands = _bh_cands()
        outcome = _bh_decide(cands)
        row = _bh_row(_bh_wrong())
        del row["existing_matcher_version"]
        assert produced_by_other_matcher(row) is False
        assert should_replace(row, outcome, cands) is False

    def test_lateral_needs_candidates_and_consensus_evidence(self):
        row = _synthetic_row(_BODY_A, 1)
        outcome = _picked(_BODY_B, 2, 3)
        assert assess_lateral(row, outcome, []) is None
        assert should_replace(row, outcome, None) is False
        outcome.evidence.pop("content_cluster_size")
        assert assess_lateral(row, outcome, [_c(2, _BODY_B)]) is None


# --------------------------------------------------------------------------
# The strictly-larger-cluster boundary, on synthetic bodies
# --------------------------------------------------------------------------
class TestClusterBoundary:
    def test_equal_clusters_keep_the_stored_body(self):
        cands = [_c(1, _BODY_A), _c(2, _BODY_A), _c(3, _BODY_B), _c(4, _BODY_B)]
        row = _synthetic_row(_BODY_A, 1)
        outcome = _picked(_BODY_B, 3, 2)
        lateral = assess_lateral(row, outcome, cands)
        assert (lateral.existing_cluster_size, lateral.new_cluster_size) == (2, 2)
        assert should_replace(row, outcome, cands) is False
        v = replacement_verdict(row, outcome, cands)
        assert (v.write, v.counter) == (False, "resweep_kept")

    def test_one_larger_replaces(self):
        cands = [_c(1, _BODY_A), _c(2, _BODY_A), _c(3, _BODY_B), _c(4, _BODY_B), _c(5, _BODY_B)]
        row = _synthetic_row(_BODY_A, 1)
        outcome = _picked(_BODY_B, 3, 3)
        assert should_replace(row, outcome, cands) is True

    def test_same_body_is_never_an_upgrade_whatever_the_cluster_sizes(self):
        """A same-song transcription swap is churn, not a fix — even when the new pick's
        cluster reads larger than the stored body's (e.g. the stored upload's stance
        splits the count)."""
        row = _synthetic_row(_BODY_A, 1)
        outcome = _picked(_BODY_A, 7, 5)
        lateral = assess_lateral(row, outcome, [_c(1, _BODY_A)])
        assert (lateral.same_content, lateral.existing_cluster_size) == (True, 1)
        assert lateral.upgrade is False
        assert should_replace(row, outcome, [_c(1, _BODY_A)]) is False

    def test_implausible_candidates_do_not_vote_for_the_stored_body(self):
        """The stored side is counted through the matcher's own plausibility gate: an
        off-duration upload of the same body is not a vote."""
        cands = [_c(1, _BODY_A), _c(2, _BODY_A, duration=400.0),
                 _c(3, _BODY_B), _c(4, _BODY_B)]
        row = _synthetic_row(_BODY_A, 1)
        outcome = _picked(_BODY_B, 3, 2)
        assert assess_lateral(row, outcome, cands).existing_cluster_size == 1
        assert should_replace(row, outcome, cands) is True

    def test_returned_stored_upload_is_not_double_counted(self):
        cands = [_c(1, _BODY_A), _c(3, _BODY_B), _c(4, _BODY_B)]
        assert assess_lateral(_synthetic_row(_BODY_A, 1), _picked(_BODY_B, 3, 2),
                              cands).existing_cluster_size == 1
        # not returned → it still gets its own vote, once
        assert assess_lateral(_synthetic_row(_BODY_A, 99), _picked(_BODY_B, 3, 2),
                              cands).existing_cluster_size == 2

    def test_stored_body_without_text_counts_zero(self):
        """A no_lyrics sentinel stores no body: any matched pick at the same basis wins."""
        row = _synthetic_row(None, None, existing_status="no_lyrics")
        lateral = assess_lateral(row, _picked(_BODY_B, 3, 1), [_c(3, _BODY_B)])
        assert (lateral.same_content, lateral.existing_cluster_size) == (False, 0)
        assert lateral.upgrade is True

    def test_parked_outcome_never_replaces_and_is_stamped_parked(self):
        row = _synthetic_row(_BODY_A, 1)
        parked = MatchOutcome(track_id=uuid.uuid4(), match_status=STATUS_NOT_FOUND,
                              evidence={"reason": "no_candidates"})
        assert should_replace(row, parked, []) is False
        v = replacement_verdict(row, parked, [])
        assert (v.write, v.counter) == (False, "resweep_parked")
        assert v.evidence["resweep"]["outcome_reason"] == "no_candidates"
        assert "same_content" not in v.evidence["resweep"]

    def test_stronger_basis_is_superseded_not_replaced(self):
        row = _synthetic_row(_BODY_A, 1, existing_basis="best-of-ambiguous")
        v = replacement_verdict(row, _picked(_BODY_A, 1, 1), [_c(1, _BODY_A)])
        assert (v.write, v.counter) == (True, "resweep_superseded")

    def test_unresolved_rows_get_no_stamp(self):
        row = _synthetic_row(None, None, existing_status="not_found")
        v = replacement_verdict(row, _picked(_BODY_B, 3, 1), [_c(3, _BODY_B)])
        assert v.write is True
        assert (v.counter, v.evidence) == (None, None)


# --------------------------------------------------------------------------
# Shadow mode (the owner's 2026-10-16 decision after review Y1/Y5)
# --------------------------------------------------------------------------
_GO_GO = _FIXTURE["go_go"]


class TestShadowMode:
    @pytest.fixture(autouse=True)
    def _shadow(self, monkeypatch):
        monkeypatch.setattr(settings, "LYRICS_RESWEEP_SHADOW", True)

    def test_go_go_correct_minority_body_would_be_overwritten(self):
        """Review Y1, on real data: an older matcher stored the Korean original; today's
        pick is the Japanese majority (cluster 16 vs 1). The real rule would replace it —
        which is exactly why the drain runs in shadow first."""
        cands = [Candidate.from_api(r) for r in _GO_GO["candidates"]]
        kor = next(c for c in cands if str(c.id) == str(_GO_GO["korean_id"]))
        outcome = decide_match(uuid.uuid4(), "Go Go", ["BTS"], None, kor.duration_sec, cands)
        row = {
            "id": uuid.uuid4(), "title": "Go Go", "duration_sec": kor.duration_sec,
            "artist_names": ["BTS"], "aliases": [],
            "existing_status": STATUS_MATCHED, "existing_basis": "exact-title",
            "existing_matcher_version": "step3-v1", "existing_locked": False,
            "existing_lrclib_id": str(kor.id),
            "existing_lyric_plain": kor.plain_lyrics, "existing_lyric_synced": kor.synced_lyrics,
        }
        assert should_replace(row, outcome, cands) is True       # the rule itself
        v = replacement_verdict(row, outcome, cands)
        assert (v.write, v.counter) == (False, "resweep_would_replace")
        block = v.evidence["resweep_shadow"]
        assert block["verdict"] == "would_replace"
        assert block["replaced_lrclib_id"] == str(kor.id)
        assert "resweep" not in v.evidence                        # the real pass stays open

    def test_no_lyrics_ladder_supersession_is_shadowed(self):
        """Review Y5: a no_lyrics row (basis None) loses to any matched pick via the ladder,
        not the lateral rule. The sweep newly reaches these rows, so it is shadowed too."""
        row = _synthetic_row(None, None, existing_status="no_lyrics", existing_basis=None)
        outcome = _picked(_BODY_B, 3, 1)
        assert should_replace(row, outcome, [_c(3, _BODY_B)]) is True
        v = replacement_verdict(row, outcome, [_c(3, _BODY_B)])
        assert (v.write, v.counter) == (False, "resweep_would_supersede")

    def test_best_of_supersession_still_writes(self):
        """Pre-Step-2 behaviour from the regular queue; shadow must not pause it."""
        row = _synthetic_row(_BODY_A, 1, existing_basis="best-of-ambiguous")
        v = replacement_verdict(row, _picked(_BODY_A, 1, 1), [_c(1, _BODY_A)])
        assert (v.write, v.counter) == (True, "resweep_superseded")
        assert v.evidence["resweep_shadow"]["verdict"] == "superseded"

    def test_best_of_lateral_swap_is_shadowed(self):
        """Review R1: a same-rung best-of → best-of body swap is the Step 2 lateral rule,
        not a pre-Step-2 supersession — it must stay shadowed."""
        row = _synthetic_row(_BODY_A, 1, existing_basis="best-of-ambiguous",
                             existing_matcher_version="step2-v2+best-of-v1")
        outcome = _picked(_BODY_B, 3, 2)
        outcome.match_basis = "best-of-review"
        cands = [_c(1, _BODY_A), _c(3, _BODY_B), _c(4, _BODY_B)]
        assert should_replace(row, outcome, cands) is True
        v = replacement_verdict(row, outcome, cands)
        assert (v.write, v.counter) == (False, "resweep_would_replace")

    def test_best_of_no_lyrics_supersession_is_shadowed(self):
        """The queue's best-of arm only ever selected MATCHED best-of rows; a best-of
        no_lyrics row is reached by the sweep alone."""
        row = _synthetic_row(None, None, existing_status="no_lyrics",
                             existing_basis="best-of-all_instrumental")
        v = replacement_verdict(row, _picked(_BODY_B, 3, 1), [_c(3, _BODY_B)])
        assert (v.write, v.counter) == (False, "resweep_would_supersede")

    def test_exact_title_row_superseded_by_a_stronger_basis_is_shadowed(self):
        """Only best-of rows were in the pre-Step-2 queue; an exact-title row reaches the
        ladder through the sweep alone."""
        row = _synthetic_row(_BODY_A, 1)
        outcome = _picked(_BODY_A, 1, 1)
        outcome.match_basis = "isrc"
        v = replacement_verdict(row, outcome, [_c(1, _BODY_A)])
        assert (v.write, v.counter) == (False, "resweep_would_supersede")

    def test_kept_rows_stamp_the_shadow_key(self):
        row = _synthetic_row(_BODY_A, 1)
        v = replacement_verdict(row, _picked(_BODY_A, 1, 1), [_c(1, _BODY_A)])
        assert (v.write, v.counter) == (False, "resweep_unchanged")
        assert list(v.evidence) == ["resweep_shadow"]

    def test_selection_excludes_on_the_shadow_stamp(self):
        session = MagicMock()
        session.execute.return_value.fetchall.return_value = []
        LyricsReassessmentService(session, client=_FakeClient())._fetch_stale_version_tracks(5)
        sql = " ".join(str(session.execute.call_args[0][0]).split())
        assert "'{resweep_shadow,matcher_version}'" in sql
        assert "'{resweep,matcher_version}'" not in sql

    def test_real_pass_is_the_shipped_default(self, monkeypatch):
        """Flipped after the owner-reviewed shadow drain (2026-10-16)."""
        from worker.core.config import Settings
        assert Settings.model_fields["LYRICS_RESWEEP_SHADOW"].default is False


class TestLockedBeatsEverything:
    def test_locked_unresolved_row_is_not_rewritten(self):
        """Review G1: reassess_track selects any status; a lock must win over 'unresolved
        rows are always rewritable'."""
        row = _synthetic_row(None, None, existing_status="not_found", existing_locked=True)
        assert should_replace(row, _picked(_BODY_B, 3, 2), [_c(3, _BODY_B)]) is False


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------
class _FakeClient:
    def __init__(self, result=None):
        self.result = result if result is not None else []

    def search_candidates(self, title, artist, **kw):
        return list(self.result)

    def close(self):
        pass


def _service(client, *, sweep=(), queue=()):
    session = MagicMock()
    svc = LyricsReassessmentService(session, client=client)
    svc._fetch_stale_version_tracks = MagicMock(return_value=list(sweep))
    svc._fetch_unresolved_tracks = MagicMock(return_value=list(queue))
    svc._attach_stored_bodies = lambda rows: rows
    return svc, session


def _statements(session):
    return [(" ".join(str(c[0][0]).split()), c[0][1] if len(c[0]) > 1 else None)
            for c in session.execute.call_args_list]


class TestConsistencyViolation:
    def test_violating_row_is_rotated_not_left_at_the_head(self, monkeypatch):
        """Review Y4: a deterministic invariant violation is never written, but it must
        rotate like a guard-kept row or it holds a stalest-first slot every run."""
        import worker.service.lyrics_eval_core as core
        monkeypatch.setattr(core, "_consistency_ok", lambda outcome: False)
        svc, session = _service(_FakeClient(_bh_cands()), sweep=[_bh_row(_bh_wrong())])
        m = svc.reassess()
        assert (m["consistency_errors"], m["evaluated"]) == (1, 0)
        [(sql, _)] = _statements(session)
        assert sql == "UPDATE track_lyrics SET updated_at = NOW() WHERE track_id = :track_id"


    def test_incremental_and_demand_callers_do_not_rotate_violations(self, monkeypatch):
        """Review Y-a: the demand-source job reads a touch as "evaluated" and would advance
        its backoff ladder; only reassessment callers opt in."""
        import worker.service.lyrics_eval_core as core
        monkeypatch.setattr(core, "_consistency_ok", lambda outcome: False)
        session = MagicMock()
        m = core.run_eval_batch(
            session, [_bh_row(_bh_wrong())], concurrency=1, time_budget_sec=30,
            client=_FakeClient(_bh_cands()), touch_on_guard_kept=True,
        )
        assert m["consistency_errors"] == 1
        session.execute.assert_not_called()


class TestReassessSweep:
    def test_replaced_row_is_written_with_the_stamp(self):
        svc, session = _service(_FakeClient(_bh_cands()), sweep=[_bh_row(_bh_wrong())])
        m = svc.reassess()
        assert (m["resweep_selected"], m["resweep_replaced"], m["evaluated"]) == (1, 1, 1)
        assert m["guard_kept"] == 0
        [(sql, params)] = _statements(session)
        assert sql.startswith("INSERT INTO track_lyrics")
        written = params[0] if isinstance(params, list) else params
        evidence = json.loads(written["evidence"])
        assert evidence["resweep"]["verdict"] == "replaced"
        assert evidence["content_cluster_size"] >= 2  # the outcome's own evidence survives
        assert written["matcher_version"] == MATCHER_VERSION

    def test_kept_row_is_stamped_not_rewritten(self):
        cands = _bh_cands()
        good_id = str(_bh_decide(cands).evidence["lrclib_id"])
        good = next(c for c in cands if str(c.id) == good_id)
        svc, session = _service(_FakeClient(cands), sweep=[_bh_row(good)])
        m = svc.reassess()
        assert (m["resweep_unchanged"], m["guard_kept"], m["evaluated"]) == (1, 1, 0)
        [(sql, params)] = _statements(session)
        assert sql.startswith("UPDATE track_lyrics SET evidence = COALESCE(evidence")
        set_clause = sql[sql.index(" SET "):sql.index(" WHERE ")]
        assert "lyric_" not in set_clause and "matcher_version" not in set_clause
        assert "match_status" not in set_clause
        assert json.loads(params["patch"])["resweep"]["verdict"] == "unchanged"

    def test_limits_default_to_settings_and_zero_empties_an_arm(self):
        svc, _ = _service(_FakeClient())
        svc.reassess()
        svc._fetch_stale_version_tracks.assert_called_once_with(settings.LYRICS_RESWEEP_BATCH_LIMIT)
        svc._fetch_unresolved_tracks.assert_called_once_with(settings.LYRICS_REASSESS_BATCH_LIMIT)

        svc, _ = _service(_FakeClient())
        svc.reassess(limit=0, sweep_limit=7)  # the by-hand drain
        svc._fetch_unresolved_tracks.assert_not_called()
        svc._fetch_stale_version_tracks.assert_called_once_with(7)

        svc, _ = _service(_FakeClient())
        svc.reassess(sweep_limit=0)
        svc._fetch_stale_version_tracks.assert_not_called()

    def test_a_row_due_in_both_arms_is_judged_once(self):
        row = _bh_row(_bh_wrong())
        svc, session = _service(_FakeClient(_bh_cands()), sweep=[row], queue=[dict(row)])
        m = svc.reassess()
        assert m["resweep_replaced"] == 1
        assert session.execute.call_count == 1

    def test_reassess_track_runs_one_row_through_the_same_gate(self):
        session = MagicMock()
        svc = LyricsReassessmentService(session, client=_FakeClient(_bh_cands()))
        svc._fetch_track = MagicMock(return_value=[_bh_row(_bh_wrong(), existing_locked=True)])
        svc._attach_stored_bodies = lambda rows: rows
        m = svc.reassess_track("815e9a06-ad9a-4ae7-bfdf-66638f90eeb7", cooldown_sec=0)
        svc._fetch_track.assert_called_once_with("815e9a06-ad9a-4ae7-bfdf-66638f90eeb7", 0)
        assert (m["resweep_locked"], m["evaluated"]) == (1, 0)
        [(sql, params)] = _statements(session)
        assert sql.startswith("UPDATE track_lyrics SET evidence")
        assert json.loads(params["patch"])["resweep"]["verdict"] == "locked"


# --------------------------------------------------------------------------
# SQL text
# --------------------------------------------------------------------------
def _sql_of(call):
    session = MagicMock()
    session.execute.return_value.fetchall.return_value = []
    svc = LyricsReassessmentService(session, client=_FakeClient())
    call(svc)
    return " ".join(str(session.execute.call_args[0][0]).split()), session.execute.call_args[0][1]


class TestSweepSelectionSQL:
    def _sql(self):
        return _sql_of(lambda s: s._fetch_stale_version_tracks(25))

    def test_resolved_rows_of_another_decision_core_only(self):
        sql, params = self._sql()
        assert "tl.match_status IN ('matched', 'no_lyrics')" in sql
        assert "split_part(COALESCE(tl.matcher_version, ''), '+', 1) <> :current_version" in sql
        assert params == {"current_version": MATCHER_VERSION, "limit": 25}

    def test_the_stamp_is_what_terminates_the_sweep(self):
        sql, _ = self._sql()
        assert "COALESCE(tl.evidence #>> '{resweep,matcher_version}', '') <> :current_version" in sql

    def test_hand_fixed_and_excluded_rows_are_not_spent_on(self):
        sql, _ = self._sql()
        assert "NOT (tl.evidence ? 'manual_fix')" in sql
        assert "COALESCE(tl.evidence ->> 'match_basis', '') <> 'manual'" in sql
        assert "NOT (tl.evidence ? 'excluded_by')" in sql

    def test_oldest_version_first_by_number_then_stalest(self):
        sql, _ = self._sql()
        order = sql[sql.index("ORDER BY"):]
        assert order.index("'^step(\\d+)'") < order.index("'^step\\d+-v(\\d+)'") \
            < order.index("tl.updated_at ASC")

    def test_projects_the_guard_fields(self):
        sql, _ = self._sql()
        assert "tl.matcher_version AS existing_matcher_version" in sql
        assert "AS existing_locked" in sql and "AS existing_lrclib_id" in sql
        assert "lyric_plain" not in sql  # bodies come later, for the chosen rows only


class TestTrackSelectionSQL:
    def test_scoped_with_cooldown_and_no_pool_filters(self):
        sql, params = _sql_of(lambda s: s._fetch_track("tid", 0))
        assert "WHERE t.id = :track_id" in sql
        assert "make_interval(secs => :cooldown_sec)" in sql
        assert params == {"track_id": "tid", "cooldown_sec": 0}
        assert "excluded_by" not in sql
        assert "? 'manual_fix')" not in sql.split("WHERE", 1)[1]


class TestAttachStoredBodies:
    def test_fetches_bodies_for_matched_rows_only(self):
        session = MagicMock()
        m_id, nf_id = uuid.uuid4(), uuid.uuid4()
        session.execute.return_value.fetchall.return_value = [(m_id, "plain", "[00:00.00]s")]
        svc = LyricsReassessmentService(session, client=_FakeClient())
        rows = svc._attach_stored_bodies([
            {"id": m_id, "existing_status": STATUS_MATCHED},
            {"id": nf_id, "existing_status": STATUS_NOT_FOUND},
        ])
        assert session.execute.call_args[0][1] == {"ids": [m_id]}
        assert (rows[0]["existing_lyric_plain"], rows[0]["existing_lyric_synced"]) == (
            "plain", "[00:00.00]s")
        assert (rows[1]["existing_lyric_plain"], rows[1]["existing_lyric_synced"]) == (None, None)

    def test_no_query_when_nothing_is_matched(self):
        session = MagicMock()
        svc = LyricsReassessmentService(session, client=_FakeClient())
        rows = svc._attach_stored_bodies([{"id": 1, "existing_status": STATUS_NOT_FOUND}])
        session.execute.assert_not_called()
        assert rows[0]["existing_lyric_plain"] is None


class TestPatchEvidence:
    def test_shallow_merge_bumps_updated_at_and_touches_nothing_else(self):
        session = MagicMock()
        assert TrackLyricsWriter(session).patch_evidence("tid", {"resweep": {"verdict": "kept"}})
        sql = " ".join(str(session.execute.call_args[0][0]).split())
        params = session.execute.call_args[0][1]
        assert sql == ("UPDATE track_lyrics SET evidence = COALESCE(evidence, '{}'::jsonb) "
                       "|| CAST(:patch AS jsonb), updated_at = NOW() WHERE track_id = :track_id")
        assert json.loads(params["patch"]) == {"resweep": {"verdict": "kept"}}
        session.commit.assert_called_once()
