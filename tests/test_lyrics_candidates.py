"""FEAT-lyrics-match-correction Step 3a: candidate panel, rejections, lock, owner pick.

Judging runs on the real LRCLIB responses in ``tests/fixtures/lyrics_consensus.json`` (Bad
Habit, Go Go) and on small synthetic bodies. Everything here runs on MagicMock sessions;
the real-Postgres checks of the lock and the pick are in
``tests/integration/test_lyrics_pick_db.py``.
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from worker.clients.lrclib_client import LrclibTransientError
from worker.service import lyrics_candidates as lc
from worker.service.lyrics_eval_core import run_eval_batch
from worker.service.lyrics_matcher import (
    BASIS_MANUAL,
    STATUS_MATCHED,
    STATUS_NO_LYRICS,
    Candidate,
    MatchOutcome,
    TrackLyricsWriter,
    decide_match,
    locked_row_sql,
)
from worker.service.lyrics_pick_service import LyricsPickService
from worker.service.lyrics_reassessment_service import should_replace

_FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "lyrics_consensus.json").read_text()
)
_BH = _FIXTURE["bad_habit"]
_BH_TRACK = _BH["track"]


def _cands(key):
    return [Candidate.from_api(r) for r in _FIXTURE[key]["candidates"]]


def _c(cid, body, *, title="Song", artist="Artist", duration=200.0, instrumental=False):
    return Candidate(
        id=cid, title=title, artist=artist, album="Album", duration_sec=duration,
        instrumental=instrumental, plain_lyrics=body,
        synced_lyrics=f"[00:00.00]{body}" if body else None,
    )


_BODY_A = "the river runs down to the sea and carries every stone along with it tonight"
_BODY_B = "neon signs burn bright over empty streets while the city keeps on humming low"


def _judge(cands, outcome, *, title="Song", artists=("Artist",), duration=200.0, rejected=None):
    return {
        r["lrclib_id"]: r
        for r in lc.judge_candidates(
            title=title, artist_names=list(artists), aliases=None, duration_sec=duration,
            candidates=cands, outcome=outcome, rejected=rejected,
        )
    }


def _decide(cands, *, title="Song", artists=("Artist",), aliases=None, duration=200.0):
    return decide_match(uuid.uuid4(), title, list(artists), aliases, duration, cands)


# --------------------------------------------------------------------------
# judging
# --------------------------------------------------------------------------
class TestJudgeBadHabit:
    def _rows(self):
        cands = _cands("bad_habit")
        outcome = decide_match(
            uuid.uuid4(), _BH_TRACK["title"], _BH_TRACK["artists"], None,
            _BH_TRACK["duration_sec"], cands,
        )
        return outcome, _judge(
            cands, outcome, title=_BH_TRACK["title"], artists=_BH_TRACK["artists"],
            duration=_BH_TRACK["duration_sec"],
        )

    def test_the_pick_and_the_ed_sheeran_body(self):
        outcome, rows = self._rows()
        picked = int(outcome.evidence["lrclib_id"])
        assert rows[picked]["verdict"] == lc.VERDICT_PICKED
        assert rows[picked]["cluster_size"] == outcome.evidence["content_cluster_size"]
        wrong = rows[int(_BH["wrong_id"])]
        assert wrong["verdict"] == lc.VERDICT_OUTVOTED
        assert wrong["cluster_size"] == 1

    def test_every_row_is_one_distinct_id_with_a_bounded_preview(self):
        _, rows = self._rows()
        assert len(rows) == len({c.id for c in _cands("bad_habit")})
        for r in rows.values():
            assert r["preview"] is None or len(r["preview"]) <= lc.PREVIEW_MAX_CHARS
            assert "[" not in (r["preview"] or "")[:1]  # timestamps stripped

    def test_rejecting_the_winner_shows_rejected_and_takes_it_out_of_the_vote(self):
        outcome, rows = self._rows()
        picked = int(outcome.evidence["lrclib_id"])
        rejected = _judge(
            _cands("bad_habit"), outcome, title=_BH_TRACK["title"],
            artists=_BH_TRACK["artists"], duration=_BH_TRACK["duration_sec"],
            rejected={picked},
        )
        assert rejected[picked]["verdict"] == lc.VERDICT_REJECTED
        assert rejected[picked]["cluster_size"] is None
        same = [r for i, r in rejected.items() if rows[i]["verdict"] == lc.VERDICT_SAME_BODY]
        assert same and all(r["cluster_size"] == rows[i]["cluster_size"] - 1
                            for i, r in rejected.items() if rows[i]["verdict"] == lc.VERDICT_SAME_BODY)


class TestJudgeGoGo:
    def test_the_japanese_labelled_upload_is_a_version_mismatch(self):
        cands = _cands("go_go")
        outcome = decide_match(uuid.uuid4(), "Go Go", ["BTS"], ["방탄소년단"], 237.0, cands)
        rows = _judge(cands, outcome, title="Go Go", artists=["BTS", "방탄소년단"], duration=237.0)
        ja = rows[int(_FIXTURE["go_go"]["japanese_labelled_id"])]
        assert ja["verdict"] == lc.VERDICT_VERSION_MISMATCH

    def test_the_korean_minority_body_is_visible_as_outvoted(self):
        """Class B′: the panel's reason to exist. The Korean upload loses the vote and
        must still be on the panel for the owner to pick."""
        cands = _cands("go_go")
        outcome = decide_match(uuid.uuid4(), "Go Go", ["BTS"], ["방탄소년단"], 237.0, cands)
        rows = _judge(cands, outcome, title="Go Go", artists=["BTS", "방탄소년단"], duration=237.0)
        ko = rows[int(_FIXTURE["go_go"]["korean_id"])]
        assert ko["verdict"] == lc.VERDICT_OUTVOTED


class TestJudgeGates:
    def test_metadata_gates_in_matcher_order(self):
        cands = [
            _c(1, _BODY_A),
            _c(8, _BODY_A),
            _c(2, _BODY_A, artist="Somebody Else"),
            _c(3, _BODY_A, title="Entirely Different"),
            _c(4, _BODY_A, duration=260.0),
            _c(5, None, instrumental=True),
            _c(6, _BODY_A, title="Song (Live)"),
            _c(7, _BODY_B),
        ]
        outcome = _decide(cands)
        rows = _judge(cands, outcome)
        picked = int(outcome.evidence["lrclib_id"])
        assert picked in (1, 8)
        assert rows[picked]["verdict"] == lc.VERDICT_PICKED
        assert rows[9 - picked]["verdict"] == lc.VERDICT_SAME_BODY
        assert rows[picked]["cluster_size"] == 2
        assert rows[2]["verdict"] == lc.VERDICT_ARTIST_MISMATCH
        assert rows[3]["verdict"] == lc.VERDICT_TITLE_MISMATCH
        assert rows[4]["verdict"] == lc.VERDICT_DURATION_MISMATCH
        assert rows[5]["verdict"] == lc.VERDICT_NO_BODY
        assert rows[6]["verdict"] == lc.VERDICT_VERSION_MISMATCH  # plausible, other stance
        assert rows[7]["verdict"] == lc.VERDICT_OUTVOTED
        assert rows[2]["cluster_size"] is None  # failed a gate: no vote

    def test_a_language_version_reads_as_a_version_not_another_song(self):
        cands = [_c(1, _BODY_A), _c(2, _BODY_A), _c(3, _BODY_B, title="Song (Japanese Ver.)")]
        rows = _judge(cands, _decide(cands))
        assert rows[3]["verdict"] == lc.VERDICT_VERSION_MISMATCH
        assert rows[3]["cluster_size"] is None

    def test_nothing_picked_reads_undecided(self):
        # Two bodies, one each: the matcher parks (content_disagreement) and picks nothing.
        cands = [_c(1, _BODY_A), _c(2, _BODY_B)]
        outcome = _decide(cands)
        assert outcome.match_status != STATUS_MATCHED
        rows = _judge(cands, outcome)
        assert {r["verdict"] for r in rows.values()} == {lc.VERDICT_UNDECIDED}

    def test_duplicate_and_idless_uploads_collapse(self):
        cands = [_c(1, _BODY_A), _c(1, _BODY_A), _c(None, _BODY_A)]
        assert list(_judge(cands, _decide(cands))) == [1]


class TestPreview:
    def test_strips_timestamps_caps_lines_and_length(self):
        body = "\n".join(f"[00:0{i}.00] line {i} " + "x" * 80 for i in range(9))
        p = lc.preview_of(Candidate(1, "t", "a", None, 1.0, False, None, body))
        assert p.startswith("line 0") and "[" not in p
        assert p.count(" / ") <= lc.PREVIEW_MAX_LINES - 1
        assert len(p) <= lc.PREVIEW_MAX_CHARS and p.endswith("…")

    def test_empty_body_has_no_preview(self):
        assert lc.preview_of(Candidate(1, "t", "a", None, 1.0, True, "", None)) is None


# --------------------------------------------------------------------------
# the eval loop: rejections and the stored set
# --------------------------------------------------------------------------
class _FakeClient:
    def __init__(self, result=None):
        self.result = result or []

    def search_candidates(self, title, artist, **kw):
        return list(self.result)

    def close(self):
        pass


def _track_row(**over):
    row = {"id": uuid.uuid4(), "title": "Song", "duration_sec": 200.0,
           "artist_names": ["Artist"], "aliases": []}
    row.update(over)
    return row


def _written(session):
    for call in session.execute.call_args_list:
        sql = " ".join(str(call[0][0]).split())
        if sql.startswith("INSERT INTO track_lyrics ("):
            return call[0][1]
    return None


@pytest.mark.rejections
class TestRejectionsInTheLoop:
    def test_a_rejected_majority_never_wins(self, monkeypatch):
        """Two uploads share body A, one carries B. A wins the vote — until the owner
        rejects both A uploads; then B is the only candidate left and wins."""
        row = _track_row()
        cands = [_c(1, _BODY_A), _c(2, _BODY_A), _c(3, _BODY_B)]
        monkeypatch.setattr(
            "worker.service.lyrics_eval_core.load_rejections",
            lambda session, ids: {str(row["id"]): {1, 2}},
        )
        session = MagicMock()
        m = run_eval_batch(session, [row], concurrency=1, time_budget_sec=30,
                           client=_FakeClient(cands))
        assert m[STATUS_MATCHED] == 1
        assert json.loads(_written(session)["evidence"])["lrclib_id"] == 3

    def test_the_real_loader_reads_by_track_and_commits_before_lrclib(self):
        session = MagicMock()
        tid = uuid.uuid4()
        session.execute.return_value.fetchall.return_value = [(tid, 7), (tid, 9)]
        assert lc.load_rejections(session, [tid]) == {str(tid): {7, 9}}
        sql = " ".join(str(session.execute.call_args[0][0]).split())
        assert sql.startswith("SELECT track_id, lrclib_id FROM track_lyrics_rejections")
        session.commit.assert_called_once()

    def test_no_tracks_no_read(self):
        session = MagicMock()
        assert lc.load_rejections(session, []) == {}
        session.execute.assert_not_called()


class TestStoredSet:
    def test_scheduled_runs_never_store_the_set(self):
        session = MagicMock()
        run_eval_batch(session, [_track_row()], concurrency=1, time_budget_sec=30,
                       client=_FakeClient([_c(1, _BODY_A)]))
        assert not any("track_lyrics_candidates" in str(c[0][0])
                       for c in session.execute.call_args_list)

    def test_persisted_set_is_replaced_in_id_order(self):
        session = MagicMock()
        row = _track_row()
        run_eval_batch(session, [row], concurrency=1, time_budget_sec=30,
                       client=_FakeClient([_c(9, _BODY_A), _c(4, _BODY_A)]),
                       persist_candidates=True)
        panel = [(" ".join(str(c[0][0]).split()), c[0][1]) for c in session.execute.call_args_list
                 if "track_lyrics_candidates" in str(c[0][0])]
        assert panel[0] == ("DELETE FROM track_lyrics_candidates WHERE track_id = :tid",
                            {"tid": row["id"]})
        assert [p["lrclib_id"] for _, p in panel[1:]] == [4, 9]
        assert {p["verdict"] for _, p in panel[1:]} == {lc.VERDICT_PICKED, lc.VERDICT_SAME_BODY}

    def test_a_failed_store_never_costs_the_evaluation(self, monkeypatch):
        def boom(session, track_id, rows):
            raise RuntimeError("db hiccup")
        monkeypatch.setattr("worker.service.lyrics_eval_core.replace_candidate_set", boom)
        session = MagicMock()
        m = run_eval_batch(session, [_track_row()], concurrency=1, time_budget_sec=30,
                           client=_FakeClient([_c(1, _BODY_A)]), persist_candidates=True)
        assert m["evaluated"] == 1
        session.rollback.assert_called()


# --------------------------------------------------------------------------
# the lock
# --------------------------------------------------------------------------
def _outcome(basis="exact-title", status=STATUS_MATCHED):
    return MatchOutcome(
        track_id=uuid.uuid4(), match_status=status, match_basis=basis,
        evidence={"lrclib_id": 1}, lyric_plain=_BODY_A, version_agrees=True,
    )


class TestWriterLock:
    def test_automatic_upsert_refuses_a_locked_row(self):
        session = MagicMock()
        session.execute.return_value.rowcount = 1
        TrackLyricsWriter(session).write_outcomes([_outcome()])
        sql = " ".join(str(session.execute.call_args[0][0]).split())
        assert sql.endswith("WHERE NOT " + locked_row_sql("track_lyrics"))

    def test_a_refused_write_is_counted_locked_not_written(self):
        session = MagicMock()
        session.execute.return_value.rowcount = 0
        assert TrackLyricsWriter(session).write_outcomes([_outcome()]) == 0
        session.commit.assert_called_once()
        session.rollback.assert_not_called()

    def test_the_manual_path_writes_over_a_lock_and_never_commits_itself(self):
        session = MagicMock()
        TrackLyricsWriter(session).write_manual(_outcome(basis=BASIS_MANUAL))
        sql = " ".join(str(session.execute.call_args[0][0]).split())
        assert "WHERE NOT" not in sql
        session.commit.assert_not_called()

    def test_the_manual_path_only_writes_manual_outcomes(self):
        with pytest.raises(ValueError):
            TrackLyricsWriter(MagicMock()).write_manual(_outcome())

    def test_lock_predicate_names_both_lock_keys(self):
        assert locked_row_sql("tl") == (
            "((tl.evidence ? 'manual_fix') "
            "OR COALESCE(tl.evidence ->> 'match_basis', '') = 'manual')"
        )


class TestManualRung:
    def test_no_automatic_basis_outranks_a_manual_no_lyrics(self):
        """The demand-source selection projects no ``existing_locked``; the ladder alone
        must still refuse to replace the owner's "no lyrics" (writer refuses too)."""
        row = {"existing_status": STATUS_NO_LYRICS, "existing_basis": BASIS_MANUAL}
        for basis in ("exact-title", "mb-recording", "isrc"):
            assert should_replace(row, _outcome(basis=basis)) is False


# --------------------------------------------------------------------------
# the owner's pick
# --------------------------------------------------------------------------
class _GetClient:
    def __init__(self, cand=None, exc=None):
        self.cand, self.exc, self.calls = cand, exc, []

    def get_by_id(self, lrclib_id, **kw):
        self.calls.append(lrclib_id)
        if self.exc:
            raise self.exc
        return self.cand

    def close(self):
        pass


_TID = "815e9a06-ad9a-4ae7-bfdf-66638f90eeb7"


def _state(**over):
    st = {"has_track": True, "in_set": True, "rejected": False, "match_status": STATUS_MATCHED,
          "basis": "exact-title", "current_id": "8996695", "matcher_version": "step4-v1",
          "lyric_plain": "old body", "lyric_synced": None}
    st.update(over)
    return st


def _pick_session(state, requeued=1):
    session = MagicMock()
    read = MagicMock()
    read.mappings.return_value.one.return_value = state
    write = MagicMock()
    requeue = MagicMock(rowcount=requeued)
    session.execute.side_effect = [read, write, requeue]
    return session


def _sqls(session):
    return [" ".join(str(c[0][0]).split()) for c in session.execute.call_args_list]


class TestPick:
    @pytest.mark.parametrize("over,reason", [
        ({"has_track": False}, "unknown_track"),
        ({"rejected": True}, "rejected"),
        ({"in_set": False}, "not_a_candidate"),
    ])
    def test_refusals_never_call_lrclib(self, over, reason):
        client = _GetClient(_c(5, _BODY_A))
        session = _pick_session(_state(**over))
        assert LyricsPickService(session, client).pick(_TID, 5, "sub") == {
            "outcome": "refused", "reason": reason}
        assert client.calls == []
        assert len(_sqls(session)) == 1

    def test_malformed_message_is_refused(self):
        assert LyricsPickService(MagicMock(), _GetClient()).pick("nope", 5, "sub")["reason"] == "malformed"

    def test_pick_writes_manual_locked_and_requeues_a_changed_translation(self):
        client = _GetClient(_c(5, _BODY_A))
        session = _pick_session(_state())
        res = LyricsPickService(session, client).pick(_TID, "5", "owner-sub")
        assert res == {"outcome": "picked", "status": STATUS_MATCHED, "translation_requeued": 1}
        assert client.calls == [5]
        read, write, requeue = _sqls(session)
        assert write.startswith("INSERT INTO track_lyrics") and "WHERE NOT" not in write
        params = session.execute.call_args_list[1][0][1]
        ev = json.loads(params["evidence"])
        assert ev["match_basis"] == BASIS_MANUAL and ev["lrclib_id"] == 5
        assert ev["manual"]["by"] == "owner-sub" and ev["manual"]["action"] == "pick"
        assert ev["manual"]["previous"]["lrclib_id"] == "8996695"
        assert params["matcher_version"] == "manual"
        assert requeue.startswith("UPDATE track_lyrics_translations")
        assert "origin IS DISTINCT FROM 'manual'" in requeue
        assert session.commit.call_count == 2  # the read, then write + requeue together

    def test_same_body_does_not_requeue(self):
        cand = _c(5, _BODY_A)
        session = _pick_session(_state(lyric_plain=cand.plain_lyrics, lyric_synced=cand.synced_lyrics))
        res = LyricsPickService(session, _GetClient(cand)).pick(_TID, 5, "sub")
        assert res["translation_requeued"] == 0
        assert len(_sqls(session)) == 2

    def test_instrumental_pick_is_locked_no_lyrics(self):
        session = _pick_session(_state())
        res = LyricsPickService(session, _GetClient(_c(5, None, instrumental=True))).pick(_TID, 5, "sub")
        assert res["status"] == STATUS_NO_LYRICS
        params = session.execute.call_args_list[1][0][1]
        assert params["lyric_plain"] == "" and params["lyric_synced"] is None
        assert len(_sqls(session)) == 2

    def test_vanished_record_is_dropped(self):
        session = _pick_session(_state())
        assert LyricsPickService(session, _GetClient(None)).pick(_TID, 5, "sub") == {"outcome": "gone"}
        assert len(_sqls(session)) == 1

    def test_transient_lrclib_error_propagates_for_sqs_redelivery(self):
        session = _pick_session(_state())
        with pytest.raises(LrclibTransientError):
            LyricsPickService(session, _GetClient(exc=LrclibTransientError("429"))).pick(_TID, 5, "sub")
        assert len(_sqls(session)) == 1


class TestHandlerRoute:
    def test_sqs_pick_message_reaches_the_service(self, monkeypatch):
        import worker.handler as handler
        seen = []
        monkeypatch.setattr(handler, "_run_lyrics_pick", lambda *a: seen.append(a))
        res = handler.lambda_handler({"Records": [{
            "messageId": "m1",
            "body": json.dumps({"job": "lyrics_pick", "track_id": _TID, "lrclib_id": 5, "actor": "s"}),
        }]}, None)
        assert seen == [(_TID, 5, "s")]
        assert res == {"batchItemFailures": []}
