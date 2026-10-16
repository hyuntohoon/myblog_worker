"""FEAT-lyrics-match-correction Step 3a — the lock, rejections and the owner's pick against a
REAL engine.

Why this exists ([[feedback-sa-session-lifecycle-mock-blind]]): the unit tests assert the
SQL text on MagicMock sessions. Whether ``ON CONFLICT … DO UPDATE … WHERE NOT locked``
really leaves a locked row alone, whether ``CAST(:ids AS uuid[])`` binds, and whether the
pick commits its write and the translation re-queue together, only a real driver can say.

CI provides TEST_DB_URL and the pinned canonical schema (V63). A local run without
TEST_DB_URL is skipped; an older local schema without V63 is skipped with a reason.
"""
from __future__ import annotations

import json
import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from worker.service.lyrics_candidates import load_rejections, replace_candidate_set
from worker.service.lyrics_matcher import (
    BASIS_MANUAL,
    STATUS_MATCHED,
    Candidate,
    MatchOutcome,
    TrackLyricsWriter,
)
from worker.service.lyrics_pick_service import LyricsPickService

_TEST_DB_URL = os.environ.get("TEST_DB_URL")

pytestmark = [
    pytest.mark.skipif(
        not _TEST_DB_URL,
        reason="integration test requires TEST_DB_URL env var (Postgres test database)",
    ),
    pytest.mark.rejections,  # the real rejection read, not the autouse stub
]


@pytest.fixture(scope="module")
def factory():
    eng = create_engine(_TEST_DB_URL, pool_pre_ping=True, future=True)
    with eng.connect() as conn:
        if conn.execute(text("SELECT to_regclass('track_lyrics_rejections')")).scalar() is None:
            pytest.skip("V63 (track_lyrics_rejections) missing from test database")
    return sessionmaker(bind=eng)


@pytest.fixture
def tracks(factory):
    """One album with three tracks; deleted (cascading every lyrics row) afterwards."""
    album = str(uuid.uuid4())
    ids = [str(uuid.uuid4()) for _ in range(3)]
    session = factory()
    try:
        with session.begin():
            session.execute(
                text("INSERT INTO albums (id, spotify_id, title) VALUES (:id, :sid, 'LMC 3a test')"),
                {"id": album, "sid": f"test_album_{uuid.uuid4()}"},
            )
            for tid in ids:
                session.execute(
                    text("INSERT INTO tracks (id, album_id, spotify_id, title) "
                         "VALUES (:id, :album, :sid, 'Song')"),
                    {"id": tid, "album": album, "sid": f"test_track_{uuid.uuid4()}"},
                )
        yield ids
    finally:
        session.rollback()
        with session.begin():
            session.execute(text("DELETE FROM albums WHERE id = :id"), {"id": album})
        session.close()


def _seed_row(session, tid, *, evidence, body="old body"):
    session.execute(
        text(
            "INSERT INTO track_lyrics (track_id, match_status, evidence, lyric_plain, matcher_version) "
            "VALUES (:tid, 'matched', CAST(:ev AS jsonb), :body, 'step4-v1')"
        ),
        {"tid": tid, "ev": json.dumps(evidence), "body": body},
    )
    session.commit()


def _row(session, tid):
    return session.execute(
        text("SELECT lyric_plain, evidence, matcher_version FROM track_lyrics WHERE track_id = :tid"),
        {"tid": tid},
    ).one()


def _auto(tid, body="new body"):
    return MatchOutcome(
        track_id=uuid.UUID(tid), match_status=STATUS_MATCHED, match_basis="exact-title",
        evidence={"lrclib_id": 2}, lyric_plain=body, version_agrees=True,
    )


def test_automatic_writes_never_touch_a_locked_row(factory, tracks):
    manual, hand_fixed, open_row = tracks
    session = factory()
    try:
        _seed_row(session, manual, evidence={"match_basis": BASIS_MANUAL, "lrclib_id": 1})
        _seed_row(session, hand_fixed, evidence={"match_basis": "exact-title", "manual_fix": {"by": "owner"}})
        _seed_row(session, open_row, evidence={"match_basis": "exact-title", "lrclib_id": 1})

        written = TrackLyricsWriter(session).write_outcomes([_auto(t) for t in tracks])

        assert written == 1
        assert _row(session, manual)[0] == "old body"
        assert _row(session, hand_fixed)[0] == "old body"
        assert _row(session, open_row)[0] == "new body"
    finally:
        session.close()


def test_the_manual_path_replaces_a_lock(factory, tracks):
    tid = tracks[0]
    session = factory()
    try:
        _seed_row(session, tid, evidence={"match_basis": "exact-title", "manual_fix": {"by": "owner"}})
        outcome = _auto(tid, body="owner pick")
        outcome.match_basis = BASIS_MANUAL
        TrackLyricsWriter(session).write_manual(outcome)
        session.commit()
        body, evidence, _ = _row(session, tid)
        assert body == "owner pick"
        assert evidence["match_basis"] == BASIS_MANUAL and "manual_fix" not in evidence
    finally:
        session.close()


def test_rejections_read_by_track_and_the_stored_set_is_replaced(factory, tracks):
    a, b, _ = tracks
    session = factory()
    try:
        session.execute(
            text("INSERT INTO track_lyrics_rejections (track_id, lrclib_id, rejected_by) "
                 "VALUES (:a, 11, 'owner'), (:a, 12, 'owner'), (:b, 13, 'owner')"),
            {"a": a, "b": b},
        )
        session.commit()
        assert load_rejections(session, [a, uuid.UUID(b)]) == {a: {11, 12}, b: {13}}

        row = {"lrclib_id": 5, "title": "Song", "artist": "Artist", "album": None,
               "duration_sec": 200.0, "has_synced": True, "has_plain": True,
               "instrumental": False, "preview": "line one / line two", "cluster_size": 2,
               "verdict": "picked"}
        replace_candidate_set(session, a, [row, {**row, "lrclib_id": 6, "verdict": "outvoted"}])
        replace_candidate_set(session, a, [{**row, "lrclib_id": 7}])
        ids = session.execute(
            text("SELECT lrclib_id FROM track_lyrics_candidates WHERE track_id = :a"), {"a": a}
        ).scalars().all()
        assert ids == [7]  # replaced wholesale, not merged
    finally:
        session.close()


class _GetClient:
    def __init__(self, cand):
        self.cand = cand

    def get_by_id(self, lrclib_id, **kw):
        return self.cand

    def close(self):
        pass


def test_pick_locks_the_row_and_requeues_only_a_non_manual_translation(factory, tracks):
    poller_tr, manual_tr, _ = tracks
    session = factory()
    try:
        for tid, origin in ((poller_tr, "poller"), (manual_tr, "manual")):
            _seed_row(session, tid, evidence={"match_basis": "exact-title", "lrclib_id": 1})
            session.execute(
                text("INSERT INTO track_lyrics_candidates (track_id, lrclib_id, title, artist, verdict, matcher_version) "
                     "VALUES (:tid, 23791915, 'Go Go', 'BTS', 'outvoted', 'step4-v1')"),
                {"tid": tid},
            )
            session.execute(
                text("INSERT INTO track_lyrics_translations (track_id, status, segments, source_fingerprint, origin) "
                     "VALUES (:tid, 'done', '[]'::jsonb, 'fp', :origin)"),
                {"tid": tid, "origin": origin},
            )
        session.commit()
        cand = Candidate(23791915, "Go Go", "BTS", "Love Yourself 轉 'Tear'", 235.0, False,
                         "korean body", "[00:01.00]korean body")

        for tid in (poller_tr, manual_tr):
            res = LyricsPickService(session, _GetClient(cand)).pick(tid, 23791915, "owner-sub")
            assert res["outcome"] == "picked"
            assert not session.in_transaction()  # the pick left nothing open behind it

        statuses = dict(session.execute(
            text("SELECT track_id::text, status FROM track_lyrics_translations "
                 "WHERE track_id = ANY(CAST(:ids AS uuid[]))"),
            {"ids": [poller_tr, manual_tr]},
        ).all())
        assert statuses == {poller_tr: "requested", manual_tr: "done"}

        body, evidence, version = _row(session, poller_tr)
        assert body == "korean body" and version == "manual"
        assert evidence["match_basis"] == BASIS_MANUAL and evidence["lrclib_id"] == 23791915

        # Locked: an automatic write now leaves it alone.
        assert TrackLyricsWriter(session).write_outcomes([_auto(poller_tr, "japanese body")]) == 0
        assert _row(session, poller_tr)[0] == "korean body"
    finally:
        session.close()


def test_a_rejection_made_after_the_decision_is_refused_at_write_time(factory, tracks):
    """Review finding 1: the run read rejections before its LRCLIB loop; the owner rejects
    the candidate meanwhile and takes the row down. The late write must not put it back."""
    tid, promoted, _ = tracks
    session = factory()
    try:
        for t in (tid, promoted):
            session.execute(
                text("INSERT INTO track_lyrics (track_id, match_status, evidence, matcher_version) "
                     "VALUES (:t, 'review_required', '{\"reason\": \"owner_rejected\"}'::jsonb, 'step4-v1')"),
                {"t": t},
            )
            session.execute(
                text("INSERT INTO track_lyrics_rejections (track_id, lrclib_id, rejected_by) "
                     "VALUES (:t, 2, 'owner')"), {"t": t},
            )
        session.commit()
        late = _auto(tid)  # evidence.lrclib_id = 2, the rejected id
        best_of = _auto(promoted)
        best_of.match_basis = "best-of-ambiguous"
        best_of.evidence = {"promotion": {"lrclib_id": 2}}
        writer = TrackLyricsWriter(session)
        assert writer.write_one(late) == "refused"
        assert writer.write_one(best_of) == "refused"
        for t in (tid, promoted):
            assert session.execute(
                text("SELECT match_status FROM track_lyrics WHERE track_id = :t"), {"t": t}
            ).scalar() == "review_required"
        other = _auto(tid)
        other.evidence = {"lrclib_id": 3}
        assert writer.write_one(other) == "written"  # any other candidate still may
    finally:
        session.close()
