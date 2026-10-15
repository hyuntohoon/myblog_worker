"""FEAT-lyrics-match-correction Step 2 re-sweep — real engine on disposable Postgres 16.

The sweep's correctness is mostly SQL: which resolved rows count as "another matcher's",
the numeric version ordering, the stamp that makes the arm terminate, the lock and
exclusion filters, and the shallow ``||`` merge that must not lose ``manual_fix``. A mocked
session asserts only the text ([[feedback-sa-session-lifecycle-mock-blind]]); this file
executes it.

Two tiers:

  * selection — seed one track per filter case and read ``_fetch_stale_version_tracks``;
  * round trip — the Bad Habit row (stored with the wrong upload's body by ``step2-v2``)
    through ``reassess_track`` with a fake LRCLIB client serving the real 2026-10-16
    response: a replaced row leaves the sweep by its new ``matcher_version``; a kept row
    leaves it by its stamp, with its body and version untouched; a locked row is judged
    but never rewritten.

Only rows seeded here are asserted on; the selection runs with a large limit so a shared
test database's other rows cannot push ours out. Guarded by TEST_DB_URL; skipped when unset.
"""
from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from worker.service.lyrics_matcher import MATCHER_VERSION, Candidate, TrackLyricsWriter
from worker.service.lyrics_reassessment_service import LyricsReassessmentService

_TEST_DB_URL = os.environ.get("TEST_DB_URL")

pytestmark = pytest.mark.skipif(
    not _TEST_DB_URL,
    reason="integration test requires TEST_DB_URL env var (Postgres test database)",
)

_PREFIX = "lyr_resweep_"
_FIXTURE = json.loads(
    (Path(__file__).parent.parent / "fixtures" / "lyrics_consensus.json").read_text()
)
_BAD_HABIT = _FIXTURE["bad_habit"]
_WRONG_ID = str(_BAD_HABIT["wrong_id"])
_ALL = 1_000_000  # selection limit: "everything", so only our own rows are asserted on


@pytest.fixture(scope="module")
def factory():
    eng = create_engine(_TEST_DB_URL, pool_pre_ping=True, future=True)
    Session = sessionmaker(bind=eng)
    yield Session
    _cleanup(Session)
    eng.dispose()


def _cleanup(Session):
    with Session() as s, s.begin():
        s.execute(
            text("DELETE FROM track_lyrics WHERE track_id IN "
                 "(SELECT id FROM tracks WHERE spotify_id LIKE :p)"),
            {"p": f"{_PREFIX}%"},
        )
        s.execute(text("DELETE FROM tracks WHERE spotify_id LIKE :p"), {"p": f"{_PREFIX}%"})
        s.execute(text("DELETE FROM albums WHERE spotify_id LIKE :p"), {"p": f"{_PREFIX}%"})
        s.execute(text("DELETE FROM artists WHERE spotify_id LIKE :p"), {"p": f"{_PREFIX}%"})


def _seed(session, *, tag, status="matched", version="step2-v2", evidence=None,
          plain="la la la", synced=None, title="Resweep Song", artist="Resweep Artist",
          duration=200, age_days=1):
    """One album + artist + track + track_lyrics row. Returns the track id."""
    sfx = f"{_PREFIX}{tag}_{uuid.uuid4().hex[:8]}"
    album_id, track_id, artist_id = (str(uuid.uuid4()) for _ in range(3))
    session.execute(
        text("INSERT INTO albums (id, spotify_id, title) VALUES (:id, :sid, 'Resweep')"),
        {"id": album_id, "sid": f"{sfx}_alb"},
    )
    session.execute(
        text("INSERT INTO tracks (id, album_id, spotify_id, title, duration_sec) "
             "VALUES (:id, :alb, :sid, :title, :dur)"),
        {"id": track_id, "alb": album_id, "sid": f"{sfx}_trk", "title": title, "dur": duration},
    )
    session.execute(
        text("INSERT INTO artists (id, spotify_id, name, popularity) "
             "VALUES (:id, :sid, :name, 50)"),
        {"id": artist_id, "sid": f"{sfx}_art", "name": artist},
    )
    session.execute(
        text("INSERT INTO track_artists (track_id, artist_id) VALUES (:t, :a)"),
        {"t": track_id, "a": artist_id},
    )
    resolved = status in ("matched", "no_lyrics")
    session.execute(
        text("INSERT INTO track_lyrics (track_id, match_status, evidence, lyric_plain, "
             "lyric_synced, matcher_version, updated_at) VALUES (:t, :st, CAST(:ev AS jsonb), "
             ":plain, :synced, :ver, NOW() - make_interval(days => :age))"),
        {"t": track_id, "st": status,
         "ev": json.dumps(evidence if evidence is not None else {"match_basis": "exact-title"}),
         "plain": plain if resolved else None, "synced": synced if resolved else None,
         "ver": version, "age": age_days},
    )
    return track_id


def _swept_ids(session, ours):
    rows = LyricsReassessmentService(session)._fetch_stale_version_tracks(_ALL)
    session.commit()
    return [str(r["id"]) for r in rows if str(r["id"]) in ours]


def _stored(session, track_id):
    r = session.execute(
        text("SELECT match_status, evidence, lyric_synced, matcher_version "
             "FROM track_lyrics WHERE track_id = :t"),
        {"t": track_id},
    ).one()
    session.commit()
    return {"status": r[0], "evidence": r[1], "synced": r[2], "version": r[3]}


# ── Tier 1: the sweep selection ─────────────────────────────────────────────────────


def test_sweep_selects_only_resolved_rows_of_another_matcher_in_version_order(factory):
    basis = {"match_basis": "exact-title"}
    with factory() as s:
        with s.begin():
            ids = {
                "null_version": _seed(s, tag="nullv", version=None),
                "step2": _seed(s, tag="s2", version="step2-v2", age_days=3),
                "step2_bestof": _seed(s, tag="s2bo", version="step2-v2+best-of-v1",
                                      evidence={"match_basis": "best-of-ambiguous"}),
                "step3": _seed(s, tag="s3", version="step3-v1", age_days=9),
                "step10": _seed(s, tag="s10", version="step10-v1", age_days=30),
                "no_lyrics": _seed(s, tag="nol", status="no_lyrics", version="step3-v2"),
                # never selected:
                "current": _seed(s, tag="cur", version=MATCHER_VERSION),
                "current_bestof": _seed(s, tag="curbo", version=f"{MATCHER_VERSION}+best-of-v1"),
                "stamped": _seed(s, tag="stamp", evidence={
                    **basis, "resweep": {"matcher_version": MATCHER_VERSION}}),
                "manual_fix": _seed(s, tag="mfix", evidence={**basis, "manual_fix": {"by": "owner"}}),
                "manual_basis": _seed(s, tag="mbas", evidence={"match_basis": "manual"}),
                "excluded": _seed(s, tag="excl", evidence={**basis, "excluded_by": "label"}),
                "unresolved": _seed(s, tag="unres", status="not_found"),
            }
        # an older stamp does not count: it was judged under another version
        with s.begin():
            ids["old_stamp"] = _seed(s, tag="oldst", version="step3-v1", age_days=2, evidence={
                **basis, "resweep": {"matcher_version": "step3-v1"}})

        swept = _swept_ids(s, set(ids.values()))

    by_id = {v: k for k, v in ids.items()}
    assert [by_id[i] for i in swept] == [
        "null_version",                    # unparseable ⇒ 0, first
        "step2", "step2_bestof",           # step2-v2, stalest first (3 days, then 1 day)
        "step3", "old_stamp",              # step3-v1, stalest first (9 days, then 2 days)
        "no_lyrics",                       # step3-v2
        "step10",                          # numeric, not lexical: after step3
    ]


def test_bodies_are_attached_for_matched_rows_only(factory):
    with factory() as s:
        with s.begin():
            m = _seed(s, tag="bodym", plain="plain body", synced="[00:00.00]synced body")
            n = _seed(s, tag="bodyn", status="not_found")
        svc = LyricsReassessmentService(s)
        rows = svc._attach_stored_bodies(
            [{"id": m, "existing_status": "matched"}, {"id": n, "existing_status": "not_found"}]
        )
        s.commit()
    assert (rows[0]["existing_lyric_plain"], rows[0]["existing_lyric_synced"]) == (
        "plain body", "[00:00.00]synced body")
    assert rows[1]["existing_lyric_plain"] is None


def test_patch_evidence_merges_shallowly_and_stamps_the_row_out(factory):
    with factory() as s:
        with s.begin():
            t = _seed(s, tag="patch", evidence={"match_basis": "exact-title",
                                                "promotion": {"x": 1}, "lrclib_id": 7})
        assert TrackLyricsWriter(s).patch_evidence(
            t, {"resweep": {"matcher_version": MATCHER_VERSION, "verdict": "kept"}})
        after = _stored(s, t)
        assert after["evidence"]["promotion"] == {"x": 1}       # other keys survive
        assert after["evidence"]["lrclib_id"] == 7
        assert after["evidence"]["resweep"]["verdict"] == "kept"
        assert after["version"] == "step2-v2"                   # still the old matcher's row
        assert _swept_ids(s, {t}) == []                         # ...but out of the sweep


# ── Tier 2: round trip through reassess_track ───────────────────────────────────────


class _FakeClient:
    def __init__(self, records):
        self.records = records

    def search_candidates(self, title, artist, **kw):
        return [Candidate.from_api(r) for r in self.records]

    def close(self):
        pass


def _wrong_upload():
    return next(r for r in _BAD_HABIT["candidates"] if str(r["id"]) == _WRONG_ID)


def _seed_bad_habit(s, tag, stored, evidence_extra=None):
    t = _BAD_HABIT["track"]
    return _seed(
        s, tag=tag, title=t["title"], artist=t["artists"][0], duration=t["duration_sec"],
        plain=stored.get("plainLyrics") or "-", synced=stored.get("syncedLyrics"),
        evidence={"match_basis": "exact-title", "lrclib_id": int(stored["id"]),
                  **(evidence_extra or {})},
    )


def test_wrong_body_is_replaced_and_leaves_the_sweep(factory):
    with factory() as s:
        with s.begin():
            t = _seed_bad_habit(s, "bh_wrong", _wrong_upload())
        svc = LyricsReassessmentService(s, client=_FakeClient(_BAD_HABIT["candidates"]))
        m = svc.reassess_track(t, cooldown_sec=0)
        assert (m["resweep_replaced"], m["evaluated"]) == (1, 1)
        after = _stored(s, t)
        assert after["version"] == MATCHER_VERSION
        assert str(after["evidence"]["lrclib_id"]) != _WRONG_ID
        assert after["evidence"]["resweep"]["verdict"] == "replaced"
        assert str(after["evidence"]["resweep"]["replaced_lrclib_id"]) == _WRONG_ID
        assert after["synced"] != _wrong_upload()["syncedLyrics"]
        assert _swept_ids(s, {t}) == []


def test_kept_row_is_stamped_with_body_and_version_untouched(factory):
    client = _FakeClient(_BAD_HABIT["candidates"])
    with factory() as s:
        # Find the majority pick first, then store exactly that body.
        with s.begin():
            probe = _seed_bad_habit(s, "bh_probe", _wrong_upload())
        LyricsReassessmentService(s, client=client).reassess_track(probe, cooldown_sec=0)
        good_id = str(_stored(s, probe)["evidence"]["lrclib_id"])
        good = next(r for r in _BAD_HABIT["candidates"] if str(r["id"]) == good_id)

        with s.begin():
            t = _seed_bad_habit(s, "bh_good", good)
        assert _swept_ids(s, {t}) == [t]
        m = LyricsReassessmentService(s, client=client).reassess_track(t, cooldown_sec=0)
        assert (m["resweep_unchanged"], m["evaluated"]) == (1, 0)
        after = _stored(s, t)
        assert after["version"] == "step2-v2"
        assert after["synced"] == good.get("syncedLyrics")
        assert after["evidence"]["resweep"]["verdict"] == "unchanged"
        assert _swept_ids(s, {t}) == []


def test_locked_row_is_judged_but_never_rewritten(factory):
    """The production Bad Habit row carries manual_fix; reassess_track bypasses the sweep's
    lock filter at selection but not the guard."""
    with factory() as s:
        with s.begin():
            t = _seed_bad_habit(s, "bh_locked", _wrong_upload(),
                                evidence_extra={"manual_fix": {"by": "owner"}})
        assert _swept_ids(s, {t}) == []
        svc = LyricsReassessmentService(s, client=_FakeClient(_BAD_HABIT["candidates"]))
        m = svc.reassess_track(t, cooldown_sec=0)
        assert (m["resweep_locked"], m["evaluated"]) == (1, 0)
        after = _stored(s, t)
        assert after["version"] == "step2-v2"
        assert str(after["evidence"]["lrclib_id"]) == _WRONG_ID
        assert after["evidence"]["manual_fix"] == {"by": "owner"}
        assert after["evidence"]["resweep"]["verdict"] == "locked"
