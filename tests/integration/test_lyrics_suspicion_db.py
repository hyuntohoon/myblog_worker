"""FEAT-lyrics-match-correction Step 4 suspicion sweep — real engine.

What only a real driver can say ([[feedback-sa-session-lifecycle-mock-blind]]): whether the
timing pre-filter regex selects the right rows, whether the shared-id grouping finds a
language version collapsed into the original, whether the report insert respects the V64
author CHECK and partial unique, and whether shadow mode really writes nothing.

One sweep per test over rows seeded here only (``track_ids`` scope, so a shared test
database's other rows are never touched), with a fake LRCLIB client keyed by title.
CI provides TEST_DB_URL and the pinned canonical schema (V64). Skipped when unset.
"""
from __future__ import annotations

import json
import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from worker.service.lyrics_matcher import Candidate
from worker.service.lyrics_suspicion_service import LyricsSuspicionService, body_sha

_TEST_DB_URL = os.environ.get("TEST_DB_URL")

pytestmark = pytest.mark.skipif(
    not _TEST_DB_URL,
    reason="integration test requires TEST_DB_URL env var (Postgres test database)",
)

_PREFIX = "lyr_suspicion_"
DURATION = 200
WORDS = [
    "walking down the empty road tonight",
    "every light is fading out of sight",
    "i keep on holding to the line",
    "you said that we would be just fine",
    "but the river carries all the time",
    "and the morning never feels like mine",
]
PLAIN = "\n".join(WORDS)


def _sync(start=10.0, step=25.0):
    return "\n".join(
        "[%02d:%05.2f] %s" % (*divmod(start + i * step, 60), line) for i, line in enumerate(WORDS)
    )


FIT, OVERRUN, EARLY = _sync(), _sync(start=90.0), _sync(start=5.0, step=10.0)


@pytest.fixture(scope="module")
def factory():
    eng = create_engine(_TEST_DB_URL, pool_pre_ping=True, future=True)
    with eng.connect() as conn:
        if conn.execute(text("SELECT to_regclass('track_lyrics_reports')")).scalar() is None:
            pytest.skip("V64 (track_lyrics_reports) missing from test database")
    Session = sessionmaker(bind=eng)
    yield Session
    with Session() as s, s.begin():
        # track_lyrics, reports and translations cascade from tracks.
        s.execute(text("DELETE FROM tracks WHERE spotify_id LIKE :p"), {"p": f"{_PREFIX}%"})
        s.execute(text("DELETE FROM albums WHERE spotify_id LIKE :p"), {"p": f"{_PREFIX}%"})
        s.execute(text("DELETE FROM artists WHERE spotify_id LIKE :p"), {"p": f"{_PREFIX}%"})
    eng.dispose()


def _seed(s, *, title, synced, lrclib_id, evidence=None, translated=False):
    sfx = f"{_PREFIX}{uuid.uuid4().hex[:10]}"
    album_id, track_id, artist_id = (str(uuid.uuid4()) for _ in range(3))
    s.execute(text("INSERT INTO albums (id, spotify_id, title) VALUES (:id, :sid, 'Suspicion')"),
              {"id": album_id, "sid": f"{sfx}_alb"})
    s.execute(text("INSERT INTO tracks (id, album_id, spotify_id, title, duration_sec) "
                   "VALUES (:id, :alb, :sid, :title, :dur)"),
              {"id": track_id, "alb": album_id, "sid": f"{sfx}_trk", "title": title, "dur": DURATION})
    s.execute(text("INSERT INTO artists (id, spotify_id, name, popularity) VALUES (:id, :sid, 'River Band', 50)"),
              {"id": artist_id, "sid": f"{sfx}_art"})
    s.execute(text("INSERT INTO track_artists (track_id, artist_id) VALUES (:t, :a)"),
              {"t": track_id, "a": artist_id})
    ev = {"match_basis": "exact-title", "lrclib_id": lrclib_id, **(evidence or {})}
    s.execute(text("INSERT INTO track_lyrics (track_id, match_status, evidence, lyric_plain, lyric_synced, "
                   "matcher_version, updated_at) VALUES (:t, 'matched', CAST(:ev AS jsonb), :plain, :synced, "
                   "'step4-v1', NOW() - interval '30 days')"),
              {"t": track_id, "ev": json.dumps(ev), "plain": PLAIN, "synced": synced})
    if translated:
        s.execute(text("INSERT INTO track_lyrics_translations (track_id, status, source_fingerprint, segments, origin) "
                       "VALUES (:t, 'done', 'f', CAST('[{\"i\": 0, \"text_ko\": \"x\"}]' AS jsonb), 'poller')"),
                  {"t": track_id})
    return track_id


def _cand(cid, title, synced):
    return Candidate(id=cid, title=title, artist="River Band", album="A", duration_sec=DURATION,
                     instrumental=False, plain_lyrics=PLAIN, synced_lyrics=synced)


class FakeLrclib:
    def __init__(self, by_title):
        self.by_title = by_title
        self.calls = []

    def search_candidates(self, title, artist, *, max_retries=3):
        self.calls.append(title)
        return list(self.by_title.get(title, []))

    def close(self):
        pass


@pytest.fixture
def world(factory):
    """Seven rows, one per case. Titles and LRCLIB ids are unique per test: rows from earlier
    tests in this module stay until teardown, and two of them sharing an id under different
    titles is exactly what the shared-id signal reports."""
    tag = uuid.uuid4().hex[:6]
    base = uuid.uuid4().int % 10**9 * 100
    t = {k: f"{k} {tag}" for k in ("fixable", "vouched", "lonely", "translated", "locked")}
    with factory() as s, s.begin():
        ids = {
            "fixable": _seed(s, title=t["fixable"], synced=OVERRUN, lrclib_id=base + 1),
            "vouched": _seed(s, title=t["vouched"], synced=OVERRUN, lrclib_id=base + 11),
            "lonely": _seed(s, title=t["lonely"], synced=EARLY, lrclib_id=base + 21),
            "translated": _seed(s, title=t["translated"], synced=OVERRUN, lrclib_id=base + 31,
                                translated=True),
            "locked": _seed(s, title=t["locked"], synced=OVERRUN, lrclib_id=base + 41,
                            evidence={"manual_fix": {"by": "owner"}}),
            # BTS Go Go's shape: one LRCLIB id behind the original and its Japanese version.
            "original": _seed(s, title=f"Go {tag}", synced=FIT, lrclib_id=base + 90),
            "language": _seed(s, title=f"Go {tag} - Japanese ver.", synced=FIT, lrclib_id=base + 90),
        }

    def c(n, key, synced):
        return _cand(base + n, t[key], synced)

    client = FakeLrclib({
        t["fixable"]: [c(1, "fixable", OVERRUN), c(2, "fixable", OVERRUN), c(3, "fixable", FIT)],
        t["vouched"]: [c(11, "vouched", OVERRUN), c(12, "vouched", OVERRUN), c(13, "vouched", OVERRUN)],
        t["lonely"]: [],
        t["translated"]: [c(31, "translated", OVERRUN), c(32, "translated", FIT), c(33, "translated", FIT)],
        t["locked"]: [c(42, "locked", FIT), c(43, "locked", FIT)],
    })
    client.base = base
    return ids, client


def _sweep(factory, world, *, shadow):
    ids, client = world
    with factory() as s:
        return LyricsSuspicionService(s, client, concurrency=1, time_budget_sec=60).sweep(
            shadow=shadow, track_ids=list(ids.values()),
        )


def _state(factory, track_id):
    with factory() as s:
        row = s.execute(text("SELECT lyric_synced, lyric_plain, evidence, updated_at FROM track_lyrics "
                             "WHERE track_id = :t"), {"t": track_id}).one()
        reports = s.execute(text("SELECT reason, user_id, status, reported_status, reported_lrclib_id, "
                                 "reported_body_sha FROM track_lyrics_reports WHERE track_id = :t"),
                            {"t": track_id}).fetchall()
    return {"synced": row[0], "plain": row[1], "evidence": row[2], "updated_at": row[3], "reports": reports}


def test_shadow_writes_nothing_and_counts_what_it_would_do(factory, world):
    ids, client = world
    before = {k: _state(factory, v) for k, v in ids.items()}

    m = _sweep(factory, world, shadow=True)

    assert m["shadow"] is True
    assert m["selected_timing"] == 4  # fixable, vouched, lonely, translated — not the locked row
    assert m["selected_shared_id"] == 2
    assert m.get("would_timing_fix") == 1
    assert m.get("timing_fix_held_translation") == 1
    assert m.get("timing_vouched") == 1
    assert m["would_report"] == 4  # lonely, translated, original, language
    for k, v in ids.items():
        after = _state(factory, v)
        assert after == before[k], k
    assert sorted(client.calls) == sorted(
        c for c in client.by_title if not c.startswith("locked"))


def test_real_pass_fixes_reports_and_settles(factory, world):
    ids, _ = world

    m = _sweep(factory, world, shadow=False)

    assert m.get("timing_fixed") == 1
    assert m["reports_opened"] == 4

    fixed = _state(factory, ids["fixable"])
    assert fixed["synced"] == FIT
    base = world[1].base
    assert fixed["evidence"]["lrclib_id"] == base + 3
    assert fixed["evidence"]["timing_fix"]["replaced_lrclib_id"] == str(base + 1)
    assert fixed["evidence"]["timing_fix"]["signal"] == "duration_overrun"
    assert fixed["reports"] == []

    assert _state(factory, ids["vouched"])["reports"] == []
    assert _state(factory, ids["vouched"])["synced"] == OVERRUN

    lonely = _state(factory, ids["lonely"])
    assert lonely["reports"] == [
        ("early_end", None, "open", "matched", world[1].base + 21, body_sha(EARLY, PLAIN))]

    held = _state(factory, ids["translated"])
    assert held["synced"] == OVERRUN  # a done translation holds the automatic fix
    assert [r[0] for r in held["reports"]] == ["duration_overrun"]

    locked = _state(factory, ids["locked"])
    assert locked["synced"] == OVERRUN and locked["reports"] == []

    for k in ("original", "language"):
        assert [r[0] for r in _state(factory, ids[k])["reports"]] == ["shared_lrclib_id"]


def test_a_second_pass_opens_nothing_and_a_kept_body_stays_kept(factory, world):
    ids, _ = world
    _sweep(factory, world, shadow=False)

    assert _sweep(factory, world, shadow=False)["reports_opened"] == 0

    # The owner closes the lonely row's report with "keep" (3b): same body, never reopened.
    with factory() as s, s.begin():
        s.execute(text("UPDATE track_lyrics_reports SET status = 'resolved', resolution = 'keep', "
                       "resolved_at = NOW(), resolved_by = 'owner' WHERE track_id = :t"),
                  {"t": ids["lonely"]})
    m = _sweep(factory, world, shadow=False)
    assert m["reports_opened"] == 0
    # Only the vouched row is re-checked: lonely and translated are settled by their
    # reports (one kept, one open) and the fixed row fits now.
    assert m["selected_timing"] == 1
    assert [r[2] for r in _state(factory, ids["lonely"])["reports"]] == ["resolved"]


def test_a_member_report_does_not_block_the_system_one(factory, world):
    """V64's two partial uniques are separate: a member's open report on the same track
    leaves room for the signal's own."""
    ids, _ = world
    member = str(uuid.uuid4())
    with factory() as s, s.begin():
        s.execute(text("INSERT INTO users (id, handle, display_name) VALUES (:id, :h, 'Suspicion test')"),
                  {"id": member, "h": f"lsx{member[:8]}"})
        s.execute(text("INSERT INTO track_lyrics_reports (track_id, user_id, reason) VALUES (:t, :u, 'member')"),
                  {"t": ids["lonely"], "u": member})
    try:
        _sweep(factory, world, shadow=False)
        reasons = sorted(r[0] for r in _state(factory, ids["lonely"])["reports"])
        assert reasons == ["early_end", "member"]
    finally:
        with factory() as s, s.begin():
            s.execute(text("DELETE FROM users WHERE id = :id"), {"id": member})


def test_the_unscoped_selection_finds_the_seeded_rows(factory, world):
    """The production path passes no ``track_ids``: the NULL scope must select everything,
    and shared-id grouping must run over the whole table. Selection only — a full unscoped
    sweep would touch a shared test database's other rows."""
    ids, _ = world
    with factory() as s:
        svc = LyricsSuspicionService(s)
        timing = {str(r["id"]): r["signal"] for r in svc._fetch_timing_rows()}
        shared = {str(r["id"]) for r in svc._fetch_shared_id_rows()}
        s.rollback()
    assert timing[ids["fixable"]] == "duration_overrun"
    assert timing[ids["lonely"]] == "early_end"
    assert ids["locked"] not in timing
    assert ids["original"] not in timing  # its sync fits
    assert {ids["original"], ids["language"]} <= shared
    assert ids["fixable"] not in shared


def test_the_insert_itself_refuses_a_body_already_reported(factory, world):
    """The insert's own guard, for a pass that selected the row before another pass's report
    (or the owner's keep) landed: the selection-time filter cannot see that one."""
    ids, _ = world
    tid = ids["lonely"]
    with factory() as s, s.begin():
        s.execute(text("INSERT INTO track_lyrics_reports (track_id, user_id, reason, reported_body_sha, status, "
                       "resolution, resolved_at, resolved_by) VALUES (:t, NULL, 'early_end', :sha, 'resolved', "
                       "'keep', NOW(), 'owner')"),
                  {"t": tid, "sha": body_sha(EARLY, PLAIN)})
    row = {"id": uuid.UUID(tid), "duration_sec": DURATION}
    with factory() as s:
        svc = LyricsSuspicionService(s)
        assert svc._open_report(row, "early_end") is False
        assert svc._open_report(row, "shared_lrclib_id") is True  # another signal is its own
    assert sorted(r[0] for r in _state(factory, tid)["reports"]) == ["early_end", "shared_lrclib_id"]
