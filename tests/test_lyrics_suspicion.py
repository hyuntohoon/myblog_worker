"""FEAT-lyrics-match-correction Step 4: sync timing in the matcher, and the suspicion sweep.

DB-free. Three units:

  * the timing helpers and the representative / best-of ranking, which now prefer a sync
    that fits the track among bodies with the same content support;
  * the sweep's own write path (``timing_upgrade``) and report filter (``_timing_vouch``);
  * the two values the sweep must reproduce from the backend byte for byte (the report
    body hash and the per-track advisory key) — golden values computed from
    ``myblog_backend/app/services/lyrics_report_service.py`` on 2026-10-17.

The selection SQL, the reports and the full sweep against a real engine live in
tests/integration/test_lyrics_suspicion_db.py.
RFC: ``docs/rfcs/FEAT-lyrics-match-correction.md`` (Step 4).
"""
from __future__ import annotations

import uuid

import pytest

from worker.service.lyrics_matcher import (
    STATUS_MATCHED,
    Candidate,
    MatchOutcome,
    decide_match,
    lrc_timestamps,
    sync_fits,
    timing_signal,
)
from worker.service.lyrics_promote import promote_best
from worker.service.lyrics_suspicion_service import (
    _timing_vouch,
    body_sha,
    report_advisory_key,
    timing_upgrade,
)

WORDS = [
    "walking down the empty road tonight",
    "every light is fading out of sight",
    "i keep on holding to the line",
    "you said that we would be just fine",
    "but the river carries all the time",
    "and the morning never feels like mine",
]
OTHER_WORDS = [
    "cold water rising over stone",
    "nobody here to call my own",
    "silver birds are flying home",
    "under a sky of broken chrome",
]
DURATION = 200


def _sync(lines, start=10.0, step=25.0):
    return "\n".join(
        "[%02d:%05.2f] %s" % (*divmod(start + i * step, 60), line) for i, line in enumerate(lines)
    )


FIT = _sync(WORDS)  # 0:10 → 2:15 on a 3:20 track
OVERRUN = _sync(WORDS, start=90.0)  # → 3:35, past 3:20 + 5 s
EARLY = _sync(WORDS, start=5.0, step=10.0)  # → 0:55, before 40 % of 3:20
STUB = _sync(WORDS[:2], start=60.0)  # fits the clock, two lines of six


def _cand(cid, synced, title="Fading Road", plain=None):
    return Candidate(id=cid, title=title, artist="River Band", album="A", duration_sec=DURATION,
                     instrumental=False, plain_lyrics=plain or "\n".join(WORDS), synced_lyrics=synced)


def _decide(cands, title="Fading Road"):
    return decide_match(uuid.uuid4(), title, ["River Band"], None, DURATION, cands)


# ── helpers ────────────────────────────────────────────────────────────────────────


def test_lrc_timestamps_reads_both_fraction_separators_and_none():
    assert lrc_timestamps("[01:02.50] a\n[03:04:25] b\n[05:06] c") == [62.5, 184.25, 306.0]
    assert lrc_timestamps(None) == []


@pytest.mark.parametrize("synced,expected", [
    (FIT, None), (OVERRUN, "duration_overrun"), (EARLY, "early_end"), (None, None),
])
def test_timing_signal(synced, expected):
    assert timing_signal(synced, DURATION) == expected


def test_timing_signal_boundaries_are_the_measured_ones():
    assert timing_signal("[03:25.00] x", 200) is None  # exactly duration + 5
    assert timing_signal("[03:25.01] x", 200) == "duration_overrun"
    assert timing_signal("[01:20.00] x", 200) is None  # exactly 40 %
    assert timing_signal("[01:19.99] x", 200) == "early_end"


def test_sync_fits_is_unknown_without_a_sync_or_a_duration():
    assert sync_fits(None, DURATION) is None
    assert sync_fits(FIT, None) is None
    assert sync_fits(FIT, DURATION) is True
    assert sync_fits(OVERRUN, DURATION) is False


# ── ranking ────────────────────────────────────────────────────────────────────────


def test_representative_prefers_a_fitting_sync_among_the_same_words():
    out = _decide([_cand(1, OVERRUN), _cand(2, OVERRUN), _cand(3, FIT)])
    assert out.match_status == STATUS_MATCHED
    assert out.evidence["lrclib_id"] == 3
    assert out.evidence["content_cluster_size"] == 3


def test_a_fitting_stub_does_not_beat_the_full_body():
    out = _decide([_cand(1, OVERRUN), _cand(2, STUB)])
    assert out.evidence["lrclib_id"] == 1


def test_timing_never_outranks_content_support():
    other = _cand(9, _sync(OTHER_WORDS), plain="\n".join(OTHER_WORDS))
    out = _decide([other, _cand(1, OVERRUN), _cand(2, OVERRUN)])
    assert out.evidence["lrclib_id"] in (1, 2)


def test_order_is_unchanged_when_nothing_fits():
    """No candidate fits ⇒ the old keys decide, exactly as before Step 4."""
    out = _decide([_cand(1, OVERRUN), _cand(2, EARLY)])
    assert out.evidence["lrclib_id"] == 1


def test_best_of_promotion_prefers_a_fitting_sync_too():
    parked = MatchOutcome(track_id=uuid.uuid4(), match_status="ambiguous",
                          evidence={"reason": "multiple_plausible"})
    cands = [_cand(1, OVERRUN), _cand(2, OVERRUN), _cand(3, FIT)]
    out = promote_best(parked, title="Fading Road", artist_names=["River Band"], aliases=None,
                       duration_sec=DURATION, candidates=cands)
    assert out.match_status == STATUS_MATCHED
    assert out.evidence["promotion"]["chosen"]["lrclib_id"] == 3


# ── the sweep's lateral path ───────────────────────────────────────────────────────


def _row(stored_synced=OVERRUN, *, basis="exact-title", locked=False, lrclib_id=1):
    return {
        "id": uuid.uuid4(), "title": "Fading Road", "duration_sec": DURATION,
        "artist_names": ["River Band"], "aliases": [],
        "existing_status": STATUS_MATCHED, "existing_basis": basis, "existing_locked": locked,
        "existing_lrclib_id": lrclib_id, "existing_matcher_version": "step4-v1",
        "existing_lyric_synced": stored_synced, "existing_lyric_plain": "\n".join(WORDS),
    }


def _picked(synced=FIT, *, basis="exact-title", words=WORDS):
    return MatchOutcome(track_id=uuid.uuid4(), match_status=STATUS_MATCHED, match_basis=basis,
                        evidence={"lrclib_id": 3, "content_cluster_size": 3},
                        lyric_synced=synced, lyric_plain="\n".join(words), version_agrees=True)


def test_timing_upgrade_same_words_misfit_to_fit():
    assert timing_upgrade(_row(), _picked()) is True
    assert timing_upgrade(_row(EARLY), _picked()) is True


@pytest.mark.parametrize("row,picked", [
    (_row(FIT), _picked()),  # stored already fits: monotone, never churns
    (_row(), _picked(OVERRUN)),  # new one misfits too
    (_row(), _picked(STUB, words=WORDS[:2])),  # a stub of the stored body
    (_row(), _picked(_sync(OTHER_WORDS), words=OTHER_WORDS)),  # different words
    (_row(locked=True), _picked()),
    (_row(basis="best-of-ambiguous"), _picked()),  # another rung: the ladder decides
    (_row(None), _picked()),  # plain-only stored body: nothing to judge
])
def test_timing_upgrade_refuses(row, picked):
    assert timing_upgrade(row, picked) is False


def test_vouch_when_every_same_words_upload_shares_the_timing():
    cands = [_cand(1, OVERRUN), _cand(2, OVERRUN), _cand(4, OVERRUN)]
    v = _timing_vouch(_row(), cands)
    assert (v.same_words, v.fitting, v.vouched) == (2, 0, True)  # the stored id 1 excluded


def test_no_vouch_when_one_upload_fits_or_too_few_agree():
    assert not _timing_vouch(_row(), [_cand(2, OVERRUN), _cand(3, FIT)]).vouched
    assert not _timing_vouch(_row(), [_cand(2, OVERRUN)]).vouched
    other = _cand(5, _sync(OTHER_WORDS, start=90), plain="\n".join(OTHER_WORDS))
    assert _timing_vouch(_row(), [other, _cand(6, _sync(OTHER_WORDS, start=95))]).same_words == 0


def test_vouch_ignores_another_rendition():
    live = [_cand(2, OVERRUN, title="Fading Road (Live)"), _cand(3, OVERRUN, title="Fading Road (Live)")]
    assert _timing_vouch(_row(), live).same_words == 0


# ── parity with the backend ────────────────────────────────────────────────────────


def test_body_sha_matches_the_backend():
    assert body_sha("[00:01.00] hi", "hi") == (
        "e0f3f6610730b4e39d933493326a8098aa779134bdb503e609d673ddfd52469a"
    )


@pytest.mark.parametrize("track_id", [
    uuid.UUID("1af21702-6a26-4f71-a2b4-6e852a22bc40"),
    "1af21702-6a26-4f71-a2b4-6e852a22bc40",
    "1AF21702-6A26-4F71-A2B4-6E852A22BC40",
])
def test_report_advisory_key_matches_the_backend(track_id):
    assert report_advisory_key(track_id) == 4301988762828746905
