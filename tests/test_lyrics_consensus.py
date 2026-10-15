"""FEAT-lyrics-match-correction Step 1: content consensus + rendition markers.

The real-data cases come from ``tests/fixtures/lyrics_consensus.json`` (LRCLIB
responses fetched 2026-10-16, lyric words replaced by hash tokens so content
similarity is preserved without storing lyric text). RFC:
``docs/rfcs/FEAT-lyrics-match-correction.md`` (RC1, RC2, OQ3).
"""

import itertools
import json
import uuid
from pathlib import Path

import pytest

from worker.service.lyrics_matcher import (
    CONTENT_AGREE_THRESHOLD,
    STATUS_MATCHED,
    STATUS_NOT_FOUND,
    STATUS_REVIEW_REQUIRED,
    Candidate,
    _strip_noise,
    canonical_base_title,
    content_shingles,
    content_similarity,
    decide_match,
    extract_version_tokens,
)
from worker.service.lyrics_promote import promote_best

_FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "lyrics_consensus.json").read_text()
)


def _cands(case: str):
    return [Candidate.from_api(r) for r in _FIXTURE[case]["candidates"]]


def _decide(case: str, candidates, **track_override):
    track = {**_FIXTURE[case]["track"], **track_override}
    return decide_match(
        uuid.uuid4(), track["title"], track["artists"], None, track["duration_sec"],
        candidates,
    )


def _c(cid, body, *, title="Song", artist="Artist", duration=200.0, synced=True):
    return Candidate(
        id=cid, title=title, artist=artist, album=None, duration_sec=duration,
        instrumental=False, plain_lyrics=body,
        synced_lyrics=f"[00:00.00]{body}" if synced else None,
    )


# --------------------------------------------------------------------------
# RC1 — content consensus
# --------------------------------------------------------------------------
class TestBadHabit:
    """One upload with correct metadata and Ed Sheeran's body vs many good ones."""

    @pytest.mark.parametrize("wrong_first", [True, False])
    def test_wrong_body_is_not_chosen_in_either_order(self, wrong_first):
        cands = _cands("bad_habit")
        wrong_id = _FIXTURE["bad_habit"]["wrong_id"]
        wrong = [c for c in cands if c.id == wrong_id]
        rest = [c for c in cands if c.id != wrong_id]
        out = _decide("bad_habit", wrong + rest if wrong_first else rest + wrong)
        assert out.match_status == STATUS_MATCHED
        assert out.evidence["lrclib_id"] != wrong_id
        # the pick is in the majority body, and the evidence says how large it is
        assert out.evidence["content_cluster_size"] >= 2
        assert out.evidence["plausible_count"] > out.evidence["content_cluster_size"] - 1

    def test_wrong_body_does_not_agree_with_the_good_ones(self):
        cands = _cands("bad_habit")
        wrong_id = _FIXTURE["bad_habit"]["wrong_id"]
        wrong = content_shingles(next(c for c in cands if c.id == wrong_id))
        for c in cands:
            if c.id != wrong_id:
                assert content_similarity(wrong, content_shingles(c)) < CONTENT_AGREE_THRESHOLD


class TestMustAgreeTranscriptionVariants:
    """OQ3: the two false positives of the 2026-10-15 probe must read as one body."""

    @pytest.mark.parametrize("case", ["big_poe", "beep_beep"])
    def test_every_pair_agrees(self, case):
        shingles = [content_shingles(c) for c in _cands(case)]
        shingles = [s for s in shingles if s]
        assert len(shingles) >= 10
        for a, b in itertools.combinations(shingles, 2):
            assert content_similarity(a, b) >= CONTENT_AGREE_THRESHOLD

    @pytest.mark.parametrize("case", ["big_poe", "beep_beep"])
    def test_still_matches(self, case):
        out = _decide(case, _cands(case))
        assert out.match_status == STATUS_MATCHED
        assert out.evidence.get("reason") != "content_disagreement"


class TestContentDisagreement:
    def test_two_unrelated_bodies_park_instead_of_picking(self):
        out = decide_match(
            uuid.uuid4(), "Song", ["Artist"], None, 200.0,
            [_c(1, "the river runs down to the sea tonight"),
             _c(2, "neon lights over the empty parking lot")],
        )
        assert out.match_status == STATUS_REVIEW_REQUIRED
        assert out.evidence["reason"] == "content_disagreement"
        assert {p["lrclib_id"] for p in out.evidence["candidates"]} == {1, 2}

    def test_single_candidate_never_parks_for_content(self):
        out = decide_match(
            uuid.uuid4(), "Song", ["Artist"], None, 200.0,
            [_c(1, "the river runs down to the sea tonight")],
        )
        assert out.match_status == STATUS_MATCHED
        assert out.evidence["content_cluster_size"] == 1

    def test_majority_beats_a_richer_minority(self):
        good = "the river runs down to the sea tonight and i follow"
        out = decide_match(
            uuid.uuid4(), "Song", ["Artist"], None, 200.0,
            [_c(1, "neon lights over the empty parking lot again"),  # synced, alone
             _c(2, good, synced=False), _c(3, good, synced=False)],
        )
        assert out.match_status == STATUS_MATCHED
        assert out.evidence["lrclib_id"] in (2, 3)
        assert out.evidence["content_cluster_size"] == 2

    def test_timestamps_and_parenthesised_adlibs_are_ignored(self):
        a = _c(1, "pop out she creepin (pop out) all night long we ride")
        b = Candidate(
            id=2, title="Song", artist="Artist", album=None, duration_sec=200.0,
            instrumental=False, plain_lyrics=None,
            synced_lyrics="[00:01.20]Pop out, she creepin'\n[00:03.00](yeah)\n"
                          "[00:04.10]All night long we ride",
        )
        assert content_similarity(content_shingles(a), content_shingles(b)) >= 0.9

    def test_best_of_never_promotes_a_content_disagreement(self):
        cands = [_c(1, "the river runs down to the sea tonight"),
                 _c(2, "neon lights over the empty parking lot")]
        parked = decide_match(uuid.uuid4(), "Song", ["Artist"], None, 200.0, cands)
        out = promote_best(parked, "Song", ["Artist"], None, 200.0, cands)
        assert out.match_status == STATUS_REVIEW_REQUIRED
        assert out.evidence["promotion"]["reason"] == "content_disagreement"


class TestPromotionEvidence:
    def test_promoted_row_carries_top_level_lrclib_id_and_majority_pick(self):
        good = "the river runs down to the sea tonight and i follow"
        cands = [
            _c(1, "neon lights over the empty parking lot again"),
            _c(2, good, synced=False), _c(3, good, synced=False),
            _c(9, good, title="Song (Remix)"),  # forces review_required via version tokens
        ]
        parked = decide_match(uuid.uuid4(), "Song (Remix)", ["Artist"], None, 200.0, cands[3:])
        assert parked.match_status == STATUS_MATCHED  # sanity: remix vs remix agrees
        # A parked outcome to promote: version mismatch on a remix-only pool.
        parked = decide_match(uuid.uuid4(), "Song", ["Artist"], None, 200.0, [cands[3]])
        assert parked.match_status == STATUS_REVIEW_REQUIRED
        out = promote_best(parked, "Song", ["Artist"], None, 200.0, cands)
        assert out.match_status == STATUS_MATCHED
        assert out.evidence["lrclib_id"] in (2, 3)
        assert out.evidence["lrclib_id"] == out.evidence["promotion"]["chosen"]["lrclib_id"]
        assert out.evidence["content_cluster_size"] == 2


# --------------------------------------------------------------------------
# RC2 — rendition markers
# --------------------------------------------------------------------------
class TestRenditionMarkers:
    @pytest.mark.parametrize("title,tokens", [
        ("Go Go (Japanese Ver.)", {"japanese", "version"}),
        ("Go Go - Japanese ver.", {"japanese", "version"}),
        ("Song (2023 Ver.)", {"version"}),
        ("Song - English Version", {"english", "version"}),
        ("노래 (일본어 버전)", {"일본어", "version"}),
    ])
    def test_tokens(self, title, tokens):
        assert extract_version_tokens(title) == tokens

    def test_ver_and_version_spellings_agree(self):
        assert extract_version_tokens("Song (English Ver.)") == extract_version_tokens(
            "Song - English Version")

    @pytest.mark.parametrize("title", ["English Girl", "Japanese Denim", "Korean Dinner"])
    def test_language_in_the_base_title_is_not_a_rendition(self, title):
        assert extract_version_tokens(title) == set()

    def test_language_segment_survives_noise_strip(self):
        assert _strip_noise("Go Go (Japanese Ver.)") == "Go Go (Japanese Ver.)"
        assert _strip_noise("Song (feat. X)") == "Song"
        assert canonical_base_title("Go Go (Japanese Ver.)") != canonical_base_title("Go Go")

    def test_dated_rerelease_is_not_the_original(self):
        out = decide_match(
            uuid.uuid4(), "Song", ["Artist"], None, 200.0,
            [_c(1, "words of the 2023 take", title="Song (2023 Ver.)")],
        )
        assert out.match_status != STATUS_MATCHED


class TestGoGo:
    """BTS ``Go Go``: the Korean tracks were matched to LRCLIB 13611969 ``(Japanese Ver.)``."""

    def test_japanese_labelled_upload_never_matches_the_korean_title(self):
        ja_id = _FIXTURE["go_go"]["japanese_labelled_id"]
        ja = [c for c in _cands("go_go") if c.id == ja_id]
        out = decide_match(uuid.uuid4(), "Go Go", ["BTS"], ["방탄소년단"], 235.0, ja)
        assert out.match_status == STATUS_NOT_FOUND

    def test_japanese_track_still_gets_the_japanese_labelled_upload(self):
        ja_id = _FIXTURE["go_go"]["japanese_labelled_id"]
        out = decide_match(
            uuid.uuid4(), "Go Go - Japanese ver.", ["BTS"], ["방탄소년단"], 236.0,
            _cands("go_go"),
        )
        assert out.match_status == STATUS_MATCHED
        assert out.evidence["lrclib_id"] == ja_id

    def test_korean_title_never_picks_the_japanese_labelled_upload(self):
        ja_id = _FIXTURE["go_go"]["japanese_labelled_id"]
        out = decide_match(
            uuid.uuid4(), "Go Go", ["BTS"], ["방탄소년단"], 237.0, _cands("go_go"),
        )
        assert out.evidence.get("lrclib_id") != ja_id
