"""Unit tests for the best-of promotion layer (FEAT-lyrics-best-of-promotion Step 1).

``promote_best`` is pure and tested with in-memory ``Candidate`` lists — no DB,
no LRCLIB. The differential guarantee (conservative matcher outputs pass
through bit-identical) is asserted with object identity. RFC:
``docs/rfcs/FEAT-lyrics-best-of-promotion.md``.
"""

import uuid

from worker.service.lyrics_matcher import (
    STATUS_AMBIGUOUS,
    STATUS_MATCHED,
    STATUS_NO_LYRICS,
    STATUS_NOT_FOUND,
    STATUS_REVIEW_REQUIRED,
    Candidate,
    decide_match,
)
from worker.service.lyrics_promote import (
    BASIS_BEST_OF_AMBIGUOUS,
    BASIS_BEST_OF_REVIEW,
    BEST_OF_VERSION,
    CRITERION_TIER1,
    promote_best,
)

TRACK_ID = uuid.uuid4()


def _cand(title, artist="Mac Miller", duration=162.0, plain="la la", synced=None,
          instrumental=False, cid=1):
    return Candidate(
        id=cid, title=title, artist=artist, album=None, duration_sec=duration,
        instrumental=instrumental, plain_lyrics=plain, synced_lyrics=synced,
    )


def _decide(title, candidates, artists=None, duration=161.0):
    return decide_match(
        track_id=TRACK_ID,
        title=title,
        artist_names=artists or ["Mac Miller"],
        aliases=None,
        duration_sec=duration,
        candidates=candidates,
    )


def _promote(outcome, title, candidates, artists=None, duration=161.0):
    return promote_best(
        outcome, title, artists or ["Mac Miller"], None, duration, candidates,
    )


# --------------------------------------------------------------------------
# The canonical duplicate-noise case from the RFC
# --------------------------------------------------------------------------
class TestComeBackToEarth:
    CANDIDATES = [
        _cand("Come Back to Earth", duration=162.0, cid=10),
        _cand("Come Back to Earth (Paused)", duration=161.84, cid=11),
        _cand("01 - Come Back to Earth", duration=162.0, cid=12),
    ]

    def test_parked_ambiguous_by_conservative_matcher(self):
        outcome = _decide("Come Back to Earth", self.CANDIDATES)
        assert outcome.match_status == STATUS_AMBIGUOUS
        assert outcome.evidence["reason"] == "multiple_plausible"

    def test_promotes_the_clean_title(self):
        outcome = _decide("Come Back to Earth", self.CANDIDATES)
        promoted = _promote(outcome, "Come Back to Earth", self.CANDIDATES)
        assert promoted.match_status == STATUS_MATCHED
        assert promoted.match_basis == BASIS_BEST_OF_AMBIGUOUS
        assert promoted.evidence["promotion"]["chosen"]["title"] == "Come Back to Earth"
        assert promoted.evidence["promotion"]["criterion"] == CRITERION_TIER1
        assert promoted.evidence["promotion"]["from_status"] == STATUS_AMBIGUOUS
        assert promoted.version_agrees is True
        assert promoted.lyric_plain == "la la"

    def test_original_evidence_preserved(self):
        outcome = _decide("Come Back to Earth", self.CANDIDATES)
        promoted = _promote(outcome, "Come Back to Earth", self.CANDIDATES)
        assert promoted.evidence["reason"] == "multiple_plausible"
        assert len(promoted.evidence["candidates"]) == 3

    def test_matcher_version_tagged(self):
        outcome = _decide("Come Back to Earth", self.CANDIDATES)
        promoted = _promote(outcome, "Come Back to Earth", self.CANDIDATES)
        assert promoted.matcher_version.endswith(f"+{BEST_OF_VERSION}")


# --------------------------------------------------------------------------
# Differential guarantee: only ambiguous / review_required are ever touched
# --------------------------------------------------------------------------
class TestPassThrough:
    def test_matched_outcome_is_identical_object(self):
        cands = [_cand("Come Back to Earth")]
        outcome = _decide("Come Back to Earth", cands)
        assert outcome.match_status == STATUS_MATCHED
        assert _promote(outcome, "Come Back to Earth", cands) is outcome

    def test_not_found_outcome_is_identical_object(self):
        outcome = _decide("Nonexistent Song", [])
        assert outcome.match_status == STATUS_NOT_FOUND
        assert _promote(outcome, "Nonexistent Song", []) is outcome

    def test_no_lyrics_outcome_is_identical_object(self):
        cands = [_cand("Come Back to Earth", plain=None, instrumental=True)]
        outcome = _decide("Come Back to Earth", cands)
        assert outcome.match_status == STATUS_NO_LYRICS
        assert _promote(outcome, "Come Back to Earth", cands) is outcome


# --------------------------------------------------------------------------
# review_required promotion via a version-agreeing sibling
# --------------------------------------------------------------------------
class TestReviewSibling:
    # decide_match groups both under base "song" and picks the richest (synced)
    # representative — the Live one — which fails the version gate. The clean
    # sibling agrees on version and is the tier-1 pick.
    CANDIDATES = [
        _cand("Song", artist="Adele", duration=200.0, plain="hello", cid=20),
        _cand("Song (Live)", artist="Adele", duration=200.5, plain="hello",
              synced="[00:01.00] hello", cid=21),
    ]

    def test_parked_review_by_conservative_matcher(self):
        outcome = _decide("Song", self.CANDIDATES, artists=["Adele"], duration=200.0)
        assert outcome.match_status == STATUS_REVIEW_REQUIRED
        assert outcome.evidence["reason"] == "version_token_mismatch"

    def test_promotes_the_agreeing_sibling(self):
        outcome = _decide("Song", self.CANDIDATES, artists=["Adele"], duration=200.0)
        promoted = _promote(outcome, "Song", self.CANDIDATES,
                            artists=["Adele"], duration=200.0)
        assert promoted.match_status == STATUS_MATCHED
        assert promoted.match_basis == BASIS_BEST_OF_REVIEW
        assert promoted.evidence["promotion"]["chosen"]["lrclib_id"] == 20
        assert promoted.version_agrees is True

    def test_version_disagreeing_only_candidate_stays_parked(self):
        # Only a (Remix) candidate exists: same base title, but tier 1 requires
        # token agreement — never promoted in v1.
        cands = [_cand("Song (Remix)", artist="Adele", duration=200.0, cid=22)]
        outcome = _decide("Song", cands, artists=["Adele"], duration=200.0)
        assert outcome.match_status == STATUS_REVIEW_REQUIRED
        promoted = _promote(outcome, "Song", cands, artists=["Adele"], duration=200.0)
        assert promoted.match_status == STATUS_REVIEW_REQUIRED
        assert promoted.evidence["promotion"]["reason"] == "no_tier1_candidate"
        assert promoted.lyric_plain is None


# --------------------------------------------------------------------------
# Body filter (promotion precondition)
# --------------------------------------------------------------------------
class TestBodyFilter:
    # Two distinct fuzzy bases -> genuinely ambiguous groups.
    def _ambiguous_pair(self, **overrides):
        base = dict(artist="Adele", duration=200.0)
        base.update(overrides)
        return [
            _cand("Hello Worlds", cid=30, **base),
            _cand("Hello Worldz", cid=31, **base),
        ]

    def test_all_instrumental_resolves_no_lyrics(self):
        cands = self._ambiguous_pair(plain=None, instrumental=True)
        outcome = _decide("Hello World", cands, artists=["Adele"], duration=200.0)
        assert outcome.match_status == STATUS_AMBIGUOUS
        promoted = _promote(outcome, "Hello World", cands,
                            artists=["Adele"], duration=200.0)
        assert promoted.match_status == STATUS_NO_LYRICS
        assert promoted.lyric_plain == ""  # V33 CHECK: resolved -> non-NULL
        assert promoted.match_basis == BASIS_BEST_OF_AMBIGUOUS
        assert promoted.evidence["promotion"]["resolution"] == "all_instrumental"

    def test_bodyless_non_instrumental_stays_parked(self):
        cands = [
            _cand("Hello Worlds", artist="Adele", duration=200.0,
                  plain=None, instrumental=True, cid=32),
            _cand("Hello Worldz", artist="Adele", duration=200.0,
                  plain=None, instrumental=False, cid=33),
        ]
        outcome = _decide("Hello World", cands, artists=["Adele"], duration=200.0)
        assert outcome.match_status == STATUS_AMBIGUOUS
        promoted = _promote(outcome, "Hello World", cands,
                            artists=["Adele"], duration=200.0)
        assert promoted.match_status == STATUS_AMBIGUOUS
        assert promoted.evidence["promotion"]["reason"] == "no_body_candidate"

    def test_no_tier1_among_fuzzy_only(self):
        cands = self._ambiguous_pair()
        outcome = _decide("Hello World", cands, artists=["Adele"], duration=200.0)
        assert outcome.match_status == STATUS_AMBIGUOUS
        promoted = _promote(outcome, "Hello World", cands,
                            artists=["Adele"], duration=200.0)
        assert promoted.match_status == STATUS_AMBIGUOUS
        assert promoted.evidence["promotion"]["reason"] == "no_tier1_candidate"


# --------------------------------------------------------------------------
# Tiebreak: richest lyric among tier-1 duplicates
# --------------------------------------------------------------------------
class TestRichestTiebreak:
    def test_synced_beats_plain(self):
        cands = [
            _cand("Come Back to Earth", plain="la la", cid=40),
            _cand("Come Back to Earth (Paused)", plain="la la", cid=41),
            _cand("Come Back to Earth", plain="la la",
                  synced="[00:01.00] la la", cid=42),
        ]
        outcome = _decide("Come Back to Earth", cands)
        assert outcome.match_status == STATUS_AMBIGUOUS
        promoted = _promote(outcome, "Come Back to Earth", cands)
        assert promoted.match_status == STATUS_MATCHED
        assert promoted.evidence["promotion"]["chosen"]["lrclib_id"] == 42
        assert promoted.lyric_synced == "[00:01.00] la la"
