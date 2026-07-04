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
# The canonical duplicate-noise case from the best-of RFC — now resolved AT
# SOURCE by FIX-lyrics-matcher-noise (prefix/parenthetical strip collapses the
# trio into one group). Promotion mechanics are exercised with a shape that
# STILL parks: genuine fuzzy sibling ambiguity around an exact group.
# --------------------------------------------------------------------------
class TestComeBackToEarth:
    NOISE_TRIO = [
        _cand("Come Back to Earth", duration=162.0, cid=10),
        _cand("Come Back to Earth (Paused)", duration=161.84, cid=11),
        _cand("01 - Come Back to Earth", duration=162.0, cid=12),
    ]
    # An exact group + a genuinely distinct fuzzy base title -> still ambiguous.
    STILL_AMBIGUOUS = [
        _cand("Come Back to Earth", duration=162.0, cid=10),
        _cand("Come Back to Earth II", duration=161.5, cid=13),
    ]

    def test_noise_trio_resolves_at_source(self):
        # was: 3 groups -> ambiguous; FIX-lyrics-matcher-noise collapses to 1
        outcome = _decide("Come Back to Earth", self.NOISE_TRIO)
        assert outcome.match_status == STATUS_MATCHED
        assert outcome.match_basis == "exact-title"

    def test_promotes_the_clean_title(self):
        outcome = _decide("Come Back to Earth", self.STILL_AMBIGUOUS)
        assert outcome.match_status == STATUS_AMBIGUOUS
        promoted = _promote(outcome, "Come Back to Earth", self.STILL_AMBIGUOUS)
        assert promoted.match_status == STATUS_MATCHED
        assert promoted.match_basis == BASIS_BEST_OF_AMBIGUOUS
        assert promoted.evidence["promotion"]["chosen"]["title"] == "Come Back to Earth"
        assert promoted.evidence["promotion"]["criterion"] == CRITERION_TIER1
        assert promoted.evidence["promotion"]["from_status"] == STATUS_AMBIGUOUS
        assert promoted.version_agrees is True
        assert promoted.lyric_plain == "la la"

    def test_original_evidence_preserved(self):
        outcome = _decide("Come Back to Earth", self.STILL_AMBIGUOUS)
        promoted = _promote(outcome, "Come Back to Earth", self.STILL_AMBIGUOUS)
        assert promoted.evidence["reason"] == "multiple_plausible"
        assert len(promoted.evidence["candidates"]) == 2

    def test_matcher_version_tagged(self):
        outcome = _decide("Come Back to Earth", self.STILL_AMBIGUOUS)
        promoted = _promote(outcome, "Come Back to Earth", self.STILL_AMBIGUOUS)
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
# version-agreeing sibling — now elected AT SOURCE by the representative fix
# (FIX-lyrics-matcher-noise: _richest -> version-agreeing preference)
# --------------------------------------------------------------------------
class TestReviewSibling:
    # Both group under base "song"; the old _richest elected the synced Live
    # upload -> version_token_mismatch parked review_required. The rep now
    # prefers the version-AGREEING clean sibling -> matched at source.
    CANDIDATES = [
        _cand("Song", artist="Adele", duration=200.0, plain="hello", cid=20),
        _cand("Song (Live)", artist="Adele", duration=200.5, plain="hello",
              synced="[00:01.00] hello", cid=21),
    ]

    def test_agreeing_sibling_resolves_at_source(self):
        outcome = _decide("Song", self.CANDIDATES, artists=["Adele"], duration=200.0)
        assert outcome.match_status == STATUS_MATCHED
        assert outcome.match_basis == "exact-title"
        assert outcome.version_agrees is True
        assert outcome.evidence["lrclib_id"] == 20
        assert outcome.lyric_plain == "hello"

    def test_source_resolved_outcome_passes_through_promotion(self):
        # differential guarantee holds for the newly source-resolved shape
        outcome = _decide("Song", self.CANDIDATES, artists=["Adele"], duration=200.0)
        promoted = _promote(outcome, "Song", self.CANDIDATES,
                            artists=["Adele"], duration=200.0)
        assert promoted is outcome

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
    def test_synced_beats_plain_at_source(self):
        # same-group duplicates now resolve at source; richest is the tiebreak
        cands = [
            _cand("Come Back to Earth", plain="la la", cid=40),
            _cand("Come Back to Earth (Paused)", plain="la la", cid=41),
            _cand("Come Back to Earth", plain="la la",
                  synced="[00:01.00] la la", cid=42),
        ]
        outcome = _decide("Come Back to Earth", cands)
        assert outcome.match_status == STATUS_MATCHED
        assert outcome.lyric_synced == "[00:01.00] la la"

    def test_synced_beats_plain_in_promotion(self):
        # still-ambiguous shape (fuzzy distinct base) -> tier-1 tiebreak in promote
        cands = [
            _cand("Come Back to Earth", plain="la la", cid=40),
            _cand("Come Back to Earth II", plain="la la", cid=41),
            _cand("Come Back to Earth", plain="la la",
                  synced="[00:01.00] la la", cid=42),
        ]
        outcome = _decide("Come Back to Earth", cands)
        assert outcome.match_status == STATUS_AMBIGUOUS
        promoted = _promote(outcome, "Come Back to Earth", cands)
        assert promoted.match_status == STATUS_MATCHED
        assert promoted.evidence["promotion"]["chosen"]["lrclib_id"] == 42
        assert promoted.lyric_synced == "[00:01.00] la la"
