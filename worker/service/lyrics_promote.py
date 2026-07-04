"""
FEAT-lyrics-best-of-promotion Step 1: best-of promotion decision layer (v1).

A pure, no-I/O layer that runs AFTER ``decide_match`` and only ever rewrites an
``ambiguous`` / ``review_required`` outcome — every other status passes through
untouched (the conservative matcher and the meaning of an ``exact-title`` match
are preserved verbatim). v1 promotes on **tier 1 only**: the candidate whose
stripped base title equals the catalog's, whose duration is within tolerance,
and whose version tokens agree with the catalog's — ties broken by richest
lyric (synced > plain). Tiers 2-4 (duration-nearest / fuzzy / richest) are
specified in the RFC but gated off until the Step-1 probe justifies them.

The chosen candidate and why are recorded under ``evidence["promotion"]`` and
the original parked ``reason`` + candidate previews are kept, so any wrong
promotion is fully reconstructable and reversible. Promoted rows carry the
distinct bases ``best-of-ambiguous`` / ``best-of-review`` — weaker than
``exact-title`` in the reassessment replacement ladder (slotted at Step 2).

RFC: ``docs/rfcs/FEAT-lyrics-best-of-promotion.md``.
"""

import dataclasses
from typing import List, Optional

from worker.service.lyrics_matcher import (
    STATUS_AMBIGUOUS,
    STATUS_MATCHED,
    STATUS_NO_LYRICS,
    STATUS_REVIEW_REQUIRED,
    Candidate,
    MatchOutcome,
    TitleNormalizer,
    _artist_identity_ok,
    canonical_base_title,
    duration_matches,
    exact_base_equal,
    extract_version_tokens,
    plain_base_title,
)

BEST_OF_VERSION = "best-of-v1"

BASIS_BEST_OF_AMBIGUOUS = "best-of-ambiguous"
BASIS_BEST_OF_REVIEW = "best-of-review"
CRITERION_TIER1 = "exact-base-title"

_PROMOTABLE = {
    STATUS_AMBIGUOUS: BASIS_BEST_OF_AMBIGUOUS,
    STATUS_REVIEW_REQUIRED: BASIS_BEST_OF_REVIEW,
}


def _plausible(
    title: str,
    artist_names: List[str],
    aliases: Optional[List[str]],
    duration_sec: Optional[float],
    candidates: List[Candidate],
) -> List[Candidate]:
    """Re-derive the plausible candidate set with ``decide_match``'s own gates.

    Same artist-identity gate, exact-or-fuzzy stripped-base-title gate, and
    duration tolerance — promotion never considers a candidate the conservative
    matcher would have dropped outright.
    """
    stripped_track = canonical_base_title(title or "")
    plain_track = plain_base_title(title or "")
    if not stripped_track:
        return []

    identity_norms = [TitleNormalizer.normalize(n) for n in (artist_names or []) if n]
    for a in (aliases or []):
        if isinstance(a, str) and a:
            identity_norms.append(TitleNormalizer.normalize(a))

    out: List[Candidate] = []
    for cand in candidates:
        if not _artist_identity_ok(TitleNormalizer.normalize(cand.artist), identity_norms):
            continue
        if not TitleNormalizer.normalize(cand.title):
            continue
        plain_cand = plain_base_title(cand.title)
        if not plain_cand:
            continue
        if not exact_base_equal(title or "", cand.title):
            # fuzzy candidates stay in the plausible pool (they inform the
            # body-filter classification) but can never win tier 1 — gated on
            # the PLAIN canonicals, mirroring decide_match.
            if not plain_track or TitleNormalizer.similarity(plain_track, plain_cand) < 0.80:
                continue
        if not duration_matches(duration_sec, cand.duration_sec):
            continue
        out.append(cand)
    return out


def _parked(outcome: MatchOutcome, reason: str, from_status: str) -> MatchOutcome:
    """Keep the row parked, recording the promotion attempt in evidence."""
    ev = dict(outcome.evidence or {})
    ev["promotion"] = {"attempted": True, "reason": reason, "from_status": from_status}
    return dataclasses.replace(
        outcome,
        evidence=ev,
        matcher_version=f"{outcome.matcher_version}+{BEST_OF_VERSION}",
    )


def promote_best(
    outcome: MatchOutcome,
    title: str,
    artist_names: List[str],
    aliases: Optional[List[str]],
    duration_sec: Optional[float],
    candidates: List[Candidate],
) -> MatchOutcome:
    """Pure v1 best-of promotion over a parked ``decide_match`` outcome.

    Any status other than ``ambiguous`` / ``review_required`` is returned
    unchanged (same object) — the conservative matcher's outputs are
    bit-identical before/after this layer.
    """
    basis = _PROMOTABLE.get(outcome.match_status)
    if basis is None:
        return outcome

    from_status = outcome.match_status
    plausible = _plausible(title, artist_names, aliases, duration_sec, candidates)
    if not plausible:
        # Re-fetch drift: the pool that parked this row is no longer visible.
        return _parked(outcome, "no_plausible_candidate", from_status)

    # Body filter — a promotion precondition, not a tiebreak. A promoted
    # matched row must always carry non-empty lyric text.
    with_body = [
        c for c in plausible
        if not c.instrumental and (bool(c.plain_lyrics) or bool(c.synced_lyrics))
    ]
    if not with_body:
        if all(c.instrumental for c in plausible):
            # Mirrors decide_match's instrumental_or_empty resolution.
            ev = dict(outcome.evidence or {})
            ev["promotion"] = {
                "attempted": True,
                "resolution": "all_instrumental",
                "from_status": from_status,
            }
            return dataclasses.replace(
                outcome,
                match_status=STATUS_NO_LYRICS,
                evidence=ev,
                lyric_plain="",  # V33 CHECK: resolved statuses need non-NULL
                lyric_synced=None,
                match_basis=basis,
                matcher_version=f"{outcome.matcher_version}+{BEST_OF_VERSION}",
            )
        return _parked(outcome, "no_body_candidate", from_status)

    # Tier 1: exact stripped-base-title + version-token agreement (duration
    # already gated in _plausible). The only tier live in v1.
    track_tokens = extract_version_tokens(title or "")
    tier1 = [
        c for c in with_body
        if exact_base_equal(title or "", c.title)
        and len(track_tokens ^ extract_version_tokens(c.title)) == 0
    ]
    if not tier1:
        return _parked(outcome, "no_tier1_candidate", from_status)

    # Richest lyric wins (synced > plain); duration gap then id only make the
    # pick deterministic among same-song duplicate uploads — they are NOT the
    # gated tier-2/4 promotion criteria.
    def _rank(c: Candidate):
        gap = (
            abs(float(duration_sec) - float(c.duration_sec))
            if duration_sec is not None and c.duration_sec is not None
            else float("inf")
        )
        return (not bool(c.synced_lyrics), not bool(c.plain_lyrics), gap, str(c.id))

    chosen = sorted(tier1, key=_rank)[0]
    cand_tokens = extract_version_tokens(chosen.title)

    ev = dict(outcome.evidence or {})
    ev["promotion"] = {
        "chosen": {
            "lrclib_id": chosen.id,
            "title": chosen.title,
            "artist": chosen.artist,
            "duration_sec": chosen.duration_sec,
        },
        "criterion": CRITERION_TIER1,
        "from_status": from_status,
    }
    return MatchOutcome(
        track_id=outcome.track_id,
        match_status=STATUS_MATCHED,
        evidence=ev,
        lyric_plain=chosen.plain_lyrics,
        lyric_synced=chosen.synced_lyrics,
        matcher_version=f"{outcome.matcher_version}+{BEST_OF_VERSION}",
        match_basis=basis,
        version_tokens_track=sorted(track_tokens),
        version_tokens_candidate=sorted(cand_tokens),
        version_agrees=True,
    )
