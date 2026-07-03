# worker/service/lyrics_eval_core.py
"""Shared bounded LRCLIB-evaluation loop for FEAT-lyrics-corpus Steps 3 & 4.

Both the incremental collector (Step 3) and the reassessment job (Step 4) run the same
subtle loop: fetch LRCLIB candidates for each track across a bounded thread pool, decide
with the canonical ``decide_match``, and commit per row inside a hard wall-clock budget so
the worker Lambda (120s) never times out mid-run. They differ only in (a) which tracks they
select and (b) whether a given outcome may overwrite the existing row (Step 4's replacement
guard). This module owns the loop; each service owns its selection SQL + write gate. RFC:
``docs/rfcs/FEAT-lyrics-corpus.md``.
"""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional

from sqlalchemy.orm import Session

from worker.clients.lrclib_client import LrclibClient, LrclibTransientError
from worker.service.lyrics_matcher import (
    STATUS_AMBIGUOUS,
    STATUS_MATCHED,
    STATUS_REVIEW_REQUIRED,
    MatchOutcome,
    TrackLyricsWriter,
    decide_match,
)
from worker.service.lyrics_promote import (
    BASIS_BEST_OF_AMBIGUOUS,
    BASIS_BEST_OF_REVIEW,
    promote_best,
)

logger = logging.getLogger(__name__)

# should_write(row, outcome) -> bool. ``row`` may carry 'existing_status'/'existing_basis'.
WriteGate = Callable[[Dict[str, Any], MatchOutcome], bool]

# The conservative matcher's exact-title/fuzzy bases, whose ``matched`` rows must carry
# version agreement (a ``matched`` + ``version_agrees=False`` is a matcher bug). Best-of
# bases (FEAT-lyrics-best-of-promotion) are validated by the presence of an
# ``evidence["promotion"]`` block instead — the basis-aware form of the invariant so
# that opening the gated tiers later cannot silently zero out best-of writes as
# consistency errors (RFC §Target state).
_BEST_OF_BASES = {BASIS_BEST_OF_AMBIGUOUS, BASIS_BEST_OF_REVIEW}


def _consistency_ok(outcome: MatchOutcome) -> bool:
    """Basis-aware matched-row invariant.

    ``exact-title`` / ``fuzzy-title`` / unknown bases ⇒ the conservative rule: a
    matched row must agree on version tokens. ``best-of-*`` ⇒ the promotion block must
    be present (the matched row came from a promotion, which records its choice + why).
    Under v1 tier-1-only every promotion also carries ``version_agrees=True``, so the
    version rule still holds for best-of rows; this just refuses a best-of ``matched``
    row that lost its provenance block.
    """
    if outcome.match_status != STATUS_MATCHED:
        return True
    if outcome.match_basis in _BEST_OF_BASES:
        ev = outcome.evidence or {}
        return isinstance(ev.get("promotion"), dict)
    # Conservative basis: version agreement is the invariant.
    return outcome.version_agrees is not False


def new_metrics() -> Dict[str, Any]:
    """Fresh zeroed metrics dict (RFC §Metrics + loop bookkeeping).

    ``promoted_ambiguous`` / ``promoted_review`` count rows this run lifted out of the
    parked pool via ``promote_best`` (a ``best-of-*`` ``matched`` or the
    all-instrumental ``no_lyrics`` resolution). ``promotion_criteria`` is the
    per-criterion breakdown (tier-1 = ``exact-base-title``); ``promotion_parked`` counts
    rows that *stayed* parked after a promotion attempt (reason recorded in evidence).
    """
    return {
        "evaluated": 0,          # rows written this run
        STATUS_MATCHED: 0,
        "no_lyrics": 0,
        "not_found": 0,
        "ambiguous": 0,
        "review_required": 0,
        "skipped_transient": 0,  # LRCLIB error — left for the next run, never poisoned
        "skipped_budget": 0,     # not reached before the wall-clock budget expired
        "consistency_errors": 0, # invariant breach (never written) — basis-aware now
        "guard_kept": 0,         # existing row protected by the write gate (not overwritten)
        "promoted_ambiguous": 0,  # best-of-ambiguous promotions this run
        "promoted_review": 0,     # best-of-review promotions this run
        "promotion_parked": 0,    # promotion attempted, row stayed parked
        "promotion_criteria": {  # per-criterion breakdown of promotions
            "exact-base-title": 0,
        },
    }


def run_eval_batch(
    session: Session,
    tracks: List[Dict[str, Any]],
    *,
    concurrency: int,
    time_budget_sec: float,
    client: Optional[LrclibClient] = None,
    should_write: Optional[WriteGate] = None,
    log_prefix: str = "Lyrics eval",
) -> Dict[str, Any]:
    """Evaluate ``tracks`` against LRCLIB and persist outcomes, bounded by a wall clock.

    ``tracks``: dicts ``{id, title, duration_sec, artist_names, aliases}`` (+ optional
    ``existing_status`` / ``existing_basis`` for a write gate). The caller materializes the
    selection and owns the session; per-row commits keep the connection warm through the slow
    LRCLIB loop (reference-db-session-across-long-external-loop).

    ``should_write``: consulted before persisting each outcome (default: always write).
    Returning False leaves the existing row intact and counts it under ``guard_kept``.
    """
    metrics = new_metrics()
    if not tracks:
        logger.info("%s: nothing to process", log_prefix)
        return metrics

    logger.info("%s: %d tracks, concurrency=%d, budget=%.0fs",
                log_prefix, len(tracks), concurrency, time_budget_sec)

    owns_client = client is None
    client = client or LrclibClient(max_connections=concurrency + 4)
    writer = TrackLyricsWriter(session)
    deadline = time.monotonic() + time_budget_sec

    def fetch_one(row: Dict[str, Any]):
        """Worker thread: only the slow LRCLIB fetch — no DB, no shared mutable state."""
        artist = (row["artist_names"] or [""])[0]
        try:
            return row, client.search_candidates(row["title"], artist), None
        except LrclibTransientError as exc:
            return row, None, exc

    # as_completed (NOT executor.map): map submits every fetch eagerly and the executor's
    # context-exit waits for ALL of them, so a stalled LRCLIB could run past the 120s Lambda
    # timeout. Here, hitting the deadline cancels not-yet-started fetches (cancel_futures) and
    # returns without waiting on in-flight ones — a real wall-clock cap. Completion order is
    # immaterial: rows are independent, and selection order only governs which tracks we chose.
    ex = ThreadPoolExecutor(max_workers=concurrency)
    try:
        futures = [ex.submit(fetch_one, row) for row in tracks]
        for fut in as_completed(futures):
            if time.monotonic() > deadline:
                logger.warning("%s: time budget hit — deferring remainder", log_prefix)
                break
            row, candidates, exc = fut.result()
            if exc is not None:
                metrics["skipped_transient"] += 1
                logger.warning("%s: skip track %s (%r) — LRCLIB transient: %s",
                               log_prefix, row["id"], row["title"], exc)
                continue

            outcome = decide_match(
                track_id=row["id"],
                title=row["title"],
                artist_names=list(row["artist_names"] or []),
                aliases=list(row["aliases"] or []),
                duration_sec=row["duration_sec"],
                candidates=candidates,
            )
            # Step 2 (FEAT-lyrics-best-of-promotion): lift a parked ambiguous /
            # review_required outcome to a tagged best-of-* matched/no_lyrics. Runs
            # before the consistency invariant + the writer, reusing the SAME
            # in-memory candidates the fetch just produced — the candidate bodies live
            # here (only stripped when flattened into evidence), so promotion attaches a
            # lyric with no second LRCLIB round trip.
            if outcome.match_status in (STATUS_AMBIGUOUS, STATUS_REVIEW_REQUIRED):
                outcome = promote_best(
                    outcome,
                    title=row["title"],
                    artist_names=list(row["artist_names"] or []),
                    aliases=list(row["aliases"] or []),
                    duration_sec=row["duration_sec"],
                    candidates=candidates,
                )

            # Consistency invariant (basis-aware): a matched row must satisfy the
            # invariant for its basis — exact-title requires version agreement; a
            # best-of-* row requires its promotion block. A violation is a matcher bug
            # — never write the bad row.
            if not _consistency_ok(outcome):
                metrics["consistency_errors"] += 1
                logger.error("%s: CONSISTENCY VIOLATION track %s matched (basis=%s) "
                             "but invariant failed — row NOT written",
                             log_prefix, row["id"], outcome.match_basis)
                continue

            if should_write is not None and not should_write(row, outcome):
                metrics["guard_kept"] += 1
                continue

            writer.write_outcomes([outcome])  # per-row commit (sentinel included)
            metrics["evaluated"] += 1
            metrics[outcome.match_status] = metrics.get(outcome.match_status, 0) + 1
            _count_promotion(metrics, outcome)
    finally:
        # Whatever we didn't reach (budget) simply stays selectable for the next run.
        metrics["skipped_budget"] = (
            len(tracks) - metrics["evaluated"] - metrics["skipped_transient"]
            - metrics["consistency_errors"] - metrics["guard_kept"]
        )
        ex.shutdown(wait=False, cancel_futures=True)
        if owns_client:
            client.close()

    logger.info("%s complete: %s", log_prefix, metrics)
    return metrics


def _count_promotion(metrics: Dict[str, Any], outcome: MatchOutcome) -> None:
    """Tally best-of promotion counters for a written outcome.

    A parked outcome that ``promote_best`` lifted to ``matched`` / ``no_lyrics`` carries
    a ``best-of-*`` basis + an ``evidence["promotion"]`` block; count it by its origin
    status and the criterion that fired. A best-of outcome still parked (no body / no
    tier-1 candidate) keeps its parked status and counts under ``promotion_parked``.
    No-op for every non-best-of outcome (the conservative matcher's rows are unaffected).
    """
    ev = outcome.evidence or {}
    promotion = ev.get("promotion")
    if not isinstance(promotion, dict):
        return
    # A row that stayed parked (no_body_candidate / no_tier1_candidate /
    # no_plausible_candidate) is a promotion attempt that did not lift the row.
    if outcome.match_status in (STATUS_AMBIGUOUS, STATUS_REVIEW_REQUIRED):
        metrics["promotion_parked"] += 1
        return
    from_status = promotion.get("from_status")
    if from_status == STATUS_AMBIGUOUS:
        metrics["promoted_ambiguous"] += 1
    elif from_status == STATUS_REVIEW_REQUIRED:
        metrics["promoted_review"] += 1
    criterion = promotion.get("criterion")
    if criterion:
        criteria = metrics["promotion_criteria"]
        criteria[criterion] = criteria.get(criterion, 0) + 1
