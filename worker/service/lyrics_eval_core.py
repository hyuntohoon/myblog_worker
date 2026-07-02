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
    STATUS_MATCHED,
    MatchOutcome,
    TrackLyricsWriter,
    decide_match,
)

logger = logging.getLogger(__name__)

# should_write(row, outcome) -> bool. ``row`` may carry 'existing_status'/'existing_basis'.
WriteGate = Callable[[Dict[str, Any], MatchOutcome], bool]


def new_metrics() -> Dict[str, Any]:
    """Fresh zeroed metrics dict (RFC §Metrics + loop bookkeeping)."""
    return {
        "evaluated": 0,          # rows written this run
        STATUS_MATCHED: 0,
        "no_lyrics": 0,
        "not_found": 0,
        "ambiguous": 0,
        "review_required": 0,
        "skipped_transient": 0,  # LRCLIB error — left for the next run, never poisoned
        "skipped_budget": 0,     # not reached before the wall-clock budget expired
        "consistency_errors": 0, # matched-but-version-disagrees invariant breach (never written)
        "guard_kept": 0,         # existing row protected by the write gate (not overwritten)
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
            # Consistency invariant: a matched outcome must always agree on version.
            # A violation is a matcher bug — never write the bad row.
            if outcome.match_status == STATUS_MATCHED and outcome.version_agrees is False:
                metrics["consistency_errors"] += 1
                logger.error("%s: CONSISTENCY VIOLATION track %s matched but "
                             "version_agrees=False — row NOT written", log_prefix, row["id"])
                continue

            if should_write is not None and not should_write(row, outcome):
                metrics["guard_kept"] += 1
                continue

            writer.write_outcomes([outcome])  # per-row commit (sentinel included)
            metrics["evaluated"] += 1
            metrics[outcome.match_status] = metrics.get(outcome.match_status, 0) + 1
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
