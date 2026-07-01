# worker/service/lyrics_incremental_service.py
"""FEAT-lyrics-corpus Step 3: incremental lyrics collection (worker, alias-fill pattern).

A periodic EventBridge job that keeps the private lyrics corpus current as new tracks
are ingested. It mirrors the MusicBrainz alias-fill job exactly:

  * **select** recently-added catalog tracks that lack a ``track_lyrics`` row,
  * **evaluate** each via the LRCLIB ``/api/search`` API (freshness over the stale dump)
    with the **same canonical matcher** as Step 2 (``decide_match`` — identical states +
    conservative thresholds),
  * **commit per row + sentinel on miss**: every evaluated track gets exactly one
    ``track_lyrics`` row (``matched`` / ``no_lyrics`` / ``not_found`` / ``ambiguous`` /
    ``review_required``). That row *is* the sentinel — it removes the track from the
    selection pool, so no track is re-evaluated every run,
  * **failure-isolated**: a transient LRCLIB error skips the track (unwritten -> retried
    next run, never poisoned as ``not_found``); this job is a *separate* Lambda invocation
    from the SQS album-sync path, so a lyrics-source outage can never block album sync.

Bounded per invocation (``settings.LYRICS_INCR_BATCH_LIMIT`` + a wall-clock budget) so it
always finishes inside the 120s worker Lambda timeout. Because writes commit per row, an
over-budget run simply leaves the remaining tracks for the next tick. RFC:
``docs/rfcs/FEAT-lyrics-corpus.md``.
"""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from worker.clients.lrclib_client import LrclibClient, LrclibTransientError
from worker.core.config import settings
from worker.service.lyrics_matcher import (
    STATUS_MATCHED,
    TrackLyricsWriter,
    decide_match,
)

logger = logging.getLogger(__name__)


class LyricsIncrementalService:
    """Evaluate newly-ingested tracks against LRCLIB and persist match outcomes."""

    def __init__(
        self,
        session: Session,
        client: Optional[LrclibClient] = None,
        *,
        concurrency: Optional[int] = None,
        time_budget_sec: Optional[float] = None,
    ) -> None:
        self.session = session
        self._client = client
        self._owns_client = client is None
        self.concurrency = concurrency or settings.LYRICS_INCR_CONCURRENCY
        self.time_budget_sec = (
            time_budget_sec if time_budget_sec is not None else settings.LYRICS_INCR_TIME_BUDGET_SEC
        )

    def collect(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """Process up to ``limit`` uncorpused tracks. Returns per-status metrics.

        Metrics denominators (RFC §Metrics): ``evaluated`` = tracks that got a row this
        run; ``skipped_transient`` = LRCLIB-error tracks left for the next run;
        ``skipped_budget`` = tracks not reached before the wall-clock budget expired.
        """
        limit = limit or settings.LYRICS_INCR_BATCH_LIMIT
        metrics = {
            "evaluated": 0,
            STATUS_MATCHED: 0,
            "no_lyrics": 0,
            "not_found": 0,
            "ambiguous": 0,
            "review_required": 0,
            "skipped_transient": 0,
            "skipped_budget": 0,
            "consistency_errors": 0,
        }

        # Materialize the selection up front and release the read cursor before the slow
        # LRCLIB loop (reference-db-session-across-long-external-loop). Per-row write
        # commits then keep the same connection warm.
        tracks = self._fetch_uncorpused_tracks(limit)
        if not tracks:
            logger.info("Lyrics incremental: no uncorpused tracks to process")
            return metrics

        logger.info(
            "Lyrics incremental: %d uncorpused tracks, concurrency=%d, budget=%.0fs",
            len(tracks), self.concurrency, self.time_budget_sec,
        )

        client = self._client or LrclibClient(max_connections=self.concurrency + 4)
        writer = TrackLyricsWriter(self.session)
        deadline = time.monotonic() + self.time_budget_sec

        def fetch_one(row: Dict[str, Any]):
            """Worker thread: only the slow LRCLIB fetch — no DB, no shared mutable state."""
            artist = (row["artist_names"] or [""])[0]
            try:
                return row, client.search_candidates(row["title"], artist), None
            except LrclibTransientError as exc:
                return row, None, exc

        # as_completed (NOT executor.map): map submits every fetch eagerly and the
        # executor's context-exit waits for ALL of them, so a stalled LRCLIB could run
        # past the 120s Lambda timeout. Here, hitting the deadline cancels not-yet-started
        # fetches (cancel_futures) and returns without waiting on in-flight ones — a real
        # wall-clock cap. Processing order (completion order) is immaterial: rows are
        # independent, and newest-first only governs which tracks were *selected*.
        ex = ThreadPoolExecutor(max_workers=self.concurrency)
        try:
            futures = [ex.submit(fetch_one, row) for row in tracks]
            for fut in as_completed(futures):
                if time.monotonic() > deadline:
                    logger.warning("Lyrics incremental: time budget hit — deferring remainder")
                    break
                row, candidates, exc = fut.result()
                if exc is not None:
                    metrics["skipped_transient"] += 1
                    logger.warning(
                        "Lyrics incremental: skip track %s (%r) — LRCLIB transient: %s",
                        row["id"], row["title"], exc,
                    )
                    continue

                outcome = decide_match(
                    track_id=row["id"],
                    title=row["title"],
                    artist_names=list(row["artist_names"] or []),
                    aliases=list(row["aliases"] or []),
                    duration_sec=row["duration_sec"],
                    candidates=candidates,
                )
                # Consistency invariant: a matched outcome must always agree on
                # version. A violation is a matcher bug — never write the bad row.
                if outcome.match_status == STATUS_MATCHED and outcome.version_agrees is False:
                    metrics["consistency_errors"] += 1
                    logger.error(
                        "Lyrics incremental: CONSISTENCY VIOLATION track %s matched but "
                        "version_agrees=False — row NOT written", row["id"],
                    )
                    continue

                writer.write_outcomes([outcome])  # per-row commit (sentinel included)
                metrics["evaluated"] += 1
                metrics[outcome.match_status] = metrics.get(outcome.match_status, 0) + 1
        finally:
            # Untouched tracks keep no row, so the next tick re-selects them.
            metrics["skipped_budget"] = (
                len(tracks) - metrics["evaluated"]
                - metrics["skipped_transient"] - metrics["consistency_errors"]
            )
            ex.shutdown(wait=False, cancel_futures=True)
            if self._owns_client:
                client.close()

        logger.info("Lyrics incremental complete: %s", metrics)
        return metrics

    def _fetch_uncorpused_tracks(self, limit: int) -> List[Dict[str, Any]]:
        """Recently-added tracks lacking a ``track_lyrics`` row, with artist names + aliases.

        Newest-first so a fresh ingest is corpus-covered promptly; the ``tl IS NULL`` gate
        is the sentinel filter (a written row — including ``not_found`` — drops the track
        from this pool). Query shape mirrors ``tools/lyrics_batch_api.fetch_catalog_tracks``.
        """
        rows = self.session.execute(
            text(
                """
                SELECT t.id, t.title, t.duration_sec,
                       ARRAY_REMOVE(ARRAY_AGG(DISTINCT a.name), NULL)   AS artist_names,
                       ARRAY_REMOVE(ARRAY_AGG(DISTINCT al.alias), NULL) AS aliases
                FROM tracks t
                JOIN track_artists ta ON ta.track_id = t.id
                JOIN artists a        ON a.id = ta.artist_id
                LEFT JOIN LATERAL jsonb_array_elements_text(a.aliases) AS al(alias) ON true
                LEFT JOIN track_lyrics tl ON tl.track_id = t.id
                WHERE tl.track_id IS NULL
                GROUP BY t.id, t.title, t.duration_sec, t.created_at
                ORDER BY t.created_at DESC
                LIMIT :limit
                """
            ),
            {"limit": limit},
        ).fetchall()
        return [
            {
                "id": r[0],
                "title": r[1],
                "duration_sec": r[2],
                "artist_names": list(r[3] or []),
                "aliases": list(r[4] or []),
            }
            for r in rows
        ]
