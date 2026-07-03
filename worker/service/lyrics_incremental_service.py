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
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from worker.clients.lrclib_client import LrclibClient
from worker.core.config import settings
from worker.service.lyrics_eval_core import run_eval_batch

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
        self.concurrency = concurrency or settings.LYRICS_INCR_CONCURRENCY
        self.time_budget_sec = (
            time_budget_sec if time_budget_sec is not None else settings.LYRICS_INCR_TIME_BUDGET_SEC
        )

    def collect(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """Process up to ``limit`` uncorpused tracks (newest first). Returns per-status metrics.

        No write gate: an uncorpused track has no existing row to protect, so every outcome
        is written (the row is the sentinel that drops the track from the pool).
        """
        limit = limit or settings.LYRICS_INCR_BATCH_LIMIT
        # Materialize the selection up front, before the slow LRCLIB loop
        # (reference-db-session-across-long-external-loop).
        tracks = self._fetch_uncorpused_tracks(limit)
        return run_eval_batch(
            self.session, tracks,
            concurrency=self.concurrency,
            time_budget_sec=self.time_budget_sec,
            client=self._client,
            should_write=self._still_uncorpused,
            log_prefix="Lyrics incremental",
        )

    def _still_uncorpused(self, row: Dict[str, Any], outcome: Any) -> bool:
        """Concurrent-run guard (near-real-time SQS chaining). The selection is
        materialized before the slow LRCLIB loop, so a chained peer invocation may
        corpus a track while this run is still fetching; without a gate the writer's
        upsert is last-writer-wins and this run could replace the peer's row with a
        weaker outcome. Re-check just before writing; a lost race is counted as
        ``guard_kept``. (The remaining check→write window is ms-scale and both sides
        evaluated the same LRCLIB candidates, so it is benign.)"""
        exists = self.session.execute(
            text("SELECT 1 FROM track_lyrics WHERE track_id = :tid"),
            {"tid": row["id"]},
        ).first()
        return exists is None

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
