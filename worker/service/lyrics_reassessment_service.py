# worker/service/lyrics_reassessment_service.py
"""FEAT-lyrics-corpus Step 4: periodic reassessment + replacement guard (worker).

LRCLIB coverage grows over time, so tracks that parked as ``not_found`` / ``ambiguous`` /
``review_required`` earlier may become matchable later. This periodic EventBridge job
re-checks the **unresolved pool** (stalest first) with the same canonical ``decide_match``,
and:

  * **promotes** an unresolved track to ``matched`` / ``no_lyrics`` when the evidence now
    supports it,
  * **refreshes** a still-unresolved row (bumps ``updated_at`` so the queue rotates fairly
    across the whole unresolved pool instead of re-hitting the same stale rows),
  * **never silently overwrites a good match**: the replacement guard (``should_replace``)
    only lets a resolved ``matched`` / ``no_lyrics`` row be replaced by a new ``matched``
    outcome carrying **strictly stronger** evidence, never a downgrade or a lateral swap.

Step 4 *selects* only unresolved rows, so a good match is never even a candidate here; the
guard is the tested, defensive proof of the never-downgrade rule (and future-proofs the day
ISRC / MusicBrainz-recording evidence lets a re-check legitimately supersede a title+duration
match). Bounded to the 120s worker Lambda by the same batch-limit + wall-clock budget as
Step 3 (shared ``run_eval_batch``). RFC: ``docs/rfcs/FEAT-lyrics-corpus.md`` (Step 4).
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from worker.clients.lrclib_client import LrclibClient
from worker.core.config import settings
from worker.service.lyrics_eval_core import run_eval_batch
from worker.service.lyrics_matcher import (
    STATUS_AMBIGUOUS,
    STATUS_MATCHED,
    STATUS_NOT_FOUND,
    STATUS_REVIEW_REQUIRED,
    MatchOutcome,
)

logger = logging.getLogger(__name__)

_UNRESOLVED = (STATUS_NOT_FOUND, STATUS_AMBIGUOUS, STATUS_REVIEW_REQUIRED)

# Evidence-strength ladder for the replacement guard. A resolved (matched / no_lyrics) row is
# replaced only when a NEW matched outcome carries a STRICTLY higher basis. The current matcher
# only ever emits 'exact-title' for a matched row, so a matched row is never replaced in
# practice — the higher tiers are reserved for future ISRC / MusicBrainz-recording evidence.
_BASIS_STRENGTH = {None: 0, "fuzzy-title": 1, "exact-title": 2, "mb-recording": 3, "isrc": 4}


def should_replace(row: Dict[str, Any], outcome: MatchOutcome) -> bool:
    """Replacement guard consulted before persisting a re-evaluated outcome.

    ``row`` carries ``existing_status`` + ``existing_basis`` (the current row's state).
    Unresolved rows carry no good match to protect and are always rewritten (promote on
    success, refresh + rotate otherwise). A resolved good row is protected: only a NEW
    ``matched`` outcome with strictly stronger evidence may replace it.
    """
    existing_status = row.get("existing_status")
    if existing_status in _UNRESOLVED:
        return True
    if outcome.match_status != STATUS_MATCHED:
        return False  # never downgrade a resolved good row to an unresolved/no_lyrics state
    return (
        _BASIS_STRENGTH.get(outcome.match_basis, 0)
        > _BASIS_STRENGTH.get(row.get("existing_basis"), 0)
    )


class LyricsReassessmentService:
    """Re-evaluate unresolved corpus rows against current LRCLIB coverage."""

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
        self.concurrency = concurrency or settings.LYRICS_REASSESS_CONCURRENCY
        self.time_budget_sec = (
            time_budget_sec if time_budget_sec is not None else settings.LYRICS_REASSESS_TIME_BUDGET_SEC
        )

    def reassess(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """Re-check up to ``limit`` unresolved tracks (stalest first). Returns per-status metrics.

        In this all-unresolved pool the metrics read directly as the reassessment story:
        ``matched`` / ``no_lyrics`` counts are *promotions*; ``not_found`` / ``ambiguous`` /
        ``review_required`` are *refreshed* rows that stayed parked; ``guard_kept`` protects
        any (defensively passed) resolved row from being overwritten.
        """
        limit = limit or settings.LYRICS_REASSESS_BATCH_LIMIT
        tracks = self._fetch_unresolved_tracks(limit)
        return run_eval_batch(
            self.session, tracks,
            concurrency=self.concurrency,
            time_budget_sec=self.time_budget_sec,
            client=self._client,
            should_write=should_replace,
            log_prefix="Lyrics reassessment",
        )

    def _fetch_unresolved_tracks(self, limit: int) -> List[Dict[str, Any]]:
        """Unresolved corpus rows (stalest first) with artist names + aliases + current state.

        ``ORDER BY tl.updated_at ASC`` re-checks the longest-parked rows first; because a
        rewrite bumps ``updated_at`` (the writer's ``ON CONFLICT ... updated_at = NOW()``),
        the reassessment rotates fairly through the whole unresolved pool over successive runs.
        """
        rows = self.session.execute(
            text(
                """
                SELECT t.id, t.title, t.duration_sec,
                       ARRAY_REMOVE(ARRAY_AGG(DISTINCT a.name), NULL)   AS artist_names,
                       ARRAY_REMOVE(ARRAY_AGG(DISTINCT al.alias), NULL) AS aliases,
                       tl.match_status                     AS existing_status,
                       (tl.evidence ->> 'match_basis')     AS existing_basis
                FROM track_lyrics tl
                JOIN tracks t         ON t.id = tl.track_id
                JOIN track_artists ta ON ta.track_id = t.id
                JOIN artists a        ON a.id = ta.artist_id
                LEFT JOIN LATERAL jsonb_array_elements_text(a.aliases) AS al(alias) ON true
                WHERE tl.match_status IN ('not_found', 'ambiguous', 'review_required')
                GROUP BY t.id, t.title, t.duration_sec,
                         tl.match_status, (tl.evidence ->> 'match_basis'), tl.updated_at
                ORDER BY tl.updated_at ASC
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
                "existing_status": r[5],
                "existing_basis": r[6],
            }
            for r in rows
        ]
