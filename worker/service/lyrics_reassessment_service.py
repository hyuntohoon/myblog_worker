# worker/service/lyrics_reassessment_service.py
"""FEAT-lyrics-corpus Step 4: periodic reassessment + replacement guard (worker).

LRCLIB coverage grows over time, so tracks that parked as ``not_found`` / ``ambiguous`` /
``review_required`` earlier may become matchable later. This periodic EventBridge job
re-checks the **unresolved pool** (stalest first, behind the best-of supersession backlog —
DATA-catalog-noise Step 3b) with the same canonical ``decide_match``, and:

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
from worker.service.lyrics_eval_core import PRIMARY_ARTIST_NAMES_LATERAL, run_eval_batch
from worker.service.lyrics_label_yield import sync_exclusions
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
# replaced only when a NEW matched outcome carries a STRICTLY higher basis. The conservative
# matcher emits 'exact-title' for a real matched row; the best-of-* bases (FEAT-lyrics-best-of-
# promotion) sit just above fuzzy-title and below exact-title. Both best-of bases share the
# same rung so the strict-``>`` guard (a) lets an exact-title match supersede a best-of row,
# (b) refuses lateral best-of→best-of churn, (c) never lets a best-of displace an exact-title.
_BASIS_STRENGTH_MAP = {
    None: 0, "fuzzy-title": 1,
    "best-of-ambiguous": 2, "best-of-review": 2,
    "exact-title": 3, "mb-recording": 4, "isrc": 5,
}


def _basis_strength(basis: Optional[str]) -> int:
    """Strength of a basis on the replacement ladder (unknown basis ⇒ weakest)."""
    return _BASIS_STRENGTH_MAP.get(basis, 0)


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
    # A best-of-* matched row is now re-selected by the widened _fetch_unresolved_tracks
    # (FEAT-lyrics-best-of-promotion Step 2), so it reaches this guard. The strictly-``>``
    # ladder lets an exact-title / mb-recording / isrc match supersede it, refuses a
    # lateral best-of→best-of swap (both at rung 2), and never lets a best-of displace an
    # exact-title (3 > 3 is False) — the RFC's supersession-without-churn rule.
    return _basis_strength(outcome.match_basis) > _basis_strength(row.get("existing_basis"))


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
        """Re-check up to ``limit`` rows — best-of backlog first, then unresolved (stalest first).

        Reading the metrics: ``matched`` / ``no_lyrics`` are rows that left the unresolved
        pool (*promotions*); ``not_found`` / ``ambiguous`` / ``review_required`` are
        *refreshed* rows that stayed parked. Since Step 3b the batch also carries best-of
        rows, for which the story is different — ``bestof_superseded`` counts the ones
        upgraded to a stronger basis, and ``guard_kept`` counts the ones whose content the
        replacement guard protected (those are re-checked and rotated, never overwritten).
        """
        limit = limit or settings.LYRICS_REASSESS_BATCH_LIMIT
        # Recompute label-yield exclusions before selecting. This is what makes rule F
        # self-correcting: a label that starts matching releases its own rows here, in
        # the same pass that a newly-dead label claims its rows. It also repairs marks
        # that the matcher's `ON CONFLICT ... evidence = EXCLUDED.evidence` overwrote.
        exclusions = sync_exclusions(self.session)
        tracks = self._fetch_unresolved_tracks(limit)
        # `_fetch_unresolved_tracks` opens a read transaction. Release it before
        # `run_eval_batch` starts the external LRCLIB loop.
        self.session.commit()
        metrics = run_eval_batch(
            self.session, tracks,
            concurrency=self.concurrency,
            time_budget_sec=self.time_budget_sec,
            client=self._client,
            should_write=should_replace,
            touch_on_guard_kept=True,
            log_prefix="Lyrics reassessment",
        )
        metrics["exclusions"] = exclusions
        return metrics

    def reassess_album(
        self,
        album_id: str,
        limit: Optional[int] = None,
        cooldown_sec: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Expedite one album: re-check its unresolved tracks NOW, out of turn.

        The periodic pass rotates stalest-first across a five-figure pool, so a just-released
        album whose lyrics DO exist on LRCLIB can sit months behind the queue head. This is the
        same evaluation under the same replacement guard, scoped to one album — nothing here
        can promote a row the daily job would not have promoted eventually.

        Two deliberate differences from ``reassess``:

        * **The label-yield exclusion is bypassed** (the selection below omits the
          ``excluded_by`` filter). An excluded row is one the pool decided not to spend a
          scheduled re-check on; an explicit request for THIS album overrides that, and a
          promotion here is also the strongest possible evidence that the exclusion was wrong.
        * **No ``sync_exclusions``.** That is pool-wide maintenance the daily pass owns; running
          it on a single-album request would add two writes and a lock for no benefit, since
          this path ignores the marks anyway.

        ``cooldown_sec`` is the idempotency bound: rows re-checked within the window are not
        selected. SQS delivers at least once and the writer's ``ON CONFLICT`` sets
        ``updated_at = NOW()``, so the cooldown is what makes a double-fire cheap instead of a
        second full LRCLIB sweep. Pass ``0`` to force an immediate re-run (``None`` ⇒ setting).

        Only tracks that already have a ``track_lyrics`` row are candidates; a brand-new album
        whose tracks have never been evaluated belongs to the incremental collector (Step 3),
        which the album-sync path chains automatically.
        """
        limit = limit if limit is not None else settings.LYRICS_REASSESS_BATCH_LIMIT
        cooldown_sec = (
            cooldown_sec if cooldown_sec is not None else settings.LYRICS_EXPEDITE_COOLDOWN_SEC
        )
        tracks = self._fetch_album_tracks(album_id, limit, cooldown_sec)
        # As above, do not leave the selection transaction open across LRCLIB.
        self.session.commit()
        metrics = run_eval_batch(
            self.session, tracks,
            concurrency=self.concurrency,
            time_budget_sec=self.time_budget_sec,
            client=self._client,
            should_write=should_replace,
            touch_on_guard_kept=True,
            log_prefix=f"Lyrics expedite (album {album_id})",
        )
        metrics["album_id"] = str(album_id)
        return metrics

    def _fetch_album_tracks(
        self, album_id: str, limit: int, cooldown_sec: float
    ) -> List[Dict[str, Any]]:
        """Expedite targets: one album's unresolved (+ best-of) rows, exclusion bypassed.

        Deliberately a near-copy of ``_fetch_unresolved_tracks``: same status arms, same
        projection, same shared LATERAL — so the expedite evaluates exactly what the scheduled
        pass would, and the two cannot drift into disagreeing about what "unresolved" means.
        Two differences are the point of the method: the album filter, and the **absence**
        of the ``NOT (tl.evidence ? 'excluded_by')`` guard.

        Two further differences are scoping artefacts, not policy, and must stay that way:
        this path keeps unresolved rows FIRST (the scheduled pass inverts that to drain the
        best-of backlog — DATA-catalog-noise Step 3b), and it applies no best-of rest
        interval. Both are queue-fairness devices for a five-figure pool; an album holds a
        few dozen tracks and ``limit`` covers all of them, so ordering decides nothing here
        and a rest interval would only refuse work a human explicitly asked for.

        ``make_interval(secs => :cooldown_sec)`` rather than ``:cooldown_sec * INTERVAL '1 s'``:
        the function signature pins the bind parameter to ``double precision``, where the
        multiplication form leaves Postgres to infer a bare parameter's type and fail with
        "could not determine data type" (the workspace #84 → #85 failure, verbatim).
        """
        rows = self.session.execute(
            text(
                f"""
                SELECT t.id, t.title, t.duration_sec,
                       primary_artists.artist_names                     AS artist_names,
                       ARRAY_REMOVE(ARRAY_AGG(DISTINCT al.alias), NULL) AS aliases,
                       tl.match_status                     AS existing_status,
                       (tl.evidence ->> 'match_basis')     AS existing_basis
                FROM track_lyrics tl
                JOIN tracks t         ON t.id = tl.track_id
                JOIN track_artists ta ON ta.track_id = t.id
                JOIN artists a        ON a.id = ta.artist_id
                LEFT JOIN LATERAL jsonb_array_elements_text(a.aliases) AS al(alias) ON true
{PRIMARY_ARTIST_NAMES_LATERAL}
                WHERE t.album_id = :album_id
                  AND tl.updated_at < NOW() - make_interval(secs => :cooldown_sec)
                  AND (tl.match_status IN ('not_found', 'ambiguous', 'review_required')
                       OR (tl.match_status = 'matched'
                           AND tl.evidence ->> 'match_basis' LIKE 'best-of-%'))
                GROUP BY t.id, t.title, t.duration_sec,
                         tl.match_status, (tl.evidence ->> 'match_basis'), tl.updated_at,
                         primary_artists.artist_names
                ORDER BY
                    CASE WHEN tl.match_status IN ('not_found', 'ambiguous', 'review_required')
                         THEN 0 ELSE 1 END,
                    tl.updated_at ASC
                LIMIT :limit
                """
            ),
            {"album_id": album_id, "cooldown_sec": cooldown_sec, "limit": limit},
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

    def _fetch_unresolved_tracks(self, limit: int) -> List[Dict[str, Any]]:
        """Reassessment targets: best-of-* matched rows first, then the unresolved pool.

        FEAT-lyrics-best-of-promotion Step 2 widens the selection: a ``best-of-*``
        ``matched`` row must stay a reassessment target so a later ``exact-title`` match
        can supersede it via the replacement guard — without this arm a promoted row
        would leave the pool forever and the promised supersession could never occur.

        **That arm had never executed.** It sat behind the unresolved pool, and a
        stalest-first ``ORDER BY`` with ``LIMIT 150`` reaches it only when fewer than 150
        unresolved rows exist. Measured against prod 2026-08-03: 11,033 unresolved rows
        ahead of 2,765 best-of rows, with ~57 new unresolved rows arriving per day. The
        pool never empties, so the promise was structurally unreachable rather than merely
        rare.

        DATA-catalog-noise Step 3b therefore **inverts the priority**. The justification is
        measured yield per re-check, live against LRCLIB on 2026-08-03: **83.6% of best-of
        rows (112/134 sampled) now resolve to a strictly stronger ``exact-title`` match**,
        against **~5.5%** for the unresolved pool. A best-of slot is worth ~15 unresolved
        slots, so the backlog is drained first. (The RFC's own proposal for this method — a
        recency-tiered re-order of the unresolved pool — was measured and dropped: yield is
        flat across release-recency tiers, 10.95 / 10.77 / 8.70%. See the RFC's Step 3b.)

        **Why this terminates.** A superseded row leaves the ``best-of-%`` arm by changing
        its own basis, so the backlog is self-consuming: ~2,765 rows at 150/run ≈ 19 runs.
        The ~16% that cannot be superseded are the hazard — the guard refuses a lateral
        best-of→best-of swap, so they are re-checked but never rewritten. Two things stop
        them pinning the queue head forever: ``TrackLyricsWriter.touch`` advances their
        rotation cursor even when the guard keeps the row, and the interval below then
        rests them for 30 days. Once the backlog clears, this arm goes quiet on its own and
        the budget returns to unresolved recovery, no human action and no follow-up PR.

        Within each arm ``ORDER BY tl.updated_at ASC`` re-checks the longest-parked rows
        first; because a rewrite bumps ``updated_at`` (the writer's
        ``ON CONFLICT ... updated_at = NOW()``), reassessment rotates fairly across the
        whole pool over successive runs.

        ``make_interval(days => :n)`` rather than ``:n * INTERVAL '1 day'``: the function
        signature pins the bind parameter's type, where the multiplication form leaves
        Postgres to infer a bare parameter and fail with "could not determine data type"
        (the worker #84 → #85 failure, verbatim).
        """
        rows = self.session.execute(
            text(
                f"""
                SELECT t.id, t.title, t.duration_sec,
                       primary_artists.artist_names                     AS artist_names,
                       ARRAY_REMOVE(ARRAY_AGG(DISTINCT al.alias), NULL) AS aliases,
                       tl.match_status                     AS existing_status,
                       (tl.evidence ->> 'match_basis')     AS existing_basis
                FROM track_lyrics tl
                JOIN tracks t         ON t.id = tl.track_id
                JOIN track_artists ta ON ta.track_id = t.id
                JOIN artists a        ON a.id = ta.artist_id
                LEFT JOIN LATERAL jsonb_array_elements_text(a.aliases) AS al(alias) ON true
{PRIMARY_ARTIST_NAMES_LATERAL}
                WHERE NOT (tl.evidence ? 'excluded_by')
                  AND (tl.match_status IN ('not_found', 'ambiguous', 'review_required')
                       OR (tl.match_status = 'matched'
                           AND tl.evidence ->> 'match_basis' LIKE 'best-of-%'
                           AND tl.updated_at
                               < NOW() - make_interval(days => :bestof_rest_days)))
                GROUP BY t.id, t.title, t.duration_sec,
                         tl.match_status, (tl.evidence ->> 'match_basis'), tl.updated_at,
                         primary_artists.artist_names
                ORDER BY
                    CASE WHEN tl.match_status = 'matched' THEN 0 ELSE 1 END,
                    tl.updated_at ASC
                LIMIT :limit
                """
            ),
            {"limit": limit, "bestof_rest_days": settings.LYRICS_BESTOF_RECHECK_INTERVAL_DAYS},
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
