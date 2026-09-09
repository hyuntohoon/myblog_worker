# worker/service/lyrics_demand_source_service.py
"""FEAT-lyrics-listening-experience Step 3 — targeted source collection for album demand.

V57 (Step 2) stores durable album-level translation demand but nothing fills it: the
existing collectors cannot serve it.

  * ``LyricsIncrementalService`` selects **globally**, newest-first, and only tracks that
    have no ``track_lyrics`` row at all. A demanded album from 2019 is behind every new
    ingest forever.
  * ``LyricsReassessmentService.reassess_album`` is album-scoped but selects **only rows
    that already exist** (``FROM track_lyrics tl JOIN tracks``), so a demanded album whose
    tracks were never evaluated yields an empty batch — the exact "no source row" case
    Step 3 has to cover.

This job is the album-scoped union of the two, driven by live demand rather than by
ingest recency: for one due album job it evaluates *both* the tracks with no corpus row
(first fetch) and the tracks parked unresolved (reassessment), through the same shared
``run_eval_batch`` loop, the same canonical ``decide_match``, and the same never-downgrade
replacement guard. It adds no matching policy of its own.

What it then does that the corpus jobs do not is record the outcome **against the
demand**, so waiting demand advances without a second manual request:

    no_lyrics                        -> not_required('no_lyrics')      [OQ5 terminal-ish]
    matched + non-blank body         -> left source_pending, untouched [poller links it]
    not_found/ambiguous/review_req.  -> source_pending + backoff ladder
    LRCLIB transient (no row written) -> not written at all            [retried next run]

**Why ``matched`` is deliberately NOT linked here.** ``ensure_work`` needs the source
fingerprint, and that fingerprint must equal what the read path re-derives — it comes from
``normalize_lyrics``/``compute_source_fingerprint`` in *myblog_backend*, which this repo
cannot import. Re-implementing the normalizer here would create exactly the duplicated
cross-repo text this project has been bitten by before. Instead the two consumers partition
``source_pending`` by a join the database can answer:

    this job's pool  : source_pending AND NOT (usable source exists)   -> fetch it
    the poller's pool: source_pending AND      (usable source exists)  -> link + translate

so a track never sits in both, and ``ensure_work`` moving it to ``linked`` removes it from
both at once. The poller owns the fingerprint because it already owns the normalizer.

Bounded per invocation like every other worker job (batch limit + wall-clock budget) so a
run always finishes inside the 120s Lambda; per-row commits make an over-budget run
resumable. RFC: ``docs/rfcs/FEAT-lyrics-listening-experience.md`` (Step 3, OQ5).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from myblog_shared_db.lyrics_demand import LyricsDemandStore, StaleDiscovery

from worker.clients.lrclib_client import LrclibClient
from worker.core.config import settings
from worker.service.lyrics_eval_core import PRIMARY_ARTIST_NAMES_LATERAL, run_eval_batch
from worker.service.lyrics_matcher import MatchOutcome
from worker.service.lyrics_reassessment_service import should_replace

logger = logging.getLogger(__name__)

# A track_lyrics row that the read path can actually show. Mirrors the poller's SWEEP_SQL
# predicate and `normalize_lyrics` availability=="ok" (matched + a non-blank synced or
# plain body), so this job and the poller agree on which pool a track belongs to. A
# `matched` row with an empty body normalizes to "unavailable", so it stays OUR problem.
USABLE_SOURCE_SQL = """(
    tl.match_status = 'matched'
    AND (btrim(coalesce(tl.lyric_synced, '')) <> ''
         OR btrim(coalesce(tl.lyric_plain, '')) <> '')
)"""

# A job whose demand is still live: at least one un-revoked member scope wants it, and the
# job has not been cancelled by the last removal. `claim_work` applies the same rule to
# translation work, so an album cannot keep spending LRCLIB calls after its last member
# unsaved it.
LIVE_DEMAND_SQL = """(
    NOT j.cancelled
    AND EXISTS (
        SELECT 1 FROM lyrics_album_demands d
        JOIN lyrics_discovery_scopes s ON s.id = d.scope_id AND s.active
        WHERE d.job_id = j.id
    )
)"""


def next_attempt(
    now: datetime,
    previous_gap: Optional[timedelta],
    *,
    base_sec: float,
    cap_sec: float,
) -> datetime:
    """OQ5 backoff ladder: ``min(cap, max(base, 2 x previous))``, no counter column.

    V57 gives ``lyrics_album_tracks`` / ``lyrics_album_jobs`` a ``next_attempt_at`` and an
    ``updated_at`` but deliberately no ``attempts`` — the owner's rule is that demand is
    never discarded on a failure count, and the absence of the column is what enforces it.
    The ladder therefore has to be recoverable without one, and it is: both columns are
    written by the *same* statement, so ``next_attempt_at - updated_at`` IS the gap that
    statement chose. Reading it back recovers the ladder position exactly, and the clamps
    below mean a hand-edited or clock-skewed row re-enters the ladder rather than escaping
    it (a negative or absent gap restarts at ``base``, never at zero and never past ``cap``).

    The caller passes the DATABASE's ``now()`` so the gap it encodes is exact rather than
    skewed by the Lambda's clock.
    """
    if previous_gap is None or previous_gap.total_seconds() <= 0:
        gap = base_sec
    else:
        gap = min(cap_sec, max(base_sec, 2.0 * previous_gap.total_seconds()))
    return now + timedelta(seconds=gap)


def _previous_gap(row: Dict[str, Any]) -> Optional[timedelta]:
    """Recover the last scheduled gap from the pair of timestamps that encode it."""
    nxt, updated = row.get("next_attempt_at"), row.get("updated_at")
    if nxt is None or updated is None:
        return None
    return nxt - updated


class LyricsDemandSourceService:
    """Fill V57 source state for albums that members are actually waiting on."""

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
        self.concurrency = concurrency or settings.LYRICS_DEMAND_CONCURRENCY
        self.time_budget_sec = (
            time_budget_sec
            if time_budget_sec is not None
            else settings.LYRICS_DEMAND_TIME_BUDGET_SEC
        )

    # ── entry point ────────────────────────────────────────────────────────────────────
    def collect(
        self, limit: Optional[int] = None, job_limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """Resolve due album catalogs, then evaluate due tracks for the demanded albums.

        Catalog resolution runs first and in its own short transactions: it is pure DB work
        and it is what turns a brand-new demand row into an enumerated track list for the
        LRCLIB pass below to work on. A job that cannot be resolved is deferred on the
        catalog ladder and does not block the others.
        """
        limit = limit or settings.LYRICS_DEMAND_BATCH_LIMIT
        job_limit = job_limit or settings.LYRICS_DEMAND_JOB_LIMIT

        catalog = self._resolve_due_catalogs(job_limit)

        # Materialize the whole selection BEFORE the slow LRCLIB loop; the snapshot's
        # work_id/source_revision are what the write-back validates against, so an
        # observation that raced with a member removal or a re-enumeration is rejected
        # instead of overwriting fresher state.
        tracks = self._fetch_due_tracks(limit)

        # `clock_timestamp()`, not `now()`: `now()` is the enclosing transaction's start
        # time, which would predate rows written moments earlier and let them masquerade as
        # this run's outcomes. Read it BEFORE the commit below so the marker cannot land
        # after an evaluation write.
        run_started_at = self.session.execute(
            text("SELECT clock_timestamp()")
        ).scalar_one()

        # Close the read transaction before any external call. The SELECT above leaves this
        # session idle-in-transaction otherwise, and `run_eval_batch` submits every LRCLIB
        # fetch up front — so the connection would sit open for the whole batch. That is the
        # exact shape behind this project's Neon `ProtocolViolation`
        # (reference-db-session-across-long-external-loop), and a regression test asserts
        # against `pg_stat_activity` that it does not happen here.
        self.session.commit()

        metrics = run_eval_batch(
            self.session,
            tracks,
            concurrency=self.concurrency,
            time_budget_sec=self.time_budget_sec,
            client=self._client,
            should_write=self._write_gate,
            log_prefix="Lyrics demand source",
        )
        metrics["catalog"] = catalog
        metrics["demand"] = self._record_source_outcomes(tracks, run_started_at)
        return metrics

    # ── catalog resolution ─────────────────────────────────────────────────────────────
    def _resolve_due_catalogs(self, job_limit: int) -> Dict[str, int]:
        """Attach the catalog album + its track list to due jobs, one short txn each.

        ``set_catalog`` validates the provider identity and the track membership itself and
        refuses a partial set as ``complete``; this method only decides *whether* the album
        is fully enumerable right now. ``total_tracks`` is the provider's own count, so
        "we hold every track the provider says exists" is the completeness test — anything
        less stays incomplete and is retried, which is what stops a half-ingested album
        from ever reporting done.
        """
        counts = {"resolved": 0, "complete": 0, "deferred": 0, "stale": 0}
        rows = self.session.execute(
            text(
                f"""
                SELECT j.id, j.spotify_album_id, j.next_attempt_at, j.updated_at
                FROM lyrics_album_jobs j
                WHERE {LIVE_DEMAND_SQL}
                  AND (j.album_id IS NULL OR NOT j.enumeration_complete)
                  AND (j.next_attempt_at IS NULL OR j.next_attempt_at <= now())
                ORDER BY j.next_attempt_at NULLS FIRST, j.created_at
                LIMIT :limit
                """
            ),
            {"limit": job_limit},
        ).mappings().all()
        if not rows:
            return counts

        for job in [dict(r) for r in rows]:
            try:
                self._resolve_one_catalog(job, counts)
                self.session.commit()
            except StaleDiscovery:
                self.session.rollback()
                counts["stale"] += 1
            except Exception:  # noqa: BLE001 — one bad job must not sink the batch
                self.session.rollback()
                counts["deferred"] += 1
                logger.exception(
                    "Lyrics demand source: catalog resolution failed for job %s", job["id"]
                )
        logger.info("Lyrics demand source: catalog %s", counts)
        return counts

    def _resolve_one_catalog(self, job: Dict[str, Any], counts: Dict[str, int]) -> None:
        store = LyricsDemandStore(self.session.connection())
        album = self.session.execute(
            text(
                "SELECT id, total_tracks FROM albums WHERE spotify_id = :sid"
            ),
            {"sid": job["spotify_album_id"]},
        ).mappings().one_or_none()

        now = self.session.execute(text("SELECT now() AS now")).scalar_one()
        if album is None:
            # Not an error: the album simply has not been ingested yet. Demand is
            # preserved and the catalog ladder paces the re-check.
            store.defer_catalog(
                job["id"],
                "album_not_in_catalog",
                next_attempt(
                    now,
                    _previous_gap(job),
                    base_sec=settings.LYRICS_DEMAND_CATALOG_RETRY_BASE_SEC,
                    cap_sec=settings.LYRICS_DEMAND_CATALOG_RETRY_CAP_SEC,
                ),
            )
            counts["deferred"] += 1
            return

        track_ids = list(
            self.session.execute(
                text("SELECT id FROM tracks WHERE album_id = :aid ORDER BY id"),
                {"aid": album["id"]},
            ).scalars()
        )
        expected = album["total_tracks"]
        complete = bool(track_ids) and (expected is None or len(track_ids) == expected)
        if not track_ids:
            store.defer_catalog(
                job["id"],
                "album_has_no_tracks",
                next_attempt(
                    now,
                    _previous_gap(job),
                    base_sec=settings.LYRICS_DEMAND_CATALOG_RETRY_BASE_SEC,
                    cap_sec=settings.LYRICS_DEMAND_CATALOG_RETRY_CAP_SEC,
                ),
            )
            counts["deferred"] += 1
            return

        store.set_catalog(job["id"], album["id"], track_ids, complete=complete)
        counts["resolved"] += 1
        if complete:
            counts["complete"] += 1
        else:
            # Enumeration is attached but partial: schedule the re-check explicitly, since
            # set_catalog clears next_attempt_at on success.
            store.defer_catalog(
                job["id"],
                "album_partially_ingested",
                next_attempt(
                    now,
                    _previous_gap(job),
                    base_sec=settings.LYRICS_DEMAND_CATALOG_RETRY_BASE_SEC,
                    cap_sec=settings.LYRICS_DEMAND_CATALOG_RETRY_CAP_SEC,
                ),
            )

    # ── source evaluation ──────────────────────────────────────────────────────────────
    def _write_gate(self, row: Dict[str, Any], outcome: MatchOutcome) -> bool:
        """Per-arm write gate: first-fetch rows use a concurrency guard, parked rows the
        canonical replacement guard.

        The two arms of this job's selection are the two existing collectors' pools, so
        each keeps the gate its own collector uses. ``existing_status is None`` is the
        incremental collector's case — there is no row to protect, only a peer invocation
        to lose a race to (this job runs alongside the 15-minute global collector, which
        may corpus the same track between our selection and our write). Everything else is
        a parked row and goes through ``should_replace``, so the never-downgrade rule
        stays defined in exactly one place.
        """
        if row.get("existing_status") is None:
            return (
                self.session.execute(
                    text("SELECT 1 FROM track_lyrics WHERE track_id = :tid"),
                    {"tid": row["id"]},
                ).first()
                is None
            )
        return should_replace(row, outcome)

    def _fetch_due_tracks(self, limit: int) -> List[Dict[str, Any]]:
        """Due tracks of demanded albums, in both arms, with their V57 observation keys.

        The ``NOT usable`` filter is what splits this job's pool from the poller's: a track
        whose source is already showable is not re-fetched here, it is waiting to be linked.
        ``LEFT JOIN track_lyrics`` (rather than the reassessment job's inner join) is what
        admits the never-evaluated tracks — the case the album-scoped expedite cannot see.

        ``next_attempt_at``/``updated_at`` come back so the ladder position can be recovered
        for the write-back, and ``work_id``/``source_revision`` so a superseded observation
        is rejected rather than applied.
        """
        rows = self.session.execute(
            text(
                f"""
                SELECT t.id, t.title, t.duration_sec,
                       primary_artists.artist_names                     AS artist_names,
                       ARRAY_REMOVE(ARRAY_AGG(DISTINCT al.alias), NULL) AS aliases,
                       tl.match_status                     AS existing_status,
                       (tl.evidence ->> 'match_basis')     AS existing_basis,
                       lat.job_id                          AS job_id,
                       lat.work_id                         AS work_id,
                       lat.source_revision                 AS source_revision,
                       lat.next_attempt_at                 AS next_attempt_at,
                       lat.updated_at                      AS updated_at
                FROM lyrics_album_tracks lat
                JOIN lyrics_album_jobs j ON j.id = lat.job_id
                JOIN tracks t         ON t.id = lat.track_id
                JOIN track_artists ta ON ta.track_id = t.id
                JOIN artists a        ON a.id = ta.artist_id
                LEFT JOIN LATERAL jsonb_array_elements_text(a.aliases) AS al(alias) ON true
                LEFT JOIN track_lyrics tl ON tl.track_id = t.id
{PRIMARY_ARTIST_NAMES_LATERAL}
                WHERE lat.source_state = 'source_pending'
                  AND (lat.next_attempt_at IS NULL OR lat.next_attempt_at <= now())
                  AND {LIVE_DEMAND_SQL}
                  AND NOT (tl.track_id IS NOT NULL AND {USABLE_SOURCE_SQL})
                GROUP BY t.id, t.title, t.duration_sec,
                         tl.match_status, (tl.evidence ->> 'match_basis'),
                         lat.job_id, lat.work_id, lat.source_revision,
                         lat.next_attempt_at, lat.updated_at,
                         primary_artists.artist_names
                ORDER BY lat.next_attempt_at NULLS FIRST, lat.updated_at ASC
                LIMIT :limit
                """
            ),
            {"limit": limit},
        ).mappings().all()
        return [dict(r) for r in rows]

    # ── write-back ─────────────────────────────────────────────────────────────────────
    def _record_source_outcomes(
        self, tracks: List[Dict[str, Any]], run_started_at: datetime
    ) -> Dict[str, int]:
        """Translate each evaluated track's resulting corpus state into V57 source state.

        Runs AFTER ``run_eval_batch`` returns, so no transaction is open across the LRCLIB
        loop; each track is one short transaction of its own.

        Two outcomes deliberately write nothing:

        * **source is now usable** — the track leaves this job's pool by the join, and the
          poller links it with the fingerprint. Writing ``source_pending`` again here would
          only re-schedule a fetch we no longer need.
        * **this run did not evaluate the track** — LRCLIB was down, or the wall-clock budget
          cut the batch. Leaving the row untouched keeps both its due-ness and its ladder
          position, which is precisely the OQ5 rule that a provider outage must not push
          waiting demand out to the cap. It is also what the incremental collector already
          does with a transient error.

        Detecting the second case needs ``run_started_at``, not merely "is there a corpus
        row": a track that was ALREADY parked as ``not_found`` still has one, so a transient
        skip on a re-check would otherwise read the old verdict as a fresh one and advance
        the ladder anyway — silently converting a provider outage into weeks of delay for
        every album in the pool.
        """
        counts = {
            "not_required": 0, "backoff": 0, "source_ready": 0,
            "unevaluated": 0, "stale": 0,
        }
        if not tracks:
            return counts

        for row in tracks:
            try:
                self._record_one(row, counts, run_started_at)
                self.session.commit()
            except StaleDiscovery:
                # The snapshot was superseded while we were talking to LRCLIB (member
                # removal + re-add, a re-enumeration, or a peer run). Correct outcome:
                # drop this observation, keep whatever is now on the row.
                self.session.rollback()
                counts["stale"] += 1
            except Exception:  # noqa: BLE001
                self.session.rollback()
                logger.exception(
                    "Lyrics demand source: source write-back failed for track %s", row["id"]
                )
        logger.info("Lyrics demand source: demand %s", counts)
        return counts

    def _record_one(
        self, row: Dict[str, Any], counts: Dict[str, int], run_started_at: datetime
    ) -> None:
        current = self.session.execute(
            text(
                f"""
                SELECT tl.match_status,
                       coalesce({USABLE_SOURCE_SQL}, false) AS usable,
                       (tl.updated_at >= :started)          AS evaluated_this_run
                FROM track_lyrics tl WHERE tl.track_id = :tid
                """
            ),
            {"tid": row["id"], "started": run_started_at},
        ).mappings().one_or_none()

        if current is None:
            counts["unevaluated"] += 1
            return
        if current["usable"]:
            # Reported whether or not this run produced it: either way the track has left
            # our pool for the poller's, and re-scheduling a fetch would be wrong.
            counts["source_ready"] += 1
            return
        if not current["evaluated_this_run"]:
            # A stale verdict from an earlier run. This attempt produced no evidence, so it
            # must not move the ladder.
            counts["unevaluated"] += 1
            return

        store = LyricsDemandStore(self.session.connection())
        now = self.session.execute(text("SELECT now() AS now")).scalar_one()

        if current["match_status"] == "no_lyrics":
            # OQ5: a confirmed instrumental / no-lyrics track is not a failure and not
            # coverage either — it is an explicit not-required observation. It carries a
            # fresh source_revision, so if the corpus later gains a body for this track a
            # new observation can reopen it; a stale one cannot.
            store.set_source_state(
                row["job_id"], row["id"], "not_required", "no_lyrics",
                expected_work_id=row["work_id"],
                expected_source_revision=row["source_revision"],
            )
            counts["not_required"] += 1
            return

        # not_found / ambiguous / review_required, or a matched row with an empty body:
        # still waiting for a usable source. Demand is preserved; only the re-check pace
        # changes. There is no attempt ceiling — the ladder caps, it never terminates.
        store.set_source_state(
            row["job_id"], row["id"], "source_pending",
            str(current["match_status"] or "unresolved"),
            next_attempt_at=next_attempt(
                now,
                _previous_gap(row),
                base_sec=settings.LYRICS_DEMAND_SOURCE_RETRY_BASE_SEC,
                cap_sec=settings.LYRICS_DEMAND_SOURCE_RETRY_CAP_SEC,
            ),
            expected_work_id=row["work_id"],
            expected_source_revision=row["source_revision"],
        )
        counts["backoff"] += 1
