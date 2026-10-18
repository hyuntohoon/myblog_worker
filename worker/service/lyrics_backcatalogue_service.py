# worker/service/lyrics_backcatalogue_service.py
"""OPS-project-stabilization Step 2C — back-catalogue ingest for follow demand.

Step 5 enumerates a followed artist's whole discography into `lyrics_artist_albums`, and
the follow producer turns every release into a `lyrics_album_jobs` row. But a job can only
be served once the release is in `albums`, and nothing ever put it there:
`_resolve_one_catalog` defers a missing album as `album_not_in_catalog` with the comment
"it simply has not been ingested yet" — an assumption no code made true. So a follow
translated only the part of a discography the catalog already happened to hold (~35%),
and 686 jobs for 28 artists waited from 2026-09-19 on a re-check that could never
succeed. This job is what makes the assumption true.

**The curation gate is the owner's decision, not an optimisation** (2026-10-01). Only
releases at or above `ALBUM_POP_MIN` — the bar the scheduled ingest already applies — are
written. Popularity is not in the `/artists/{id}/albums` listing, so the gate can only be
applied to the `GET /albums` response: every waiting release is read once, and the ones
under the bar are not written.

**A release under the gate rests; it is not dropped and not retried at once.** Left as
`album_not_in_catalog` it would be read again on every tick. It is deferred instead with
its own reason for `rest_days`, after which the Step 3 catalog resolver's ordinary
re-check returns it to `album_not_in_catalog` and it is read once more — so a release that
later clears the bar is picked up, and one that never does costs one twentieth of a
provider call per rest period.

**Claimed before it is read.** The selection moves each job to `CLAIMED` with a short
lease in the same statement, so a duplicated message (SQS is at-least-once, and both the
15-minute poll and a member connecting can nudge) does not read the same releases twice.
The lease needs no reaper: a run that dies leaves `next_attempt_at` to expire, and the
same resolver re-check puts the job back in the pool.

**The catalog cap is checked here because this is a path that could silently blow it.**
`MAX_CATALOG_ALBUMS` is enforced only by `run_album_ingest`, which *returns before
sweeping* once the cap is reached — and that sweep is also the only thing that confirms
release-calendar rows. Writing past the cap from here would stop both without an error
anywhere. So this job stops at the cap and says so.

**Session discipline** (the recurring bug class): select-and-claim -> close -> provider
read with NO session held -> album sync in its own short transactions -> one short
write-back session.
"""
from __future__ import annotations

import logging
import time
from datetime import timedelta
from typing import Any, Callable, Dict, List, Optional

from sqlalchemy import text

from myblog_shared_db.lyrics_demand import LyricsDemandStore

from worker.service.lyrics_member_demand_service import FOLLOW_ORIGIN

logger = logging.getLogger(__name__)

# Written by `LyricsDemandSourceService._resolve_one_catalog`; the reason this job reads.
NOT_IN_CATALOG = "album_not_in_catalog"
# Written here.
CLAIMED = "album_ingest_claimed"
INGESTED = "album_ingested"
BELOW_GATE = "album_below_popularity_gate"
UNAVAILABLE = "album_unavailable"
FAILED = "album_ingest_failed"

# GET /albums takes at most 20 ids, so one batch is exactly one provider call.
BATCH_SIZE = 20

# A release somebody's follow still wants, that the catalog does not hold, and that no
# run is already reading. One spelling, shared by the count the nudge asks and the claim
# the run makes: a looser count would enqueue a run every 15 minutes that claims nothing.
#
# Follow demand only. A saved or recently-played album is one a member chose by hand and
# has its own ingest path; the owner's decision was about the back catalogue a follow
# implies, so this job is not widened to them.
#
# `last_reason IS NULL` admits a job the resolver has not looked at yet, so a new release
# does not wait a resolver tick before it can be read; the `albums` anti-join is what
# keeps an album that IS in the catalog (and merely unresolved) out of the pool.
_WAITING = """
           j.album_id IS NULL
       AND NOT j.cancelled
       AND (j.last_reason IS NULL OR j.last_reason = :waiting)
       AND NOT EXISTS (SELECT 1 FROM albums a WHERE a.spotify_id = j.spotify_album_id)
       AND EXISTS (
             SELECT 1
               FROM lyrics_album_demands d
               JOIN lyrics_discovery_scopes s ON s.id = d.scope_id AND s.active
              WHERE d.job_id = j.id AND s.origin = :follow)
"""

_COUNT_WAITING = text("SELECT count(*) FROM lyrics_album_jobs j WHERE " + _WAITING.strip())

_CLAIM = text(
    """
    WITH picked AS (
        SELECT j.id
          FROM lyrics_album_jobs j
         WHERE """ + _WAITING.strip() + """
         ORDER BY j.created_at, j.id
         LIMIT :lim
         FOR UPDATE OF j SKIP LOCKED
    )
    UPDATE lyrics_album_jobs u
       SET last_reason = :claimed,
           next_attempt_at = now() + make_interval(secs => :lease), updated_at = now()
      FROM picked
     WHERE u.id = picked.id
    RETURNING u.id, u.spotify_album_id
    """
)


def waiting_count(session_factory: Callable[[], Any]) -> int:
    """How many releases a run would claim right now (0 => nothing to enqueue)."""
    with session_factory() as session:
        return int(
            session.execute(
                _COUNT_WAITING, {"waiting": NOT_IN_CATALOG, "follow": FOLLOW_ORIGIN}
            ).scalar_one()
        )


def run_backcatalogue_ingest(
    session_factory: Callable[[], Any],
    catalog_client,
    sync_fetched: Callable[[List[Dict[str, Any]]], None],
    *,
    pop_min: int,
    max_catalog_albums: int,
    max_batches: int = 2,
    rest_days: int = 30,
    failure_rest_hours: int = 24,
    lease_seconds: int = 300,
    time_budget_sec: float = 60.0,
    market: Optional[str] = None,
) -> Dict[str, int]:
    """Read waiting follow releases, write the ones that clear the gate, rest the others.

    `sync_fetched` receives the GET /albums objects that passed and writes them to the
    catalog in its own transactions (`AlbumSyncService.sync_fetched_albums`).

    Never raises for a provider or sync failure. The batch that failed rests for
    `failure_rest_hours` under its own reason and the run stops. Without that rest the
    claim would lapse inside one 15-minute tick (the lease is shorter than the tick), the
    oldest-first claim would pick the very same batch again, and one release that fails
    every time would cost a provider call per tick for ever while holding the whole
    backlog behind it. Claimed jobs the run did not reach keep only their lease.
    Returns a metrics dict; `remaining` is what is still waiting afterwards.
    """
    metrics = {
        "claimed": 0, "read": 0, "ingested": 0, "below_gate": 0, "unavailable": 0,
        "failed": 0, "capped": 0, "remaining": 0,
    }
    if max_batches <= 0:
        return metrics

    with session_factory() as session:
        album_count = int(session.execute(text("SELECT count(*) FROM albums")).scalar_one())
        if album_count >= max_catalog_albums:
            # Nothing is claimed and nothing is read: a read whose result cannot be
            # written would be spent again on every tick for as long as the cap holds.
            metrics["capped"] = 1
            logger.warning(
                "backcatalogue_ingest: catalog cap reached (%d >= %d) — not reading",
                album_count, max_catalog_albums,
            )
            return metrics
        claimed = session.execute(
            _CLAIM,
            {"waiting": NOT_IN_CATALOG, "follow": FOLLOW_ORIGIN, "claimed": CLAIMED,
             "lim": BATCH_SIZE * max_batches, "lease": lease_seconds},
        ).fetchall()
        session.commit()
    metrics["claimed"] = len(claimed)
    headroom = max_catalog_albums - album_count
    started = time.monotonic()

    def _rest_failed(batch: List[tuple]) -> None:
        # Sorted by job id, like the verdict write-back below.
        with session_factory() as session:
            now = session.execute(text("SELECT now()")).scalar_one()
            store = LyricsDemandStore(session.connection())
            for job_id, _sid in sorted(batch, key=lambda b: str(b[0])):
                store.defer_catalog(job_id, FAILED, now + timedelta(hours=failure_rest_hours))
            session.commit()

    for start in range(0, len(claimed), BATCH_SIZE):
        if time.monotonic() - started >= time_budget_sec:
            break
        batch = claimed[start : start + BATCH_SIZE]
        ids = [sid for _job_id, sid in batch]

        # --- provider read, NO session held ----------------------------------
        try:
            albums = catalog_client.get_albums(ids, market=market) or []
        except Exception as e:
            metrics["failed"] += 1
            logger.warning(
                "backcatalogue_ingest: GET /albums failed for %d id(s): %s — "
                "batch rests %dh", len(ids), type(e).__name__, failure_rest_hours,
            )
            _rest_failed(batch)
            break
        if len(albums) != len(ids):
            # The response is positional. A short one cannot be matched to the request,
            # and guessing would file one release's verdict against another's job.
            metrics["failed"] += 1
            logger.warning(
                "backcatalogue_ingest: GET /albums returned %d object(s) for %d id(s) — "
                "batch rests %dh", len(albums), len(ids), failure_rest_hours,
            )
            _rest_failed(batch)
            break
        metrics["read"] += len(ids)

        passed: List[Dict[str, Any]] = []
        verdicts: List[tuple] = []  # (job_id, spotify_album_id, reason)
        for (job_id, sid), album in zip(batch, albums):
            # A null element is an id Spotify no longer serves. An object under a
            # DIFFERENT id is a relinked release: written, it would land in `albums`
            # under an id this job can never resolve to, so it rests like a missing one.
            if not album or album.get("id") != sid:
                verdicts.append((job_id, sid, UNAVAILABLE))
            elif (album.get("popularity") or 0) < pop_min:
                verdicts.append((job_id, sid, BELOW_GATE))
            elif len(passed) >= headroom:
                # Cleared the gate but the cap has no room. Not written and not rested:
                # the claim expires and the release waits for the cap to be raised.
                metrics["capped"] += 1
            else:
                passed.append(album)
                verdicts.append((job_id, sid, INGESTED))

        # --- catalog write, in the sync service's own short transactions -------
        if passed:
            try:
                sync_fetched(passed)
            except Exception:
                metrics["failed"] += 1
                logger.exception(
                    "backcatalogue_ingest: catalog write failed for %d album(s) — "
                    "batch rests %dh", len(passed), failure_rest_hours,
                )
                _rest_failed(batch)
                break
            headroom -= len(passed)

        # --- one short write-back session ---------------------------------------
        # Sorted by job id so two runs that somehow share jobs lock them in one order.
        with session_factory() as session:
            now = session.execute(text("SELECT now()")).scalar_one()
            store = LyricsDemandStore(session.connection())
            for job_id, _sid, reason in sorted(verdicts, key=lambda v: str(v[0])):
                # An ingested release is made due NOW so the resolver attaches it on its
                # next tick instead of waiting out a ladder that had reached 24 hours.
                due = now if reason == INGESTED else now + timedelta(days=rest_days)
                store.defer_catalog(job_id, reason, due)
            session.commit()
        for _job_id, _sid, reason in verdicts:
            metrics[
                {INGESTED: "ingested", BELOW_GATE: "below_gate", UNAVAILABLE: "unavailable"}[reason]
            ] += 1

    metrics["remaining"] = waiting_count(session_factory)
    # WARNING, not INFO: an INFO line from this Lambda does not reach CloudWatch, and this
    # is a job that spends provider quota and grows the catalog.
    logger.warning("backcatalogue_ingest metrics: %s", metrics)
    return metrics
