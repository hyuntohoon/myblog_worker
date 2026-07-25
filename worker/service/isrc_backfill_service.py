# worker/service/isrc_backfill_service.py
"""
FEAT-lyrics-corpus Step 1b: ISRC population via Spotify get_tracks fetch.

Bounded backfill over existing tracks lacking ISRC, following the alias-fill
failure-isolation pattern. Fetches in chunks of 50, writes per-batch, records a
miss marker (track lacks ISRC in Spotify). No impact on album sync.

**The `isrc` column holds real ISRCs only.** An earlier revision wrote the string
sentinels ``"not_found"`` / ``"no_isrc"`` straight into ``tracks.isrc``, which would
have made every ``isrc IS NOT NULL`` / ``isrc = :code`` predicate return garbage the
day the job first ran. It never ran (it was never scheduled), so the column is still
100% clean — the marker now lives in ``tracks.ext_refs->>'isrc_status'`` instead.

``ext_refs.isrc`` is NOT the marker key: it already means "a real ISRC" on 7,335 rows
and ``myblog_shared_db/scripts/backfill_genres.py`` reads it as its "already fetched,
skip" predicate, so a miss written there would permanently suppress a legitimate later
fetch. A matched row mirrors its ISRC into that key for exactly that reason — so the
genre CLI stops re-fetching a track this job has already resolved.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from worker.clients.spotify_client import spotify
from worker.core.config import settings

logger = logging.getLogger(__name__)

# ext_refs marker values for a track Spotify cannot give us an ISRC for. Written under
# `isrc_status`, never into the `isrc` column, and never under the `isrc` key.
STATUS_NOT_FOUND = "not_found"   # Spotify returned no object for this id
STATUS_NO_ISRC = "no_isrc"       # object exists but carries no external_ids.isrc

_CHUNK = 50  # Spotify GET /v1/tracks id limit


class IsrcBackfillService:
    """Populate Track.isrc column via Spotify GET /v1/tracks?ids=..."""

    def __init__(self, session: Session) -> None:
        # Own the transaction via the SESSION (commit/rollback per batch), NOT a
        # cached connection. Committing a raw connection while the handler holds a
        # session.begin() deassociates the session transaction → InvalidRequestError
        # on context exit; caching session.connection() across a commit returns a
        # stale handle to the pool (BUG-17). session.execute + per-batch
        # session.commit()/rollback() is the failure-isolation pattern used by
        # generate_and_save_aliases / the lyrics pipeline.
        self.session = session

    def backfill_isrc(
        self,
        limit: Optional[int] = None,
        market: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Bounded backfill: fetch tracks without ISRC, enrich from Spotify, commit
        per batch.

        Each batch commits via the owning session; a batch that raises is rolled
        back (recovering the aborted transaction so LATER batches still commit —
        without the rollback the first failure poisons the tx and every subsequent
        batch fails with InFailedSqlTransaction).

        ``market`` defaults to **None** (no market param). Passing a market activates
        Spotify Track Relinking, which can return an object whose ``id`` differs from
        the one requested (the requested id moves to ``linked_from.id``); keyed naively
        that reads as "Spotify has no such track" and would write a *permanent* miss
        marker on a perfectly live track. ISRC is market-independent, so the safest
        thing is not to ask for a market at all — and the lookup below also accepts
        ``linked_from.id`` so an explicit market stays correct.

        Returns metrics: {fetched, matched, sentinel_written, errors, skipped_budget}.
        """
        limit = limit or settings.ISRC_BACKFILL_BATCH_LIMIT
        metrics = {
            "fetched": 0,
            "matched": 0,
            "sentinel_written": 0,
            "errors": 0,
            "skipped_budget": 0,
        }

        # Fetch tracks lacking ISRC (chunk 50 at a time for the API call)
        # but process a bounded total per invocation (limit param).
        tracks_to_enrich = self._fetch_tracks_without_isrc(limit=limit)
        # Close the read transaction BEFORE the first Spotify call. The SELECT
        # autobegins a transaction that would otherwise sit idle-in-transaction across
        # a multi-second HTTP round trip, and Neon drops such connections
        # (ProtocolViolation) — the hardened "never hold a session open across an
        # external-API loop" rule. Batches 2..N are already covered by their
        # predecessor's commit; this covers batch 1.
        self.session.commit()
        if not tracks_to_enrich:
            logger.info("No tracks to enrich")
            return metrics

        logger.info(f"Found {len(tracks_to_enrich)} tracks without ISRC")

        deadline = time.monotonic() + settings.ISRC_BACKFILL_TIME_BUDGET_SEC

        # Process in batches of 50 (Spotify API limit)
        for i in range(0, len(tracks_to_enrich), _CHUNK):
            batch = tracks_to_enrich[i : i + _CHUNK]

            # Wall-clock cap: a sustained 429 burst can burn ~16s of retry backoff per
            # chunk (3 attempts, 8s cap), which would run a full invocation past the
            # 120s Lambda timeout. Stop cleanly instead — committed batches are durable
            # and the remainder is simply re-selected on the next run.
            if time.monotonic() > deadline:
                metrics["skipped_budget"] = len(tracks_to_enrich) - i
                logger.warning(
                    "ISRC backfill: time budget hit — deferring %d tracks",
                    metrics["skipped_budget"],
                )
                break

            spotify_ids = [t["spotify_id"] for t in batch]

            try:
                # Fetch full track objects with external_ids
                tracks_from_spotify = spotify.get_tracks(spotify_ids, market=market)

                # Build a lookup for quick access. get_tracks preserves Spotify's
                # `null` placeholders for unknown ids, so filter them out; index each
                # object under BOTH its own id and its `linked_from.id` so a relinked
                # track is found by the id we asked for.
                spotify_tracks: Dict[str, Dict[str, Any]] = {}
                for st in tracks_from_spotify:
                    if not st:
                        continue
                    metrics["fetched"] += 1
                    if st.get("id"):
                        spotify_tracks[st["id"]] = st
                    linked = (st.get("linked_from") or {}).get("id")
                    if linked:
                        spotify_tracks[linked] = st

                # Split into the two write paths: real ISRCs go to the column, misses
                # go to ext_refs.isrc_status and leave the column NULL.
                matched: List[Dict[str, Any]] = []
                missed: List[Dict[str, Any]] = []
                for track_record in batch:
                    track_id = track_record["id"]
                    spotify_id = track_record["spotify_id"]

                    spotify_track = spotify_tracks.get(spotify_id)
                    if not spotify_track:
                        # Spotify returned null for this ID — mark, don't retry.
                        missed.append({"track_id": track_id, "status": STATUS_NOT_FOUND})
                        logger.debug(f"Miss: track {spotify_id} not found in Spotify")
                        continue

                    isrc = (spotify_track.get("external_ids") or {}).get("isrc")
                    if isrc:
                        matched.append({"track_id": track_id, "isrc": isrc})
                        logger.debug(f"Matched: track {spotify_id} → ISRC {isrc}")
                    else:
                        # Track exists in Spotify but has no ISRC — mark, don't retry.
                        missed.append({"track_id": track_id, "status": STATUS_NO_ISRC})
                        logger.debug(f"Miss: track {spotify_id} has no ISRC")

                # Commit batch to DB (per-batch commit via the owning session)
                if matched or missed:
                    self._update_isrc_batch(matched, missed)
                    self.session.commit()
                    metrics["matched"] += len(matched)
                    metrics["sentinel_written"] += len(missed)
                    logger.info(
                        f"Batch committed: {len(matched)} matched, {len(missed)} marked"
                    )

            except Exception as e:
                # Roll back the aborted transaction so the NEXT batch starts clean.
                # Without this, an ON-CONFLICT/Spotify error leaves the tx in a
                # failed state and every later batch fails with InFailedSqlTransaction.
                self.session.rollback()
                logger.error(f"Batch failed (batch start={i}): {e}", exc_info=True)
                metrics["errors"] += 1
                # Don't re-raise; failure-isolation pattern means one batch failure
                # doesn't block the job. The tracks in this batch will be retried
                # on the next invocation.
                continue

        logger.info(
            f"Backfill complete: fetched={metrics['fetched']}, "
            f"matched={metrics['matched']}, marked={metrics['sentinel_written']}, "
            f"errors={metrics['errors']}, skipped_budget={metrics['skipped_budget']}"
        )
        return metrics

    def _fetch_tracks_without_isrc(
        self,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """Fetch up to `limit` tracks that have neither an ISRC nor a prior attempt.

        Three exclusions, all required now that a miss no longer poisons the column:
        ``isrc IS NULL`` (nothing resolved yet), ``ext_refs->>'isrc' IS NULL`` (the
        genre CLI already carries a real ISRC for this track — free, skip the API), and
        ``ext_refs->>'isrc_status' IS NULL`` (this job already tried and Spotify had
        nothing). Without the last one every permanent miss would be re-fetched on every
        run forever, which is exactly what the old in-column sentinel prevented.

        ``ORDER BY spotify_id`` gives deterministic paging across runs and a stable
        UPDATE lock order against concurrent album syncs (artist-photo precedent).
        """
        result = self.session.execute(
            text("""
                SELECT id, spotify_id
                FROM tracks
                WHERE isrc IS NULL
                  AND ext_refs->>'isrc' IS NULL
                  AND ext_refs->>'isrc_status' IS NULL
                ORDER BY spotify_id
                LIMIT :limit
            """),
            {"limit": limit},
        )
        return [
            {"id": str(row[0]), "spotify_id": row[1]}
            for row in result.fetchall()
        ]

    def _update_isrc_batch(
        self,
        matched: List[Dict[str, Any]],
        missed: List[Dict[str, Any]],
    ) -> None:
        """Write one batch: real ISRCs to the column, misses to ext_refs. Caller commits.

        Each list is sorted by ``track_id`` so concurrent writers acquire row locks in a
        consistent order (the bulk-write deadlock rule). A matched row also mirrors the
        ISRC into ``ext_refs.isrc`` so ``backfill_genres.py`` — which treats that key as
        "already fetched" — stops re-requesting the track.
        """
        if matched:
            self.session.execute(
                text("""
                    UPDATE tracks
                    SET isrc     = :isrc,
                        ext_refs = ext_refs || jsonb_build_object('isrc', CAST(:isrc AS text))
                    WHERE id = CAST(:track_id AS UUID)
                """),
                sorted(matched, key=lambda r: r["track_id"]),
            )
        if missed:
            self.session.execute(
                text("""
                    UPDATE tracks
                    SET ext_refs = ext_refs || jsonb_build_object('isrc_status', :status)
                    WHERE id = CAST(:track_id AS UUID)
                """),
                sorted(missed, key=lambda r: r["track_id"]),
            )
