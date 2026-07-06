# worker/service/isrc_backfill_service.py
"""
FEAT-lyrics-corpus Step 1b: ISRC population via Spotify get_tracks fetch.

Bounded backfill over existing tracks lacking ISRC, following the alias-fill
failure-isolation pattern. Fetches in chunks of 50, writes per-batch, sentinel
on miss (track lacks ISRC in Spotify). No impact on album sync.
"""
from __future__ import annotations
from typing import Dict, Any, List, Optional
import logging
from sqlalchemy import text
from sqlalchemy.orm import Session

from worker.clients.spotify_client import spotify

logger = logging.getLogger(__name__)


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
        limit: int = 1000,
        market: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Bounded backfill: fetch tracks without ISRC, enrich from Spotify, commit
        per batch.

        Each batch commits via the owning session; a batch that raises is rolled
        back (recovering the aborted transaction so LATER batches still commit —
        without the rollback the first failure poisons the tx and every subsequent
        batch fails with InFailedSqlTransaction).

        Returns metrics: {fetched, matched, sentinel_written, errors}.
        """
        mkt = market or "KR"
        metrics = {"fetched": 0, "matched": 0, "sentinel_written": 0, "errors": 0}

        # Fetch tracks lacking ISRC (chunk 50 at a time for the API call)
        # but process a bounded total per invocation (limit param).
        tracks_to_enrich = self._fetch_tracks_without_isrc(limit=limit)
        if not tracks_to_enrich:
            logger.info("No tracks to enrich")
            return metrics

        logger.info(f"Found {len(tracks_to_enrich)} tracks without ISRC")

        # Process in batches of 50 (Spotify API limit)
        for i in range(0, len(tracks_to_enrich), 50):
            batch = tracks_to_enrich[i : i + 50]
            spotify_ids = [t["spotify_id"] for t in batch]

            try:
                # Fetch full track objects with external_ids
                tracks_from_spotify = spotify.get_tracks(spotify_ids, market=mkt)
                metrics["fetched"] += len(tracks_from_spotify)

                # Build a lookup for quick access
                spotify_tracks = {t.get("id"): t for t in tracks_from_spotify if t}

                # Update DB with ISRC or sentinel
                updates = []
                for track_record in batch:
                    track_id = track_record["id"]
                    spotify_id = track_record["spotify_id"]

                    spotify_track = spotify_tracks.get(spotify_id)
                    if not spotify_track:
                        # Spotify returned null for this ID — sentinel (no retry)
                        updates.append({"track_id": track_id, "isrc": "not_found"})
                        metrics["sentinel_written"] += 1
                        logger.debug(f"Sentinel: track {spotify_id} not found in Spotify")
                        continue

                    isrc = (spotify_track.get("external_ids") or {}).get("isrc")
                    if isrc:
                        updates.append({"track_id": track_id, "isrc": isrc})
                        metrics["matched"] += 1
                        logger.debug(f"Matched: track {spotify_id} → ISRC {isrc}")
                    else:
                        # Track exists in Spotify but has no ISRC — sentinel
                        updates.append({"track_id": track_id, "isrc": "no_isrc"})
                        metrics["sentinel_written"] += 1
                        logger.debug(f"Sentinel: track {spotify_id} has no ISRC")

                # Commit batch to DB (per-batch commit via the owning session)
                if updates:
                    self._update_isrc_batch(updates)
                    self.session.commit()
                    logger.info(f"Batch committed: {len(updates)} tracks updated")

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
            f"matched={metrics['matched']}, sentinel={metrics['sentinel_written']}, "
            f"errors={metrics['errors']}"
        )
        return metrics

    def _fetch_tracks_without_isrc(
        self,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """Fetch up to `limit` tracks with NULL isrc."""
        result = self.session.execute(
            text("""
                SELECT id, spotify_id
                FROM tracks
                WHERE isrc IS NULL
                LIMIT :limit
            """),
            {"limit": limit},
        )
        return [
            {"id": str(row[0]), "spotify_id": row[1]}
            for row in result.fetchall()
        ]

    def _update_isrc_batch(self, updates: List[Dict[str, Any]]) -> None:
        """Write isrc for a batch of tracks. The caller commits per batch."""
        self.session.execute(
            text("""
                UPDATE tracks
                SET isrc = :isrc
                WHERE id = CAST(:track_id AS UUID)
            """),
            updates,
        )
