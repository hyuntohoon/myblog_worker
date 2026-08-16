# worker/service/disc_no_backfill_service.py
"""
DATA-multidisc-track-order Step 2b: one-off `disc_no` backfill for the albums
that collide today under `ORDER BY track_no` (Spotify restarts `track_number`
at 1 per disc). Every future sync captures `disc_no` naturally via Step 2a
(`AlbumSyncService`), so this is a bounded, non-recurring job over the
currently-colliding population only — not a full-catalog backfill
(`necessity-gate-reviews`).

Failure-isolation shape matches `IsrcBackfillService`: read the flagged album
ids, close that session, fetch from Spotify, then open a fresh short write
session per album. Never hold a transaction across the Spotify HTTP loop
(idle-in-transaction caused Neon `ProtocolViolation` — the lyrics-pipeline
precedent).
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from worker.clients.spotify_client import spotify

logger = logging.getLogger(__name__)


class DiscNoBackfillService:
    """Populate tracks.disc_no for albums whose track_no collides today."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def backfill_disc_no(self, limit: Optional[int] = None, market: Optional[str] = None) -> Dict[str, Any]:
        """Re-fetch each colliding album from Spotify and set disc_no on its
        existing tracks, matched by spotify_id. UPDATE only, never INSERT — a
        track Spotify returns with no local row is skipped and counted, not
        inserted (some of the 78 albums are locally truncated at 50 tracks).

        Returns metrics: {albums_total, albums_processed, tracks_matched,
        tracks_skipped_no_local_row, errors}.
        """
        metrics = {
            "albums_total": 0,
            "albums_processed": 0,
            "tracks_matched": 0,
            "tracks_skipped_no_local_row": 0,
            "errors": 0,
        }

        albums = self._fetch_colliding_albums(limit=limit)
        # Close the read transaction before the first Spotify call — same rule as
        # IsrcBackfillService: don't sit idle-in-transaction across HTTP.
        self.session.commit()
        metrics["albums_total"] = len(albums)
        if not albums:
            logger.info("No colliding albums left to backfill")
            return metrics

        logger.info("disc_no backfill: %d colliding albums to process", len(albums))

        for album in albums:
            alb_sid = album["spotify_id"]
            try:
                items = spotify.get_album_tracks(alb_sid, market=market)
                local_sids = self._fetch_local_track_sids(album["id"])

                # Resolve each Spotify item to a LOCAL spotify_id before counting it as
                # matched or skipped — market-scoped Track Relinking (a live prod finding:
                # 2 of the 78 albums, e.g. Queen's "Sheer Heart Attack (Deluxe Remastered
                # Version)", return a market=KR id that differs from the id stored locally,
                # with the matching id only reachable via `linked_from.id`) means an item
                # can carry two candidate ids for the same track. Resolving per-item (not
                # per-id) keeps a relinked track from being double-counted as one match
                # plus one spurious skip. Same defense as IsrcBackfillService.backfill_isrc.
                updates: Dict[str, int] = {}
                skipped = 0
                for it in items:
                    disc = it.get("disc_number")
                    if disc is None:
                        continue
                    candidates = [cid for cid in (it.get("id"), (it.get("linked_from") or {}).get("id")) if cid]
                    local_match = next((cid for cid in candidates if cid in local_sids), None)
                    if local_match:
                        updates[local_match] = disc
                    else:
                        skipped += 1

                if not updates and not skipped:
                    logger.warning("disc_no backfill: album %s returned no usable tracks", alb_sid)
                    continue

                matched = self._apply_disc_no_updates(album["id"], updates)
                self.session.commit()
                metrics["albums_processed"] += 1
                metrics["tracks_matched"] += matched
                metrics["tracks_skipped_no_local_row"] += skipped
                logger.info(
                    "disc_no backfill: album %s — matched=%d skipped=%d",
                    alb_sid, matched, skipped,
                )
            except Exception as e:
                # Roll back so a single album's failure doesn't poison the tx for
                # the rest of the batch (same recovery pattern as IsrcBackfillService).
                self.session.rollback()
                logger.error("disc_no backfill: album %s failed: %s", alb_sid, e, exc_info=True)
                metrics["errors"] += 1
                continue

        logger.info(
            "disc_no backfill complete: albums_total=%d albums_processed=%d "
            "tracks_matched=%d tracks_skipped_no_local_row=%d errors=%d",
            metrics["albums_total"], metrics["albums_processed"],
            metrics["tracks_matched"], metrics["tracks_skipped_no_local_row"], metrics["errors"],
        )
        return metrics

    def _fetch_colliding_albums(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Albums with a repeated (album_id, track_no) pair that still have at
        least one track missing disc_no — the collision population, made
        idempotent so a re-run skips albums a prior invocation fully resolved.

        ORDER BY spotify_id for a deterministic, stable-lock-order scan.
        """
        result = self.session.execute(
            text("""
                SELECT a.id, a.spotify_id
                FROM albums a
                WHERE a.id IN (
                    SELECT t.album_id
                    FROM tracks t
                    GROUP BY t.album_id, t.track_no
                    HAVING count(*) > 1
                )
                AND EXISTS (
                    SELECT 1 FROM tracks t2
                    WHERE t2.album_id = a.id AND t2.disc_no IS NULL
                )
                ORDER BY a.spotify_id
                """ + ("LIMIT :limit" if limit else "")
            ),
            {"limit": limit} if limit else {},
        )
        return [{"id": row[0], "spotify_id": row[1]} for row in result.fetchall()]

    def _fetch_local_track_sids(self, album_id: Any) -> set:
        """spotify_ids of this album's LOCAL track rows — matches by spotify_id
        AND album_id, so a spotify_id existing on a different local album
        (shouldn't happen, but not assumed) is never a candidate."""
        rows = self.session.execute(
            text("SELECT spotify_id FROM tracks WHERE album_id = :album_id"),
            {"album_id": album_id},
        ).fetchall()
        return {row[0] for row in rows}

    def _apply_disc_no_updates(self, album_id: Any, updates: Dict[str, int]) -> int:
        """UPDATE only, never INSERT — `updates` keys are already confirmed
        local spotify_ids. Sorted by track_id for the bulk-write lock-ordering
        rule. Returns the number of rows written."""
        if not updates:
            return 0
        self.session.execute(
            text("""
                UPDATE tracks
                SET disc_no = :disc
                WHERE spotify_id = :track_id AND album_id = :album_id
            """),
            [
                {"track_id": sid, "disc": disc, "album_id": album_id}
                for sid, disc in sorted(updates.items())
            ],
        )
        return len(updates)
