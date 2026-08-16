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
                disc_by_sid = {
                    it["id"]: it.get("disc_number")
                    for it in items
                    if it.get("id") and it.get("disc_number") is not None
                }
                if not disc_by_sid:
                    logger.warning("disc_no backfill: album %s returned no usable tracks", alb_sid)
                    continue

                matched, skipped = self._update_album_disc_no(album["id"], disc_by_sid)
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

    def _update_album_disc_no(self, album_id: Any, disc_by_sid: Dict[str, int]) -> tuple[int, int]:
        """Apply one album's Spotify-fetched disc numbers to its local tracks.

        Matches by spotify_id AND album_id — a spotify_id existing on a
        different local album (shouldn't happen, but not assumed) is left
        untouched. Returns (matched, skipped_no_local_row).
        """
        rows = self.session.execute(
            text("SELECT id, spotify_id FROM tracks WHERE album_id = :album_id"),
            {"album_id": album_id},
        ).fetchall()
        local_sids = {row[1] for row in rows}

        updates = [
            {"track_id": sid, "disc": disc}
            for sid, disc in sorted(disc_by_sid.items())
            if sid in local_sids
        ]
        skipped = len(disc_by_sid) - len(updates)

        if updates:
            self.session.execute(
                text("""
                    UPDATE tracks
                    SET disc_no = :disc
                    WHERE spotify_id = :track_id AND album_id = :album_id
                """),
                [{**u, "album_id": album_id} for u in updates],
            )
        return len(updates), skipped
