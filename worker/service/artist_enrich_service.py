"""Artist detail enrichment for BUG-artist-image-backfill.

An empty ``photo_url`` string is the persisted sentinel meaning Spotify has no
artist image; only NULL rows remain eligible for future sweeps. The Neon session
contract is materialize IDs → close → Spotify loop → fresh short write session,
so no database session is held across external HTTP calls.
"""
from __future__ import annotations

import json
import logging

from sqlalchemy import text

from worker.clients.spotify_client import spotify

logger = logging.getLogger(__name__)


UPDATE_SQL = text("""
    UPDATE artists SET
        photo_url  = :photo,
        genres     = CAST(:genres AS jsonb),
        followers  = :followers,
        popularity = :popularity
    WHERE spotify_id = :sid
""")


def _build_enrich_rows(details: list[dict]) -> list[dict]:
    """Normalize Spotify artist objects into bulk UPDATE parameters."""
    rows: list[dict] = []
    for art in details:
        if not art or not art.get("id"):
            continue

        imgs = art.get("images") or []
        # Empty string is a durable "Spotify has no image" sentinel. NULL alone
        # means the artist is still eligible for enrichment/backfill.
        photo = imgs[0].get("url") if imgs else ""
        followers = art.get("followers") or {}
        if isinstance(followers, dict):
            followers = followers.get("total")

        rows.append(dict(
            sid=art["id"],
            photo=photo,
            genres=json.dumps(art.get("genres") or []),
            followers=followers,
            popularity=art.get("popularity"),
        ))
    return rows


def enrich_artists(conn, spotify_ids: list[str]) -> int:
    """Enrich IDs on the caller's connection and transaction."""
    written = 0
    for i in range(0, len(spotify_ids), 50):
        chunk = spotify_ids[i : i + 50]
        rows = _build_enrich_rows(spotify.get_artists_batch(chunk))
        # Concurrent syncs must lock shared artist rows in a stable order.
        rows.sort(key=lambda row: row["sid"])
        if rows:
            conn.execute(UPDATE_SQL, rows)
            written += len(rows)
    return written


def run_artist_photo_backfill(session_factory, limit: int | None = None) -> dict:
    """Sweep NULL artist photos without holding a DB session across Spotify."""
    select_sql = """
        SELECT spotify_id
        FROM artists
        WHERE photo_url IS NULL
        ORDER BY spotify_id
    """
    params = None
    if limit is not None:
        select_sql += " LIMIT :limit"
        params = {"limit": limit}

    # Materialize and close before the first external call (Neon session rule).
    with session_factory() as session:
        if params is None:
            result = session.execute(text(select_sql))
        else:
            result = session.execute(text(select_sql), params)
        spotify_ids = [row[0] for row in result.fetchall()]
        session.commit()

    metrics = {
        "selected": len(spotify_ids),
        "enriched": 0,
        "sentinel_written": 0,
        "errors": 0,
    }

    for i in range(0, len(spotify_ids), 50):
        chunk = spotify_ids[i : i + 50]
        try:
            details = spotify.get_artists_batch(chunk)
            rows = _build_enrich_rows(details)
            # Keep UPDATE lock acquisition consistent with concurrent album syncs.
            rows.sort(key=lambda row: row["sid"])
            if not rows:
                continue

            # Each write owns a fresh, short-lived session after Spotify returns.
            with session_factory() as session:
                session.execute(UPDATE_SQL, rows)
                session.commit()

            metrics["enriched"] += len(rows)
            metrics["sentinel_written"] += sum(row["photo"] == "" for row in rows)
        except Exception as exc:
            logger.error(
                "Artist photo backfill chunk failed (start=%d): %s",
                i,
                exc,
                exc_info=True,
            )
            metrics["errors"] += 1
            continue

    logger.info("Artist photo backfill metrics: %s", metrics)
    return metrics
