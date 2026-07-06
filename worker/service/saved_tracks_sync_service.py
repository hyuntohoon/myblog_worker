# FEAT-genre-artist-distribution Step 2 — Spotify saved-tracks (좋아요) cache sync.
#
# Mirrors the owner's Spotify saved-tracks set into the spotify_saved_tracks cache
# table that feeds the /profile 분석 버킷. HYBRID sync (RFC OQ3):
#   - incremental: since = max(added_at) → fetch only newer 좋아요 → upsert, NO prune
#     (the early-stop read can't observe un-likes).
#   - full: page the whole library → upsert all + prune cache rows no longer present
#     (drops un-likes).
# Invoked from EventBridge (daily incremental + weekly full) and the manual SQS
# {"job": "spotify_saved_tracks_sync", "mode": "incremental|full"} message
# (worker/handler.py). Never called from a user-facing endpoint (hard rule #9).
#
# Raw text() SQL (mirrors listening_sync_service) so the worker needs no shared_db
# ORM pin bump — only the V24 migration must be applied first.
from __future__ import annotations

import logging
from typing import Any, Dict, List

from sqlalchemy.sql import text

logger = logging.getLogger(__name__)

_UPSERT_SQL = text(
    """
    INSERT INTO spotify_saved_tracks
        (spotify_track_id, track_name, artist_name, album_name, album_sid,
         track_id, album_id, added_at, duration_ms, synced_at)
    VALUES
        (:spotify_track_id, :track_name, :artist_name, :album_name, :album_sid,
         :track_id, :album_id, CAST(:added_at AS timestamptz), :duration_ms, now())
    ON CONFLICT (spotify_track_id) DO UPDATE
       SET track_name  = EXCLUDED.track_name,
           artist_name = EXCLUDED.artist_name,
           album_name  = EXCLUDED.album_name,
           album_sid   = EXCLUDED.album_sid,
           track_id    = EXCLUDED.track_id,
           album_id    = EXCLUDED.album_id,
           added_at    = EXCLUDED.added_at,
           duration_ms = EXCLUDED.duration_ms,
           synced_at   = now()
    """
)


def run_saved_tracks_sync(
    session_factory,
    spotify_user,
    mode: str = "incremental",
) -> Dict[str, int]:
    """Sync the owner's Spotify 좋아요 tracks into spotify_saved_tracks.

    mode='incremental' (frequent): fetch only likes newer than the cache's
    ``max(added_at)``, upsert them, never prune.
    mode='full' (periodic): page the whole library, upsert all, then prune cache
    rows no longer in the saved set (drops un-likes).

    The full-mode prune deletes only CACHE rows — this table has no source of truth
    and there is NO Spotify write-back — so a message-sourced mode is safe here
    (unlike library_sync's settings-gated PUT/DELETE writes)."""
    if mode not in ("incremental", "full"):
        raise ValueError(f"unknown saved-tracks sync mode: {mode!r}")

    # Read the incremental cursor in a short session, then CLOSE before the slow
    # Spotify saved-tracks paging. Neon's pooler drops a connection left
    # idle-in-transaction across the external paging loop
    # (reference-db-session-across-long-external-loop).
    since = None
    if mode == "incremental":
        with session_factory() as session:
            since = session.execute(
                text("SELECT max(added_at) FROM spotify_saved_tracks")
            ).scalar()

    # Slow external paging — no DB session held open. incremental fetches only
    # rows newer than the cursor; full pages the whole library.
    rows = spotify_user.get_saved_tracks(since=since)

    # Fresh short write transaction for the upsert (+ full-mode prune).
    with session_factory() as session:
        with session.begin():
            upserted = _upsert_rows(session, rows)

            pruned = 0
            if mode == "full":
                pruned = _prune_absent(
                    session, [r["spotify_track_id"] for r in rows]
                )

    logger.info(
        "saved-tracks sync (%s): upserted=%d pruned=%d", mode, upserted, pruned
    )
    return {"upserted": upserted, "pruned": pruned}


def _upsert_rows(session, rows: List[Dict[str, Any]]) -> int:
    """Resolve track/album catalog UUIDs for the fetched rows and upsert them.
    Catalog resolution is best-effort: a saved track whose album/track isn't in our
    catalog stores NULL track_id/album_id (the denormalized text columns still
    render). ``added_at`` has no DB default, so every row supplies it explicitly."""
    if not rows:
        return 0

    album_sids = sorted({r["album_sid"] for r in rows if r.get("album_sid")})
    track_sids = [r["spotify_track_id"] for r in rows]

    album_map: Dict[str, Any] = {}
    if album_sids:
        album_map = {
            r.spotify_id: r.id
            for r in session.execute(
                text("SELECT id, spotify_id FROM albums WHERE spotify_id = ANY(:sids)"),
                {"sids": album_sids},
            ).fetchall()
        }

    track_map: Dict[str, Any] = {}
    if track_sids:
        track_map = {
            r.spotify_id: r.id
            for r in session.execute(
                text("SELECT id, spotify_id FROM tracks WHERE spotify_id = ANY(:tids)"),
                {"tids": track_sids},
            ).fetchall()
        }

    params = [
        {
            "spotify_track_id": r["spotify_track_id"],
            "track_name": r["track_name"],
            "artist_name": r.get("artist_name"),
            "album_name": r.get("album_name"),
            "album_sid": r.get("album_sid"),
            "track_id": track_map.get(r["spotify_track_id"]),
            "album_id": album_map.get(r.get("album_sid")),
            "added_at": r["added_at"],
            "duration_ms": r.get("duration_ms"),
        }
        for r in rows
    ]
    session.execute(_UPSERT_SQL, params)
    return len(params)


def _prune_absent(session, keep_ids: List[str]) -> int:
    """Delete cache rows whose spotify_track_id is no longer in the saved set. An
    empty keep set means the library is empty → the cache is emptied honestly
    (mirrors listening_sync_service's prune)."""
    if keep_ids:
        return session.execute(
            text(
                "DELETE FROM spotify_saved_tracks "
                "WHERE NOT (spotify_track_id = ANY(:keep))"
            ),
            {"keep": keep_ids},
        ).rowcount
    return session.execute(text("DELETE FROM spotify_saved_tracks")).rowcount
