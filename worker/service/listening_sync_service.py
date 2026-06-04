# FEAT-member-dashboard Step 3 — Spotify listening cache sync.
#
# Reads the user's recently-played + currently-playing from Spotify and writes them
# to the spotify_recent_albums / spotify_now_playing cache tables. Invoked from the
# EventBridge 1h cron and the manual "지금 새로고침" SQS trigger (worker/handler.py).
# Never called from a user-facing endpoint (hard rule #9).
#
# Raw text() SQL (mirrors generate_and_save_aliases) so the worker needs no ORM pin
# bump for the new tables — only the migration must be applied first.
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

from sqlalchemy.sql import text

logger = logging.getLogger(__name__)


def sync_recent_albums(
    session_factory,
    client,
    enqueue_unknown: Optional[Callable[[List[str]], None]] = None,
) -> Dict[str, int]:
    """Upsert the distinct album set of the recently-played 50-item window (D25) and
    prune rows that fell out of it, so the cache mirrors the current rolling window.

    Albums not yet in our catalog are best-effort enqueued for the normal
    candidates→SQS sync (they surface on a later tick); their IDs are not cached
    here because spotify_recent_albums.album_id FKs albums(id)."""
    items = client.get_recently_played(limit=50)
    if not items:
        logger.info("recently-played returned no items; leaving cache unchanged")
        return {"known": 0, "unknown": 0, "pruned": 0}

    # most-recent-first → first occurrence per album is its latest play
    latest_played: Dict[str, str] = {}
    for it in items:
        album = ((it or {}).get("track") or {}).get("album") or {}
        sid = album.get("id")
        played_at = (it or {}).get("played_at")
        if not sid or not played_at:
            continue  # local files / podcasts have no album id
        latest_played.setdefault(sid, played_at)

    spotify_ids = list(latest_played.keys())
    if not spotify_ids:
        logger.info("recently-played had no album-backed tracks; cache unchanged")
        return {"known": 0, "unknown": 0, "pruned": 0}

    with session_factory() as session:
        with session.begin():
            rows = session.execute(
                text("SELECT id, spotify_id FROM albums WHERE spotify_id = ANY(:sids)"),
                {"sids": spotify_ids},
            ).fetchall()
            sid_to_uuid = {r.spotify_id: r.id for r in rows}
            keep_uuids = list(sid_to_uuid.values())

            for sid, album_uuid in sid_to_uuid.items():
                session.execute(
                    text(
                        """
                        INSERT INTO spotify_recent_albums
                            (album_id, last_played_at, source, synced_at)
                        VALUES
                            (:album_id, CAST(:last_played_at AS timestamptz), 'spotify', now())
                        ON CONFLICT (album_id) DO UPDATE
                          SET last_played_at = EXCLUDED.last_played_at,
                              synced_at      = now()
                        """
                    ),
                    {"album_id": album_uuid, "last_played_at": latest_played[sid]},
                )

            if keep_uuids:
                pruned = session.execute(
                    text("DELETE FROM spotify_recent_albums WHERE NOT (album_id = ANY(:keep))"),
                    {"keep": keep_uuids},
                ).rowcount
            else:
                # current window has no catalog albums → cache reflects that honestly
                pruned = session.execute(text("DELETE FROM spotify_recent_albums")).rowcount

    unknown_ids = [sid for sid in spotify_ids if sid not in sid_to_uuid]
    if unknown_ids and enqueue_unknown is not None:
        try:
            enqueue_unknown(unknown_ids)
        except Exception as e:  # best-effort; never blocks the cache write
            logger.warning("enqueue of unknown recently-played albums failed: %s", e)

    logger.info(
        "recent sync: known=%d unknown=%d pruned=%d",
        len(keep_uuids), len(unknown_ids), pruned,
    )
    return {"known": len(keep_uuids), "unknown": len(unknown_ids), "pruned": pruned}


def _upsert_now_playing(session, **fields: Any) -> None:
    params = {
        "is_playing": fields.get("is_playing", False),
        "track_name": fields.get("track_name"),
        "artist_name": fields.get("artist_name"),
        "album_name": fields.get("album_name"),
        "album_id": fields.get("album_id"),
        "progress_ms": fields.get("progress_ms"),
        "duration_ms": fields.get("duration_ms"),
    }
    session.execute(
        text(
            """
            INSERT INTO spotify_now_playing
                (id, is_playing, track_name, artist_name, album_name,
                 album_id, progress_ms, duration_ms, updated_at)
            VALUES
                (1, :is_playing, :track_name, :artist_name, :album_name,
                 :album_id, :progress_ms, :duration_ms, now())
            ON CONFLICT (id) DO UPDATE SET
                is_playing  = EXCLUDED.is_playing,
                track_name  = EXCLUDED.track_name,
                artist_name = EXCLUDED.artist_name,
                album_name  = EXCLUDED.album_name,
                album_id    = EXCLUDED.album_id,
                progress_ms = EXCLUDED.progress_ms,
                duration_ms = EXCLUDED.duration_ms,
                updated_at  = now()
            """
        ),
        params,
    )


def sync_now_playing(session_factory, client) -> Dict[str, Any]:
    """Upsert the single-row now-playing cache. Nothing playing (204) → is_playing
    false with cleared fields."""
    data = client.get_currently_playing()
    item = (data or {}).get("item") if data else None

    if not data or not item:
        with session_factory() as session:
            with session.begin():
                _upsert_now_playing(session, is_playing=False)
        return {"is_playing": False}

    album = item.get("album") or {}
    artists = item.get("artists") or []
    artist_name = ", ".join(a.get("name", "") for a in artists if a.get("name")) or None
    sid = album.get("id")

    with session_factory() as session:
        with session.begin():
            album_uuid = None
            if sid:
                row = session.execute(
                    text("SELECT id FROM albums WHERE spotify_id = :sid"), {"sid": sid}
                ).first()
                album_uuid = row.id if row else None
            _upsert_now_playing(
                session,
                is_playing=bool(data.get("is_playing")),
                track_name=item.get("name"),
                artist_name=artist_name,
                album_name=album.get("name"),
                album_id=album_uuid,
                progress_ms=data.get("progress_ms"),
                duration_ms=item.get("duration_ms"),
            )
    return {"is_playing": bool(data.get("is_playing"))}


def run_listening_sync(
    session_factory,
    client,
    enqueue_unknown: Optional[Callable[[List[str]], None]] = None,
) -> Dict[str, Any]:
    """Run both syncs. now-playing failure must not lose the recent-albums write
    (independent surfaces), so each is isolated."""
    result: Dict[str, Any] = {}
    result["recent"] = sync_recent_albums(session_factory, client, enqueue_unknown)
    try:
        result["now_playing"] = sync_now_playing(session_factory, client)
    except Exception as e:
        logger.error("now-playing sync failed (recent sync already committed): %s", e, exc_info=True)
        result["now_playing"] = {"error": str(e)}
    return result
