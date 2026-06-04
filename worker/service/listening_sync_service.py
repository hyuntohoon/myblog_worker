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
    all_plays: List[tuple[str, str]] = []  # D29: every play (sid, played_at) for the events table
    for it in items:
        album = ((it or {}).get("track") or {}).get("album") or {}
        sid = album.get("id")
        played_at = (it or {}).get("played_at")
        if not sid or not played_at:
            continue  # local files / podcasts have no album id
        latest_played.setdefault(sid, played_at)
        all_plays.append((sid, played_at))

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

            # D29: append every individual play (catalog-known albums only) to the
            # append-only events table, in the same tx as the snapshot upsert. The
            # (album_id, played_at) dedup key makes the rolling-window re-read a
            # no-op, so overlapping ticks / manual refreshes never double-count.
            events_appended = 0
            for sid, played_at in all_plays:
                album_uuid = sid_to_uuid.get(sid)
                if album_uuid is None:
                    continue  # not in our catalog yet (enqueued below for a later tick)
                events_appended += session.execute(
                    text(
                        """
                        INSERT INTO spotify_play_events (album_id, played_at)
                        VALUES (:album_id, CAST(:played_at AS timestamptz))
                        ON CONFLICT (album_id, played_at) DO NOTHING
                        """
                    ),
                    {"album_id": album_uuid, "played_at": played_at},
                ).rowcount

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
        "recent sync: known=%d unknown=%d pruned=%d events=%d",
        len(keep_uuids), len(unknown_ids), pruned, events_appended,
    )
    return {
        "known": len(keep_uuids),
        "unknown": len(unknown_ids),
        "pruned": pruned,
        "events": events_appended,
    }


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


DEBOUNCE_WINDOW_SECONDS = 60


def _debounce_age_seconds(session_factory) -> Optional[float]:
    """Seconds since the most recent listening-cache write, measured DB-side so
    Lambda↔DB clock skew can't move the window. None when neither cache has a row
    yet (bootstrap) — the manual-refresh debounce treats that as 'run'. GREATEST
    ignores NULLs, so it returns whichever cache has the newer write."""
    with session_factory() as session:
        row = session.execute(
            text(
                """
                SELECT EXTRACT(EPOCH FROM (now() - GREATEST(
                    (SELECT max(synced_at) FROM spotify_recent_albums),
                    (SELECT max(updated_at) FROM spotify_now_playing)
                ))) AS age_s
                """
            )
        ).first()
    age = row.age_s if row else None
    return float(age) if age is not None else None


def run_listening_sync(
    session_factory,
    client,
    enqueue_unknown: Optional[Callable[[List[str]], None]] = None,
    *,
    is_manual_refresh: bool = False,
) -> Dict[str, Any]:
    """Run both syncs, isolated symmetrically (RFC): recent-albums and now-playing
    are independent surfaces, so either can fail without aborting the other. recent
    commits first; a recent failure no longer skips the now-playing read (it used to
    abort the whole tick, losing a now-playing update that would have succeeded).

    Manual "지금 새로고침" refreshes are debounced (D31): if the cache was written
    less than DEBOUNCE_WINDOW_SECONDS ago, skip the Spotify reads entirely. This
    dedups manual-refresh spam so it can't burst Spotify into 429/DLQ, and keeps the
    non-commutative recent-albums prune serialised. The 1h cron is never debounced
    (is_manual_refresh=False; its gap dwarfs the window anyway)."""
    if is_manual_refresh:
        age = _debounce_age_seconds(session_factory)
        if age is not None and age < DEBOUNCE_WINDOW_SECONDS:
            logger.info(
                "manual refresh debounced: cache written %.1fs ago (< %ds)",
                age, DEBOUNCE_WINDOW_SECONDS,
            )
            return {"skipped": "debounced"}
    result: Dict[str, Any] = {}
    try:
        result["recent"] = sync_recent_albums(session_factory, client, enqueue_unknown)
    except Exception as e:
        logger.error("recent-albums sync failed (now-playing still attempted): %s", e, exc_info=True)
        result["recent"] = {"error": str(e)}
    try:
        result["now_playing"] = sync_now_playing(session_factory, client)
    except Exception as e:
        logger.error("now-playing sync failed (recent sync unaffected): %s", e, exc_info=True)
        result["now_playing"] = {"error": str(e)}
    return result
