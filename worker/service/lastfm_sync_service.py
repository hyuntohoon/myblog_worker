# FEAT-multi-user Phase 3a — per-user Last.fm recent-tracks poll.
#
# Reads each connected user's Last.fm scrobbles (user.getRecentTracks, incremental)
# and writes them to the V41 lastfm_recent_tracks cache + the single now-playing row.
# Invoked from the EventBridge cron (worker/handler.py, {"job":"lastfm_recent_tracks"}).
# Never from a user-facing endpoint (rule #9 principle: the cron reads Last.fm).
#
# Raw text() SQL (mirrors listening_sync_service) so the worker needs NO shared_db pin
# bump for the V41 tables — only the migration must be applied first.
#
# Session discipline (reference-db-session-across-long-external-loop): fetch the user
# list in a short session and CLOSE it; loop the per-user Last.fm HTTP calls with NO
# session held (Neon drops idle-in-txn conns → ProtocolViolation); then a fresh short
# write session per user. Bounded to max_users per tick for the 120s Lambda.
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy.sql import text

from worker.clients.lastfm_client import LastfmUserNotFound

logger = logging.getLogger(__name__)

# Connected Last.fm users + each one's incremental cursor (Unix seconds of their
# latest completed scrobble; NULL = never synced → full initial fetch). Rotated by
# last_synced_at so a bounded tick eventually covers everyone.
_SELECT_CONNECTED = text(
    """
    SELECT ui.user_id AS user_id,
           ui.username AS username,
           (SELECT EXTRACT(EPOCH FROM max(lrt.played_at))::bigint
              FROM lastfm_recent_tracks lrt
             WHERE lrt.user_id = ui.user_id AND NOT lrt.is_now_playing) AS cursor_uts
      FROM user_integrations ui
     WHERE ui.provider = 'lastfm' AND ui.status = 'connected' AND ui.username IS NOT NULL
     ORDER BY ui.last_synced_at NULLS FIRST
     LIMIT :lim
    """
)

# Dedup on the partial-unique (user_id, played_at) WHERE NOT is_now_playing via
# NOT-EXISTS (reference-onconflict-partial-index-break: a bare ON CONFLICT can't
# infer a partial index; the poll is single-instance so there is no insert race).
_INSERT_SCROBBLE = text(
    """
    INSERT INTO lastfm_recent_tracks
        (user_id, artist_name, track_name, album_name,
         artist_mbid, track_mbid, album_mbid, image_url, played_at, is_now_playing)
    SELECT :user_id, :artist, :track, :album,
           :artist_mbid, :track_mbid, :album_mbid, :image, to_timestamp(:played_at_uts), FALSE
     WHERE NOT EXISTS (
         SELECT 1 FROM lastfm_recent_tracks
          WHERE user_id = :user_id
            AND played_at = to_timestamp(:played_at_uts)
            AND NOT is_now_playing
     )
    """
)

_DELETE_NOWPLAYING = text(
    "DELETE FROM lastfm_recent_tracks WHERE user_id = :user_id AND is_now_playing"
)

_INSERT_NOWPLAYING = text(
    """
    INSERT INTO lastfm_recent_tracks
        (user_id, artist_name, track_name, album_name,
         artist_mbid, track_mbid, album_mbid, image_url, played_at, is_now_playing)
    VALUES (:user_id, :artist, :track, :album,
            :artist_mbid, :track_mbid, :album_mbid, :image, NULL, TRUE)
    """
)

_UPDATE_INTEGRATION = text(
    """
    UPDATE user_integrations
       SET status = :status, last_synced_at = now(), updated_at = now()
     WHERE user_id = :user_id AND provider = 'lastfm'
    """
)


def _scrobble_params(user_id: Any, sc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "user_id": user_id,
        "artist": sc.get("artist"),
        "track": sc.get("track"),
        "album": sc.get("album"),
        "artist_mbid": sc.get("artist_mbid"),
        "track_mbid": sc.get("track_mbid"),
        "album_mbid": sc.get("album_mbid"),
        "image": sc.get("image"),
        "played_at_uts": sc["played_at_uts"],
    }


def _nowplaying_params(user_id: Any, np: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "user_id": user_id,
        "artist": np.get("artist"),
        "track": np.get("track"),
        "album": np.get("album"),
        "artist_mbid": np.get("artist_mbid"),
        "track_mbid": np.get("track_mbid"),
        "album_mbid": np.get("album_mbid"),
        "image": np.get("image"),
    }


def run_lastfm_sync(session_factory, client, *, max_users: int = 50) -> Dict[str, Any]:
    """Poll each connected user's Last.fm recent tracks. Returns a summary dict."""
    # Phase 1 — read the connected users + cursors, then CLOSE the session.
    with session_factory() as session:
        rows = session.execute(_SELECT_CONNECTED, {"lim": max_users}).fetchall()
    users = [(r.user_id, r.username, r.cursor_uts) for r in rows]
    if not users:
        logger.info("lastfm sync: no connected users")
        return {"users": 0, "scrobbles": 0}

    total_users = 0
    total_scrobbles = 0
    for user_id, username, cursor_uts in users:
        # Phase 2 — external Last.fm read with NO DB session held.
        from_uts = (int(cursor_uts) + 1) if cursor_uts else None
        status = "connected"
        scrobbles: List[Dict[str, Any]] = []
        nowplaying: Optional[Dict[str, Any]] = None
        try:
            scrobbles, nowplaying = client.get_recent_tracks(username, from_uts=from_uts)
        except LastfmUserNotFound:
            status = "error"
            logger.warning(
                "lastfm user '%s' not found → status=error (user_id=%s)", username, user_id
            )
        except Exception as e:  # transient (network / 5xx / throttle) — keep connected
            logger.warning("lastfm fetch failed for user_id=%s (kept connected): %s", user_id, e)

        # Phase 3 — a fresh short write session per user.
        with session_factory() as session:
            with session.begin():
                for sc in scrobbles:
                    total_scrobbles += session.execute(
                        _INSERT_SCROBBLE, _scrobble_params(user_id, sc)
                    ).rowcount
                # Replace the single now-playing row (delete + insert; the partial
                # unique keeps it to one per user).
                session.execute(_DELETE_NOWPLAYING, {"user_id": user_id})
                if nowplaying is not None:
                    session.execute(_INSERT_NOWPLAYING, _nowplaying_params(user_id, nowplaying))
                session.execute(_UPDATE_INTEGRATION, {"status": status, "user_id": user_id})
        total_users += 1

    logger.info("lastfm sync: users=%d scrobbles=%d", total_users, total_scrobbles)
    return {"users": total_users, "scrobbles": total_scrobbles}
