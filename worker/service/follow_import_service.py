# FEAT-for-you-releases Step 2 — owner followed-artists snapshot import.
#
# Triggered by the backend's owner-gated POST /api/me/tracked-artists/spotify-import
# enqueue ({"job": "spotify_follow_import", "user_id": ...}) — never from a
# user-facing endpoint directly (hard rule #9: the endpoint only enqueues; the
# worker does the Spotify read). Same independence contract as the Buckit import:
# a SNAPSHOT source only — after import the tracked list has no link back to
# Spotify follows (unfollowing on Spotify never untracks here, and vice versa).
#
# Followed artists missing from the catalog (OQ1, owner-decided 2026-07-21 =
# catalog-ingest): fan out {"job": "spotify_follow_ingest", "artist_sids": [...]}
# messages that expand each artist's recent releases (one get_artist_albums page,
# album+single) onto the EXISTING album-sync SQS pipeline — artists are created by
# album sync, never directly. One delayed rerun ({"rerun": true}, 900s) then
# re-imports so freshly-ingested artists get their tracked edge without a second
# button press; rerun never fans out again, so there is no feedback loop.
#
# Session discipline (reference-db-session-across-long-external-loop): ALL Spotify
# paging happens before any DB session opens; the match+upsert is one short
# session; the SQS fan-out runs after it closes.
#
# Raw text() SQL (mirrors lastfm/spotify_member sync services) so the worker needs
# no shared_db pin bump — user_artist_tracks (V47) is already applied.
from __future__ import annotations

import logging
import uuid
from typing import Any, Callable, Dict, List

from sqlalchemy import bindparam
from sqlalchemy.sql import text

logger = logging.getLogger(__name__)

# INSERT ... SELECT keeps the row set + its deadlock-avoidance ordering DB-side:
# rows arrive sorted by the (user_id, artist_id) conflict key (user_id is constant
# here), per the bulk-upsert hard rule. RETURNING (not rowcount — prod psycopg
# reported -1 for this shape on the backend twin) counts actually-inserted rows.
_IMPORT_SQL = text(
    """
    INSERT INTO user_artist_tracks (user_id, artist_id)
    SELECT :user_id, id FROM artists WHERE spotify_id IN :sids
    ORDER BY id
    ON CONFLICT (user_id, artist_id) DO NOTHING
    RETURNING artist_id
    """
).bindparams(bindparam("sids", expanding=True))

_MATCH_SQL = text(
    "SELECT spotify_id FROM artists WHERE spotify_id IN :sids"
).bindparams(bindparam("sids", expanding=True))

_USER_SQL = text("SELECT 1 FROM users WHERE id = :user_id")


def run_follow_import(
    session_factory,
    user_client,
    *,
    enqueue_ingest: Callable[[List[str]], int],
    enqueue_rerun: Callable[[str], bool],
    user_id: Any,
    rerun: bool = False,
) -> Dict[str, Any]:
    """Import the owner's Spotify followed artists into user_artist_tracks.

    Returns a metrics dict (followed / matched / imported / unmatched /
    ingest_enqueued / rerun_chained). Malformed user_id or a missing users row
    drops the message with a warning (permanent — retrying can't fix it);
    SpotifyScopeError is caught likewise (re-auth is a human step, not a retry).
    Transient Spotify/DB failures raise so the SQS retry/DLQ path applies.
    """
    metrics: Dict[str, Any] = {
        "followed": 0, "matched": 0, "imported": 0, "unmatched": 0,
        "ingest_enqueued": 0, "rerun_chained": False, "rerun": bool(rerun),
    }
    try:
        user_uuid = uuid.UUID(str(user_id))
    except (TypeError, ValueError, AttributeError):
        logger.warning("follow_import: invalid user_id %r — dropping message", user_id)
        return metrics

    from worker.clients.spotify_user_client import SpotifyScopeError

    try:
        followed = user_client.get_followed_artists()
    except SpotifyScopeError as e:
        # Missing user-follow-read consent — a retry can only 403 again. The owner
        # re-runs the bootstrap, then re-triggers the import.
        logger.warning("follow_import: %s", e)
        metrics["scope_error"] = True
        return metrics

    sids = sorted({a["id"] for a in followed if a and a.get("id")})
    metrics["followed"] = len(sids)
    if not sids:
        logger.info("follow_import metrics: %s", metrics)
        return metrics

    with session_factory() as session:
        # user_uuid is bound as a uuid.UUID (psycopg adapts it to the uuid type —
        # a str would bind as text and fail the uuid = text comparison).
        if session.execute(_USER_SQL, {"user_id": user_uuid}).first() is None:
            logger.warning("follow_import: user %s not found — dropping message", user_uuid)
            return metrics
        matched_sids = set(
            session.execute(_MATCH_SQL, {"sids": sids}).scalars()
        )
        imported = list(
            session.execute(
                _IMPORT_SQL, {"user_id": user_uuid, "sids": sids}
            ).scalars()
        )
        session.commit()

    metrics["matched"] = len(matched_sids)
    metrics["imported"] = len(imported)
    unmatched = [s for s in sids if s not in matched_sids]
    metrics["unmatched"] = len(unmatched)

    # Fan-out AFTER the session is closed. First run only — the rerun pass just
    # attaches whatever the ingest managed to catalog (leftovers wait for the
    # owner's next manual import; snapshot semantics make re-runs idempotent).
    if unmatched and not rerun:
        metrics["ingest_enqueued"] = enqueue_ingest(unmatched)
        metrics["rerun_chained"] = bool(enqueue_rerun(str(user_uuid)))

    logger.info("follow_import metrics: %s", metrics)
    return metrics


def run_follow_ingest(
    catalog_client,
    enqueue_album_sync: Callable[[List[str]], None],
    artist_sids: List[str],
) -> Dict[str, Any]:
    """Expand uncatalogued followed artists into album-sync jobs.

    One get_artist_albums page (50, most-recent-first, album+single — matching the
    ingest sweep's watchlist grouping) per artist; deeper back-catalog stays on the
    reactive candidates path. Per-artist failure isolation: one dead artist lookup
    never kills the chunk. No DB session is used at all — artists/albums are
    created downstream by the album-sync consumer."""
    metrics: Dict[str, Any] = {"artists": 0, "failed_artists": 0, "albums_enqueued": 0}
    sids = [s for s in (artist_sids or []) if s]
    metrics["artists"] = len(sids)
    album_ids: set = set()
    for sid in sids:
        try:
            items = catalog_client.get_artist_albums(sid, include_groups="album,single")
        except Exception as e:
            metrics["failed_artists"] += 1
            logger.warning(
                "follow_ingest: artist %s album fetch failed (%s) — skipped",
                sid, type(e).__name__,
            )
            continue
        album_ids.update(a["id"] for a in (items or []) if a and a.get("id"))

    if album_ids:
        # Sorted for the album-sync consumer's own upsert ordering (deadlock rule).
        sorted_ids = sorted(album_ids)
        enqueue_album_sync(sorted_ids)
        metrics["albums_enqueued"] = len(sorted_ids)
    logger.info("follow_ingest metrics: %s", metrics)
    return metrics
