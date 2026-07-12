# FEAT-multi-user Phase 3b-d — per-user Spotify listening poll.
#
# For each member with a connected Spotify integration (user_integrations
# provider='spotify' status='connected'; payload = {v:1, ciphertext(b64 KMS envelope
# of the refresh token), scope, expires_in, obtained_at} written by backend 3b-c):
# KMS-decrypt → token refresh → rotate/re-encrypt when Spotify returns a new refresh
# token → write the V45 member listening tables (spotify_member_recent_tracks +
# spotify_member_now_playing). Invoked from EventBridge (worker/handler.py,
# {"job":"spotify_member_poll"}). Never from a user-facing endpoint (rule #9: the
# cron pulls; the API only reads the cached rows).
#
# Failure semantics (per user, all isolated — one user never kills the tick):
# - payload parse / KMS decrypt / config failure → log the exception TYPE NAME only,
#   skip the user, status untouched (transient/infra — the CMK may not be applied
#   yet; infra failures must NEVER mark reauth).
# - token 400 invalid_grant → status='reauth' (payload kept; the 3b-e front badge
#   reads it). The status filter keeps the row out of every later tick — no retry.
# - other refresh/player HTTP failures → skip the user (transient, stays connected).
# - rotation re-encrypt failure AFTER a successful refresh → keep the OLD payload
#   row untouched (Spotify's optimistic rotation usually leaves the old refresh
#   token valid); log and continue the sync. NEVER write a plaintext token.
#
# Raw text() SQL (mirrors lastfm_sync_service) so the worker needs NO shared_db pin
# bump for the V45 tables — only the migration must be applied first (it is, 3b-b).
#
# Session discipline (reference-db-session-across-long-external-loop): fetch the user
# list in a short session and CLOSE it; all KMS + Spotify HTTP happens with NO session
# held; each write (reauth flip, payload rotation, listening rows) is its own fresh
# short session. Bounded to max_users per tick for the 120s Lambda (the 5-user tier
# means one tick covers everyone).
from __future__ import annotations

import base64
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy.sql import text

from worker.core.config import settings
from worker.clients.spotify_member_client import SpotifyInvalidGrant

logger = logging.getLogger(__name__)

# Connected Spotify members, stalest-synced first so a bounded tick eventually
# rotates through everyone (same fairness as the lastfm poll).
_SELECT_CONNECTED = text(
    """
    SELECT ui.user_id AS user_id, ui.payload AS payload
      FROM user_integrations ui
     WHERE ui.provider = 'spotify' AND ui.status = 'connected' AND ui.payload IS NOT NULL
     ORDER BY ui.last_synced_at NULLS FIRST
     LIMIT :lim
    """
)

# invalid_grant → reauth. payload is deliberately KEPT (audit trail + the 3b-e badge
# only needs status); the status filter above guarantees no further refresh attempts.
_UPDATE_REAUTH = text(
    """
    UPDATE user_integrations
       SET status = 'reauth', updated_at = now()
     WHERE user_id = :user_id AND provider = 'spotify'
    """
)

# Rotation: same JSON shape as backend 3b-c connect ({v:1, ciphertext, scope,
# expires_in, obtained_at}) with the re-encrypted new refresh token.
_UPDATE_PAYLOAD = text(
    """
    UPDATE user_integrations
       SET payload = :payload, updated_at = now()
     WHERE user_id = :user_id AND provider = 'spotify'
    """
)

_TOUCH_SYNCED = text(
    """
    UPDATE user_integrations
       SET last_synced_at = now(), updated_at = now()
     WHERE user_id = :user_id AND provider = 'spotify'
    """
)

# V45 unique is FULL (user_id, played_at, spotify_track_id) — a bare ON CONFLICT can
# infer it (only PARTIAL indexes break inference, reference-onconflict-partial-index-
# break). Rows are sorted by the conflict key before insert (deadlock rule).
_INSERT_RECENT = text(
    """
    INSERT INTO spotify_member_recent_tracks
        (user_id, spotify_track_id, track_name, artist_name, album_name, image_url, played_at)
    VALUES (:user_id, :spotify_track_id, :track_name, :artist_name, :album_name,
            :image_url, :played_at)
    ON CONFLICT (user_id, played_at, spotify_track_id) DO NOTHING
    """
)

_UPSERT_NOWPLAYING_TRACK = text(
    """
    INSERT INTO spotify_member_now_playing
        (user_id, is_playing, spotify_track_id, track_name, artist_name, album_name,
         image_url, progress_ms, duration_ms, updated_at)
    VALUES (:user_id, :is_playing, :spotify_track_id, :track_name, :artist_name,
            :album_name, :image_url, :progress_ms, :duration_ms, now())
    ON CONFLICT (user_id) DO UPDATE SET
        is_playing = EXCLUDED.is_playing,
        spotify_track_id = EXCLUDED.spotify_track_id,
        track_name = EXCLUDED.track_name,
        artist_name = EXCLUDED.artist_name,
        album_name = EXCLUDED.album_name,
        image_url = EXCLUDED.image_url,
        progress_ms = EXCLUDED.progress_ms,
        duration_ms = EXCLUDED.duration_ms,
        updated_at = now()
    """
)

# 204 / no item: mark idle but KEEP the last track fields ("last played …" UX).
_UPSERT_NOWPLAYING_IDLE = text(
    """
    INSERT INTO spotify_member_now_playing (user_id, is_playing, updated_at)
    VALUES (:user_id, FALSE, now())
    ON CONFLICT (user_id) DO UPDATE SET
        is_playing = FALSE,
        updated_at = now()
    """
)


def _default_kms():
    import boto3

    return boto3.client("kms", region_name=settings.AWS_DEFAULT_REGION)


def _decrypt_refresh_token(kms, payload_doc: Dict[str, Any]) -> str:
    """b64 ciphertext (KMS envelope, key id embedded) → plaintext refresh token."""
    blob = base64.b64decode(payload_doc["ciphertext"])
    return kms.decrypt(CiphertextBlob=blob)["Plaintext"].decode("utf-8")


def _encrypt_refresh_token(kms, kms_key_id: str, refresh_token: str) -> str:
    """Plaintext refresh token → b64 KMS envelope. Raises when the CMK id is unset
    (config not yet applied) — the caller keeps the old payload."""
    if not kms_key_id:
        raise RuntimeError("USER_TOKENS_KMS_KEY_ID unset — cannot re-encrypt rotation")
    blob = kms.encrypt(KeyId=kms_key_id, Plaintext=refresh_token.encode("utf-8"))[
        "CiphertextBlob"
    ]
    return base64.b64encode(blob).decode("ascii")


def _parse_played_at(value: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _artist_names(artists: Any) -> Optional[str]:
    names = [a.get("name") for a in (artists or []) if isinstance(a, dict) and a.get("name")]
    return ", ".join(names) or None


def _largest_image(album: Dict[str, Any]) -> Optional[str]:
    images = album.get("images") or []
    if images and isinstance(images[0], dict):
        return images[0].get("url")  # Spotify orders images largest-first
    return None


def _recent_rows(user_id: Any, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Flatten /me/player/recently-played items to insert params, sorted by the
    conflict key (played_at, spotify_track_id; user_id constant per user)."""
    rows: List[Dict[str, Any]] = []
    for item in items or []:
        track = (item or {}).get("track") or {}
        tid = track.get("id")
        played_at = _parse_played_at((item or {}).get("played_at") or "")
        if not tid or played_at is None:
            continue  # local files / malformed items have no stable identity
        album = track.get("album") or {}
        rows.append(
            {
                "user_id": user_id,
                "spotify_track_id": tid,
                "track_name": track.get("name") or "",
                "artist_name": _artist_names(track.get("artists")),
                "album_name": album.get("name"),
                "image_url": _largest_image(album),
                "played_at": played_at,
            }
        )
    rows.sort(key=lambda r: (r["played_at"], r["spotify_track_id"]))
    return rows


def _nowplaying_params(user_id: Any, state: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Flatten a /me/player state to upsert params; None ⇒ idle (204, empty body,
    or a non-track item such as an episode/ad — no stable track identity)."""
    if not state:
        return None
    item = state.get("item") or {}
    if not item.get("id"):
        return None
    album = item.get("album") or {}
    return {
        "user_id": user_id,
        "is_playing": bool(state.get("is_playing")),
        "spotify_track_id": item["id"],
        "track_name": item.get("name") or "",
        "artist_name": _artist_names(item.get("artists")),
        "album_name": album.get("name"),
        "image_url": _largest_image(album),
        "progress_ms": state.get("progress_ms"),
        "duration_ms": item.get("duration_ms"),
    }


def _rotated_payload(old_doc: Dict[str, Any], token_body: Dict[str, Any], new_ciphertext: str) -> str:
    """Same JSON shape backend 3b-c writes — v:1, refreshed obtained_at."""
    return json.dumps(
        {
            "v": 1,
            "ciphertext": new_ciphertext,
            "scope": token_body.get("scope") or old_doc.get("scope", ""),
            "expires_in": int(token_body.get("expires_in") or old_doc.get("expires_in", 3600)),
            "obtained_at": datetime.now(timezone.utc).isoformat(),
        }
    )


def _sync_one(session_factory, client, kms, kms_key_id: str, user_id: Any, payload_raw: str) -> Dict[str, int]:
    """One member's full poll. Raises on transient failures (caller isolates);
    returns {"recent": inserted_count, "reauth": 0|1}."""
    # -- decrypt (no session held; KMS/parse failure propagates → skip user) --
    payload_doc = json.loads(payload_raw)
    refresh_token = _decrypt_refresh_token(kms, payload_doc)

    # -- refresh exchange --
    try:
        token_body = client.refresh(refresh_token)
    except SpotifyInvalidGrant:
        logger.warning(
            "spotify member refresh rejected (invalid_grant) → status=reauth (user_id=%s)",
            user_id,
        )
        with session_factory() as session, session.begin():
            session.execute(_UPDATE_REAUTH, {"user_id": user_id})
        return {"recent": 0, "reauth": 1}

    access_token = token_body["access_token"]

    # -- rotation: persist BEFORE the player reads so a later failure can't lose it --
    rotated = token_body.get("refresh_token")
    if rotated and rotated != refresh_token:
        try:
            new_ct = _encrypt_refresh_token(kms, kms_key_id, rotated)
            new_payload = _rotated_payload(payload_doc, token_body, new_ct)
            with session_factory() as session, session.begin():
                session.execute(_UPDATE_PAYLOAD, {"user_id": user_id, "payload": new_payload})
        except Exception as e:
            # Old refresh token generally stays valid under Spotify's optimistic
            # rotation — keep the old payload, never write plaintext.
            logger.warning(
                "spotify member rotation re-encrypt failed (old payload kept) "
                "for user_id=%s: %s",
                user_id, type(e).__name__,
            )

    # -- player reads (still no session held) --
    player_state = client.get_player_state(access_token)
    recent_items = client.get_recently_played(access_token, limit=50)

    # -- materialize, then one fresh short write session --
    recent_rows = _recent_rows(user_id, recent_items)
    np_params = _nowplaying_params(user_id, player_state)

    inserted = 0
    with session_factory() as session, session.begin():
        for row in recent_rows:  # sorted by conflict key above
            inserted += session.execute(_INSERT_RECENT, row).rowcount
        if np_params is not None:
            session.execute(_UPSERT_NOWPLAYING_TRACK, np_params)
        else:
            session.execute(_UPSERT_NOWPLAYING_IDLE, {"user_id": user_id})
        session.execute(_TOUCH_SYNCED, {"user_id": user_id})
    return {"recent": inserted, "reauth": 0}


def run_spotify_member_sync(
    session_factory,
    client,
    *,
    kms=None,
    kms_key_id: Optional[str] = None,
    max_users: int = 10,
) -> Dict[str, int]:
    """Poll each connected member's Spotify listening state. Returns a summary dict.
    Logs only user counts / exception type names — never tokens or ciphertext."""
    if kms is None:
        kms = _default_kms()
    if kms_key_id is None:
        kms_key_id = settings.USER_TOKENS_KMS_KEY_ID

    # Phase 1 — read the connected members, then CLOSE the session.
    with session_factory() as session:
        rows = session.execute(_SELECT_CONNECTED, {"lim": max_users}).fetchall()
    users = [(r.user_id, r.payload) for r in rows]
    if not users:
        logger.info("spotify member sync: no connected users")
        return {"users": 0, "recent": 0, "reauth": 0, "skipped": 0}

    synced = 0
    total_recent = 0
    total_reauth = 0
    skipped = 0
    for user_id, payload_raw in users:
        try:
            result = _sync_one(session_factory, client, kms, kms_key_id, user_id, payload_raw)
        except Exception as e:
            # Transient (KMS/config/network/5xx) or malformed payload — skip this
            # user, keep status, never log token material.
            skipped += 1
            logger.warning(
                "spotify member sync skipped user_id=%s (kept connected): %s",
                user_id, type(e).__name__,
            )
            continue
        total_reauth += result["reauth"]
        if result["reauth"]:
            continue
        synced += 1
        total_recent += result["recent"]

    logger.info(
        "spotify member sync: users=%d recent=%d reauth=%d skipped=%d",
        synced, total_recent, total_reauth, skipped,
    )
    return {"users": synced, "recent": total_recent, "reauth": total_reauth, "skipped": skipped}
