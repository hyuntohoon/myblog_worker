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

from myblog_shared_db.lyrics_demand import LyricsDemandStore

from worker.core.config import settings
from worker.clients.spotify_member_client import (
    SpotifyInvalidGrant,
    SpotifyMemberFollowScopeError,
    SpotifyMemberScopeError,
)
from worker.service.lyrics_follow_demand_service import sync_follow_demand
from worker.service.lyrics_member_demand_service import (
    DISCOVERY_ORIGINS,
    sync_member_demand,
)

logger = logging.getLogger(__name__)

# Connected Spotify members, stalest-synced first so a bounded tick eventually
# rotates through everyone (same fairness as the lastfm poll).
_SELECT_CONNECTED = text(
    """
    SELECT ui.user_id AS user_id, ui.payload AS payload
      FROM user_integrations ui
     WHERE ui.provider = 'spotify' AND ui.status = 'connected' AND ui.payload IS NOT NULL
       AND (CAST(:only AS uuid) IS NULL OR ui.user_id = CAST(:only AS uuid))
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


def _produce_demand(session_factory, client, access_token: str, user_id: Any,
                    recent_items: List[Dict[str, Any]]) -> Dict[str, int]:
    """Step 4 producer, isolated from the listening poll (see the module docstring).

    Runs with NO DB session held by the caller, because the library read below is a
    fully paginated Spotify call and a session left open across it is the exact
    idle-in-transaction shape that has bitten this project before.

    A library read that fails passes `saved_albums=None`, never `[]`: an empty list is
    a truthful observation that the member saved nothing and would reconcile every
    saved-origin demand away. A failed read must not be able to delete demand.
    """
    saved: Optional[List[Dict[str, Any]]] = None
    try:
        saved = client.get_saved_albums(access_token)
    except SpotifyMemberScopeError:
        # The grant predates library consent (or the member declined it). The token is
        # still valid — do NOT touch status; the front already prompts for reconsent
        # from the stored scope string. Recent-listening demand still runs below.
        logger.info(
            "member library skipped — grant lacks user-library-read (user_id=%s)", user_id
        )
    except Exception as e:
        logger.warning(
            "member library read failed, saved-origin left untouched (user_id=%s): %s",
            user_id, type(e).__name__,
        )
    return sync_member_demand(
        session_factory, user_id, saved_albums=saved, recent_items=recent_items
    )


def _produce_follow_demand(session_factory, client, access_token: str, user_id: Any) -> Dict[str, int]:
    """Step 5 producer, isolated from Step 4's the same way Step 4 is from the poll.

    Same None-vs-[] rule as the library read, and it matters more here: `user-follow-read`
    was not in the authorize URL before Step 5, so EVERY member who connected earlier
    gets the 403 path until they re-consent. Passing `[]` for those members would
    reconcile away every Spotify-origin edge and every follow demand they have — the
    failed read must not be able to delete.
    """
    followed: Optional[List[Dict[str, Any]]] = None
    try:
        followed = client.get_followed_artists(access_token)
    except SpotifyMemberFollowScopeError:
        # A missing follow grant is not a broken token: leave status alone, leave the
        # other two origins running, and let the front prompt for reconsent from the
        # stored scope string.
        logger.info(
            "member follows skipped — grant lacks user-follow-read (user_id=%s)", user_id
        )
    except Exception as e:
        logger.warning(
            "member follow read failed, follow origin left untouched (user_id=%s): %s",
            user_id, type(e).__name__,
        )
    return sync_follow_demand(session_factory, user_id, followed_artists=followed)


def _sync_one(session_factory, client, kms, kms_key_id: str, user_id: Any, payload_raw: str,
              demand_enabled: bool = False, follow_enabled: bool = False) -> Dict[str, int]:
    """One member's full poll. Raises on transient failures (caller isolates);
    returns {"recent": inserted_count, "reauth": 0|1, "demand_*": …}."""
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
        # `invalid_grant` is the member revoking us at Spotify (or deleting the app from
        # their account page) — the strongest "stop using my library" signal there is, and
        # the one that never touches our UI. Step 4's disconnect fence lives in the backend
        # DELETE route, which this path never reaches, so without the revoke below a member
        # who withdraws at the provider keeps a live `saved`/`recent` scope and every demand
        # row derived from their private library: the poll stops producing NEW demand (they
        # fall out of `_SELECT_CONNECTED`), but the durable artefact keeps generating
        # translation work with no way for them to reach it short of reconnecting in order
        # to disconnect.
        #
        # Same transaction as the status flip, for the same reason the backend's disconnect
        # is atomic: between a committed reauth and a separate revoke there is a window
        # where consent is withdrawn and the demand is still live. A failure here rolls the
        # flip back too, leaving the member 'connected' so the next tick retries — the
        # refresh fails first, so no production happens in the meantime.
        #
        # Deliberately NOT gated on `demand_enabled`: the switch stops new production, it
        # does not make an existing scope legitimate. `revoke_scopes` is a no-op when the
        # member has no scope rows.
        with session_factory() as session, session.begin():
            session.execute(_UPDATE_REAUTH, {"user_id": user_id})
            store = LyricsDemandStore(session.connection())
            store.revoke_scopes(user_id, list(DISCOVERY_ORIGINS))
            # Step 5's second artefact. `revoke_scopes` removes the member's demand;
            # this removes the copy of WHOM THEY FOLLOW that Step 5 writes into their
            # site tracking. Withdrawing at the provider is the strongest "stop using my
            # account" signal there is, and after it the reconciler can no longer run —
            # so a mirror left behind here is permanent, not merely stale.
            store.revoke_provider_follows(user_id)
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

    # -- Step 4 demand production (session closed again; never fails the poll) --
    result = {"recent": inserted, "reauth": 0, "demand_failed": 0, "follow_failed": 0}
    if demand_enabled:
        try:
            result.update(_produce_demand(session_factory, client, access_token, user_id, recent_items))
        except Exception:
            # Isolated on purpose: a demand-store failure must not cost this member
            # their listening data, and must not look like a credential problem.
            result["demand_failed"] = 1
            logger.error(
                "member demand production failed (listening sync kept) for user_id=%s",
                user_id, exc_info=True,
            )

    # -- Step 5 follow production, isolated from Step 4's as well as from the poll --
    # Its own switch, not a reuse of LYRICS_MEMBER_DEMAND_ENABLED: this producer is the
    # expensive one (a member's whole followed back catalogue rather than their saved
    # albums), so the owner must be able to stop it without also losing saved/recent.
    if follow_enabled:
        try:
            result.update(_produce_follow_demand(session_factory, client, access_token, user_id))
        except Exception:
            result["follow_failed"] = 1
            logger.error(
                "member follow production failed (listening sync kept) for user_id=%s",
                user_id, exc_info=True,
            )
    return result


def run_spotify_member_sync(
    session_factory,
    client,
    *,
    kms=None,
    kms_key_id: Optional[str] = None,
    max_users: int = 10,
    only_user_id: Optional[str] = None,
    demand_enabled: Optional[bool] = None,
    follow_enabled: Optional[bool] = None,
) -> Dict[str, int]:
    """Poll each connected member's Spotify listening state. Returns a summary dict.
    Logs only user counts / exception type names — never tokens or ciphertext.

    `only_user_id` narrows the pass to a single member — the connect-time bootstrap
    (Step 4) runs the identical code for one member rather than a parallel
    implementation, which is what makes the 15-minute cron a true recovery path for a
    bootstrap message that was never delivered. A member id that is not connected
    selects nothing and is a clean no-op.

    `demand_enabled` and `follow_enabled` default to the worker's OWN settings, never a
    message field, so a stray or replayed SQS message can never switch the producers on.
    """
    if kms is None:
        kms = _default_kms()
    if kms_key_id is None:
        kms_key_id = settings.USER_TOKENS_KMS_KEY_ID
    if demand_enabled is None:
        demand_enabled = settings.LYRICS_MEMBER_DEMAND_ENABLED
    if follow_enabled is None:
        follow_enabled = settings.LYRICS_FOLLOW_DEMAND_ENABLED

    # Phase 1 — read the connected members, then CLOSE the session.
    with session_factory() as session:
        rows = session.execute(
            _SELECT_CONNECTED, {"lim": max_users, "only": only_user_id}
        ).fetchall()
    users = [(r.user_id, r.payload) for r in rows]
    if not users:
        logger.info("spotify member sync: no connected users (only_user_id=%s)", only_user_id)
        return {"users": 0, "recent": 0, "reauth": 0, "skipped": 0,
                "saved_added": 0, "saved_removed": 0, "recent_albums": 0, "demand_failed": 0,
                "follow_added": 0, "follow_removed": 0, "follow_artists": 0,
                "follow_skipped": 0, "follow_failed": 0}

    synced = 0
    total_recent = 0
    total_reauth = 0
    skipped = 0
    demand = {"saved_added": 0, "saved_removed": 0, "recent_albums": 0, "demand_failed": 0,
              "follow_added": 0, "follow_removed": 0, "follow_artists": 0,
              "follow_skipped": 0, "follow_failed": 0}
    for user_id, payload_raw in users:
        try:
            result = _sync_one(
                session_factory, client, kms, kms_key_id, user_id, payload_raw,
                demand_enabled=demand_enabled, follow_enabled=follow_enabled,
            )
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
        demand["saved_added"] += result.get("saved_added", 0)
        demand["saved_removed"] += result.get("saved_removed", 0)
        demand["recent_albums"] += result.get("recent_added", 0)
        demand["demand_failed"] += result.get("demand_failed", 0)
        for key in ("follow_added", "follow_removed", "follow_artists",
                    "follow_skipped", "follow_failed"):
            demand[key] += result.get(key, 0)

    logger.info(
        "spotify member sync: users=%d recent=%d reauth=%d skipped=%d "
        "demand(saved +%d/-%d, recent +%d, failed=%d, enabled=%s) "
        "follow(artists=%d, +%d/-%d, no-grant=%d, failed=%d, enabled=%s)",
        synced, total_recent, total_reauth, skipped,
        demand["saved_added"], demand["saved_removed"], demand["recent_albums"],
        demand["demand_failed"], demand_enabled,
        demand["follow_artists"], demand["follow_added"], demand["follow_removed"],
        demand["follow_skipped"], demand["follow_failed"], follow_enabled,
    )
    return {"users": synced, "recent": total_recent, "reauth": total_reauth,
            "skipped": skipped, **demand}
