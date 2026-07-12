# Spotify *member*-scoped client (FEAT-multi-user Phase 3b-d).
#
# Distinct from spotify_user_client.SpotifyUserClient, which owns the OWNER's single
# refresh token (Secrets/SSM myblog/spotify) and caches an access token per Lambda.
# Members each have their own KMS-encrypted refresh token in user_integrations.payload
# (written by backend connect, 3b-c), so this client is STATELESS per call: the poll
# service decrypts a member's refresh token, exchanges it here, and uses the returned
# access token for that member's player reads only. Nothing is cached across users.
#
# Shares the app client_id/client_secret with the owner client (same Spotify app,
# myblog/spotify SSM blob) and reuses its transient-retry helper. Tokens are never
# logged (only status codes / exception type names).
from __future__ import annotations

import base64
import logging
from typing import Any, Dict, List, Optional

from worker.core.config import settings
from worker.clients.spotify_user_client import (
    _is_invalid_grant,
    _load_spotify_creds,
    _request_with_retry,
)

logger = logging.getLogger(__name__)


class SpotifyInvalidGrant(RuntimeError):
    """The member's refresh token was revoked/expired (token-endpoint 400
    error=invalid_grant) — the ONLY signal that maps to status='reauth'. Transient
    failures (5xx / 429 / network) must never raise this."""


class SpotifyMemberClient:
    """Per-member Spotify token refresh + player reads. No token state is kept."""

    def __init__(self, creds: Optional[Dict[str, str]] = None) -> None:
        # creds = {"client_id": …, "client_secret": …} — injectable for tests.
        self._creds = creds

    def _app_creds(self) -> Dict[str, str]:
        if self._creds is None:
            # Reuses the owner path's myblog/spotify reader (SSM-preferred, env
            # fallback); only client_id/client_secret are used here.
            self._creds = _load_spotify_creds()
        return self._creds

    def refresh(self, refresh_token: str) -> Dict[str, Any]:
        """Exchange a member refresh token for an access token.

        Returns the raw token body ({"access_token", optional rotated
        "refresh_token", "scope", "expires_in", …}). Raises SpotifyInvalidGrant on a
        400 invalid_grant (re-auth needed); any other non-2xx raises httpx.HTTPStatusError
        (transient — the caller skips the user this tick)."""
        creds = self._app_creds()
        if not creds.get("client_id") or not creds.get("client_secret"):
            raise RuntimeError("Spotify app client credentials not configured")
        auth = f"{creds['client_id']}:{creds['client_secret']}".encode()
        headers = {
            "Authorization": "Basic " + base64.b64encode(auth).decode(),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {"grant_type": "refresh_token", "refresh_token": refresh_token}
        r = _request_with_retry(
            "POST", settings.SPOTIFY_TOKEN_URL, headers=headers, data=data, timeout=20
        )
        if r.status_code == 400 and _is_invalid_grant(r):
            raise SpotifyInvalidGrant("member refresh token rejected (invalid_grant)")
        r.raise_for_status()
        return r.json()

    def get_player_state(self, access_token: str) -> Optional[Dict[str, Any]]:
        """GET /me/player → playback state object, or None when nothing is playing
        (Spotify returns 204 No Content / an empty body)."""
        url = f"{settings.SPOTIFY_API_BASE}/me/player"
        r = _request_with_retry(
            "GET", url, headers={"Authorization": f"Bearer {access_token}"}, timeout=20
        )
        if r.status_code == 204:
            return None
        r.raise_for_status()
        if not r.content:
            return None
        return r.json()

    def get_recently_played(self, access_token: str, limit: int = 50) -> List[Dict[str, Any]]:
        """GET /me/player/recently-played → raw play items (most recent first).
        Spotify caps the rolling window at 50 items."""
        url = f"{settings.SPOTIFY_API_BASE}/me/player/recently-played"
        params = {"limit": min(max(int(limit), 1), 50)}
        r = _request_with_retry(
            "GET", url,
            headers={"Authorization": f"Bearer {access_token}"},
            params=params, timeout=20,
        )
        r.raise_for_status()
        return r.json().get("items") or []


spotify_member = SpotifyMemberClient()
