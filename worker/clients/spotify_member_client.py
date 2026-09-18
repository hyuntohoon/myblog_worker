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
    FOLLOW_PAGE_LIMIT,
    LIBRARY_PAGE_LIMIT,
    _is_invalid_grant,
    _load_spotify_creds,
    _request_with_retry,
)

logger = logging.getLogger(__name__)


class SpotifyInvalidGrant(RuntimeError):
    """The member's refresh token was revoked/expired (token-endpoint 400
    error=invalid_grant) — the ONLY signal that maps to status='reauth'. Transient
    failures (5xx / 429 / network) must never raise this."""


class SpotifyMemberScopeError(RuntimeError):
    """The member's grant lacks `user-library-read` (403 on /me/albums).

    Distinct from SpotifyInvalidGrant: the refresh token is still valid, so the
    integration must NOT be flipped to status='reauth' (that would break the
    member's player and recent reads over a library-only gap). The front already
    renders a library-scope reconsent prompt from the stored scope string
    (`spotifyGrantLacksLibraryScopes`), so the caller just skips the saved-album
    origin for this member and leaves everything else running."""


class SpotifyMemberFollowScopeError(RuntimeError):
    """The member's grant lacks `user-follow-read` (403 on /me/following).

    Deliberately a third class rather than a reuse of SpotifyMemberScopeError.
    Both mean "the token is fine, one grant is missing", but they gate different
    origins, and every member who connected before Step 5 has exactly this gap:
    `user-follow-read` was not in the authorize URL, so their stored grant cannot
    read follows until they re-consent. Collapsing the two would make the front's
    library-reconsent prompt fire for a follow-only gap, and would let a follow gap
    silently skip the saved-album origin as well.
    """


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

    def get_saved_albums(self, access_token: str) -> List[Dict[str, Any]]:
        """GET /me/albums?limit=50&offset=… — paginate the MEMBER's saved-albums
        library and return the unwrapped album objects (callers read album["id"]).

        Same pagination contract as the owner client's get_saved_albums (`next` is
        authoritative, `total` guards a never-null `next`), but stateless: the access
        token is passed in per call because members share no cached token. A 403 is a
        missing `user-library-read` grant, not a revoked token — see
        SpotifyMemberScopeError."""
        url = f"{settings.SPOTIFY_API_BASE}/me/albums"
        albums: List[Dict[str, Any]] = []
        offset = 0
        while True:
            r = _request_with_retry(
                "GET", url,
                headers={"Authorization": f"Bearer {access_token}"},
                params={"limit": LIBRARY_PAGE_LIMIT, "offset": offset},
                timeout=20,
            )
            if r.status_code == 403:
                raise SpotifyMemberScopeError(
                    "Spotify GET /me/albums returned 403 (grant lacks user-library-read)"
                )
            r.raise_for_status()
            payload = r.json() or {}
            items = payload.get("items") or []
            for it in items:
                album = (it or {}).get("album")
                if album and album.get("id"):
                    albums.append(album)
            total = payload.get("total")
            offset += len(items)
            if not items or not payload.get("next"):
                break
            if isinstance(total, int) and offset >= total:
                break
        return albums

    def get_followed_artists(self, access_token: str) -> List[Dict[str, Any]]:
        """GET /me/following?type=artist — the MEMBER's followed artists, cursor-paged.

        Cursor pagination, not offset: Spotify returns `artists.cursors.after` and the
        page is requested with `after=<last artist id>`. Same contract as the owner
        client's twin (`spotify_user_client.get_followed_artists`) but stateless — the
        access token is passed per call because members share no cached token.

        A 403 is a missing `user-follow-read` grant, not a revoked token: raise
        SpotifyMemberFollowScopeError so the caller skips the follow origin and leaves
        the member's player, library and recent reads running.
        """
        url = f"{settings.SPOTIFY_API_BASE}/me/following"
        artists: List[Dict[str, Any]] = []
        after: Optional[str] = None
        while True:
            params: Dict[str, Any] = {"type": "artist", "limit": FOLLOW_PAGE_LIMIT}
            if after:
                params["after"] = after
            r = _request_with_retry(
                "GET", url,
                headers={"Authorization": f"Bearer {access_token}"},
                params=params, timeout=20,
            )
            if r.status_code == 403:
                raise SpotifyMemberFollowScopeError(
                    "Spotify GET /me/following returned 403 (grant lacks user-follow-read)"
                )
            r.raise_for_status()
            block = (r.json() or {}).get("artists") or {}
            items = block.get("items") or []
            artists.extend(it for it in items if it and it.get("id"))
            after = (block.get("cursors") or {}).get("after")
            # `after` is the authoritative paginator. An empty page or a null cursor
            # ends it; there is no `total`-based guard because /me/following's total
            # is not a page bound.
            if not items or not after:
                break
        return artists

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
