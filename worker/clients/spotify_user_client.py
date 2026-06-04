# Spotify *user-scoped* client (Authorization-Code refresh-token flow).
#
# Distinct from spotify_client.SpotifyClient, which uses the client-credentials
# flow and can only read the public catalog. The /me/player/* endpoints are tied
# to a specific user account, so they require a user grant — minted once out-of-band
# (scripts/spotify_bootstrap_token.py) and stored as a long-lived refresh token in
# Secrets Manager myblog/spotify (RFC Q17). This client exchanges that refresh token
# for short-lived access tokens on demand.
#
# Scopes: user-read-recently-played, user-read-currently-playing (read-only; write
# scopes deferred per D11).
from __future__ import annotations

import base64
import json
import logging
import time
from typing import Any, Dict, List, Optional

import httpx

from worker.core.config import settings

logger = logging.getLogger(__name__)


def _load_spotify_creds() -> Dict[str, str]:
    """Resolve client_id/secret/refresh_token from Secrets Manager myblog/spotify,
    falling back to settings (env) for local dev / tests. Never logs the values."""
    creds: Dict[str, str] = {
        "client_id": settings.SPOTIFY_CLIENT_ID,
        "client_secret": settings.SPOTIFY_CLIENT_SECRET,
        "refresh_token": settings.SPOTIFY_REFRESH_TOKEN,
    }
    arn = settings.SPOTIFY_SECRETS_ARN
    if arn:
        try:
            import boto3

            sm = boto3.client("secretsmanager", region_name=settings.AWS_DEFAULT_REGION)
            payload = json.loads(sm.get_secret_value(SecretId=arn)["SecretString"])
            for k in ("client_id", "client_secret", "refresh_token"):
                # accept both lowercase and SPOTIFY_-prefixed upper keys
                v = payload.get(k) or payload.get(f"SPOTIFY_{k.upper()}")
                if v:
                    creds[k] = v
        except Exception as e:  # pragma: no cover - network/IAM failure path
            # Don't mask a credential-availability problem as "not configured" —
            # let the EventBridge tick fail loudly and retry rather than no-op.
            logger.error("Failed to load Spotify user creds from %s: %s", arn, e)
            raise
    return creds


class SpotifyUserClient:
    """User-scoped Spotify client backed by a refresh token."""

    def __init__(self, creds: Optional[Dict[str, str]] = None) -> None:
        self._creds = creds
        self._token: Optional[str] = None
        self._exp: float = 0.0

    def _resolve_creds(self) -> Dict[str, str]:
        if self._creds is None:
            self._creds = _load_spotify_creds()
        return self._creds

    # ---------- auth ----------
    def _get_access_token(self) -> str:
        now = time.time()
        if self._token and now < self._exp:
            return self._token

        creds = self._resolve_creds()
        if not creds.get("refresh_token"):
            raise RuntimeError(
                "No Spotify refresh token configured (run scripts/spotify_bootstrap_token.py)"
            )

        auth = f"{creds['client_id']}:{creds['client_secret']}".encode()
        headers = {
            "Authorization": "Basic " + base64.b64encode(auth).decode(),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {
            "grant_type": "refresh_token",
            "refresh_token": creds["refresh_token"],
        }
        r = httpx.post(settings.SPOTIFY_TOKEN_URL, headers=headers, data=data, timeout=20)
        r.raise_for_status()
        payload = r.json()
        token: str = payload["access_token"]
        self._token = token
        # refresh slightly early (90% of lifetime)
        self._exp = now + float(payload.get("expires_in", 3600)) * 0.9
        return token

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._get_access_token()}"}

    # ---------- player reads ----------
    def get_recently_played(self, limit: int = 50) -> List[Dict[str, Any]]:
        """GET /me/player/recently-played → list of play items (most recent first).

        Each item: {"track": {"album": {...}, ...}, "played_at": ISO8601}.
        Spotify caps this at 50 items (rolling window) — no full history exists.
        """
        url = f"{settings.SPOTIFY_API_BASE}/me/player/recently-played"
        params = {"limit": min(max(int(limit), 1), 50)}
        r = httpx.get(url, headers=self._headers(), params=params, timeout=20)
        r.raise_for_status()
        return r.json().get("items") or []

    def get_currently_playing(self) -> Optional[Dict[str, Any]]:
        """GET /me/player/currently-playing → playback object, or None when nothing
        is playing (Spotify returns 204 No Content)."""
        url = f"{settings.SPOTIFY_API_BASE}/me/player/currently-playing"
        r = httpx.get(url, headers=self._headers(), timeout=20)
        if r.status_code == 204:
            return None
        r.raise_for_status()
        if not r.content:
            return None
        return r.json()


spotify_user = SpotifyUserClient()
