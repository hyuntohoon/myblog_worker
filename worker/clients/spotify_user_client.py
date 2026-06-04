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
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

from worker.core.config import settings

logger = logging.getLogger(__name__)

# ---------- transient-failure retry (RFC: 3 tries, honour Retry-After on 429) ----------
# All Spotify player/token reads go through one helper so a transient 429/5xx or a
# dropped connection self-heals within a tick instead of aborting the sync. Only
# transient statuses retry; any non-retryable response (2xx/3xx/4xx — including the
# token 400 invalid_grant and a now-playing 403 missing-scope) is returned as-is for
# the caller's existing status handling.
RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
MAX_TRIES = 3
BASE_BACKOFF_SECONDS = 0.5
MAX_BACKOFF_SECONDS = 8.0


def _parse_retry_after(resp: "httpx.Response") -> Optional[float]:
    """Spotify sends Retry-After as integer seconds on a 429. Returns the delay, or
    None when absent/unparseable (e.g. the HTTP-date form) so the caller falls back
    to backoff."""
    raw = resp.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return max(0.0, float(int(raw)))
    except (TypeError, ValueError):
        return None


def _request_with_retry(
    method: str, url: str, *, max_tries: int = MAX_TRIES, **kwargs: Any
) -> "httpx.Response":
    """Issue an httpx request, retrying transient failures up to ``max_tries``.

    Retries on 429 (honouring Retry-After, capped at MAX_BACKOFF_SECONDS) and 5xx,
    plus httpx transport errors (timeout / connection reset) with capped exponential
    backoff. Any non-retryable response is returned immediately — the caller keeps
    its existing handling (raise_for_status, 204 → None, 400 invalid_grant). On the
    final attempt the last response is returned (so a still-5xx surfaces via the
    caller's raise_for_status) or its transport exception re-raised."""
    for attempt in range(max_tries):
        is_last = attempt + 1 >= max_tries
        try:
            resp = httpx.request(method, url, **kwargs)
        except httpx.HTTPError as exc:  # timeout / connection / transport
            if is_last:
                raise
            delay = min(BASE_BACKOFF_SECONDS * (2 ** attempt), MAX_BACKOFF_SECONDS)
            logger.warning(
                "Spotify %s %s transport error (%s); retry %d/%d after %.1fs",
                method, url.rsplit("/", 1)[-1], type(exc).__name__,
                attempt + 1, max_tries, delay,
            )
            time.sleep(delay)
            continue
        if resp.status_code in RETRYABLE_STATUSES and not is_last:
            delay = _parse_retry_after(resp) if resp.status_code == 429 else None
            if delay is None:
                delay = BASE_BACKOFF_SECONDS * (2 ** attempt)
            delay = min(delay, MAX_BACKOFF_SECONDS)
            logger.warning(
                "Spotify %s %s → %d; retry %d/%d after %.1fs",
                method, url.rsplit("/", 1)[-1], resp.status_code,
                attempt + 1, max_tries, delay,
            )
            time.sleep(delay)
            continue
        return resp
    raise RuntimeError("unreachable: max_tries must be >= 1")  # pragma: no cover


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


def _is_invalid_grant(resp: "httpx.Response") -> bool:
    """True iff a token-endpoint 400 carries Spotify's invalid_grant error — i.e. the
    refresh token was revoked/expired (re-auth needed), not a transient failure."""
    try:
        return resp.json().get("error") == "invalid_grant"
    except Exception:
        return False


def _persist_token_state(
    *, rotated_refresh_token: Optional[str] = None, needs_reauth: bool = False
) -> None:
    """Best-effort write-back of Spotify token state to Secrets Manager myblog/spotify (D30).

    On a successful refresh: record ``last_successful_refresh_at`` (so the 연동 tab can
    show when the token last worked), clear any ``needs_reauth`` marker, and persist a
    rotated ``refresh_token`` when Spotify returned one. On an ``invalid_grant``: set
    ``needs_reauth`` so the connection status reflects token *validity*, not presence.

    Non-fatal: a write failure is logged (never the token value) and swallowed so a
    transient Secrets Manager / IAM hiccup can't break listening sync. The caller has
    already updated its in-memory creds, so a warm Lambda keeps using the live token.
    Reads-then-writes (like the bootstrap script) to preserve unrelated keys.
    """
    arn = settings.SPOTIFY_SECRETS_ARN
    if not arn:
        return  # local/dev: token comes from env, nothing to write back
    try:
        import boto3

        sm = boto3.client("secretsmanager", region_name=settings.AWS_DEFAULT_REGION)
        payload = json.loads(sm.get_secret_value(SecretId=arn)["SecretString"])
        if needs_reauth:
            if payload.get("needs_reauth"):
                return  # already flagged — don't churn a new secret version each tick
            payload["needs_reauth"] = True
        else:
            payload.pop("needs_reauth", None)
            if rotated_refresh_token and rotated_refresh_token != payload.get("refresh_token"):
                payload["refresh_token"] = rotated_refresh_token
            payload["last_successful_refresh_at"] = datetime.now(timezone.utc).isoformat()
        sm.put_secret_value(SecretId=arn, SecretString=json.dumps(payload))
    except Exception as e:  # pragma: no cover - network/IAM failure path
        logger.error("Spotify token write-back failed (non-fatal): %s", e)


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
        r = _request_with_retry(
            "POST", settings.SPOTIFY_TOKEN_URL, headers=headers, data=data, timeout=20
        )

        # A 400 invalid_grant means the refresh token was revoked/expired — the only
        # authoritative "재인증 필요" signal (token validity, not staleness). Flag it in
        # the secret so the 연동 tab surfaces it, then fail this tick loudly. Other
        # statuses (5xx, rate-limit) are transient and must NOT trip re-auth.
        if r.status_code == 400 and _is_invalid_grant(r):
            _persist_token_state(needs_reauth=True)
            raise RuntimeError(
                "Spotify refresh token rejected (invalid_grant) — re-auth required "
                "(re-run scripts/spotify_bootstrap_token.py --write)"
            )
        r.raise_for_status()

        payload = r.json()
        token: str = payload["access_token"]
        # Spotify may rotate the refresh token on a refresh exchange. Update the
        # in-memory copy first (so a warm Lambda keeps working even if the write-back
        # fails) then persist it + last_successful_refresh_at + clear needs_reauth.
        rotated = payload.get("refresh_token")
        if rotated:
            creds["refresh_token"] = rotated
        _persist_token_state(rotated_refresh_token=rotated)

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
        r = _request_with_retry("GET", url, headers=self._headers(), params=params, timeout=20)
        r.raise_for_status()
        return r.json().get("items") or []

    def get_currently_playing(self) -> Optional[Dict[str, Any]]:
        """GET /me/player/currently-playing → playback object, or None when nothing
        is playing (Spotify returns 204 No Content)."""
        url = f"{settings.SPOTIFY_API_BASE}/me/player/currently-playing"
        r = _request_with_retry("GET", url, headers=self._headers(), timeout=20)
        if r.status_code == 204:
            return None
        r.raise_for_status()
        if not r.content:
            return None
        return r.json()


spotify_user = SpotifyUserClient()
