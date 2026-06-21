# Spotify *user-scoped* client (Authorization-Code refresh-token flow).
#
# Distinct from spotify_client.SpotifyClient, which uses the client-credentials
# flow and can only read the public catalog. The /me/player/* endpoints are tied
# to a specific user account, so they require a user grant — minted once out-of-band
# (scripts/spotify_bootstrap_token.py) and stored as a long-lived refresh token in
# Secrets Manager myblog/spotify (RFC Q17). This client exchanges that refresh token
# for short-lived access tokens on demand.
#
# Scopes: user-read-recently-played, user-read-currently-playing (listening reads),
# user-library-read + user-library-modify (Spotify Library two-way sync,
# FEAT-spotify-library-sync — the only write scopes, per D11 follow-up).
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

# Spotify /me/albums* (GET contains, PUT, DELETE) cap at 20 ids per call. The
# saved-albums LIST endpoint (GET /me/albums) is a separate paginated read capped
# at 50 per page (LIBRARY_PAGE_LIMIT below).
LIBRARY_IDS_CHUNK = 20
LIBRARY_PAGE_LIMIT = 50


def _parse_added_at(value: str) -> datetime:
    """Parse a Spotify ``added_at`` ISO-8601 string (…Z) to a tz-aware datetime, for
    the incremental saved-tracks early-stop comparison against the cache's
    ``max(added_at)`` (itself tz-aware via timestamptz)."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class SpotifyScopeError(RuntimeError):
    """Raised when a /me/albums* call returns 403 missing-scope — distinct from a
    transient failure or an invalid_grant. The caller maps this to needs_attention /
    needs_reauth (the owner must re-run the bootstrap with the user-library-* scopes)
    rather than retrying or marking the album 'failed'."""


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

    # ---------- library reads/writes (FEAT-spotify-library-sync) ----------
    @staticmethod
    def _raise_for_scope(resp: "httpx.Response", op: str) -> None:
        """A 403 on a /me/albums* call means the stored token lacks the
        user-library-* scope (the listening-only bootstrap predates this feature).
        Surface it as SpotifyScopeError so the reconcile flips rows to
        needs_attention instead of silently retrying or marking 'failed'."""
        if resp.status_code == 403:
            raise SpotifyScopeError(
                f"Spotify {op} returned 403 (missing user-library-* scope) — "
                "re-run scripts/spotify_bootstrap_token.py --write to re-consent"
            )

    def get_saved_albums(self) -> List[Dict[str, Any]]:
        """GET /me/albums?limit=50&offset=… — paginate the owner's saved-albums
        Library, following total/next, and return the raw album objects (each item
        is {"added_at": …, "album": {"id": …, "name": …, …}} → we unwrap to the
        inner album object so callers read album.id / album.name directly)."""
        url = f"{settings.SPOTIFY_API_BASE}/me/albums"
        albums: List[Dict[str, Any]] = []
        offset = 0
        while True:
            params = {"limit": LIBRARY_PAGE_LIMIT, "offset": offset}
            r = _request_with_retry(
                "GET", url, headers=self._headers(), params=params, timeout=20
            )
            self._raise_for_scope(r, "GET /me/albums")
            r.raise_for_status()
            payload = r.json() or {}
            items = payload.get("items") or []
            for it in items:
                album = (it or {}).get("album")
                if album and album.get("id"):
                    albums.append(album)
            total = payload.get("total")
            offset += len(items)
            # Stop on an empty page (next=null / past the end) or once we've read
            # `total` items. `next` is the authoritative paginator; total guards a
            # never-null next.
            if not items or not payload.get("next"):
                break
            if isinstance(total, int) and offset >= total:
                break
        return albums

    def get_saved_tracks(
        self, since: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """GET /me/tracks?limit=50&offset=… — paginate the owner's saved (좋아요)
        tracks and return flattened, denormalized dicts (exactly the columns the
        spotify_saved_tracks cache stores, so the sync renders genre/artist without a
        second catalog read).

        Each Spotify item is ``{"added_at": ISO8601, "track": {"id", "name",
        "artists": [...], "album": {...}}}``; items arrive ``added_at``-descending.

        ``since`` (a tz-aware datetime, e.g. ``max(added_at)`` from the cache) makes
        this an INCREMENTAL read: paging stops as soon as an item's ``added_at`` is
        ``<=`` ``since`` (that already-cached item is excluded). ``since=None`` pages
        the entire library (full reconcile). Reuses the ``user-library-read`` scope
        already granted for saved albums — /me/tracks needs no new consent."""
        if since is not None and since.tzinfo is None:
            # DB max(added_at) is UTC; guard a naive value so the <= compare below
            # never raises aware-vs-naive TypeError on the first real incremental.
            since = since.replace(tzinfo=timezone.utc)
        url = f"{settings.SPOTIFY_API_BASE}/me/tracks"
        tracks: List[Dict[str, Any]] = []
        offset = 0
        reached_known = False
        while not reached_known:
            params = {"limit": LIBRARY_PAGE_LIMIT, "offset": offset}
            r = _request_with_retry(
                "GET", url, headers=self._headers(), params=params, timeout=20
            )
            self._raise_for_scope(r, "GET /me/tracks")
            r.raise_for_status()
            payload = r.json() or {}
            items = payload.get("items") or []
            for it in items:
                added_at = (it or {}).get("added_at")
                if since is not None and added_at and _parse_added_at(added_at) <= since:
                    # items are added_at-desc → everything from here on is already cached
                    reached_known = True
                    break
                track = (it or {}).get("track") or {}
                tid = track.get("id")
                if not tid:
                    continue  # local files / unavailable tracks have no stable id
                artists = track.get("artists") or []
                album = track.get("album") or {}
                tracks.append(
                    {
                        "spotify_track_id": tid,
                        "track_name": track.get("name") or "",
                        "artist_name": ", ".join(
                            a.get("name", "") for a in artists if a.get("name")
                        )
                        or None,
                        "album_name": album.get("name"),
                        "album_sid": album.get("id"),
                        "duration_ms": track.get("duration_ms"),
                        "added_at": added_at,
                    }
                )
            total = payload.get("total")
            offset += len(items)
            if not items or not payload.get("next"):
                break
            if isinstance(total, int) and offset >= total:
                break
        return tracks

    def check_saved_albums(self, spotify_ids: List[str]) -> Dict[str, bool]:
        """GET /me/albums/contains?ids= (chunked ≤ 20) → {spotify_id: is_saved}.
        Order-preserving zip of the chunk against Spotify's bool array."""
        ids = [s for s in (spotify_ids or []) if s]
        result: Dict[str, bool] = {}
        if not ids:
            return result
        url = f"{settings.SPOTIFY_API_BASE}/me/albums/contains"
        for i in range(0, len(ids), LIBRARY_IDS_CHUNK):
            chunk = ids[i : i + LIBRARY_IDS_CHUNK]
            r = _request_with_retry(
                "GET", url, headers=self._headers(),
                params={"ids": ",".join(chunk)}, timeout=20,
            )
            self._raise_for_scope(r, "GET /me/albums/contains")
            r.raise_for_status()
            flags = r.json() or []
            for sid, saved in zip(chunk, flags):
                result[sid] = bool(saved)
        return result

    def save_albums(self, spotify_ids: List[str]) -> None:
        """PUT /me/albums?ids= (chunked ≤ 20). Adds albums to the owner's Library."""
        ids = [s for s in (spotify_ids or []) if s]
        if not ids:
            return
        url = f"{settings.SPOTIFY_API_BASE}/me/albums"
        for i in range(0, len(ids), LIBRARY_IDS_CHUNK):
            chunk = ids[i : i + LIBRARY_IDS_CHUNK]
            r = _request_with_retry(
                "PUT", url, headers=self._headers(),
                params={"ids": ",".join(chunk)}, timeout=20,
            )
            self._raise_for_scope(r, "PUT /me/albums")
            r.raise_for_status()

    def remove_albums(self, spotify_ids: List[str]) -> None:
        """DELETE /me/albums?ids= (chunked ≤ 20). Removes albums from the Library.
        The reconcile must NEVER pass a pre-existing album here (req 5)."""
        ids = [s for s in (spotify_ids or []) if s]
        if not ids:
            return
        url = f"{settings.SPOTIFY_API_BASE}/me/albums"
        for i in range(0, len(ids), LIBRARY_IDS_CHUNK):
            chunk = ids[i : i + LIBRARY_IDS_CHUNK]
            r = _request_with_retry(
                "DELETE", url, headers=self._headers(),
                params={"ids": ",".join(chunk)}, timeout=20,
            )
            self._raise_for_scope(r, "DELETE /me/albums")
            r.raise_for_status()


spotify_user = SpotifyUserClient()
