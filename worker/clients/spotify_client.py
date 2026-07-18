# Parallel implementation: myblog_music/app/clients/spotify_client.py
# Auth logic (_get_token/_headers) must stay in sync. See docs/decisions/ADR-0004.
import base64, logging, time
from typing import Optional, Dict, Any, List

import httpx

from worker.core.config import settings

# Shared transient-retry helper (429 Retry-After + 5xx/transport backoff). Lives in
# spotify_user_client so its existing tests keep their patch targets; this catalog
# client is the second consumer (FEAT-album-catalog-ingest Step 1 — without it a
# single 429 during batch sync propagated straight to the SQS DLQ).
from worker.clients.spotify_user_client import _request_with_retry

logger = logging.getLogger(__name__)

# Spotify batch limits
_MAX_ALBUMS  = 20
_MAX_ARTISTS = 50
_MAX_TRACKS  = 50


class SpotifyClient:
    def __init__(self):
        self._token: Optional[str] = None
        self._exp: float = 0.0

    # ---------- auth ----------
    def _get_token(self) -> str:
        now = time.time()
        if self._token and now < self._exp:
            return self._token

        auth = f"{settings.SPOTIFY_CLIENT_ID}:{settings.SPOTIFY_CLIENT_SECRET}".encode()
        headers = {
            "Authorization": "Basic " + base64.b64encode(auth).decode(),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {"grant_type": "client_credentials"}
        r = _request_with_retry(
            "POST", settings.SPOTIFY_TOKEN_URL, headers=headers, data=data, timeout=20
        )
        r.raise_for_status()
        payload = r.json()
        self._token = payload["access_token"]
        # refresh slightly early (90% of lifetime)
        self._exp = now + float(payload.get("expires_in", 3600)) * 0.9
        return self._token

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._get_token()}"}

    def _default_market(self, market: Optional[str]) -> Optional[str]:
        return market or getattr(settings, "SPOTIFY_DEFAULT_MARKET", None)

    def _default_locale(self) -> Optional[str]:
        # 설정에 SPOTIFY_LOCALE 이미 쓰고 있다면 그대로
        return getattr(settings, "SPOTIFY_LOCALE", None)
    
    def get_albums(self, ids: List[str], market: Optional[str] = None) -> List[Dict[str, Any]]:
        """GET /v1/albums?ids=... (<=20 per call)."""
        ids = [i for i in ids if i]
        if not ids:
            return []

        out: List[Dict[str, Any]] = []
        mkt = self._default_market(market)
        loc = self._default_locale()
        base_url = f"{settings.SPOTIFY_API_BASE}/albums"

        for i in range(0, len(ids), _MAX_ALBUMS):
            chunk = ids[i : i + _MAX_ALBUMS]
            params: Dict[str, Any] = {"ids": ",".join(chunk)}

            if mkt:
                params["market"] = mkt
            params["locale"] = "ko_KR"

            logger.debug("GET /albums chunk=%d size=%d", i // _MAX_ALBUMS + 1, len(chunk))

            r = _request_with_retry(
                "GET", base_url, headers=self._headers(), params=params, timeout=20
            )
            r.raise_for_status()

            albums = r.json().get("albums") or []
            logger.debug("Retrieved %d albums", len(albums))

            out.extend(albums)

        return out


    def get_artists(self, ids: list[str]) -> list[dict[str, Any]]:
        """GET /v1/artists?ids=... (<=50 per call)."""
        ids = [i for i in ids if i]
        if not ids:
            return []

        out: list[dict[str, Any]] = []
        loc = self._default_locale()
        base_url = f"{settings.SPOTIFY_API_BASE}/artists"

        for i in range(0, len(ids), _MAX_ARTISTS):
            chunk = ids[i : i + _MAX_ARTISTS]
            params = {"ids": ",".join(chunk)}

            params["locale"] = "ko_KR"

            logger.debug("GET /artists chunk=%d size=%d", i // _MAX_ARTISTS + 1, len(chunk))

            try:
                r = _request_with_retry(
                    "GET", base_url, headers=self._headers(), params=params, timeout=20
                )
                r.raise_for_status()
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if status not in (403, 404, 410):
                    raise
                logger.warning(
                    "Spotify batch GET /artists unavailable (status=%d); "
                    "falling back to %d individual requests",
                    status,
                    len(chunk),
                )
                for artist_id in chunk:
                    artist = self.get_artist(artist_id)
                    if artist:
                        out.append(artist)
                continue

            artists = r.json().get("artists") or []
            logger.debug("Retrieved %d artists", len(artists))

            out.extend(artists)

        return out

    def get_artist(self, artist_id: str) -> dict | None:
        """GET one artist, tolerating an unknown or unavailable catalog ID."""
        url = f"{settings.SPOTIFY_API_BASE}/artists/{artist_id}"
        try:
            r = _request_with_retry(
                "GET",
                url,
                headers=self._headers(),
                params={"locale": "ko_KR"},
                timeout=20,
            )
            r.raise_for_status()
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status not in (403, 404, 410):
                raise
            logger.warning(
                "Spotify artist unavailable: id=%s status=%d",
                artist_id,
                status,
            )
            return None
        return r.json()

    def get_artists_batch(self, ids: list[str]) -> list[dict[str, Any]]:
        """호환용 thin wrapper."""
        return self.get_artists(ids)

    def get_artist_albums(
        self,
        artist_id: str,
        include_groups: str = "album",
        market: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """GET /v1/artists/{id}/albums — one page, most-recent-first.

        Single page only: 50 items covers years of any artist's full-length output,
        and the album-ingest sweep (FEAT-album-catalog-ingest) only wants releases
        newer than INGEST_SINCE — deeper back-catalog stays on the reactive
        candidates path."""
        params: Dict[str, Any] = {"include_groups": include_groups, "limit": limit}
        mkt = self._default_market(market)
        if mkt:
            params["market"] = mkt

        r = _request_with_retry(
            "GET",
            f"{settings.SPOTIFY_API_BASE}/artists/{artist_id}/albums",
            headers=self._headers(),
            params=params,
            timeout=20,
        )
        r.raise_for_status()
        return r.json().get("items") or []

    def get_tracks(self, ids: list[str], market: Optional[str] = None) -> list[dict[str, Any]]:
        """GET /v1/tracks?ids=... (<=50 per call).

        Retrieves full track objects including external_ids (contains ISRC).
        Used for ISRC population (FEAT-lyrics-corpus Step 1b) — tracks obtained
        from album sync are SimplifiedTrackObjects without external_ids, so a
        separate fetch is needed for identity/version anchors."""
        ids = [i for i in ids if i]
        if not ids:
            return []

        out: list[dict[str, Any]] = []
        mkt = self._default_market(market)
        base_url = f"{settings.SPOTIFY_API_BASE}/tracks"

        for i in range(0, len(ids), _MAX_TRACKS):
            chunk = ids[i : i + _MAX_TRACKS]
            params: Dict[str, Any] = {"ids": ",".join(chunk)}

            if mkt:
                params["market"] = mkt

            logger.debug("GET /tracks chunk=%d size=%d", i // _MAX_TRACKS + 1, len(chunk))

            r = _request_with_retry(
                "GET", base_url, headers=self._headers(), params=params, timeout=20
            )
            r.raise_for_status()

            tracks = r.json().get("tracks") or []
            logger.debug("Retrieved %d tracks", len(tracks))

            out.extend(tracks)

        return out

spotify = SpotifyClient()
