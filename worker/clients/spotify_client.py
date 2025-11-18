import base64, time, httpx
from typing import Optional, Dict, Any, List
from worker.core.config import settings

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
        r = httpx.post(settings.SPOTIFY_TOKEN_URL, headers=headers, data=data, timeout=20)
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

    def _apply_locale(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        settings.SPOTIFY_LOCALE 이 설정되어 있으면
        Spotify 요청에 locale 쿼리 파라미터를 붙여준다.
        예: 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
        """
        locale = getattr(settings, "SPOTIFY_LOCALE", None)
        if locale:
            params.setdefault("locale", locale)
        return params
    
    def get_albums(self, ids: List[str], market: Optional[str] = None) -> List[Dict[str, Any]]:
        """GET /v1/albums?ids=... (<=20 per call). Returns list of AlbumObject."""
        ids = [i for i in ids if i]
        if not ids:
            return []

        out: List[Dict[str, Any]] = []
        mkt = self._default_market(market)
        base_url = f"{settings.SPOTIFY_API_BASE}/albums"

        for i in range(0, len(ids), _MAX_ALBUMS):
            chunk = ids[i : i + _MAX_ALBUMS]
            params: Dict[str, Any] = {"ids": ",".join(chunk)}
            if mkt:
                params["market"] = mkt

            params = self._apply_locale(params)

            # 🔎 요청 URL 프린트 (토큰 노출 없음)
            full_url = str(httpx.URL(base_url, params=params))
            print(f"[HTTP] GET {full_url}  (chunk={i//_MAX_ALBUMS+1}, size={len(chunk)})")

            r = httpx.get(base_url, headers=self._headers(), params=params, timeout=20)
            r.raise_for_status()
            out.extend(r.json().get("albums") or [])
        return out

    def get_artists(self, ids: list[str]) -> list[dict[str, Any]]:
        """GET /v1/artists?ids=... (<=50 per call)."""
        ids = [i for i in ids if i]
        if not ids:
            return []

        out: list[dict[str, Any]] = []
        for i in range(0, len(ids), _MAX_ARTISTS):
            chunk = ids[i : i + _MAX_ARTISTS]
            params = {"ids": ",".join(chunk)}
            params = self._apply_locale(params)
            r = httpx.get(
                f"{settings.SPOTIFY_API_BASE}/artists",
                headers=self._headers(),
                params=params,
                timeout=20,
            )
            r.raise_for_status()
            out.extend(r.json().get("artists") or [])
        return out

    def get_artists_batch(self, ids: list[str]) -> list[dict[str, Any]]:
        """호환용 thin wrapper."""
        return self.get_artists(ids)

spotify = SpotifyClient()