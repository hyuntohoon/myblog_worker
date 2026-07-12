# worker/clients/itunes_client.py
# FEAT-release-calendar Step 4 — iTunes Search API lookups (no auth, no OAuth).
#
# Two hard-ID reads only (never fuzzy name search, per the Track B redesign):
#   - lookup?upc=<upc>              → the album's artistId (resolution pre-pass)
#   - lookup?id=<artistId>&entity=album → the artist's collections incl. pre-orders
#     (probed live 2026-07-11: future-dated pre-orders DO surface with a hard
#     collectionId; iTunes responses carry NO UPC, so no cross-source hard key).
#
# iTunes throttles around ~20 req/min → a mandatory inter-request sleep
# (ITUNES_THROTTLE_S, 3.5 s) is enforced HERE so every caller pays it. Reuses the
# shared transient-retry helper (429 Retry-After + 5xx/transport backoff).
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from worker.core.config import settings
from worker.clients.spotify_user_client import _request_with_retry

logger = logging.getLogger(__name__)


class ItunesClient:
    def __init__(self) -> None:
        self._last_request_ts: float = 0.0

    def _throttled_get(self, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        wait = settings.ITUNES_THROTTLE_S - (time.monotonic() - self._last_request_ts)
        if wait > 0:
            time.sleep(wait)
        self._last_request_ts = time.monotonic()
        r = _request_with_retry("GET", settings.ITUNES_LOOKUP_URL, params=params, timeout=20)
        r.raise_for_status()
        return r.json()

    def lookup_artist_by_upc(self, upc: str) -> Optional[str]:
        """Resolve a catalog UPC to an iTunes artistId (the hard-ID chain).
        Returns None when iTunes has no entry for the UPC."""
        payload = self._throttled_get({"upc": upc, "entity": "album"}) or {}
        for item in payload.get("results", []):
            artist_id = item.get("artistId")
            if artist_id:
                return str(artist_id)
        return None

    def get_artist_albums(self, artist_id: str) -> List[Dict[str, Any]]:
        """All collections for an artistId (limit 200 = iTunes lookup max page).
        Pre-orders appear as normal future-dated collections."""
        payload = self._throttled_get(
            {"id": artist_id, "entity": "album", "limit": 200}
        ) or {}
        return [
            item
            for item in payload.get("results", [])
            if item.get("wrapperType") == "collection" and item.get("collectionId")
        ]


itunes = ItunesClient()
