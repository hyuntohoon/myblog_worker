# worker/clients/lastfm_client.py
# FEAT-multi-user Phase 3a — Last.fm public-profile reads (user.getRecentTracks).
# Needs only an api_key + username (no OAuth / token custody), so this copies the
# app-token client shape (spotify_client), NOT the OAuth spotify_user_client. Reuses
# the shared transient-retry helper (429 Retry-After + 5xx/transport backoff).
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from worker.core.config import settings
from worker.clients.spotify_user_client import _request_with_retry

logger = logging.getLogger(__name__)

# Last.fm returns {"error": N, "message": ...} (HTTP 200 or 4xx) on API errors.
_ERR_USER_NOT_FOUND = 6


class LastfmError(Exception):
    """A Last.fm API error other than user-not-found. Transient → the caller keeps
    the integration connected and retries next tick."""


class LastfmUserNotFound(Exception):
    """error 6 — the username does not exist / is private. Terminal → status='error'."""


def _pick_image(images: Optional[List[Dict[str, str]]]) -> Optional[str]:
    """Largest available image url (Last.fm sizes small<medium<large<extralarge<mega)."""
    if not images:
        return None
    order = {"small": 0, "medium": 1, "large": 2, "extralarge": 3, "mega": 4}
    best: Optional[str] = None
    best_rank = -1
    for im in images:
        url = (im or {}).get("#text")
        if not url:
            continue
        rank = order.get((im or {}).get("size", ""), 0)
        if rank >= best_rank:
            best, best_rank = url, rank
    return best


def _row(t: Dict[str, Any]) -> Dict[str, Any]:
    artist = t.get("artist") or {}
    album = t.get("album") or {}
    return {
        "artist": artist.get("#text") or artist.get("name"),
        "track": t.get("name"),
        "album": (album.get("#text") or None),
        "artist_mbid": (artist.get("mbid") or None),
        "track_mbid": (t.get("mbid") or None),
        "album_mbid": (album.get("mbid") or None),
        "image": _pick_image(t.get("image")),
    }


class LastfmClient:
    def get_recent_tracks(
        self, username: str, from_uts: Optional[int] = None, limit: int = 200
    ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """Return (completed_scrobbles, now_playing_or_None). Completed scrobbles
        carry a `played_at_uts` (Unix seconds); the now-playing track (@attr
        nowplaying) has no timestamp. Incremental: `from_uts` fetches only plays
        scrobbled strictly after it. Raises LastfmUserNotFound / LastfmError."""
        params: Dict[str, Any] = {
            "method": "user.getrecenttracks",
            "user": username,
            "api_key": settings.LASTFM_API_KEY,
            "format": "json",
            "limit": limit,
        }
        if from_uts:
            params["from"] = from_uts
        r = _request_with_retry(
            "GET", settings.LASTFM_API_BASE, params=params, timeout=20
        )
        data: Optional[Dict[str, Any]] = None
        try:
            data = r.json()
        except Exception:
            data = None
        # Check the API-error body BEFORE raise_for_status (Last.fm returns 404 +
        # {"error":6} for an unknown user).
        if isinstance(data, dict) and "error" in data:
            code = data.get("error")
            if code == _ERR_USER_NOT_FOUND:
                raise LastfmUserNotFound(username)
            raise LastfmError(f"lastfm error {code}: {data.get('message')}")
        r.raise_for_status()

        tracks = ((data or {}).get("recenttracks") or {}).get("track")
        if not tracks:
            return [], None
        if isinstance(tracks, dict):  # a single scrobble comes back as an object
            tracks = [tracks]

        scrobbles: List[Dict[str, Any]] = []
        nowplaying: Optional[Dict[str, Any]] = None
        for t in tracks:
            attr = t.get("@attr") or {}
            row = _row(t)
            if str(attr.get("nowplaying", "")).lower() == "true":
                nowplaying = row
                continue
            uts = (t.get("date") or {}).get("uts")
            if not uts:
                continue
            try:
                row["played_at_uts"] = int(uts)
            except (TypeError, ValueError):
                continue
            scrobbles.append(row)
        return scrobbles, nowplaying


lastfm = LastfmClient()
