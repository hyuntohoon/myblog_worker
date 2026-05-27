from __future__ import annotations

import logging
from typing import Optional

import musicbrainzngs

logger = logging.getLogger(__name__)

musicbrainzngs.set_useragent("myblog-music-review", "1.0", "zlxlgus123@gmail.com")
musicbrainzngs.set_rate_limit(limit_or_interval=1.0)

# Score threshold for considering a search result a confident match
_MIN_SCORE = 90

# Sentinel stored in musicbrainz_id when we searched but found no match.
# Prevents the worker from re-querying the same artist on every EventBridge run.
MBID_NOT_FOUND = "not_found"


def fetch_artist_mbid_and_aliases(name: str) -> tuple[Optional[str], list[str]]:
    """Search MusicBrainz for an artist by name and return (mbid, aliases).

    Returns (MBID_NOT_FOUND, []) when no confident match exists so the caller
    can write a sentinel to prevent re-querying.
    Returns (mbid, aliases) on success — aliases may be an empty list.
    """
    try:
        result = musicbrainzngs.search_artists(artist=name, limit=1)
        artist_list = result.get("artist-list", [])

        if not artist_list:
            logger.info("MB: no results for '%s'", name)
            return MBID_NOT_FOUND, []

        best = artist_list[0]
        score = int(best.get("ext:score", 0))
        if score < _MIN_SCORE:
            logger.info("MB: low-confidence match for '%s' (score=%d)", name, score)
            return MBID_NOT_FOUND, []

        mbid = best["id"]

        detail = musicbrainzngs.get_artist_by_id(mbid, includes=["aliases"])
        artist_data = detail.get("artist", {})

        raw_aliases = artist_data.get("alias-list", [])
        aliases = [
            a["alias"].strip()
            for a in raw_aliases
            if a.get("alias", "").strip() and a["alias"].strip().lower() != name.lower()
        ]

        logger.info("MB: '%s' → mbid=%s aliases=%s", name, mbid, aliases)
        return mbid, aliases

    except musicbrainzngs.ResponseError as exc:
        logger.warning("MB API error for '%s': %s", name, exc)
        return MBID_NOT_FOUND, []
    except Exception as exc:
        logger.warning("MB lookup failed for '%s': %s", name, exc)
        return MBID_NOT_FOUND, []
