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

# Spotify-genre substring → ISO 3166-1 alpha-2 country code.
# Initial mapping starts narrow on purpose (BUG-15 RFC §Open Q1) — widen by
# inspecting prod cross-check reject logs first.
_COUNTRY_HINTS: tuple[tuple[str, str], ...] = (
    ("k-pop", "KR"),
    ("korean", "KR"),
    ("j-pop", "JP"),
    ("japanese", "JP"),
    ("british", "GB"),
    ("uk ", "GB"),
    ("american", "US"),
    ("us ", "US"),
)


def _country_hint_from_genres(genres: Optional[list[str]]) -> Optional[str]:
    """Return the first matched country code, or None if no hint fires."""
    for g in genres or []:
        gl = g.lower()
        for needle, code in _COUNTRY_HINTS:
            if needle in gl:
                return code
    return None


def _is_plausible_match(candidate: dict, spotify_genres: Optional[list[str]]) -> bool:
    """Cross-check the MB candidate against the Spotify-derived country hint.

    False only when BOTH the hint and the candidate country are present AND
    disagree. Missing data on either side → True (avoid false-negatives).
    """
    hint = _country_hint_from_genres(spotify_genres)
    if hint is None:
        return True
    candidate_country = candidate.get("country")
    if not candidate_country:
        return True
    return candidate_country == hint


def fetch_artist_mbid_and_aliases(
    name: str,
    spotify_genres: Optional[list[str]] = None,
) -> tuple[Optional[str], list[str]]:
    """Search MusicBrainz for an artist by name and return (mbid, aliases).

    Iterates the top-10 candidates by score descending; rejects any that fail
    the Spotify-genre cross-check. Returns MBID_NOT_FOUND when no confident +
    plausible match exists so the caller can write a sentinel to prevent
    re-querying. Aliases may be an empty list on success.
    """
    try:
        result = musicbrainzngs.search_artists(artist=name, limit=10)
        artist_list = result.get("artist-list", [])

        if not artist_list:
            logger.info("MB: no results for '%s'", name)
            return MBID_NOT_FOUND, []

        candidates = sorted(
            artist_list,
            key=lambda c: int(c.get("ext:score", 0)),
            reverse=True,
        )

        for candidate in candidates:
            score = int(candidate.get("ext:score", 0))
            if score < _MIN_SCORE:
                break

            if not _is_plausible_match(candidate, spotify_genres):
                logger.info(
                    "MB cross-check reject: name=%s candidate=%s candidate_country=%s spotify_genres=%s",
                    name,
                    candidate.get("name"),
                    candidate.get("country"),
                    spotify_genres,
                )
                continue

            mbid = candidate["id"]
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

        logger.info("MB: no plausible match for '%s' after cross-check", name)
        return MBID_NOT_FOUND, []

    except musicbrainzngs.ResponseError as exc:
        logger.warning("MB API error for '%s': %s", name, exc)
        return MBID_NOT_FOUND, []
    except Exception as exc:
        logger.warning("MB lookup failed for '%s': %s", name, exc)
        return MBID_NOT_FOUND, []
