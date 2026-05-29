from __future__ import annotations

import logging
import re
from typing import Callable, Optional

import musicbrainzngs

logger = logging.getLogger(__name__)

# BUG-15 Step 4 — hangul tiebreaker. Step 1 의 country=NULL pass-through 사각지대
# (hint=KR + candidate.country 미정의 → permissive accept) 가 Step 2 reset 후 prod
# 표본에서 V.I/JA$/SUGA 류 sticky false-match 6+ 행을 남김. 정상 매치 ~30 행은
# 같은 경로지만 MB alias 에 한글 보유 — 후보 alias 의 한글 존재 여부가 결정적 신호.
_HANGUL_RE = re.compile(r"[가-힣]")

musicbrainzngs.set_useragent("myblog-music-review", "1.0", "zlxlgus123@gmail.com")
musicbrainzngs.set_rate_limit(limit_or_interval=1.0)

# Score threshold for considering a search result a confident match
_MIN_SCORE = 90

# Sentinel stored in musicbrainz_id when we searched but found no match.
# Prevents the worker from re-querying the same artist on every EventBridge run.
MBID_NOT_FOUND = "not_found"

# Spotify-genre substring → ISO 3166-1 alpha-2 country code.
# BUG-15 RFC Step 3 — 한국 origin 의 needle 을 추가. Spotify 가 ko-KR locale 로
# 보내는 토큰 ([[project-prod-artists-genres-korean]]) 이 영문 needle 에 안 잡혀
# BUG-18 pre-check 가 false-match 누적을 가속화한 부작용을 차단.
#
# prod 빈도 (2026-05-29 기준):
#   "한국 랩" 253, "K-발라드" 91, "케이팝" 49, "한국 록" 35 — 총 428행.
# 3 needle ("한국" / "케이팝" / "k-발라드") 로 위 4 토큰 모두 포괄.
#
# JP/CN/기타는 다음 라운드 (false-positive 위험 평가 후).
_COUNTRY_HINTS: tuple[tuple[str, str], ...] = (
    ("k-pop", "KR"),
    ("korean", "KR"),
    ("j-pop", "JP"),
    ("japanese", "JP"),
    ("british", "GB"),
    ("uk ", "GB"),
    ("american", "US"),
    ("us ", "US"),
    # BUG-15 Step 3 — Korean hint widening
    ("한국", "KR"),
    ("케이팝", "KR"),
    ("k-발라드", "KR"),
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


def _has_hangul(s: str) -> bool:
    return bool(_HANGUL_RE.search(s or ""))


def _aliases_have_hangul(raw_aliases: list[dict]) -> bool:
    return any(_has_hangul(a.get("alias", "")) for a in raw_aliases)


# BUG-15 Step 5 — surname-token gate. Step 4 mini-reset 잔존 케이스
# (Suh Young Eun → "Young Eun Lee") + prod 측정 표본 라벨링에서, country=KR
# 또는 country=NULL hangul-pass 후보를 통과해도 사람이 다른 false-match 가
# 30-40% 잔존. 신호: Spotify name 의 surname-token 이 candidate.name +
# aliases 어디에도 substring 으로 등장 X.
#
# 관통 원칙: 1순위 방어 대상은 "정상 매치의 과잉 거절". 단순 surname-substring
# (algorithm C) 단독은 한국 가수 영문 짧은 무대명 ↔ 한글 alias 패턴
# (B.I/CL/GooseBumps/PEEJAY 류) 을 false-positive 로 잡음 → 1-토큰 stage
# name 면제 분기 필수. 길이 캡 10 은 잠정 안전장치 (prod 표본 최댓값
# GooseBumps=10) — 라벨셋 ≥ 30 시점에 재검토.
def _spotify_name_matches_candidate(
    spotify_name: str,
    candidate_name: str,
    raw_aliases: list[dict],
) -> bool:
    tokens = [t for t in (spotify_name or "").split() if t]
    # 핵심 면제 신호 = 토큰 수 == 1 (= 본명 아님, 무대명). 길이 캡은 안전장치.
    if len(tokens) == 1 and len(spotify_name or "") <= 10:
        return True
    surname = tokens[0].lower() if tokens else ""
    if len(surname) < 2:
        return True  # 1글자 라티나이즈 토큰은 substring 신호 약함
    haystack = " ".join(
        [candidate_name or ""] + [a.get("alias", "") for a in raw_aliases]
    ).lower()
    return surname in haystack


def fetch_artist_mbid_and_aliases(
    name: str,
    spotify_genres: Optional[list[str]] = None,
    is_mbid_taken: Optional[Callable[[str], bool]] = None,
) -> tuple[Optional[str], list[str]]:
    """Search MusicBrainz for an artist by name and return (mbid, aliases).

    Iterates the top-10 candidates by score descending; rejects any that fail
    the Spotify-genre cross-check. Returns MBID_NOT_FOUND when no confident +
    plausible match exists so the caller can write a sentinel to prevent
    re-querying. Aliases may be an empty list on success.

    `is_mbid_taken` (BUG-18 pre-check): optional callable, queried with each
    candidate's MBID after the cross-check passes. If it returns True the
    candidate is rejected and iteration continues to the next one. Best-effort
    optimization to evict the same false-match MBID from re-appearing across
    cycles; safety net is the partial UNIQUE on artists.musicbrainz_id (BUG-13)
    + per-row IntegrityError catch in the caller (BUG-17). Default `None`
    preserves the pre-BUG-18 behavior (unit tests + back-compat).
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

            if is_mbid_taken is not None and is_mbid_taken(mbid):
                logger.info(
                    "MB pre-check reject: name=%s candidate=%s mbid=%s already in DB",
                    name,
                    candidate.get("name"),
                    mbid,
                )
                continue

            detail = musicbrainzngs.get_artist_by_id(mbid, includes=["aliases"])
            artist_data = detail.get("artist", {})
            raw_aliases = artist_data.get("alias-list", [])

            # BUG-15 Step 4 — hint=KR + candidate.country=NULL 으로 cross-check
            # 가 pass-through 한 경우에만 추가 게이트. alias 한글 ≥1 이어야 accept,
            # 아니면 다음 후보로. country 가 명시되면 Step 1 이 이미 통과/거절을
            # 결정했으니 tiebreaker 비활성.
            hint = _country_hint_from_genres(spotify_genres)
            if hint == "KR" and not candidate.get("country"):
                if not _aliases_have_hangul(raw_aliases):
                    logger.info(
                        "MB hangul-tiebreak reject: name=%s candidate=%s mbid=%s hint=%s",
                        name,
                        candidate.get("name"),
                        mbid,
                        hint,
                    )
                    continue

            # BUG-15 Step 5 — hint=KR 모든 후보에 surname-token gate.
            # country=KR 인 후보도 사람이 다른 false-match 가 30-40% 잔존하므로
            # Step 1/4 통과 후 추가 검사. _spotify_name_matches_candidate 내부의
            # 1-토큰 무대명 면제 분기로 정상 매치 (B.I/CL/GooseBumps 류) 보호.
            if hint == "KR" and not _spotify_name_matches_candidate(
                name, candidate.get("name", ""), raw_aliases
            ):
                logger.info(
                    "MB surname-gate reject: name=%s candidate=%s mbid=%s",
                    name,
                    candidate.get("name"),
                    mbid,
                )
                continue

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
