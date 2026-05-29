"""Unit tests for the MusicBrainz client cross-check (BUG-15)."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from worker.clients.musicbrainz_client import (
    MBID_NOT_FOUND,
    _aliases_have_hangul,
    _country_hint_from_genres,
    _has_hangul,
    _is_plausible_match,
    fetch_artist_mbid_and_aliases,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures"


# --- _country_hint_from_genres -------------------------------------------------

@pytest.mark.unit
@pytest.mark.parametrize("genres, expected", [
    (["k-pop"], "KR"),
    (["pop", "korean indie"], "KR"),
    (["j-pop", "anime"], "JP"),
    (["japanese rock"], "JP"),
    (["british rock"], "GB"),
    (["uk garage"], "GB"),
    (["american folk"], "US"),
    (["us indie"], "US"),
    ([], None),
    (None, None),
    (["jazz", "fusion"], None),
    # BUG-15 Step 3 — Korean hint widening (prod ko-KR genres)
    (["한국 랩"], "KR"),
    (["한국 록"], "KR"),
    (["케이팝"], "KR"),
    (["K-발라드"], "KR"),
    (["k-발라드"], "KR"),         # 소문자 매치도 통과 (lower() 패턴)
    (["한국 록", "케이팝"], "KR"),  # 다중 토큰 first-hit
    (["사운드트랙"], None),         # 한국어 토큰이지만 country hint 없음
    (["일본 vgm"], None),           # JP needle 미추가 라운드 — None 유지
])
def test_country_hint_from_genres(genres, expected):
    assert _country_hint_from_genres(genres) == expected


# --- _is_plausible_match -------------------------------------------------------

@pytest.mark.unit
@pytest.mark.parametrize("candidate, genres, expected", [
    # Spotify hints KR; MB says GB → reject.
    ({"country": "GB"}, ["k-pop"], False),
    # Spotify hints KR; MB also KR → accept.
    ({"country": "KR"}, ["k-pop"], True),
    # Spotify hints KR but MB candidate has no country → accept (no signal).
    ({}, ["k-pop"], True),
    # No recognised Spotify genre → accept regardless.
    ({"country": "GB"}, ["jazz"], True),
    # Both empty → accept.
    ({}, [], True),
    # spotify_genres=None → accept (back-compat path).
    ({"country": "GB"}, None, True),
])
def test_is_plausible_match(candidate, genres, expected):
    assert _is_plausible_match(candidate, genres) is expected


# --- fetch_artist_mbid_and_aliases --------------------------------------------

def _mb_candidate(id_: str, name: str, score: int, country: str | None = None) -> dict:
    c = {"id": id_, "name": name, "ext:score": str(score)}
    if country is not None:
        c["country"] = country
    return c


@pytest.mark.unit
@patch("worker.clients.musicbrainz_client.musicbrainzngs.get_artist_by_id")
@patch("worker.clients.musicbrainz_client.musicbrainzngs.search_artists")
def test_fetch_rejects_country_mismatch_picks_next(mock_search, mock_get):
    """Big Bang scenario: top GB candidate rejected, next KR accepted."""
    mock_search.return_value = {
        "artist-list": [
            _mb_candidate("gb-uuid", "Big Country", 100, "GB"),
            _mb_candidate("kr-uuid", "Big Bang", 95, "KR"),
        ]
    }
    mock_get.return_value = {
        "artist": {"alias-list": [{"alias": "빅뱅"}]}
    }

    mbid, aliases = fetch_artist_mbid_and_aliases(
        "Big Bang", spotify_genres=["k-pop"]
    )

    assert mbid == "kr-uuid"
    assert aliases == ["빅뱅"]
    mock_get.assert_called_once_with("kr-uuid", includes=["aliases"])
    mock_search.assert_called_once_with(artist="Big Bang", limit=10)


@pytest.mark.unit
@patch("worker.clients.musicbrainz_client.musicbrainzngs.get_artist_by_id")
@patch("worker.clients.musicbrainz_client.musicbrainzngs.search_artists")
def test_fetch_returns_not_found_when_all_candidates_rejected(mock_search, mock_get):
    mock_search.return_value = {
        "artist-list": [
            _mb_candidate("gb-uuid-1", "Big Country", 100, "GB"),
            _mb_candidate("gb-uuid-2", "Bigfoot", 92, "GB"),
        ]
    }

    mbid, aliases = fetch_artist_mbid_and_aliases(
        "Big Bang", spotify_genres=["k-pop"]
    )

    assert mbid == MBID_NOT_FOUND
    assert aliases == []
    mock_get.assert_not_called()


@pytest.mark.unit
@patch("worker.clients.musicbrainz_client.musicbrainzngs.get_artist_by_id")
@patch("worker.clients.musicbrainz_client.musicbrainzngs.search_artists")
def test_fetch_low_score_breaks_iteration(mock_search, mock_get):
    mock_search.return_value = {
        "artist-list": [
            _mb_candidate("low-uuid", "Some Band", 50),
        ]
    }

    mbid, _ = fetch_artist_mbid_and_aliases("Some Band", spotify_genres=["k-pop"])

    assert mbid == MBID_NOT_FOUND
    mock_get.assert_not_called()


@pytest.mark.unit
@patch("worker.clients.musicbrainz_client.musicbrainzngs.get_artist_by_id")
@patch("worker.clients.musicbrainz_client.musicbrainzngs.search_artists")
def test_fetch_no_spotify_genres_keeps_legacy_behaviour(mock_search, mock_get):
    """Back-compat: missing spotify_genres → cross-check passes → top accepted."""
    mock_search.return_value = {
        "artist-list": [
            _mb_candidate("any-uuid", "Radiohead", 100, "GB"),
        ]
    }
    mock_get.return_value = {"artist": {"alias-list": []}}

    mbid, _ = fetch_artist_mbid_and_aliases("Radiohead")  # no spotify_genres

    assert mbid == "any-uuid"


@pytest.mark.unit
@patch("worker.clients.musicbrainz_client.musicbrainzngs.search_artists")
def test_fetch_empty_artist_list_returns_not_found(mock_search):
    mock_search.return_value = {"artist-list": []}

    mbid, aliases = fetch_artist_mbid_and_aliases("Nobody", spotify_genres=["k-pop"])

    assert mbid == MBID_NOT_FOUND
    assert aliases == []


@pytest.mark.unit
@patch("worker.clients.musicbrainz_client.musicbrainzngs.search_artists")
def test_fetch_uses_real_mb_response_shape(mock_search):
    """Sanitized real-shape MB response: only GB candidates → all reject for K-pop."""
    with (FIXTURE_DIR / "mb_bigbang_search.json").open() as f:
        mock_search.return_value = json.load(f)

    mbid, _ = fetch_artist_mbid_and_aliases("Big Bang", spotify_genres=["k-pop"])

    assert mbid == MBID_NOT_FOUND


# --- BUG-18 pre-check callback -----------------------------------------------

@pytest.mark.unit
@patch("worker.clients.musicbrainz_client.musicbrainzngs.get_artist_by_id")
@patch("worker.clients.musicbrainz_client.musicbrainzngs.search_artists")
def test_fetch_is_mbid_taken_none_is_legacy_behaviour(mock_search, mock_get):
    """Back-compat: is_mbid_taken=None → no pre-check, current candidate accepted."""
    mock_search.return_value = {
        "artist-list": [_mb_candidate("kr-uuid", "Big Bang", 100, "KR")]
    }
    mock_get.return_value = {"artist": {"alias-list": [{"alias": "빅뱅"}]}}

    mbid, aliases = fetch_artist_mbid_and_aliases(
        "Big Bang", spotify_genres=["k-pop"]
    )

    assert mbid == "kr-uuid"
    assert aliases == ["빅뱅"]


@pytest.mark.unit
@patch("worker.clients.musicbrainz_client.musicbrainzngs.get_artist_by_id")
@patch("worker.clients.musicbrainz_client.musicbrainzngs.search_artists")
def test_fetch_is_mbid_taken_rejects_first_picks_next(mock_search, mock_get):
    """1st candidate's MBID already in DB → reject, try 2nd."""
    mock_search.return_value = {
        "artist-list": [
            _mb_candidate("taken-mbid", "Big Bang (orig)", 100, "KR"),
            _mb_candidate("free-mbid", "Big Bang (alt)", 95, "KR"),
        ]
    }
    mock_get.return_value = {"artist": {"alias-list": [{"alias": "빅뱅"}]}}

    seen = []

    def is_taken(mbid: str) -> bool:
        seen.append(mbid)
        return mbid == "taken-mbid"

    mbid, aliases = fetch_artist_mbid_and_aliases(
        "Big Bang", spotify_genres=["k-pop"], is_mbid_taken=is_taken,
    )

    assert mbid == "free-mbid"
    assert aliases == ["빅뱅"]
    assert seen == ["taken-mbid", "free-mbid"]
    # Detail fetched only for the accepted candidate, not for the rejected one.
    mock_get.assert_called_once_with("free-mbid", includes=["aliases"])


@pytest.mark.unit
@patch("worker.clients.musicbrainz_client.musicbrainzngs.get_artist_by_id")
@patch("worker.clients.musicbrainz_client.musicbrainzngs.search_artists")
def test_fetch_is_mbid_taken_rejects_all_returns_not_found(mock_search, mock_get):
    """All candidates' MBIDs taken → sentinel + no get_artist_by_id call."""
    mock_search.return_value = {
        "artist-list": [
            _mb_candidate("taken-1", "X", 100, "KR"),
            _mb_candidate("taken-2", "Y", 95, "KR"),
        ]
    }

    mbid, aliases = fetch_artist_mbid_and_aliases(
        "X", spotify_genres=["k-pop"], is_mbid_taken=lambda _: True,
    )

    assert mbid == MBID_NOT_FOUND
    assert aliases == []
    mock_get.assert_not_called()


# --- BUG-15 Step 4 hangul tiebreaker ------------------------------------------

@pytest.mark.unit
@pytest.mark.parametrize("s, expected", [
    ("빅뱅", True),
    ("최재성", True),
    ("Big Bang", False),
    ("Shizzy Sixx", False),
    ("ジェームス・ブラウン", False),  # 일문 only
    ("T윤미래", True),  # mixed
    ("", False),
    ("123", False),
])
def test_has_hangul(s, expected):
    assert _has_hangul(s) is expected


@pytest.mark.unit
@pytest.mark.parametrize("raw_aliases, expected", [
    ([{"alias": "빅뱅"}], True),
    ([{"alias": "Big Bang"}, {"alias": "빅뱅"}], True),
    ([{"alias": "Shizzy Sixx"}, {"alias": "Suicide Sixx"}], False),
    ([], False),
    ([{}], False),  # missing alias key
    ([{"alias": ""}], False),
])
def test_aliases_have_hangul(raw_aliases, expected):
    assert _aliases_have_hangul(raw_aliases) is expected


@pytest.mark.unit
@patch("worker.clients.musicbrainz_client.musicbrainzngs.get_artist_by_id")
@patch("worker.clients.musicbrainz_client.musicbrainzngs.search_artists")
def test_step4_kr_hint_country_null_no_hangul_rejected(mock_search, mock_get):
    """V.I 시나리오: country=NULL + alias 한글 없음 → tiebreaker reject → sentinel."""
    mock_search.return_value = {
        "artist-list": [_mb_candidate("null-uuid", "Shizzy Sixx", 100)]
    }
    mock_get.return_value = {
        "artist": {"alias-list": [{"alias": "Shizzy Sixx"}, {"alias": "Suicide Sixx"}]}
    }

    mbid, aliases = fetch_artist_mbid_and_aliases("V.I", spotify_genres=["케이팝"])

    assert mbid == MBID_NOT_FOUND
    assert aliases == []
    mock_get.assert_called_once_with("null-uuid", includes=["aliases"])


@pytest.mark.unit
@patch("worker.clients.musicbrainz_client.musicbrainzngs.get_artist_by_id")
@patch("worker.clients.musicbrainz_client.musicbrainzngs.search_artists")
def test_step4_kr_hint_country_null_with_hangul_accepted(mock_search, mock_get):
    """최엘비 시나리오: country=NULL 이지만 alias 한글 보유 → accept."""
    mock_search.return_value = {
        "artist-list": [_mb_candidate("null-uuid", "최엘비", 100)]
    }
    mock_get.return_value = {
        "artist": {"alias-list": [{"alias": "Lazy Bones"}, {"alias": "최재성"}]}
    }

    mbid, aliases = fetch_artist_mbid_and_aliases("최엘비", spotify_genres=["한국 랩"])

    assert mbid == "null-uuid"
    assert "최재성" in aliases


@pytest.mark.unit
@patch("worker.clients.musicbrainz_client.musicbrainzngs.get_artist_by_id")
@patch("worker.clients.musicbrainz_client.musicbrainzngs.search_artists")
def test_step4_kr_hint_country_explicit_kr_skips_tiebreaker(mock_search, mock_get):
    """country 가 KR 로 명시되면 Step 1 가 통과시키고 Step 4 는 비활성 (한글 없어도 accept)."""
    mock_search.return_value = {
        "artist-list": [_mb_candidate("kr-uuid", "Some Korean Artist", 100, "KR")]
    }
    mock_get.return_value = {
        "artist": {"alias-list": [{"alias": "Romanized Name"}]}  # 한글 없음
    }

    mbid, _ = fetch_artist_mbid_and_aliases(
        "Some Korean Artist", spotify_genres=["케이팝"]
    )

    assert mbid == "kr-uuid"


@pytest.mark.unit
@patch("worker.clients.musicbrainz_client.musicbrainzngs.get_artist_by_id")
@patch("worker.clients.musicbrainz_client.musicbrainzngs.search_artists")
def test_step4_no_hint_skips_tiebreaker(mock_search, mock_get):
    """hint=None (영어 장르) → tiebreaker 비활성 — country=NULL + 한글 없음도 accept."""
    mock_search.return_value = {
        "artist-list": [_mb_candidate("null-uuid", "Radiohead", 100)]
    }
    mock_get.return_value = {"artist": {"alias-list": [{"alias": "Radio Head"}]}}

    mbid, _ = fetch_artist_mbid_and_aliases("Radiohead", spotify_genres=["british rock"])
    # british → hint=GB. country=NULL → Step 1 pass-through. Step 4 는 KR 만 tiebreak.
    assert mbid == "null-uuid"


@pytest.mark.unit
@patch("worker.clients.musicbrainz_client.musicbrainzngs.get_artist_by_id")
@patch("worker.clients.musicbrainz_client.musicbrainzngs.search_artists")
def test_step4_falls_back_to_next_candidate_with_hangul(mock_search, mock_get):
    """첫 후보 country=NULL + 한글 없음 → reject. 두 번째 country=NULL + 한글 → accept."""
    mock_search.return_value = {
        "artist-list": [
            _mb_candidate("bad-uuid", "Shizzy Sixx", 100),
            _mb_candidate("good-uuid", "승리", 95),
        ]
    }
    mock_get.side_effect = [
        {"artist": {"alias-list": [{"alias": "Shizzy Sixx"}]}},
        {"artist": {"alias-list": [{"alias": "SEUNGRI"}, {"alias": "이승현"}]}},
    ]

    mbid, aliases = fetch_artist_mbid_and_aliases("V.I", spotify_genres=["케이팝"])

    assert mbid == "good-uuid"
    assert "이승현" in aliases
    assert mock_get.call_count == 2


@pytest.mark.unit
@patch("worker.clients.musicbrainz_client.musicbrainzngs.get_artist_by_id")
@patch("worker.clients.musicbrainz_client.musicbrainzngs.search_artists")
def test_step4_empty_alias_list_rejected_under_kr_null(mock_search, mock_get):
    """alias-list 키 자체가 없거나 빈 list → 한글 없음으로 간주 → reject (Open Q1: conservative)."""
    mock_search.return_value = {
        "artist-list": [_mb_candidate("null-uuid", "Some Name", 100)]
    }
    mock_get.return_value = {"artist": {}}  # alias-list 키 누락

    mbid, _ = fetch_artist_mbid_and_aliases("Some Name", spotify_genres=["한국 랩"])

    assert mbid == MBID_NOT_FOUND
