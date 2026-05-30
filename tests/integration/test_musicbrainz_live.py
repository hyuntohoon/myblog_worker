"""BUG-14 Step 1 integration test — live MusicBrainz API.

Guarded by `MB_LIVE_TEST=1`. The MB client already sets a 1 req/s rate-limit
(`musicbrainzngs.set_rate_limit(1.0)`), so the three calls below take ~3s.

Why live: the whole point of Step 1 is whether MB's `query=` (default-field
union, including `alias:`) actually finds hangul artists where `artist=` returned
0 candidates (RFC's 5/5 sample). Mocking the response would re-test escape
plumbing already covered in the unit tests but not the gating hypothesis.

Cases (from RFC):
- '아이유' → mbid = IU (b9545342-1e6d-4dae-84ac-013374ad8d7c), aliases hangul present
- 'IU'   → same mbid (regression: latin form still hits via primary-name index)
- 'Big Bang' → some mbid returned (cross-check 없는 호출이라 정확성은 BUG-15 영역)
"""
from __future__ import annotations

import os

import pytest

from worker.clients.musicbrainz_client import (
    MBID_NOT_FOUND,
    _has_hangul,
    fetch_artist_mbid_and_aliases,
)

_MB_LIVE = os.environ.get("MB_LIVE_TEST")

pytestmark = pytest.mark.skipif(
    _MB_LIVE != "1",
    reason="live MB API test requires MB_LIVE_TEST=1 (1 req/s rate-limit, ~3s)",
)

IU_MBID = "b9545342-1e6d-4dae-84ac-013374ad8d7c"


@pytest.mark.integration
def test_live_hangul_name_finds_iu():
    mbid, aliases = fetch_artist_mbid_and_aliases("아이유")
    assert mbid == IU_MBID
    assert any(_has_hangul(a) for a in aliases), f"aliases missing hangul: {aliases}"


@pytest.mark.integration
def test_live_latin_name_still_finds_iu():
    """Regression — 영문 'IU' 도 동일 mbid 로 결정돼야 한다 (query= union 이
    artist primary name 인덱스도 포함하므로)."""
    mbid, _ = fetch_artist_mbid_and_aliases("IU")
    assert mbid == IU_MBID


@pytest.mark.integration
def test_live_big_bang_returns_candidate():
    """후보 모집 자체가 되는지만 확인. 정확 mbid 결정은 BUG-15 cross-check 영역."""
    mbid, _ = fetch_artist_mbid_and_aliases("Big Bang")
    assert mbid not in (None, MBID_NOT_FOUND)
