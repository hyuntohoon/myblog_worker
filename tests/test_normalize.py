# tests/test_normalize.py
"""normalize_release_date 단위 테스트.
DB 없이 순수 함수만 검증한다.
"""
import pytest
from worker.service.sync_service import normalize_release_date


@pytest.mark.unit
@pytest.mark.parametrize("input_date, expected", [
    # 정상 케이스
    ("2024-05-15", "2024-05-15"),
    ("1997-06-16", "1997-06-16"),

    # 연도만
    ("2024", "2024-01-01"),
    ("1999", "1999-01-01"),

    # 연-월만
    ("2024-05", "2024-05-01"),
    ("2000-10", "2000-10-01"),

    # None, 빈 문자열
    (None, None),
    ("", None),

    # 비정상 연도
    ("0000", None),
    ("-001", None),

    # 비정상 형식
    ("abcd", None),
    ("20-05-15", None),
    ("2024/05/15", None),
])
def test_normalize_release_date(input_date, expected):
    assert normalize_release_date(input_date) == expected


@pytest.mark.unit
def test_normalize_release_date_preserves_valid_full_date():
    result = normalize_release_date("2024-12-25")
    assert result == "2024-12-25"


@pytest.mark.unit
def test_normalize_release_date_year_only_adds_jan_first():
    result = normalize_release_date("2024")
    assert result.endswith("-01-01")
