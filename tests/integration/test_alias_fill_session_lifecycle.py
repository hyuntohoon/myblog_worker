"""BUG-18 Step 1 integration test — exercises the pre-check SELECT against a real
SQLAlchemy engine.

Why this exists ([[feedback-sa-session-lifecycle-mock-blind]]): the unit tests in
`tests/test_sync_service.py` mock `fetch_artist_mbid_and_aliases` entirely, so the
closure's SELECT never touches a real connection. BUG-17 PR #20 → #21 showed
that mock-only coverage misses connection-lifecycle bugs (session.commit
returning the cached handle to the pool, leaving stale state). This test makes
the closure SELECT run on a live engine across multiple row commits.

Guarded by an explicit `TEST_DB_URL` env var — when unset, the test is skipped at
collection ([[feedback-local-db-smoke-fallback]]) so the CI/local matrix without
a test DB doesn't fail.

Scope: covers the three RFC-stated outcomes in one EventBridge-equivalent run:
- row 1: 1st MB candidate already in DB → pre-check rejects → 2nd candidate adopted
- row 2: all candidates taken → sentinel ('not_found') written
- row 3: clean lookup → succeeds normally
followed by an assertion that the closure SELECT after row 2's commit didn't
explode on a stale connection (BUG-17 regression guard).
"""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from worker.service.sync_service import generate_and_save_aliases

_TEST_DB_URL = os.environ.get("TEST_DB_URL")

pytestmark = pytest.mark.skipif(
    not _TEST_DB_URL,
    reason="integration test requires TEST_DB_URL env var (Neon test branch)",
)


@pytest.fixture(scope="module")
def engine():
    eng = create_engine(_TEST_DB_URL, pool_pre_ping=True, future=True)
    # Guard against Neon test-branch schema drift: BUG-13 added the partial
    # UNIQUE on artists.musicbrainz_id in prod via shared_db migration, but
    # the Neon test branch may not have been re-applied. When the column is
    # absent the seed INSERT here would explode and fail CI with a misleading
    # message that hides the schema-drift root cause.
    with eng.connect() as conn:
        has_column = conn.execute(
            text("""
                SELECT 1 FROM information_schema.columns
                 WHERE table_schema = 'public'
                   AND table_name   = 'artists'
                   AND column_name  = 'musicbrainz_id'
                 LIMIT 1
            """)
        ).first()
    if has_column is None:
        eng.dispose()
        pytest.skip(
            "Neon test branch artists.musicbrainz_id 컬럼 부재 — shared_db 마이그레이션 "
            "재적용 필요. 통합 테스트 자체는 정상이며 prod schema 와는 무관."
        )
    yield eng
    eng.dispose()


@pytest.fixture(scope="function")
def session_factory(engine):
    """Return a real `with session_factory() as session` context that the
    service consumes. Each call yields a fresh session bound to the live
    engine (no shared connection — production parity).
    """
    return sessionmaker(bind=engine, autoflush=False, future=True)


@pytest.fixture(scope="function")
def seed_rows(engine):
    """Seed 3 NULL-MBID artists + 1 pre-existing artist whose MBID collides
    with the canned MB top-1 candidate. Cleaned up at the end of the test.

    Spotify IDs use a '#' prefix so they sort BEFORE every real base62
    Spotify ID ('0'–'9' / 'A'–'Z' / 'a'–'z') under C.UTF-8 collation
    ('#' = 0x23, '0' = 0x30). This guarantees the seeds land inside the
    ``WHERE musicbrainz_id IS NULL ORDER BY spotify_id LIMIT 10`` window
    even when the shared Neon test branch already contains hundreds of
    real NULL-mbid rows.

    Verified empirically on the Neon test branch with a BEGIN/ROLLBACK
    probe (230 pre-existing NULL rows as of 2026-06-05): all 3 NULL-mbid
    seeds appeared as rows 1-3 of the LIMIT 10 result.
    """
    pre_existing_mbid = "bug18-test-occupied-mbid-0001"
    # '#' prefix sorts before '0' in C.UTF-8 (byte 0x23 < 0x30), ensuring
    # these rows are always first in ORDER BY spotify_id LIMIT 10.
    sids = [
        "#bug18-test-sid-001",  # row 1: 1st candidate taken → adopt 2nd
        "#bug18-test-sid-002",  # row 2: all candidates taken → sentinel
        "#bug18-test-sid-003",  # row 3: clean lookup
        "#bug18-test-sid-pre",  # pre-existing holder of pre_existing_mbid
    ]
    with engine.connect() as conn:
        # Clean any stale leftover from prior failed runs first.
        conn.execute(
            text("DELETE FROM artists WHERE spotify_id = ANY(:sids)"),
            {"sids": sids},
        )
        conn.execute(text("""
            INSERT INTO artists (spotify_id, name, musicbrainz_id, aliases, genres)
            VALUES
              (:s1, 'BUG18 Stuck One',  NULL,  '[]'::jsonb, '["k-pop"]'::jsonb),
              (:s2, 'BUG18 Stuck Two',  NULL,  '[]'::jsonb, '["k-pop"]'::jsonb),
              (:s3, 'BUG18 Clean',      NULL,  '[]'::jsonb, '["pop"]'::jsonb),
              (:sp, 'Existing Holder',  :mbid, '[]'::jsonb, '["pop"]'::jsonb)
        """), {"s1": sids[0], "s2": sids[1], "s3": sids[2], "sp": sids[3],
               "mbid": pre_existing_mbid})
        conn.commit()

    # Precondition guard: assert the 3 NULL-mbid seeds actually landed in
    # the LIMIT 10 window.  If a collation change or new bulk-import pushes
    # them out, this fires with a clear message instead of a later
    # misleading "None == …" assertion failure.
    with engine.connect() as conn:
        window = conn.execute(
            text("""
                SELECT spotify_id
                  FROM artists
                 WHERE musicbrainz_id IS NULL
                 ORDER BY spotify_id
                 LIMIT 10
            """)
        ).fetchall()
    window_sids = {r[0] for r in window}
    null_seeds = sids[:3]  # the 4th has a real mbid, won't appear here
    missing = [s for s in null_seeds if s not in window_sids]
    if missing:
        pre_existing_count = len(window_sids - set(null_seeds))
        pytest.fail(
            f"seed rows pushed out of LIMIT 10 window by "
            f"{pre_existing_count} pre-existing NULL rows — "
            f"test isolation broken. Missing: {missing}. "
            f"Fix: use a spotify_id prefix that sorts before all real IDs "
            f"under C.UTF-8 collation."
        )

    yield {"sids": sids, "occupied_mbid": pre_existing_mbid}

    with engine.connect() as conn:
        conn.execute(
            text("DELETE FROM artists WHERE spotify_id = ANY(:sids)"),
            {"sids": sids},
        )
        conn.commit()


def _patched_fetch(*, occupied_mbid: str):
    """Build a fetch double whose return value depends on the artist name AND
    invokes is_mbid_taken so the closure's real SELECT fires.

    - 'BUG18 Stuck One': calls is_mbid_taken(occupied_mbid) → True → returns
      an unused mbid (simulating "2nd candidate adopted").
    - 'BUG18 Stuck Two': all candidates taken → returns sentinel.
    - 'BUG18 Clean': clean path, no pre-check call needed.
    """
    def side_effect(name, spotify_genres=None, is_mbid_taken=None):
        assert callable(is_mbid_taken), "service must forward is_mbid_taken"
        if name == "BUG18 Stuck One":
            assert is_mbid_taken(occupied_mbid) is True, \
                "pre-check SELECT must see the seeded existing row"
            return ("#bug18-test-row1-resolved", ["alias-row1"])
        if name == "BUG18 Stuck Two":
            assert is_mbid_taken(occupied_mbid) is True
            return ("not_found", [])
        if name == "BUG18 Clean":
            unused = "#bug18-test-row3-clean"
            assert is_mbid_taken(unused) is False, \
                "clean lookup must see a free MBID after row1/row2 commits " \
                "— if the SELECT throws on stale connection here, BUG-17 regressed"
            return (unused, ["alias-row3"])
        return ("not_found", [])
    return side_effect


@pytest.mark.integration
def test_alias_fill_pre_check_three_outcomes_real_engine(seed_rows, session_factory):
    occupied = seed_rows["occupied_mbid"]
    sids = seed_rows["sids"]

    with patch(
        "worker.service.sync_service.fetch_artist_mbid_and_aliases",
        side_effect=_patched_fetch(occupied_mbid=occupied),
    ):
        generate_and_save_aliases(session_factory)

    # Verify DB state via a fresh connection — read-after-write must see
    # the per-row commits.
    with session_factory() as readback:
        rows = readback.execute(
            text("""
                SELECT spotify_id, musicbrainz_id
                  FROM artists
                 WHERE spotify_id = ANY(:sids)
                 ORDER BY spotify_id
            """),
            {"sids": sids[:3]},
        ).fetchall()

    assert len(rows) == 3, "all 3 seeded NULL rows should now be NOT NULL"
    by_sid = {r[0]: r[1] for r in rows}
    assert by_sid[sids[0]] == "#bug18-test-row1-resolved", \
        "row 1 should have adopted the 2nd candidate (pre-check evicted 1st)"
    assert by_sid[sids[1]] == "not_found", \
        "row 2 should have sentinel ('not_found') — all candidates taken"
    assert by_sid[sids[2]] == "#bug18-test-row3-clean", \
        "row 3 should hold the clean lookup result"
