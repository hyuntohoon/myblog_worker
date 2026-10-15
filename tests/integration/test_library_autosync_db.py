"""FEAT-spotify-library-autosync Step 3a — the run claim, the saved-time stamp and the
research enqueue, on a REAL engine.

The unit tests in tests/test_library_sync.py route these statements through a
substring-matching fake, which cannot tell whether the claim's predicate, the
unnest() casts or the ON CONFLICT target actually work in Postgres
([[feedback-sa-session-lifecycle-mock-blind]]). Every case runs inside one transaction
that is rolled back, so the test database is left untouched.
"""
from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from worker.service.library_sync_service import (
    RESEARCH_PROMPT_VERSION,
    _claim_run,
    _enqueue_research,
    _stamp_added_at,
)

_TEST_DB_URL = os.environ.get("TEST_DB_URL")

pytestmark = pytest.mark.skipif(
    not _TEST_DB_URL,
    reason="integration test requires TEST_DB_URL env var (Postgres test database)",
)


@pytest.fixture(scope="module")
def factory():
    eng = create_engine(_TEST_DB_URL, pool_pre_ping=True, future=True)
    with eng.connect() as conn:
        has_runs = conn.execute(
            text("SELECT 1 FROM information_schema.tables WHERE table_name = 'spotify_library_sync_runs'")
        ).first()
    if not has_runs:
        eng.dispose()
        pytest.skip("V61 spotify_library_sync_runs absent from test database")
    yield sessionmaker(bind=eng, autoflush=False, autocommit=False, future=True)
    eng.dispose()


@pytest.fixture
def db(factory):
    with factory() as session:
        tx = session.begin()
        try:
            yield session
        finally:
            tx.rollback()


def _bucket(session, research_mode: str = "all"):
    user_id = session.execute(
        text("INSERT INTO users (id, handle, display_name) VALUES (gen_random_uuid(), :h, :h) RETURNING id"),
        {"h": f"itest-slib-{uuid.uuid4().hex[:8]}"},
    ).scalar()
    return session.execute(
        text(
            "INSERT INTO review_buckets (name, position, kind, user_id, research_mode) "
            "VALUES ('Spotify', 0, 'spotify_library', :u, :m) RETURNING id"
        ),
        {"u": user_id, "m": research_mode},
    ).scalar()


def _album(session):
    sid = f"itest{uuid.uuid4().hex[:17]}"
    return session.execute(
        text("INSERT INTO albums (title, spotify_id) VALUES (:t, :s) RETURNING id"),
        {"t": f"itest {sid}", "s": sid},
    ).scalar()


def _item(session, bucket_id, album_id, position=0, selected=False):
    session.execute(
        text(
            "INSERT INTO review_bucket_items (bucket_id, album_id, position, status, research_selected) "
            "VALUES (:b, :a, :p, 'candidate', :sel)"
        ),
        {"b": bucket_id, "a": album_id, "p": position, "sel": selected},
    )


def _side(session, album_id):
    session.execute(
        text(
            "INSERT INTO spotify_library_albums (album_id, spotify_id, source, state) "
            "SELECT id, spotify_id, 'preexisting', 'synced' FROM albums WHERE id = :a"
        ),
        {"a": album_id},
    )


def _run(session, bucket_id, **cols):
    sets = ", ".join(f"{k} = {v}" for k, v in cols.items())
    session.execute(text(f"UPDATE spotify_library_sync_runs SET {sets} WHERE bucket_id = :b"), {"b": bucket_id})


# ── run claim ─────────────────────────────────────────────────────────────────

def test_first_claim_creates_the_row_and_a_second_concurrent_claim_is_refused(db):
    b = _bucket(db)
    assert _claim_run(db, b) is True
    assert db.execute(text("SELECT started_at IS NOT NULL FROM spotify_library_sync_runs WHERE bucket_id=:b"), {"b": b}).scalar()
    # started, not finished → a second delivery must not run alongside it
    assert _claim_run(db, b) is False


def test_a_stale_unfinished_run_is_reclaimed(db):
    b = _bucket(db)
    assert _claim_run(db, b) is True
    _run(db, b, started_at="now() - interval '10 minutes'")
    assert _claim_run(db, b) is True


def test_a_request_serviced_moments_ago_is_skipped(db):
    """Duplicate SQS delivery: the request was already served by a pass that just ended."""
    b = _bucket(db)
    _claim_run(db, b)
    _run(db, b, requested_at="now() - interval '20 seconds'",
         started_at="now() - interval '15 seconds'", finished_at="now() - interval '5 seconds'")
    assert _claim_run(db, b) is False


def test_a_new_request_after_the_last_finish_runs_even_inside_the_window(db):
    b = _bucket(db)
    _claim_run(db, b)
    _run(db, b, started_at="now() - interval '15 seconds'",
         finished_at="now() - interval '10 seconds'", requested_at="now() - interval '1 second'")
    assert _claim_run(db, b) is True


def test_an_old_finish_does_not_block(db):
    b = _bucket(db)
    _claim_run(db, b)
    _run(db, b, requested_at="now() - interval '3 minutes'",
         started_at="now() - interval '2 minutes'", finished_at="now() - interval '1 minute'")
    assert _claim_run(db, b) is True


# ── saved time ────────────────────────────────────────────────────────────────

def test_saved_time_is_written_and_only_changed_rows_are_touched(db):
    a1, a2 = _album(db), _album(db)
    _side(db, a1)
    _side(db, a2)
    n = _stamp_added_at(db, [(a2, "2026-10-02T00:00:00Z"), (a1, "2026-10-01T09:30:00Z")])
    assert n == 2
    got = dict(db.execute(
        text("SELECT album_id, to_char(spotify_added_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') "
             "FROM spotify_library_albums WHERE album_id = ANY(:ids)"),
        {"ids": [a1, a2]},
    ).fetchall())
    assert got == {a1: "2026-10-01 09:30", a2: "2026-10-02 00:00"}
    # same values again → nothing written
    assert _stamp_added_at(db, [(a1, "2026-10-01T09:30:00Z"), (a2, "2026-10-02T00:00:00Z")]) == 0
    assert _stamp_added_at(db, []) == 0


# ── research enqueue ──────────────────────────────────────────────────────────

def _research_rows(db, album_ids):
    return db.execute(
        text("SELECT album_id, prompt_version, status FROM album_research WHERE album_id = ANY(:ids) ORDER BY album_id"),
        {"ids": album_ids},
    ).fetchall()


def test_all_mode_queues_each_noteless_album_once(db):
    b = _bucket(db, "all")
    a1, a2 = _album(db), _album(db)
    _item(db, b, a1, 0)
    _item(db, b, a2, 1)
    assert _enqueue_research(db, b) == 2
    assert {(r.prompt_version, r.status) for r in _research_rows(db, [a1, a2])} == {(RESEARCH_PROMPT_VERSION, "queued")}
    # a second pass asks for nothing
    assert _enqueue_research(db, b) == 0
    assert len(_research_rows(db, [a1, a2])) == 2


def test_an_album_with_a_note_of_another_version_is_never_queued_again(db):
    b = _bucket(db, "all")
    a = _album(db)
    _item(db, b, a)
    db.execute(
        text("INSERT INTO album_research (album_id, prompt_version, status) VALUES (:a, 'v-older', 'done')"),
        {"a": a},
    )
    assert _enqueue_research(db, b) == 0
    assert [r.prompt_version for r in _research_rows(db, [a])] == ["v-older"]


def test_selected_and_off_modes(db):
    b = _bucket(db, "selected")
    a_sel, a_not = _album(db), _album(db)
    _item(db, b, a_sel, 0, selected=True)
    _item(db, b, a_not, 1, selected=False)
    assert _enqueue_research(db, b) == 1
    assert [r.album_id for r in _research_rows(db, [a_sel, a_not])] == [a_sel]

    off = _bucket(db, "off")
    a_off = _album(db)
    _item(db, off, a_off)
    assert _enqueue_research(db, off) == 0
