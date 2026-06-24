"""FEAT-pocket-buckit Step 6 integration test — proves _insert_bucket_item dedups
correctly against a REAL engine with the V30 per-kind PARTIAL unique index.

Why this exists ([[feedback-sa-session-lifecycle-mock-blind]]): the unit tests use a
_FakeSession that emulates the dedup in Python, so they cannot catch that a bare
``ON CONFLICT (bucket_id, album_id)`` raises against V30's PARTIAL album index
("no unique or exclusion constraint matching the ON CONFLICT specification"). The fix
replaced that with a schema-agnostic NOT-EXISTS guard; this test runs it on live Postgres.

Guarded by TEST_DB_URL; skipped when unset. Also skipped if the V30 partial index
``uq_review_bucket_items_album`` isn't on the test branch yet (schema-drift guard,
mirroring test_saved_tracks_sync_db.py — apply V30 to the test branch first).
"""
from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from worker.service.library_sync_service import _insert_bucket_item

_TEST_DB_URL = os.environ.get("TEST_DB_URL")

pytestmark = pytest.mark.skipif(
    not _TEST_DB_URL,
    reason="integration test requires TEST_DB_URL env var (Neon test branch)",
)


@pytest.fixture(scope="module")
def factory():
    eng = create_engine(_TEST_DB_URL, pool_pre_ping=True, future=True)
    with eng.connect() as conn:
        has_partial = conn.execute(
            text("SELECT 1 FROM pg_indexes WHERE indexname = 'uq_review_bucket_items_album'")
        ).first()
        has_albums = conn.execute(text("SELECT 1 FROM albums LIMIT 1")).first()
    if not has_partial:
        eng.dispose()
        pytest.skip(
            "uq_review_bucket_items_album partial index absent — apply V30 to the test branch first"
        )
    if not has_albums:
        eng.dispose()
        pytest.skip("no albums on the test branch to attach a bucket item to")
    yield sessionmaker(bind=eng, autoflush=False, autocommit=False, future=True)
    eng.dispose()


def test_insert_bucket_item_idempotent_against_partial_index(factory):
    """A re-PULL of the same album is a no-op and must NOT raise. The OLD bare
    ``ON CONFLICT (bucket_id, album_id)`` would raise against the partial album index;
    the NOT-EXISTS guard inserts once and skips the duplicate. All work is rolled back."""
    Session = factory
    with Session() as session:
        tx = session.begin()
        try:
            album_id = session.execute(text("SELECT id FROM albums LIMIT 1")).scalar()
            bucket_id = session.execute(
                text(
                    "INSERT INTO review_buckets (name, position, kind) "
                    "VALUES (:n, 0, 'review') RETURNING id"
                ),
                {"n": f"itest-onconflict-{uuid.uuid4().hex[:8]}"},
            ).scalar()

            _insert_bucket_item(session, bucket_id, album_id, 0)
            _insert_bucket_item(session, bucket_id, album_id, 1)  # duplicate → no-op, no raise

            count = session.execute(
                text(
                    "SELECT count(*) FROM review_bucket_items "
                    "WHERE bucket_id = :b AND item_type = 'album' AND album_id = :a"
                ),
                {"b": bucket_id, "a": album_id},
            ).scalar()
            assert count == 1, f"expected idempotent single insert, got {count}"
        finally:
            tx.rollback()  # leave the test branch untouched
