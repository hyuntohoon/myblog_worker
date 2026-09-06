"""Real-engine tests for the Step A5 retention sweep and refresh.

WHY A REAL ENGINE. Both of this job's load-bearing behaviours ARE WHERE clauses:

  * the retention DELETE is `last_verified_at < now() - 30 days`, unconditional
    on `verify_state`;
  * the work SELECT is `verify_state = 'live' ORDER BY last_verified_at ASC`.

A mock returns what it was told regardless of either, so a mutant that filtered
the DELETE on `verify_state='live'` — which is the exact defect the Step-A1
review caught in `idx_tpr_stale` — would pass a mocked suite while letting every
'gone' row escape the 30-day policy forever.

Gated on TEST_DB_URL. Each test cleans up the rows it inserted.
"""
from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from worker.service.youtube_ref_refresh_service import YouTubeRefRefreshService

_TEST_DB_URL = os.environ.get("TEST_DB_URL")

pytestmark = pytest.mark.skipif(
    not _TEST_DB_URL,
    reason="integration test requires TEST_DB_URL env var (Postgres test database)",
)

_TAG = "a5-test-"


@pytest.fixture(scope="module")
def engine():
    eng = create_engine(_TEST_DB_URL, pool_pre_ping=True, future=True)
    with eng.connect() as conn:
        cols = dict(
            conn.execute(
                text(
                    "SELECT column_name, is_nullable FROM information_schema.columns "
                    "WHERE table_name = 'track_provider_refs'"
                )
            ).all()
        )
    # FAIL, not skip. A skip for a schema problem reports the same green as a
    # pass, and this repo's CI has a gate that exists precisely because of that
    # ("Enforce zero DB-bound skips"). A wrong pin is the failure worth being
    # told about, so say it directly rather than making the gate translate it.
    if "created_by_member_id" not in cols or cols.get("embeddable") != "NO":
        eng.dispose()
        raise AssertionError(
            "track_provider_refs is not at V56 in this test database — the canonical "
            "schema load or the shared-db pin in .github/workflows/deploy.yml predates "
            f"it. Columns seen: {sorted(cols)}"
        )
    yield eng
    eng.dispose()


@pytest.fixture
def session_factory(engine):
    return sessionmaker(bind=engine, autoflush=False, future=True)


@pytest.fixture
def seed(engine):
    """Insert a track AND its mapping row, and clean both up afterwards.

    IT CREATES ITS OWN TRACK rather than borrowing one from the ambient catalog.
    The first version did `SELECT id FROM tracks … LIMIT 1` and skipped when that
    returned nothing — and CI's fixture (`tests/integration/fixtures/catalog.sql`)
    seeds albums and genres but **no tracks at all**, so every test here skipped.
    The repo's "Enforce zero DB-bound skips" gate caught it, which is exactly
    what that gate is for: a suite that skips reports the same green as a suite
    that passes.

    A test that depends on rows it did not create is a test that reports
    "skipped" for an environment problem and "passed" for a code problem, and
    cannot tell you which it saw.
    """
    created = []
    album_id = uuid.uuid4()

    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO albums (id, title, spotify_id) "
                "VALUES (:a, 'A5 fixture album', :s)"
            ),
            {"a": album_id, "s": _TAG + uuid.uuid4().hex[:12]},
        )

    def _insert(*, age_days, verify_state="live", video_id=None, embeddable=True):
        vid = video_id or (_TAG + uuid.uuid4().hex[:8])
        track_id = uuid.uuid4()
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO tracks (id, album_id, title, spotify_id) "
                    "VALUES (:t, :a, 'A5 fixture track', :s)"
                ),
                {"t": track_id, "a": album_id, "s": _TAG + uuid.uuid4().hex[:12]},
            )
            conn.execute(
                text(
                    "INSERT INTO track_provider_refs "
                    "(track_id, provider, external_id, external_kind, source, embeddable, "
                    " verify_state, last_verified_at) "
                    "VALUES (:t, 'youtube', :v, 'video', 'user_confirmed', :e, :s, "
                    "        now() - make_interval(days => :d))"
                ),
                {"t": track_id, "v": vid, "e": embeddable, "s": verify_state, "d": age_days},
            )
        created.append(vid)
        return vid

    yield _insert

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM track_provider_refs WHERE external_id LIKE :p"),
            {"p": _TAG + "%"},
        )
        # tracks cascade from albums, so one DELETE clears both.
        conn.execute(
            text("DELETE FROM albums WHERE spotify_id LIKE :p"), {"p": _TAG + "%"}
        )


class _Client:
    """videos.list stand-in returning a payload satisfying every field read."""

    def __init__(self, present=()):
        self.present, self.batches = set(present), []

    def list_videos(self, ids):
        self.batches.append(list(ids))
        return {
            i: {
                "id": i,
                "snippet": {"title": "T", "channelTitle": "C"},
                "status": {"embeddable": True, "privacyStatus": "public", "madeForKids": False},
                "contentDetails": {"duration": "PT3M33S"},
            }
            for i in ids if i in self.present
        }


def _exists(engine, vid):
    with engine.connect() as conn:
        return conn.execute(
            text("SELECT 1 FROM track_provider_refs WHERE external_id = :v"), {"v": vid}
        ).first() is not None


def _state(engine, vid):
    with engine.connect() as conn:
        return conn.execute(
            text("SELECT verify_state FROM track_provider_refs WHERE external_id = :v"),
            {"v": vid},
        ).scalar_one()


class TestRetentionSweep:
    def test_a_row_past_thirty_days_is_deleted(self, engine, session_factory, seed):
        old = seed(age_days=31)
        YouTubeRefRefreshService(session_factory, _Client(), retention_days=30).expire_stale()
        assert not _exists(engine, old)

    def test_a_row_inside_the_window_survives(self, engine, session_factory, seed):
        """The control. Without it, a sweep that deleted EVERYTHING would pass."""
        fresh = seed(age_days=29)
        YouTubeRefRefreshService(session_factory, _Client(), retention_days=30).expire_stale()
        assert _exists(engine, fresh)

    @pytest.mark.parametrize("state", ["gone", "not_embeddable"])
    def test_the_sweep_is_unconditional_on_verify_state(
        self, engine, session_factory, seed, state
    ):
        """THE test this file exists for.

        A 'gone' row still STORES API-derived data — the videoId itself,
        privacy_status, made_for_kids — so III.E.4's 30-day clock applies to it in
        full. Filtering the DELETE on verify_state='live' would let exactly the
        rows the sweep exists for escape it forever. That defect shipped once
        already, in `idx_tpr_stale`, and was caught in review.
        """
        old = seed(age_days=31, verify_state=state, embeddable=(state != "not_embeddable"))
        YouTubeRefRefreshService(session_factory, _Client(), retention_days=30).expire_stale()
        assert not _exists(engine, old), f"a '{state}' row escaped the retention sweep"


class TestRefreshSelection:
    def test_only_live_rows_are_refreshed(self, engine, session_factory, seed):
        """A settled 'gone' row must not be polled forever.

        Refreshing it would move `last_verified_at` forward every day and keep a
        dead row alive indefinitely — compliant on paper, permanently useless,
        and spending an id slot daily. Left alone it ages out and the sweep
        reclaims it, which also gives the member up to 30 days of the useful
        410 "your video died" signal.
        """
        live = seed(age_days=5, verify_state="live")
        dead = seed(age_days=5, verify_state="gone")
        client = _Client(present=[live, dead])

        YouTubeRefRefreshService(session_factory, client, retention_days=30).run(limit=100)

        asked = {v for b in client.batches for v in b}
        assert live in asked
        assert dead not in asked, "a 'gone' row must not be re-verified"

    def test_the_oldest_rows_are_taken_first(self, engine, session_factory, seed):
        """`ORDER BY last_verified_at ASC` — the rows nearest expiry go first."""
        older = seed(age_days=20)
        newer = seed(age_days=1)
        client = _Client(present=[older, newer])

        YouTubeRefRefreshService(session_factory, client, retention_days=30).run(limit=1)

        asked = [v for b in client.batches for v in b if v.startswith(_TAG)]
        assert older in asked and newer not in asked


class TestWriteBack:
    def test_a_refreshed_row_moves_its_clock_and_stays_live(
        self, engine, session_factory, seed
    ):
        vid = seed(age_days=20)
        YouTubeRefRefreshService(
            session_factory, _Client(present=[vid]), retention_days=30
        ).run(limit=100)

        with engine.connect() as conn:
            age_days, state, dur = conn.execute(
                text(
                    "SELECT EXTRACT(day FROM now() - last_verified_at), verify_state, duration_sec "
                    "FROM track_provider_refs WHERE external_id = :v"
                ),
                {"v": vid},
            ).one()
        assert int(age_days) == 0, "last_verified_at must actually have moved"
        assert state == "live"
        assert dur == 213

    def test_an_id_absent_from_the_response_is_marked_gone_not_deleted(
        self, engine, session_factory, seed
    ):
        """Marked, not deleted: the member gets a 410 "pick another" signal, and
        the retention sweep reclaims the row if they never do."""
        vid = seed(age_days=5)
        YouTubeRefRefreshService(
            session_factory, _Client(present=[]), retention_days=30
        ).run(limit=100)

        assert _exists(engine, vid)
        assert _state(engine, vid) == "gone"
