"""Targeted source collection against real Postgres (FEAT-lyrics-listening-experience Step 3).

Real engine, not mocks. Everything under test here is a property of SQL the V57 store
executes — the ``FOR UPDATE`` guards, the ``source_revision`` supersession check, the
live-demand EXISTS predicates, and the two-timestamp backoff encoding. A mock session
observes none of it ([[feedback-sa-session-lifecycle-mock-blind]]).

The suite is built around one control that justifies the whole job existing: an album whose
tracks were **never evaluated** is invisible to the existing album-scoped expedite (it
selects ``FROM track_lyrics``), so without this job that album's demand waits forever. The
first test asserts both halves — the expedite finds nothing, this job covers all of it
([[feedback-measure-with-a-control]]).

Guarded by TEST_DB_URL; skipped when unset. CI loads the pinned canonical schema, so these
run in the deploy gate.
"""
from __future__ import annotations

import os
import uuid
from datetime import timedelta

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from myblog_shared_db.lyrics_demand import LyricsDemandStore

from worker.clients.lrclib_client import LrclibTransientError
from worker.core.config import settings
from worker.service.lyrics_demand_source_service import LyricsDemandSourceService
from worker.service.lyrics_matcher import Candidate
from worker.service.lyrics_reassessment_service import LyricsReassessmentService

_TEST_DB_URL = os.environ.get("TEST_DB_URL")

pytestmark = pytest.mark.skipif(
    not _TEST_DB_URL,
    reason="integration test requires TEST_DB_URL env var (Postgres test database)",
)

_PREFIX = "lyr_demand_"
_ARTIST = "Demand Primary"

# Synthetic placeholder bodies — never real lyrics. The matcher only needs "non-empty".
_BODY = "placeholder line one\nplaceholder line two"

BASE = settings.LYRICS_DEMAND_SOURCE_RETRY_BASE_SEC


class _FakeClient:
    """Canned LRCLIB responses keyed by track title, plus a call log.

    Keyed by title rather than a single canned answer so ONE batch can carry every OQ5
    outcome at once — which is the realistic shape (an album mixes matchable tracks,
    interludes and tracks the provider has never heard of) and is the only way to prove the
    outcomes are classified independently rather than batch-wide.
    """

    def __init__(self, by_title: dict, raise_exc=None):
        self.by_title = by_title
        self.raise_exc = raise_exc
        self.seen: list[str] = []
        self.on_call = None  # probe hook, invoked while the "external call" is in flight

    def search_candidates(self, title, artist, **kw):
        self.seen.append(title)
        if self.on_call is not None:
            self.on_call()
        if self.raise_exc is not None:
            raise self.raise_exc
        return list(self.by_title.get(title, []))

    def close(self):
        pass


def _cand(title, *, instrumental=False, body=_BODY, duration=200.0):
    return Candidate(
        id=uuid.uuid4().int % 10**6, title=title, artist=_ARTIST, album="Demand Album",
        duration_sec=duration, instrumental=instrumental,
        plain_lyrics=None if instrumental else body, synced_lyrics=None,
    )


@pytest.fixture(scope="module")
def factory():
    eng = create_engine(_TEST_DB_URL, pool_pre_ping=True, future=True)
    yield sessionmaker(bind=eng, future=True)
    eng.dispose()


def _cleanup(factory):
    with factory() as s, s.begin():
        # Jobs first: lyrics_album_tracks cascades from them, and its
        # fk_lyrics_album_track_work reference is what pins the work rows.
        s.execute(text(
            "DELETE FROM lyrics_album_jobs WHERE spotify_album_id LIKE :p"),
            {"p": f"{_PREFIX}%"})
        s.execute(text(
            "DELETE FROM lyrics_translation_work WHERE track_id IN "
            "(SELECT id FROM tracks WHERE spotify_id LIKE :p)"), {"p": f"{_PREFIX}%"})
        s.execute(text(
            "DELETE FROM lyrics_discovery_scopes WHERE user_id IN "
            "(SELECT id FROM users WHERE handle LIKE :p)"), {"p": f"{_PREFIX.replace('_','-')}%"})
        s.execute(text(
            "DELETE FROM track_lyrics WHERE track_id IN "
            "(SELECT id FROM tracks WHERE spotify_id LIKE :p)"), {"p": f"{_PREFIX}%"})
        s.execute(text("DELETE FROM tracks WHERE spotify_id LIKE :p"), {"p": f"{_PREFIX}%"})
        s.execute(text("DELETE FROM albums WHERE spotify_id LIKE :p"), {"p": f"{_PREFIX}%"})
        s.execute(text("DELETE FROM artists WHERE spotify_id LIKE :p"), {"p": f"{_PREFIX}%"})
        s.execute(text("DELETE FROM users WHERE handle LIKE :p"),
                  {"p": f"{_PREFIX.replace('_','-')}%"})


@pytest.fixture
def world(factory):
    """One demanded album: 4 tracks, of which one already carries a usable source row."""
    _cleanup(factory)
    sfx = uuid.uuid4().hex[:8]
    ids = {
        "album": str(uuid.uuid4()), "artist": str(uuid.uuid4()),
        "user": str(uuid.uuid4()), "album_sid": f"{_PREFIX}alb_{sfx}",
    }
    titles = ["Track Match", "Track Instrumental", "Track Missing", "Track Ready"]
    with factory() as s, s.begin():
        s.execute(text(
            "INSERT INTO users (id, handle, display_name) VALUES (:id, :h, 'Demand Member')"),
            {"id": ids["user"], "h": f"{_PREFIX.replace('_','-')}{sfx}"})
        s.execute(text(
            "INSERT INTO albums (id, spotify_id, title, total_tracks) "
            "VALUES (:id, :sid, 'Demand Album', 4)"),
            {"id": ids["album"], "sid": ids["album_sid"]})
        s.execute(text(
            "INSERT INTO artists (id, spotify_id, name, popularity) "
            "VALUES (:id, :sid, :n, 50)"),
            {"id": ids["artist"], "sid": f"{_PREFIX}art_{sfx}", "n": _ARTIST})
        s.execute(text(
            "INSERT INTO album_artists (album_id, artist_id) VALUES (:al, :a)"),
            {"al": ids["album"], "a": ids["artist"]})
        for n, title in enumerate(titles):
            tid = str(uuid.uuid4())
            ids[title] = tid
            s.execute(text(
                "INSERT INTO tracks (id, album_id, spotify_id, title, duration_sec, track_no) "
                "VALUES (:id, :al, :sid, :t, 200, :n)"),
                {"id": tid, "al": ids["album"], "sid": f"{_PREFIX}trk_{sfx}_{n}",
                 "t": title, "n": n + 1})
            s.execute(text(
                "INSERT INTO track_artists (track_id, artist_id) VALUES (:t, :a)"),
                {"t": tid, "a": ids["artist"]})
        # "Track Ready" already has a usable source: it belongs to the POLLER's pool and
        # this job must never spend an LRCLIB call on it.
        s.execute(text(
            "INSERT INTO track_lyrics (track_id, match_status, lyric_plain) "
            "VALUES (:t, 'matched', :b)"), {"t": ids["Track Ready"], "b": _BODY})

    with factory() as s, s.begin():
        store = LyricsDemandStore(s.connection())
        scope = store.reset_scope(uuid.UUID(ids["user"]), "saved")
        ids["job"] = store.add_demand(
            uuid.UUID(ids["user"]), scope["id"], scope["generation"],
            ids["album_sid"], "album:" + ids["album_sid"],
        )
        ids["scope"] = scope["id"]
    yield ids
    _cleanup(factory)


def _client_all_outcomes():
    return _FakeClient({
        "Track Match": [_cand("Track Match")],
        "Track Instrumental": [_cand("Track Instrumental", instrumental=True)],
        "Track Missing": [],                      # provider has never heard of it
        "Track Ready": [_cand("Track Ready")],    # must never be requested
    })


def _run(factory, client, **kw):
    with factory() as s:
        return LyricsDemandSourceService(
            s, client=client, time_budget_sec=60.0
        ).collect(**kw)


def _tracks(factory, job_id):
    with factory() as s:
        return {
            r["title"]: dict(r) for r in s.execute(text(
                """SELECT t.title, lat.source_state, lat.last_reason, lat.work_id,
                          lat.source_revision, lat.next_attempt_at, lat.updated_at
                   FROM lyrics_album_tracks lat JOIN tracks t ON t.id = lat.track_id
                   WHERE lat.job_id = :j"""), {"j": job_id}).mappings().all()
        }


def _make_due(factory, job_id, title):
    """Pull one track's due date into the past without disturbing its ladder position.

    Both timestamps move by the SAME delta, so ``next_attempt_at - updated_at`` — the
    encoded gap — is preserved. Rewriting only ``next_attempt_at`` would silently reset the
    ladder and make the doubling test pass for the wrong reason.
    """
    with factory() as s, s.begin():
        s.execute(text(
            """UPDATE lyrics_album_tracks lat
               SET next_attempt_at = lat.next_attempt_at - INTERVAL '90 days',
                   updated_at      = lat.updated_at      - INTERVAL '90 days'
               FROM tracks t
               WHERE t.id = lat.track_id AND lat.job_id = :j AND t.title = :t"""),
            {"j": job_id, "t": title})


# ── the control + the first pass ───────────────────────────────────────────────────────
def test_never_evaluated_album_is_invisible_to_the_expedite_but_covered_here(world, factory):
    """The control for this whole job: prove the gap, then prove it is closed.

    The album-scoped expedite selects ``FROM track_lyrics tl JOIN tracks``, so an album
    nobody ever evaluated yields an EMPTY batch — its demand would wait forever. Same
    album, same matcher, this job: every track covered.
    """
    with factory() as s:
        expedite = LyricsReassessmentService(
            s, client=_client_all_outcomes(), time_budget_sec=30.0
        ).reassess_album(world["album"], cooldown_sec=0)
    # Only the pre-seeded "Track Ready" row exists, and a resolved `matched` row is not an
    # expedite target either — so the three tracks that actually need a source are unseen.
    assert expedite["evaluated"] == 0

    client = _client_all_outcomes()
    metrics = _run(factory, client)

    assert set(client.seen) == {"Track Match", "Track Instrumental", "Track Missing"}
    assert "Track Ready" not in client.seen  # already usable ⇒ the poller's pool, not ours
    assert metrics["catalog"]["complete"] == 1

    rows = _tracks(factory, world["job"])
    assert len(rows) == 4  # full enumeration attached

    # OQ5 classification, each arm independently:
    assert rows["Track Instrumental"]["source_state"] == "not_required"
    assert rows["Track Instrumental"]["last_reason"] == "no_lyrics"

    assert rows["Track Missing"]["source_state"] == "source_pending"
    assert rows["Track Missing"]["last_reason"] == "not_found"
    gap = rows["Track Missing"]["next_attempt_at"] - rows["Track Missing"]["updated_at"]
    assert gap == timedelta(seconds=BASE)

    # A now-matchable track is left for the poller to link, NOT re-scheduled for a fetch.
    for ready in ("Track Match", "Track Ready"):
        assert rows[ready]["source_state"] == "source_pending"
        assert rows[ready]["work_id"] is None
    assert metrics["demand"]["source_ready"] >= 1


def test_already_parked_track_is_classified_even_though_the_guard_keeps_its_row(
    world, factory
):
    """A track the replacement guard protects was still EVALUATED, and must be classified.

    This is the case the rest of the suite misses: every other track starts with no
    `track_lyrics` row, so `_write_gate` takes the first-fetch branch and `should_replace`
    is never consulted. Here the row already exists as `no_lyrics` — the commonest real
    shape, an interlude the global collector reached first — so `should_replace` returns
    False (not unresolved, and the new outcome is not a stronger `matched`) and
    `run_eval_batch` writes nothing.

    Without `touch_on_guard_kept`, `updated_at` would not move, the write-back would read
    the row as "not evaluated this run", and the track would keep `next_attempt_at IS NULL`
    — re-selected every 15 minutes forever, sorted to the HEAD of the queue by
    `ORDER BY next_attempt_at NULLS FIRST`, with its album unable to ever reach `done`.
    """
    with factory() as s, s.begin():
        s.execute(text(
            "INSERT INTO track_lyrics (track_id, match_status, lyric_plain) "
            "VALUES (:t, 'no_lyrics', '')"), {"t": world["Track Instrumental"]})

    client = _client_all_outcomes()
    metrics = _run(factory, client)

    assert "Track Instrumental" in client.seen          # it WAS re-checked
    assert metrics["guard_kept"] >= 1                   # and deliberately not rewritten
    row = _tracks(factory, world["job"])["Track Instrumental"]
    assert row["source_state"] == "not_required", (
        "a guard-kept evaluation must still reach the demand side"
    )
    assert row["last_reason"] == "no_lyrics"

    # And it is genuinely out of the queue, not merely relabelled.
    again = _FakeClient({})
    _run(factory, again)
    assert "Track Instrumental" not in again.seen


def test_second_pass_skips_everything_it_already_settled(world, factory):
    """Idempotence: a re-fire must not re-spend LRCLIB on settled or not-yet-due rows."""
    _run(factory, _client_all_outcomes())
    again = _FakeClient({})
    metrics = _run(factory, again)
    assert again.seen == []                       # not_required + backoff + ready all skipped
    assert metrics["catalog"]["resolved"] == 0    # enumeration already complete


# ── OQ5 ladder ─────────────────────────────────────────────────────────────────────────
def test_ladder_doubles_only_after_a_completed_unresolved_evaluation(world, factory):
    _run(factory, _client_all_outcomes())
    _make_due(factory, world["job"], "Track Missing")

    _run(factory, _FakeClient({"Track Missing": []}))
    row = _tracks(factory, world["job"])["Track Missing"]
    assert row["next_attempt_at"] - row["updated_at"] == timedelta(seconds=BASE * 2)


def test_transient_provider_failure_writes_nothing_and_keeps_the_ladder(world, factory):
    """An LRCLIB outage must not push waiting demand out to the cap (the OQ5 rule).

    Not writing is how that is achieved: the row keeps its due-ness AND its ladder
    position, so the next 15-minute firing retries it at the same rung.
    """
    _run(factory, _client_all_outcomes())
    _make_due(factory, world["job"], "Track Missing")
    before = _tracks(factory, world["job"])["Track Missing"]

    client = _FakeClient({}, raise_exc=LrclibTransientError("provider down"))
    metrics = _run(factory, client)

    assert "Track Missing" in client.seen          # it WAS attempted
    after = _tracks(factory, world["job"])["Track Missing"]
    assert after["next_attempt_at"] == before["next_attempt_at"]
    assert after["updated_at"] == before["updated_at"]
    assert after["source_revision"] == before["source_revision"]
    assert metrics["demand"]["unevaluated"] >= 1


def test_not_required_is_reopenable_by_a_fresh_observation(world, factory):
    """`not_required` is an observation, not a tombstone — OQ5's explicit wording.

    Proven through the store's own guard: a write carrying the CURRENT revision reopens
    the track, which is exactly what a later corpus change would do.
    """
    _run(factory, _client_all_outcomes())
    row = _tracks(factory, world["job"])["Track Instrumental"]

    with factory() as s, s.begin():
        LyricsDemandStore(s.connection()).set_source_state(
            world["job"], uuid.UUID(world["Track Instrumental"]),
            "source_pending", "corpus_changed",
            expected_work_id=None, expected_source_revision=row["source_revision"],
        )
    reopened = _tracks(factory, world["job"])["Track Instrumental"]
    assert reopened["source_state"] == "source_pending"
    assert reopened["source_revision"] != row["source_revision"]


# ── supersession + demand liveness ─────────────────────────────────────────────────────
def test_observation_superseded_during_the_provider_call_is_rejected(world, factory):
    """The write-back validates the revision it observed, so a racing change wins.

    The mutation is fired from inside the fake provider call — the actual window the race
    lives in — rather than staged around the seam, so the test exercises the whole
    ``collect()`` path exactly as production runs it.
    """
    _run(factory, _client_all_outcomes())
    _make_due(factory, world["job"], "Track Missing")
    observed = _tracks(factory, world["job"])["Track Missing"]

    client = _FakeClient({"Track Missing": []})

    def mutate_mid_flight():
        client.on_call = None  # once only
        with factory() as other, other.begin():
            LyricsDemandStore(other.connection()).set_source_state(
                world["job"], uuid.UUID(world["Track Missing"]),
                "not_required", "manually_marked",
                expected_work_id=None,
                expected_source_revision=observed["source_revision"],
            )

    client.on_call = mutate_mid_flight
    metrics = _run(factory, client)

    assert metrics["demand"]["stale"] == 1
    row = _tracks(factory, world["job"])["Track Missing"]
    assert row["source_state"] == "not_required"      # the concurrent write survived
    assert row["last_reason"] == "manually_marked"


def test_album_stops_consuming_provider_calls_once_demand_is_removed(world, factory):
    with factory() as s, s.begin():
        LyricsDemandStore(s.connection()).remove_origin(
            uuid.UUID(world["user"]), "saved", origin_key="album:" + world["album_sid"])

    _make_due(factory, world["job"], "Track Missing")
    client = _client_all_outcomes()
    metrics = _run(factory, client)
    assert client.seen == []
    assert metrics["catalog"]["resolved"] == 0


def test_uningested_album_defers_on_the_short_ladder_and_keeps_its_demand(world, factory):
    """Demand for an album the catalog does not hold yet survives, and is re-checked on the
    catalog ladder (minutes) rather than the source ladder (weeks)."""
    with factory() as s, s.begin():
        store = LyricsDemandStore(s.connection())
        scope = store.reset_scope(uuid.UUID(world["user"]), "follow")
        ghost = store.add_demand(
            uuid.UUID(world["user"]), scope["id"], scope["generation"],
            f"{_PREFIX}ghost_album", "artist:ghost")

    _run(factory, _FakeClient({}))

    with factory() as s:
        job = s.execute(text(
            "SELECT album_id, last_reason, next_attempt_at, updated_at, cancelled "
            "FROM lyrics_album_jobs WHERE id = :id"), {"id": ghost}).mappings().one()
        demands = s.execute(text(
            "SELECT count(*) FROM lyrics_album_demands WHERE job_id = :id"),
            {"id": ghost}).scalar()

    assert job["album_id"] is None and job["cancelled"] is False
    assert job["last_reason"] == "album_not_in_catalog"
    assert demands == 1                                   # demand preserved, never dropped
    assert job["next_attempt_at"] - job["updated_at"] == timedelta(
        seconds=settings.LYRICS_DEMAND_CATALOG_RETRY_BASE_SEC)


# ── transaction boundary ───────────────────────────────────────────────────────────────
def test_no_db_transaction_is_held_across_the_provider_call(world, factory):
    """The recurring bug class this repo has been bitten by: an idle-in-transaction session
    held open across an external loop (Neon ``ProtocolViolation``).

    Asserted against ``pg_stat_activity`` from a second connection *while* the fake provider
    is answering — the only moment the defect would be observable.
    """
    _run(factory, _client_all_outcomes())
    _make_due(factory, world["job"], "Track Missing")

    states: list[str] = []
    client = _FakeClient({"Track Missing": []})

    with factory() as s:
        pid = s.execute(text("SELECT pg_backend_pid()")).scalar_one()
        s.commit()

        def probe():
            with factory() as other:
                states.append(other.execute(text(
                    "SELECT state FROM pg_stat_activity WHERE pid = :p"),
                    {"p": pid}).scalar() or "gone")

        client.on_call = probe
        LyricsDemandSourceService(s, client=client, time_budget_sec=30.0).collect()

    assert states, "probe never ran — the provider call was not exercised"
    assert "idle in transaction" not in states, states
