"""Back-catalogue ingest for follow demand against real Postgres
(OPS-project-stabilization Step 2C).

Real engine because every claim here is a property of SQL: the claim that keeps a
duplicated message from reading the same releases twice, the predicate that decides
which demand counts as "a follow is waiting on the catalog", the catalog write the
resolver then has to be able to attach, and the absence of an open transaction while
the provider is answering ([[feedback-sa-session-lifecycle-mock-blind]]).

The catalog write is the real `AlbumSyncService.sync_fetched_albums`; only the Spotify
photo enrichment behind it is patched out, because it is a network call.

Guarded by TEST_DB_URL; skipped when unset. CI loads the pinned canonical schema.
"""
from __future__ import annotations

import os
import time
import uuid
from datetime import timedelta
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from worker.service.lyrics_backcatalogue_service import (
    BELOW_GATE,
    CLAIMED,
    INGESTED,
    NOT_IN_CATALOG,
    UNAVAILABLE,
    run_backcatalogue_ingest,
    waiting_count,
)
from worker.service.lyrics_demand_source_service import LyricsDemandSourceService
from worker.service.lyrics_follow_demand_service import sync_follow_demand
from worker.service.lyrics_member_demand_service import FOLLOW_ORIGIN, SAVED_ORIGIN
from worker.service.sync_service import AlbumSyncService

from myblog_shared_db.lyrics_demand import LyricsDemandStore

_TEST_DB_URL = os.environ.get("TEST_DB_URL")

pytestmark = pytest.mark.skipif(
    not _TEST_DB_URL,
    reason="integration test requires TEST_DB_URL env var (Postgres test database)",
)

_PREFIX = "lyr_backcat_"
_HANDLE = _PREFIX.replace("_", "-")
_POP_MIN = 20


@pytest.fixture(scope="module")
def factory():
    eng = create_engine(_TEST_DB_URL, pool_pre_ping=True, future=True)
    yield sessionmaker(bind=eng, future=True)
    eng.dispose()


def _cleanup(factory):
    like = {"p": f"{_PREFIX}%"}
    with factory() as s, s.begin():
        s.execute(text("DELETE FROM lyrics_album_jobs WHERE spotify_album_id LIKE :p"), like)
        s.execute(text("DELETE FROM lyrics_discovery_scopes WHERE user_id IN "
                       "(SELECT id FROM users WHERE handle LIKE :h)"), {"h": f"{_HANDLE}%"})
        s.execute(text("DELETE FROM user_integrations WHERE user_id IN "
                       "(SELECT id FROM users WHERE handle LIKE :h)"), {"h": f"{_HANDLE}%"})
        s.execute(text("DELETE FROM user_artist_tracks WHERE user_id IN "
                       "(SELECT id FROM users WHERE handle LIKE :h)"), {"h": f"{_HANDLE}%"})
        s.execute(text("DELETE FROM lyrics_artist_discographies WHERE spotify_artist_id LIKE :p"),
                  like)
        s.execute(text("DELETE FROM track_artists WHERE track_id IN "
                       "(SELECT id FROM tracks WHERE spotify_id LIKE :p)"), like)
        s.execute(text("DELETE FROM tracks WHERE spotify_id LIKE :p"), like)
        s.execute(text("DELETE FROM album_artists WHERE album_id IN "
                       "(SELECT id FROM albums WHERE spotify_id LIKE :p)"), like)
        s.execute(text("DELETE FROM album_genres WHERE album_id IN "
                       "(SELECT id FROM albums WHERE spotify_id LIKE :p)"), like)
        s.execute(text("DELETE FROM albums WHERE spotify_id LIKE :p"), like)
        s.execute(text("DELETE FROM artists WHERE spotify_id LIKE :p"), like)
        s.execute(text("DELETE FROM users WHERE handle LIKE :h"), {"h": f"{_HANDLE}%"})


@pytest.fixture
def world(factory):
    """One connected member following one artist, and a unique suffix for provider ids."""
    _cleanup(factory)
    uid, sfx = uuid.uuid4(), uuid.uuid4().hex[:8]
    with factory() as s, s.begin():
        s.execute(text("INSERT INTO users (id, handle, display_name) VALUES (:id, :h, 'M')"),
                  {"id": str(uid), "h": f"{_HANDLE}{sfx}"})
        s.execute(text("INSERT INTO user_integrations (user_id, provider, payload, status) "
                       "VALUES (:u, 'spotify', '{\"v\": 1}', 'connected')"), {"u": str(uid)})
    yield {"member": uid, "sfx": sfx, "artist": f"{_PREFIX}art_{sfx}"}
    _cleanup(factory)


def _sid(world, name):
    return f"{_PREFIX}{name}_{world['sfx']}"


def _follow(factory, world, album_sids):
    """The state Step 5 leaves: the artist enumerated, and follow demand for each release."""
    with factory() as s, s.begin():
        s.execute(text("INSERT INTO lyrics_artist_discographies (spotify_artist_id, complete) "
                       "VALUES (:a, true) ON CONFLICT DO NOTHING"), {"a": world["artist"]})
        for album in sorted(album_sids):
            s.execute(text("INSERT INTO lyrics_artist_albums "
                           "(spotify_artist_id, spotify_album_id, release_group) "
                           "VALUES (:a, :b, 'album') ON CONFLICT DO NOTHING"),
                      {"a": world["artist"], "b": album})
    sync_follow_demand(factory, world["member"],
                       followed_artists=[{"id": world["artist"], "name": "Artist"}])


def _saved_demand(factory, world, album_sid):
    """Demand for one album on the member's `saved` scope only — no follow involved."""
    with factory() as s, s.begin():
        store = LyricsDemandStore(s.connection())
        scope = store.reset_scope(world["member"], SAVED_ORIGIN)
        store.add_demand(world["member"], scope["id"], scope["generation"],
                         album_sid, album_sid)


def _job(factory, album_sid):
    with factory() as s:
        return dict(s.execute(
            text("SELECT id, album_id, last_reason, next_attempt_at, updated_at, "
                 "enumeration_complete, now() AS now FROM lyrics_album_jobs "
                 "WHERE spotify_album_id = :a"), {"a": album_sid}).mappings().one())


def _in_catalog(factory, album_sid):
    with factory() as s:
        return s.execute(text("SELECT count(*) FROM tracks t JOIN albums a ON a.id = t.album_id "
                              "WHERE a.spotify_id = :a"), {"a": album_sid}).scalar_one()


def _album_exists(factory, album_sid):
    with factory() as s:
        return s.execute(text("SELECT 1 FROM albums WHERE spotify_id = :a"),
                         {"a": album_sid}).first() is not None


def _album_count(factory):
    with factory() as s:
        return s.execute(text("SELECT count(*) FROM albums")).scalar_one()


def _resolve(factory):
    """One pass of the Step 3 catalog resolver — the thing that attaches an ingested album
    to its job, and the thing that returns an expired claim or rest to the waiting pool."""
    with factory() as s:
        return LyricsDemandSourceService(s)._resolve_due_catalogs(
            500, time.monotonic() + 60.0)


def _expire(factory, album_sid):
    with factory() as s, s.begin():
        s.execute(text("UPDATE lyrics_album_jobs SET next_attempt_at = now() - interval '1 second' "
                       "WHERE spotify_album_id = :a"), {"a": album_sid})


def _album(world, sid, popularity, tracks=2):
    return {
        "id": sid, "name": f"Album {sid}", "album_type": "album", "popularity": popularity,
        "release_date": "2020-01-01", "total_tracks": tracks,
        "artists": [{"id": world["artist"], "name": "Artist"}],
        "tracks": {"items": [
            {"id": f"{sid}_t{i}", "name": f"Track {i}", "track_number": i + 1,
             "disc_number": 1, "duration_ms": 180000,
             "artists": [{"id": world["artist"], "name": "Artist"}]}
            for i in range(tracks)]},
    }


class _Catalog:
    """GET /albums: answers positionally, like the provider, and records every request."""

    def __init__(self, by_id, fail=False):
        self.by_id = by_id
        self.calls = []
        self.fail = fail
        self.on_call = None

    def get_albums(self, ids, market=None):
        self.calls.append(list(ids))
        if self.on_call:
            self.on_call()
        if self.fail:
            raise RuntimeError("provider down")
        return [self.by_id.get(i) for i in ids]


def _run(factory, catalog, **kw):
    kw.setdefault("pop_min", _POP_MIN)
    kw.setdefault("max_catalog_albums", 10**9)
    sync = AlbumSyncService(factory).sync_fetched_albums
    with patch("worker.service.sync_service.enrich_artists", return_value=0):
        return run_backcatalogue_ingest(factory, catalog, sync, **kw)


def _read(catalog):
    return {sid for call in catalog.calls for sid in call}


# ---------------------------------------------------------------------------
# The owner's decision: the existing curation gate, applied to the response.
# ---------------------------------------------------------------------------

def test_a_release_at_the_gate_is_written_and_one_under_it_rests(factory, world):
    at, under, gone = _sid(world, "at"), _sid(world, "under"), _sid(world, "gone")
    _follow(factory, world, [at, under, gone])
    catalog = _Catalog({at: _album(world, at, _POP_MIN), under: _album(world, under, _POP_MIN - 1)})

    metrics = _run(factory, catalog, rest_days=30)

    assert _read(catalog) >= {at, under, gone}, "popularity is only in the response: all are read"
    assert _in_catalog(factory, at) == 2, "exactly at the gate is in"
    assert not _album_exists(factory, under), "under the gate is read and NOT written"
    assert (metrics["ingested"], metrics["below_gate"], metrics["unavailable"]) == (1, 1, 1)

    resting, missing, done = _job(factory, under), _job(factory, gone), _job(factory, at)
    assert resting["last_reason"] == BELOW_GATE and missing["last_reason"] == UNAVAILABLE
    for job in (resting, missing):
        assert timedelta(days=29) < job["next_attempt_at"] - job["now"] <= timedelta(days=30)
    assert done["last_reason"] == INGESTED
    assert done["next_attempt_at"] <= done["now"], "an ingested release is due at once"


def test_an_ingested_release_is_attached_to_its_job_by_the_resolver(factory, world):
    """The verification item "a newly returned release reaches album demand": the write
    this job makes has to be one `set_catalog` accepts as the complete album."""
    album = _sid(world, "attach")
    _follow(factory, world, [album])

    _run(factory, _Catalog({album: _album(world, album, 60, tracks=3)}))
    _resolve(factory)

    job = _job(factory, album)
    assert job["album_id"] is not None and job["enumeration_complete"]
    assert job["last_reason"] is None
    with factory() as s:
        assert s.execute(text("SELECT count(*) FROM lyrics_album_tracks WHERE job_id = :j"),
                         {"j": job["id"]}).scalar_one() == 3


def test_a_relinked_release_is_not_written_under_an_id_the_job_cannot_resolve(factory, world):
    asked, other = _sid(world, "asked"), _sid(world, "relinked")
    _follow(factory, world, [asked])

    _run(factory, _Catalog({asked: _album(world, other, 90)}))

    assert not _album_exists(factory, other) and not _album_exists(factory, asked)
    assert _job(factory, asked)["last_reason"] == UNAVAILABLE


def test_a_short_response_files_no_verdict_at_all(factory, world):
    """Positional matching against a short array would file one release's popularity
    against another release's job."""
    a, b = _sid(world, "short_a"), _sid(world, "short_b")
    _follow(factory, world, [a, b])

    class _Short(_Catalog):
        def get_albums(self, ids, market=None):
            return super().get_albums(ids, market)[:1]

    metrics = _run(factory, _Short({a: _album(world, a, 90), b: _album(world, b, 90)}))

    assert metrics["failed"] == 1 and metrics["ingested"] == 0
    assert not _album_exists(factory, a) and not _album_exists(factory, b)
    assert {_job(factory, a)["last_reason"], _job(factory, b)["last_reason"]} == {CLAIMED}


# ---------------------------------------------------------------------------
# Which demand this job serves.
# ---------------------------------------------------------------------------

def test_only_follow_demand_the_catalog_does_not_hold_is_read(factory, world):
    wanted, held, saved = _sid(world, "wanted"), _sid(world, "held"), _sid(world, "saved")
    with factory() as s, s.begin():
        s.execute(text("INSERT INTO albums (id, title, spotify_id) VALUES (:i, 'Held', :s)"),
                  {"i": str(uuid.uuid4()), "s": held})
    _follow(factory, world, [wanted, held])
    _saved_demand(factory, world, saved)
    catalog = _Catalog({sid: _album(world, sid, 90) for sid in (wanted, held, saved)})

    _run(factory, catalog)

    assert wanted in _read(catalog), "the control: a follow release the catalog lacks IS read"
    assert held not in _read(catalog), "already in the catalog — the resolver's job, not ours"
    assert saved not in _read(catalog), "saved-only demand is outside the owner's decision"


def test_a_revoked_follow_scope_stops_the_ingest(factory, world):
    """Member isolation and the disconnect path: demand on a scope that is no longer
    active must not spend a provider read."""
    album = _sid(world, "revoked")
    _follow(factory, world, [album])
    assert waiting_count(factory) >= 1
    with factory() as s, s.begin():
        s.execute(text("UPDATE lyrics_discovery_scopes SET active = false "
                       "WHERE user_id = :u AND origin = :o"),
                  {"u": str(world["member"]), "o": FOLLOW_ORIGIN})
    catalog = _Catalog({album: _album(world, album, 90)})

    _run(factory, catalog)

    assert album not in _read(catalog)
    assert not _album_exists(factory, album)


def test_a_cancelled_job_is_not_read(factory, world):
    """`LIVE_DEMAND_SQL`'s other half. The demand row is deliberately left in place, so
    only the `cancelled` flag can be what keeps the release out."""
    album = _sid(world, "cancelled")
    _follow(factory, world, [album])
    with factory() as s, s.begin():
        s.execute(text("UPDATE lyrics_album_jobs SET cancelled = true "
                       "WHERE spotify_album_id = :a"), {"a": album})
    catalog = _Catalog({album: _album(world, album, 90)})

    _run(factory, catalog)

    assert album not in _read(catalog)


def test_the_waiting_count_agrees_with_what_a_run_will_claim(factory, world):
    """The nudge enqueues on this count. Baseline first, then the row — measuring the
    baseline afterwards compares the count with itself."""
    baseline = waiting_count(factory)
    album = _sid(world, "count")
    _follow(factory, world, [album])
    assert waiting_count(factory) == baseline + 1

    metrics = _run(factory, _Catalog({album: _album(world, album, 1)}))

    assert metrics["below_gate"] == 1
    assert waiting_count(factory) == baseline, "a resting release is not waiting"
    assert metrics["remaining"] == baseline


# ---------------------------------------------------------------------------
# Bounded repetition: claims, rests, failures.
# ---------------------------------------------------------------------------

def test_a_second_run_in_flight_does_not_read_what_the_first_claimed(factory, world):
    album = _sid(world, "claim")
    _follow(factory, world, [album])
    inner = _Catalog({album: _album(world, album, 90)})
    outer = _Catalog({album: _album(world, album, 90)})
    # The duplicate message arrives while the first run is waiting on the provider.
    outer.on_call = lambda: _run(factory, inner)

    _run(factory, outer)

    assert album in _read(outer)
    assert album not in _read(inner), "the duplicate must not pay for the same release"


def test_a_resting_release_is_not_read_again_until_its_rest_expires(factory, world):
    album = _sid(world, "rest")
    _follow(factory, world, [album])
    low = {album: _album(world, album, 1)}
    _run(factory, _Catalog(low))

    again = _Catalog(low)
    _resolve(factory)
    _run(factory, again)
    assert album not in _read(again), "resting: neither the resolver nor a run touches it"

    # The rest expires; the resolver's ordinary re-check returns it to the pool, and by
    # then it has cleared the gate.
    _expire(factory, album)
    _resolve(factory)
    assert _job(factory, album)["last_reason"] == NOT_IN_CATALOG
    _run(factory, _Catalog({album: _album(world, album, 90)}))
    assert _in_catalog(factory, album) == 2


def test_a_provider_failure_writes_nothing_and_the_release_is_read_again_later(factory, world):
    album = _sid(world, "fail")
    _follow(factory, world, [album])

    metrics = _run(factory, _Catalog({}, fail=True), lease_seconds=300)

    job = _job(factory, album)
    assert metrics["failed"] == 1 and metrics["read"] == 0
    assert job["last_reason"] == CLAIMED, "no verdict was filed for a read that did not happen"
    assert timedelta(0) < job["next_attempt_at"] - job["now"] <= timedelta(seconds=300)

    # Until the lease expires the release is not retried, so a dead provider costs one
    # call per lease and not one per tick.
    retry = _Catalog({album: _album(world, album, 90)})
    _resolve(factory)
    _run(factory, retry)
    assert album not in _read(retry)

    _expire(factory, album)
    _resolve(factory)
    _run(factory, retry)
    assert _in_catalog(factory, album) == 2


def test_a_failed_catalog_write_files_no_verdict(factory, world):
    album = _sid(world, "wfail")
    _follow(factory, world, [album])

    def boom(_albums):
        raise RuntimeError("db down")

    metrics = run_backcatalogue_ingest(
        factory, _Catalog({album: _album(world, album, 90)}), boom,
        pop_min=_POP_MIN, max_catalog_albums=10**9)

    assert metrics["failed"] == 1 and metrics["ingested"] == 0
    assert _job(factory, album)["last_reason"] == CLAIMED


def test_one_invocation_reads_at_most_its_batch_budget(factory, world):
    albums = [_sid(world, f"bud{i:02d}") for i in range(45)]
    _follow(factory, world, albums)
    catalog = _Catalog({sid: _album(world, sid, 1, tracks=1) for sid in albums})

    metrics = _run(factory, catalog, max_batches=2)

    assert len(catalog.calls) == 2 and all(len(c) <= 20 for c in catalog.calls)
    assert metrics["read"] == 40
    assert _run(factory, _Catalog({}), max_batches=0)["claimed"] == 0, "0 batches disables it"


# ---------------------------------------------------------------------------
# The catalog cap: this path must not silently stop the daily sweep.
# ---------------------------------------------------------------------------

def test_at_the_cap_nothing_is_claimed_and_nothing_is_read(factory, world):
    album = _sid(world, "cap")
    _follow(factory, world, [album])
    catalog = _Catalog({album: _album(world, album, 90)})

    metrics = _run(factory, catalog, max_catalog_albums=_album_count(factory))

    assert catalog.calls == [] and metrics["capped"] == 1
    assert _job(factory, album)["last_reason"] is None, "left waiting, not claimed"


def test_the_run_stops_writing_when_the_cap_has_no_more_room(factory, world):
    a, b = _sid(world, "room_a"), _sid(world, "room_b")
    _follow(factory, world, [a, b])
    before = _album_count(factory)

    metrics = _run(factory, _Catalog({a: _album(world, a, 90), b: _album(world, b, 90)}),
                   max_catalog_albums=before + 1)

    assert _album_count(factory) == before + 1, "the cap is not passed from this path"
    assert metrics["ingested"] == 1 and metrics["capped"] == 1
    reasons = {_job(factory, a)["last_reason"], _job(factory, b)["last_reason"]}
    assert reasons == {INGESTED, CLAIMED}, "the one without room waits; it is not rested"


# ---------------------------------------------------------------------------
# The recurring bug class.
# ---------------------------------------------------------------------------

def test_no_db_transaction_is_held_across_the_provider_read(factory, world):
    """Asserted from a second connection WHILE the fake provider is answering — the only
    moment the defect is observable."""
    album = _sid(world, "idle")
    _follow(factory, world, [album])
    seen: list[int] = []
    catalog = _Catalog({album: _album(world, album, 90)})

    def probe():
        with factory() as other:
            seen.append(other.execute(text(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() "
                "AND pid <> pg_backend_pid() AND state = 'idle in transaction'")).scalar_one())

    catalog.on_call = probe
    _run(factory, catalog)

    assert seen, "probe never ran — the provider call was not exercised"
    assert seen == [0], seen


# ---------------------------------------------------------------------------
# The resolver's side of the rest.
# ---------------------------------------------------------------------------

def test_an_album_arriving_by_another_path_ends_the_rest(factory, world):
    """A release under the gate rests for weeks. If a member then saves or plays it, the
    ordinary ingest writes it — and the job must not sit out the rest before noticing.
    The control is a job deferred for any OTHER reason: it keeps its ladder."""
    rested, laddered = _sid(world, "ended"), _sid(world, "ladder")
    _follow(factory, world, [rested, laddered])
    _run(factory, _Catalog({rested: _album(world, rested, 1), laddered: None}))
    assert _job(factory, rested)["last_reason"] == BELOW_GATE
    with factory() as s, s.begin():
        s.execute(text("UPDATE lyrics_album_jobs SET last_reason = :r "
                       "WHERE spotify_album_id = :a"), {"r": NOT_IN_CATALOG, "a": laddered})

    sync = AlbumSyncService(factory).sync_fetched_albums
    with patch("worker.service.sync_service.enrich_artists", return_value=0):
        sync([_album(world, rested, 1), _album(world, laddered, 1)])
    _resolve(factory)

    assert _job(factory, rested)["album_id"] is not None
    assert _job(factory, laddered)["album_id"] is None, "a laddered deferral is not cut short"
