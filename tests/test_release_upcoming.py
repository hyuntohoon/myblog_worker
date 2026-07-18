# Unit tests for the multi-source upcoming-release poller (FEAT-release-calendar
# Step 4).
#
# Pure-logic: HTTP clients are stubbed; the service runs against a fake session
# that records executed SQL (blind to real Postgres semantics — the FULL-unique
# ON CONFLICT inference is by design, per V44; a prod dry-run validates the SQL
# before merge, per feedback-sa-session-lifecycle-mock-blind).
from __future__ import annotations

import uuid
from datetime import date

import pytest

from worker.core.config import settings
from worker.service import release_upcoming_service as svc
from worker.service.release_upcoming_service import (
    ITUNES_ID_NOT_FOUND,
    RESOLVED_VIA_NO_UPC,
    _bucket,
    _dedup_sorted,
    _itunes_release_type,
    _mb_release_type,
    run_release_upcoming_poll,
)

TODAY = date(2026, 7, 12)


class _Row:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class _Result:
    def __init__(self, rows=None, rowcount=1):
        self._rows = rows or []
        self.rowcount = rowcount

    def fetchall(self):
        return self._rows


class _FakeSession:
    """Routes each SELECT to canned rows and records every executed statement."""

    def __init__(self, mb_watchlist=None, itunes_watchlist=None, upc_rows=None):
        self.mb_watchlist = mb_watchlist or []
        self.itunes_watchlist = itunes_watchlist or []
        self.upc_rows = upc_rows or []
        self.executed = []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.executed.append((sql, params or {}))
        if "musicbrainz_id" in sql and "FROM artists" in sql:
            return _Result(rows=self.mb_watchlist)
        if "LEFT JOIN artist_source_ids" in sql:
            return _Result(rows=self.itunes_watchlist)
        if "ext_refs->>'upc'" in sql:
            return _Result(rows=self.upc_rows)
        return _Result()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def begin(self):
        return _FakeSession._Ctx()

    class _Ctx:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def sql_of(self, pred):
        return [e for e in self.executed if pred(e[0])]


class TestBucketRotation:
    def test_slices_and_wraps(self):
        items = list(range(10))
        # cycle 0: plain slices, start offset 0
        assert _bucket(items, 4, 0) == [0, 1, 2, 3]
        assert _bucket(items, 4, 1) == [4, 5, 6, 7]
        assert _bucket(items, 4, 2) == [8, 9]
        # modulo wrap → cycle 1: same MEMBERSHIP, start rotated by 1 (fairness fix)
        assert _bucket(items, 4, 3) == [1, 2, 3, 0]

    def test_empty_and_small(self):
        assert list(_bucket([], 4, 7)) == []
        assert _bucket([1], 4, 5) == [1]

    def test_intra_bucket_rotation_keeps_membership(self):
        # 부수 픽스 #1: the budget guard used to cut the SAME tail every cycle;
        # rotation must reorder, never change which artists belong to the bucket.
        items = list(range(10))
        for tick in (0, 3, 6, 9, 12):  # bucket 0 across 5 cycles
            assert set(_bucket(items, 4, tick)) == {0, 1, 2, 3}
        assert _bucket(items, 4, 6) == [2, 3, 0, 1]   # cycle 2 → offset 2
        assert _bucket(items, 4, 12) == [0, 1, 2, 3]  # offset wraps at len(bucket)

    def test_budget_tail_covered_across_cycles(self):
        # Simulate a budget stop after 2 of 4 artists every tick: the fixed
        # start-at-0 order would starve indices 2 and 3 forever; the rotating
        # offset covers the whole bucket within len(bucket) cycles.
        items = list(range(8))
        progress = 2
        covered = set()
        for cycle in range(4):
            tick = cycle * 2  # 8 items / 4 per tick = 2 buckets; bucket 0 ticks
            covered.update(_bucket(items, 4, tick)[:progress])
        assert covered == {0, 1, 2, 3}


class TestTypeMapping:
    def test_mb_primary_type(self):
        assert _mb_release_type("Album") == "album"
        assert _mb_release_type("EP") == "ep"
        assert _mb_release_type("Single") == "single"
        assert _mb_release_type("Broadcast") == "other"
        assert _mb_release_type(None) is None

    def test_itunes_name_suffix(self):
        assert _itunes_release_type("Song - Single") == "single"
        assert _itunes_release_type("Songs - EP") == "ep"
        assert _itunes_release_type("The Album") == "album"


class TestDedupSort:
    def test_dedups_on_conflict_key_and_sorts(self):
        rows = [
            {"source": "musicbrainz", "source_key": "b", "artist_id": 1},
            {"source": "musicbrainz", "source_key": "a", "artist_id": 2},
            {"source": "musicbrainz", "source_key": "b", "artist_id": 3},  # collab dup
        ]
        out = _dedup_sorted(rows)
        assert [r["source_key"] for r in out] == ["a", "b"]
        assert out[1]["artist_id"] == 1  # first observation kept


class TestPollScope:
    """Poll scope = popularity watchlist ∪ user-tracked artists (personal-release-
    tracking Step 4a). Guard the EXISTS clause in BOTH source eligibility queries
    — losing it silently drops tracked long-tail artists from discovery."""

    def test_mb_watchlist_query_includes_tracked_artists(self):
        session = _FakeSession()
        run_release_upcoming_poll(
            lambda: session, mode="musicbrainz", mb_search=lambda *a: [],
            tick_index=0, today=TODAY,
        )
        (sql, _), = session.sql_of(lambda s: "FROM artists" in s and "musicbrainz_id" in s)
        assert "EXISTS (SELECT 1 FROM user_artist_tracks" in sql

    def test_itunes_watchlist_query_includes_tracked_artists(self):
        session = _FakeSession()
        run_release_upcoming_poll(
            lambda: session, mode="itunes", itunes_client=object(),
            tick_index=0, today=TODAY,
        )
        (sql, _), = session.sql_of(lambda s: "LEFT JOIN artist_source_ids" in s)
        assert "EXISTS (SELECT 1 FROM user_artist_tracks" in sql


def _rg(rgid="rg-1", title="Next", first="2026-08-01", ptype="Album"):
    return {"id": rgid, "title": title, "first-release-date": first, "primary-type": ptype}


class TestMbPass:
    def test_full_date_only_and_window_filter(self):
        aid = uuid.uuid4()
        session = _FakeSession(mb_watchlist=[_Row(artist_id=aid, musicbrainz_id="mb-1")])
        rgs = [
            _rg("rg-full", first="2026-08-01"),
            _rg("rg-year", first="2026"),          # year-only placeholder → dropped
            _rg("rg-month", first="2026-09"),      # partial → dropped
            _rg("rg-past", first="2026-07-11"),    # before today → dropped
            _rg("rg-far", first="2027-06-01"),     # beyond 180 d → dropped
        ]
        res = run_release_upcoming_poll(
            lambda: session, mode="musicbrainz", mb_search=lambda *a: rgs,
            tick_index=0, today=TODAY,
        )
        assert res["found"] == 1 and res["upserted"] == 1
        inserts = session.sql_of(lambda s: "INSERT INTO artist_release_events" in s)
        assert len(inserts) == 1
        assert inserts[0][1]["source_key"] == "rg-full"
        assert inserts[0][1]["release_type"] == "album"

    def test_upsert_never_touches_status_or_spotify_album_id(self):
        aid = uuid.uuid4()
        session = _FakeSession(mb_watchlist=[_Row(artist_id=aid, musicbrainz_id="mb-1")])
        run_release_upcoming_poll(
            lambda: session, mode="musicbrainz", mb_search=lambda *a: [_rg()],
            tick_index=0, today=TODAY,
        )
        (sql, _), = session.sql_of(lambda s: "INSERT INTO artist_release_events" in s)
        update_clause = sql.split("DO UPDATE SET", 1)[1]
        assert "status" not in update_clause
        assert "spotify_album_id" not in update_clause

    def test_one_artist_error_does_not_fail_tick(self):
        rows = [_Row(artist_id=uuid.uuid4(), musicbrainz_id=f"mb-{i}") for i in range(2)]
        session = _FakeSession(mb_watchlist=rows)
        calls = []

        def search(mbid, a, b):
            calls.append(mbid)
            if mbid == "mb-0":
                raise RuntimeError("MB 503")
            return [_rg()]

        res = run_release_upcoming_poll(
            lambda: session, mode="musicbrainz", mb_search=search,
            tick_index=0, today=TODAY,
        )
        assert calls == ["mb-0", "mb-1"]
        assert res["errors"] == 1 and res["found"] == 1

    def test_rotation_covers_next_bucket_on_next_tick(self):
        rows = [_Row(artist_id=uuid.uuid4(), musicbrainz_id=f"mb-{i}") for i in range(3)]
        session = _FakeSession(mb_watchlist=rows)
        seen = []
        old = settings.RELEASE_POLL_MB_ARTISTS_PER_TICK
        settings.RELEASE_POLL_MB_ARTISTS_PER_TICK = 2
        try:
            run_release_upcoming_poll(
                lambda: session, mode="musicbrainz",
                mb_search=lambda m, a, b: seen.append(m) or [],
                tick_index=1, today=TODAY,
            )
        finally:
            settings.RELEASE_POLL_MB_ARTISTS_PER_TICK = old
        assert seen == ["mb-2"]  # tick 1 → second bucket

    def test_budget_stop_after_first_artist(self, monkeypatch):
        rows = [_Row(artist_id=uuid.uuid4(), musicbrainz_id=f"mb-{i}") for i in range(3)]
        session = _FakeSession(mb_watchlist=rows)
        clock = iter([0.0, 1e9, 1e9, 1e9])
        monkeypatch.setattr(svc.time, "monotonic", lambda: next(clock))
        res = run_release_upcoming_poll(
            lambda: session, mode="musicbrainz", mb_search=lambda *a: [],
            tick_index=0, today=TODAY,
        )
        assert res["budget_stop"] == 1 and res["polled"] == 1


def _col(cid=111, name="Upcoming", rdate="2026-08-01T07:00:00Z"):
    return {
        "wrapperType": "collection", "collectionId": cid,
        "collectionName": name, "releaseDate": rdate,
    }


class _FakeItunes:
    def __init__(self, upc_map=None, albums=None, raise_on_lookup=False):
        self.upc_map = upc_map or {}
        self.albums = albums or []
        self.raise_on_lookup = raise_on_lookup
        self.upc_calls = []
        self.album_calls = []

    def lookup_artist_by_upc(self, upc):
        self.upc_calls.append(upc)
        if self.raise_on_lookup:
            raise RuntimeError("iTunes 503")
        return self.upc_map.get(upc)

    def get_artist_albums(self, artist_id):
        self.album_calls.append(artist_id)
        return self.albums


class TestItunesPass:
    def test_resolved_artist_gets_lookup_and_events(self):
        aid = uuid.uuid4()
        session = _FakeSession(
            itunes_watchlist=[
                _Row(artist_id=aid, itunes_id="777", sentinel_stale=None, resolved_via="upc")
            ]
        )
        client = _FakeItunes(albums=[
            _col(1, "Future - Single", "2026-08-01T07:00:00Z"),
            _col(2, "Past", "2020-01-01T08:00:00Z"),      # past → dropped
            _col(3, "Too Far", "2027-08-01T07:00:00Z"),   # beyond 180 d → dropped
        ])
        res = run_release_upcoming_poll(
            lambda: session, mode="itunes", itunes_client=client,
            tick_index=0, today=TODAY,
        )
        assert client.album_calls == ["777"] and not client.upc_calls
        assert res["found"] == 1
        (_, params), = session.sql_of(lambda s: "INSERT INTO artist_release_events" in s)
        assert params["source"] == "itunes" and params["source_key"] == "1"
        assert params["release_type"] == "single" and params["release_date"] == "2026-08-01"

    def test_upc_chain_falls_back_to_next_newest(self):
        aid = uuid.uuid4()
        session = _FakeSession(
            itunes_watchlist=[_Row(artist_id=aid, itunes_id=None, sentinel_stale=None, resolved_via=None)],
            upc_rows=[_Row(artist_id=aid, upc="upc-new"), _Row(artist_id=aid, upc="upc-old")],
        )
        client = _FakeItunes(upc_map={"upc-old": "999"})  # newest misses
        res = run_release_upcoming_poll(
            lambda: session, mode="itunes", itunes_client=client,
            tick_index=0, today=TODAY,
        )
        assert client.upc_calls == ["upc-new", "upc-old"]
        assert client.album_calls == ["999"] and res["resolved"] == 1
        (_, params), = session.sql_of(lambda s: "INSERT INTO artist_source_ids" in s)
        assert params == {"artist_id": aid, "source_artist_id": "999", "resolved_via": "upc"}

    def test_all_upcs_miss_writes_sentinel(self):
        aid = uuid.uuid4()
        session = _FakeSession(
            itunes_watchlist=[_Row(artist_id=aid, itunes_id=None, sentinel_stale=None, resolved_via=None)],
            upc_rows=[_Row(artist_id=aid, upc="upc-1")],
        )
        client = _FakeItunes(upc_map={})
        res = run_release_upcoming_poll(
            lambda: session, mode="itunes", itunes_client=client,
            tick_index=0, today=TODAY,
        )
        assert res["resolve_miss"] == 1 and not client.album_calls
        (_, params), = session.sql_of(lambda s: "INSERT INTO artist_source_ids" in s)
        assert params["source_artist_id"] == ITUNES_ID_NOT_FOUND
        assert params["resolved_via"] == ITUNES_ID_NOT_FOUND

    def test_no_upc_writes_sentinel_without_http(self):
        aid = uuid.uuid4()
        session = _FakeSession(
            itunes_watchlist=[_Row(artist_id=aid, itunes_id=None, sentinel_stale=None, resolved_via=None)]
        )
        client = _FakeItunes()
        res = run_release_upcoming_poll(
            lambda: session, mode="itunes", itunes_client=client,
            tick_index=0, today=TODAY,
        )
        assert res["no_upc"] == 1 and not client.upc_calls and not client.album_calls
        (_, params), = session.sql_of(lambda s: "INSERT INTO artist_source_ids" in s)
        assert params["source_artist_id"] == ITUNES_ID_NOT_FOUND
        # Step 5 부수 픽스 #2: the no-UPC kind is now distinguishable from a
        # lookup miss so it can bypass the 30 d retry gate.
        assert params["resolved_via"] == RESOLVED_VIA_NO_UPC

    def test_no_upc_sentinel_rechecked_without_retry_gate(self):
        # FRESH (not stale) no_upc sentinel + a UPC that has since appeared
        # (OQ5 widened the catalog) → re-resolved immediately, no 30 d wait.
        aid = uuid.uuid4()
        session = _FakeSession(
            itunes_watchlist=[
                _Row(artist_id=aid, itunes_id=ITUNES_ID_NOT_FOUND,
                     sentinel_stale=False, resolved_via=RESOLVED_VIA_NO_UPC),
            ],
            upc_rows=[_Row(artist_id=aid, upc="upc-new")],
        )
        client = _FakeItunes(upc_map={"upc-new": "321"})
        res = run_release_upcoming_poll(
            lambda: session, mode="itunes", itunes_client=client,
            tick_index=0, today=TODAY,
        )
        assert res["resolved"] == 1 and res["sentinel_skip"] == 0
        assert client.upc_calls == ["upc-new"] and client.album_calls == ["321"]
        (_, params), = session.sql_of(lambda s: "INSERT INTO artist_source_ids" in s)
        assert params["source_artist_id"] == "321" and params["resolved_via"] == "upc"

    def test_unchanged_no_upc_sentinel_not_rewritten(self):
        # Still no UPC on recheck → zero HTTP AND zero write churn.
        aid = uuid.uuid4()
        session = _FakeSession(
            itunes_watchlist=[
                _Row(artist_id=aid, itunes_id=ITUNES_ID_NOT_FOUND,
                     sentinel_stale=False, resolved_via=RESOLVED_VIA_NO_UPC),
            ],
        )
        client = _FakeItunes()
        res = run_release_upcoming_poll(
            lambda: session, mode="itunes", itunes_client=client,
            tick_index=0, today=TODAY,
        )
        assert res["no_upc"] == 1
        assert not client.upc_calls and not client.album_calls
        assert not session.sql_of(lambda s: "INSERT INTO artist_source_ids" in s)

    def test_legacy_not_found_via_keeps_retry_gate(self):
        # Pre-Step-5 rows wrote resolved_via='not_found' for both kinds; they
        # keep the 30 d gate (the one-time ops DELETE clears the no-UPC ones).
        aid = uuid.uuid4()
        session = _FakeSession(
            itunes_watchlist=[
                _Row(artist_id=aid, itunes_id=ITUNES_ID_NOT_FOUND,
                     sentinel_stale=False, resolved_via=ITUNES_ID_NOT_FOUND),
            ],
            upc_rows=[_Row(artist_id=aid, upc="upc-x")],
        )
        client = _FakeItunes(upc_map={"upc-x": "999"})
        res = run_release_upcoming_poll(
            lambda: session, mode="itunes", itunes_client=client,
            tick_index=0, today=TODAY,
        )
        assert res["sentinel_skip"] == 1 and res["resolved"] == 0
        assert not client.upc_calls

    def test_fresh_sentinel_skipped_stale_sentinel_retried(self):
        fresh, stale = uuid.uuid4(), uuid.uuid4()
        session = _FakeSession(
            itunes_watchlist=[
                _Row(artist_id=fresh, itunes_id=ITUNES_ID_NOT_FOUND,
                     sentinel_stale=False, resolved_via=ITUNES_ID_NOT_FOUND),
                _Row(artist_id=stale, itunes_id=ITUNES_ID_NOT_FOUND,
                     sentinel_stale=True, resolved_via=ITUNES_ID_NOT_FOUND),
            ],
            upc_rows=[_Row(artist_id=stale, upc="upc-s")],
        )
        client = _FakeItunes(upc_map={"upc-s": "555"})
        res = run_release_upcoming_poll(
            lambda: session, mode="itunes", itunes_client=client,
            tick_index=0, today=TODAY,
        )
        assert res["sentinel_skip"] == 1 and res["resolved"] == 1
        assert client.upc_calls == ["upc-s"]

    def test_transient_resolution_error_leaves_no_sentinel(self):
        aid = uuid.uuid4()
        session = _FakeSession(
            itunes_watchlist=[_Row(artist_id=aid, itunes_id=None, sentinel_stale=None, resolved_via=None)],
            upc_rows=[_Row(artist_id=aid, upc="upc-1")],
        )
        client = _FakeItunes(raise_on_lookup=True)
        res = run_release_upcoming_poll(
            lambda: session, mode="itunes", itunes_client=client,
            tick_index=0, today=TODAY,
        )
        assert res["errors"] == 1
        assert not session.sql_of(lambda s: "INSERT INTO artist_source_ids" in s)

    def test_source_id_upserts_sorted_by_artist_id(self):
        aids = sorted([uuid.uuid4() for _ in range(3)], key=str, reverse=True)
        session = _FakeSession(
            itunes_watchlist=[
                _Row(artist_id=a, itunes_id=None, sentinel_stale=None, resolved_via=None) for a in aids
            ],
        )
        client = _FakeItunes()
        run_release_upcoming_poll(
            lambda: session, mode="itunes", itunes_client=client,
            tick_index=0, today=TODAY,
        )
        written = [
            str(p["artist_id"])
            for s, p in session.executed
            if "INSERT INTO artist_source_ids" in s
        ]
        assert written == sorted(written)


class TestHandlerDispatch:
    def test_routes_job_and_mode(self, monkeypatch):
        from worker import handler

        calls = []
        monkeypatch.setattr(handler, "_run_release_upcoming_poll", lambda m: calls.append(m))
        assert handler.lambda_handler({"job": "release_upcoming_poll", "mode": "itunes"}, None) == {}
        assert handler.lambda_handler({"job": "release_upcoming_poll"}, None) == {}
        assert calls == ["itunes", "musicbrainz"]

    def test_unknown_mode_rejected_by_service(self):
        with pytest.raises(ValueError):
            run_release_upcoming_poll(lambda: _FakeSession(), mode="spotify")
