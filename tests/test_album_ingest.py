# Unit tests for the scheduled album-catalog ingest (FEAT-album-catalog-ingest Step 2)
# + the release-day confirm wiring (FEAT-release-calendar Step 5).
#
# Pure-logic tests with a fake session recording executed SQL (same harness style as
# test_listening_sync.py): they pin the discovery → INGEST_SINCE filter → dedup →
# popularity gate → bounded enqueue wiring, the stateless day-bucket rotation, the
# OQ5 watchlist include_groups widening, and the confirm candidate window.
# Deliberately blind to real SQL semantics — the upsert side already has its own
# integration coverage via the SQS consumer path.
from __future__ import annotations

from datetime import date

import pytest

import worker.service.album_ingest_service as ais
from worker.service.album_ingest_service import (
    _confirm_candidate,
    _release_date_key,
    _spotify_release_type,
    run_album_ingest,
)

TODAY = date(2026, 7, 13)


class _Row:
    def __init__(self, *vals, **attrs):
        self._vals = vals
        self.__dict__.update(attrs)

    def __getitem__(self, i):
        return self._vals[i]


class _Result:
    def __init__(self, rows=None, scalar=None, rowcount=1):
        self._rows = rows or []
        self._scalar = scalar
        self.rowcount = rowcount

    def fetchall(self):
        return self._rows

    def scalar(self):
        return self._scalar


class _FakeSession:
    def __init__(self, album_count=0, eligible=(), known=(), events=()):
        # eligible: (spotify_id, watch) pairs — artist_id derived as f"{sid}-id"
        self.album_count = album_count
        self.eligible = list(eligible)
        self.known = set(known)          # album spotify_ids already in DB
        self.events = list(events)       # canned artist_release_events rows
        self.executed = []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.executed.append((sql, params or {}))
        if "count(*) FROM albums" in sql:
            return _Result(scalar=self.album_count)
        if "FROM artist_release_events" in sql:
            return _Result(rows=self.events)
        if "FROM artists" in sql:
            assert (params or {}).get("pop_min") is not None
            assert (params or {}).get("watch_min") is not None
            return _Result(
                rows=[_Row(sid, f"{sid}-id", watch) for sid, watch in self.eligible]
            )
        if "FROM albums WHERE spotify_id = ANY" in sql:
            sids = params["sids"]
            return _Result(rows=[_Row(s) for s in sids if s in self.known])
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


class _FakeCatalog:
    """get_artist_albums → canned simplified albums; get_albums → full albums."""

    def __init__(self, discographies, full_albums):
        self.discographies = discographies      # artist_sid -> [ {id, release_date} ]
        self.full_albums = full_albums          # album_id -> popularity
        self.artist_calls = []                  # (artist_sid, include_groups)
        self.albums_calls = []                  # list of id-lists

    def get_artist_albums(self, artist_sid, include_groups="album", **kw):
        self.artist_calls.append((artist_sid, include_groups))
        return self.discographies.get(artist_sid, [])

    def get_albums(self, ids, market=None):
        self.albums_calls.append(list(ids))
        return [{"id": i, "popularity": self.full_albums.get(i, 0)} for i in ids]


def _run(session, catalog, *, day=0):
    enqueued = []
    counters = run_album_ingest(
        lambda: session, catalog, enqueued.extend, days_since_epoch=day, today=TODAY
    )
    return counters, enqueued


@pytest.mark.unit
def test_happy_path_filters_known_and_gates_low_pop(monkeypatch):
    monkeypatch.setattr(ais.settings, "INGEST_SINCE", "2026-06-10")
    session = _FakeSession(
        album_count=900,
        eligible=[("artA", False), ("artB", False)],
        known={"already-synced"},
    )
    catalog = _FakeCatalog(
        discographies={
            "artA": [
                {"id": "new-hot", "release_date": "2026-07-01"},
                {"id": "already-synced", "release_date": "2026-06-20"},
                {"id": "old-classic", "release_date": "2020-01-01"},
            ],
            "artB": [
                {"id": "new-flop-variant", "release_date": "2026-06-15"},
            ],
        },
        full_albums={"new-hot": 55, "new-flop-variant": 3},
    )
    counters, enqueued = _run(session, catalog)

    assert enqueued == ["new-hot"]  # old filtered, known deduped, low-pop gated
    assert counters == {
        "eligible": 2, "swept": 2, "discovered": 4, "fresh": 3,
        "novel": 2, "passed_gate": 1, "enqueued": 1,
        "confirm_candidates": 0, "confirm_flipped": 0, "confirm_inserted": 0,
        "confirm_gate_skipped": 0,
    }
    # non-watchlist artists keep the full-length-only sweep (OQ5 scope bound)
    assert all(g == "album" for _, g in catalog.artist_calls)
    # the popularity probe only multi-gets novel ids, never known ones
    assert catalog.albums_calls == [["new-hot", "new-flop-variant"]]
    # no confirm traffic for non-watchlist artists
    assert not session.sql_of(lambda s: "artist_release_events" in s)


@pytest.mark.unit
def test_catalog_cap_short_circuits_without_spotify_calls(monkeypatch):
    monkeypatch.setattr(ais.settings, "MAX_CATALOG_ALBUMS", 100)
    session = _FakeSession(album_count=100, eligible=[("artA", False)])
    catalog = _FakeCatalog({"artA": [{"id": "x", "release_date": "2026-07-01"}]}, {"x": 99})
    counters, enqueued = _run(session, catalog)

    assert enqueued == []
    assert counters["swept"] == 0
    assert catalog.artist_calls == []  # no Spotify traffic at all


@pytest.mark.unit
def test_release_date_precision_year_and_month(monkeypatch):
    monkeypatch.setattr(ais.settings, "INGEST_SINCE", "2026-01-01")
    # Spotify precision variants: bare year and year-month must compare sanely
    session = _FakeSession(album_count=0, eligible=[("artA", False)])
    catalog = _FakeCatalog(
        discographies={
            "artA": [
                {"id": "year-only-new", "release_date": "2026"},
                {"id": "month-only-old", "release_date": "2025-12"},
            ],
        },
        full_albums={"year-only-new": 80},
    )
    counters, enqueued = _run(session, catalog)
    assert enqueued == ["year-only-new"]
    assert counters["fresh"] == 1


@pytest.mark.unit
def test_per_tick_enqueue_cap(monkeypatch):
    monkeypatch.setattr(ais.settings, "INGEST_SINCE", "2026-01-01")
    monkeypatch.setattr(ais.settings, "MAX_ENQUEUE_PER_TICK", 2)
    session = _FakeSession(album_count=0, eligible=[("artA", False)])
    discog = [{"id": f"alb{i}", "release_date": "2026-05-01"} for i in range(5)]
    catalog = _FakeCatalog({"artA": discog}, {f"alb{i}": 50 for i in range(5)})
    counters, enqueued = _run(session, catalog)

    assert counters["passed_gate"] == 5
    assert counters["enqueued"] == 2
    assert enqueued == ["alb0", "alb1"]


@pytest.mark.unit
def test_day_bucket_rotation_covers_all_and_wraps(monkeypatch):
    monkeypatch.setattr(ais.settings, "INGEST_SINCE", "2026-01-01")
    monkeypatch.setattr(ais.settings, "SWEEP_ARTISTS_PER_TICK", 2)
    eligible = [(f"a{i}", False) for i in range(1, 6)]  # 3 buckets: [a1,a2] [a3,a4] [a5]

    swept_by_day = {}
    for day in range(4):  # day 3 must wrap back to bucket 0
        session = _FakeSession(album_count=0, eligible=eligible)
        catalog = _FakeCatalog({}, {})
        _run(session, catalog, day=day)
        swept_by_day[day] = [sid for sid, _ in catalog.artist_calls]

    assert swept_by_day[0] == ["a1", "a2"]
    assert swept_by_day[1] == ["a3", "a4"]
    assert swept_by_day[2] == ["a5"]
    assert swept_by_day[3] == ["a1", "a2"]  # wrapped
    # one full cycle visits every eligible artist exactly once
    assert sorted(swept_by_day[0] + swept_by_day[1] + swept_by_day[2]) == [
        s for s, _ in eligible
    ]


@pytest.mark.unit
def test_collab_album_deduped_within_tick(monkeypatch):
    monkeypatch.setattr(ais.settings, "INGEST_SINCE", "2026-01-01")
    session = _FakeSession(album_count=0, eligible=[("artA", False), ("artB", False)])
    collab = {"id": "joint", "release_date": "2026-05-05"}
    catalog = _FakeCatalog({"artA": [collab], "artB": [collab]}, {"joint": 70})
    counters, enqueued = _run(session, catalog)

    assert enqueued == ["joint"]
    assert catalog.albums_calls == [["joint"]]  # probed once, not per appearing artist


@pytest.mark.unit
def test_release_date_key_padding():
    assert _release_date_key("2026-03-27") == "2026-03-27"
    assert _release_date_key("2026-03") == "2026-03-01"
    assert _release_date_key("2026") == "2026-01-01"
    assert _release_date_key("") == "0000-01-01"


# --- FEAT-release-calendar Step 5: OQ5 widening + confirm wiring -------------


@pytest.mark.unit
def test_watchlist_artist_sweeps_singles_non_watchlist_albums_only(monkeypatch):
    monkeypatch.setattr(ais.settings, "INGEST_SINCE", "2026-06-10")
    session = _FakeSession(eligible=[("watchA", True), ("plainB", False)])
    catalog = _FakeCatalog({}, {})
    _run(session, catalog)

    assert catalog.artist_calls == [
        ("watchA", "album,single"),  # OQ5: singles/EPs join the watchlist sweep
        ("plainB", "album"),         # volume increase stays watchlist-scoped
    ]


@pytest.mark.unit
def test_confirm_candidate_window_and_full_date_gate():
    win_lo, win_hi = date(2026, 4, 14), date(2027, 1, 9)

    def cand(**kw):
        alb = {"id": "alb-1", "name": "X", "release_date": "2026-07-13", **kw}
        return _confirm_candidate("aid", alb, win_lo, win_hi)

    assert cand() is not None
    assert cand(release_date="2026-04-14") is not None   # lookback edge in
    assert cand(release_date="2026-04-13") is None       # beyond lookback
    assert cand(release_date="2027-01-10") is None       # beyond horizon
    assert cand(release_date="2026-07") is None          # partial date
    assert cand(release_date="2026") is None
    assert cand(id=None) is None
    assert cand(name=None) is None
    c = cand(album_type="single")
    assert c["release_type"] == "single"


@pytest.mark.unit
def test_spotify_release_type_mapping():
    assert _spotify_release_type("album") == "album"
    assert _spotify_release_type("single") == "single"
    assert _spotify_release_type("compilation") == "other"
    assert _spotify_release_type(None) is None


@pytest.mark.unit
def test_never_announced_watchlist_release_inserts_spotify_released_row(monkeypatch):
    monkeypatch.setattr(ais.settings, "INGEST_SINCE", "2026-06-10")
    session = _FakeSession(
        eligible=[("watchA", True)],
        known={"day0-album"},  # already cataloged → not novel, confirm still runs
    )
    catalog = _FakeCatalog(
        {"watchA": [{"id": "day0-album", "name": "Fresh Drop",
                     "album_type": "album", "release_date": "2026-07-13"}]},
        {"day0-album": 55},  # clears ALBUM_POP_MIN — insert gate passes
    )
    counters, _ = _run(session, catalog)

    assert counters["confirm_candidates"] == 1
    assert counters["confirm_inserted"] == 1 and counters["confirm_flipped"] == 0
    assert counters["confirm_gate_skipped"] == 0
    # already-known candidate wasn't in the novel probe → gate fetches it
    assert catalog.albums_calls == [["day0-album"]]
    (sql, params), = session.sql_of(
        lambda s: "INSERT INTO artist_release_events" in s
    )
    assert params["source_key"] == "day0-album"
    assert params["spotify_album_id"] == "day0-album"
    assert params["artist_id"] == "watchA-id"
    assert "'spotify'" in sql and "'released'" in sql


@pytest.mark.unit
def test_below_gate_never_announced_candidate_skipped(monkeypatch):
    """Owner 2026-07-13: credit-farm compilations (watchlist artist credited as
    primary, low album popularity) must not reach the calendar via the confirm
    insert path — first live tick put 103 ungated rows in."""
    monkeypatch.setattr(ais.settings, "INGEST_SINCE", "2026-06-10")
    session = _FakeSession(eligible=[("watchA", True)], known={"comp-farm"})
    catalog = _FakeCatalog(
        {"watchA": [{"id": "comp-farm", "name": "065 Piano Essentials",
                     "album_type": "album", "release_date": "2026-07-01"}]},
        {"comp-farm": 2},  # below ALBUM_POP_MIN
    )
    counters, _ = _run(session, catalog)

    assert counters["confirm_candidates"] == 1
    assert counters["confirm_inserted"] == 0
    assert counters["confirm_gate_skipped"] == 1
    assert not session.sql_of(lambda s: "INSERT INTO artist_release_events" in s)


@pytest.mark.unit
def test_announced_rows_flip_to_released_all_sources(monkeypatch):
    monkeypatch.setattr(ais.settings, "INGEST_SINCE", "2026-06-10")
    events = [
        _Row(id="ev-mb", artist_id="watchA-id", title="Fresh Drop",
             release_date=date(2026, 7, 11), status="announced", spotify_album_id=None),
        _Row(id="ev-it", artist_id="watchA-id", title="Fresh Drop - Single",
             release_date=date(2026, 7, 13), status="announced", spotify_album_id=None),
        _Row(id="ev-other", artist_id="watchA-id", title="Different Thing",
             release_date=date(2026, 7, 13), status="announced", spotify_album_id=None),
    ]
    session = _FakeSession(eligible=[("watchA", True)], events=events)
    catalog = _FakeCatalog(
        {"watchA": [{"id": "sp-1", "name": "Fresh  Drop",  # ws-collapse must match
                     "album_type": "single", "release_date": "2026-07-13"}]},
        {},
    )
    counters, _ = _run(session, catalog)

    assert counters["confirm_flipped"] == 1  # one UPDATE (rowcount fake = 1)
    assert counters["confirm_inserted"] == 0
    (sql, params), = session.sql_of(lambda s: "UPDATE artist_release_events" in s)
    # both matching source rows collapse onto the confirmed spotify id, sorted
    assert params["ids"] == ["ev-it", "ev-mb"]
    assert params["spotify_album_id"] == "sp-1"
    # flip guard keeps re-confirms rowcount-silent
    assert "IS DISTINCT FROM" in sql
    assert not session.sql_of(lambda s: "INSERT INTO artist_release_events" in s)
