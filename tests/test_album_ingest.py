# Unit tests for the scheduled album-catalog ingest (FEAT-album-catalog-ingest Step 2).
#
# Pure-logic tests with a fake session recording executed SQL (same harness style as
# test_listening_sync.py): they pin the discovery → INGEST_SINCE filter → dedup →
# popularity gate → bounded enqueue wiring and the stateless day-bucket rotation.
# Deliberately blind to real SQL semantics — the upsert side already has its own
# integration coverage via the SQS consumer path.
from __future__ import annotations

import pytest

import worker.service.album_ingest_service as ais
from worker.service.album_ingest_service import _release_date_key, run_album_ingest


class _Row:
    def __init__(self, *vals):
        self._vals = vals

    def __getitem__(self, i):
        return self._vals[i]


class _Result:
    def __init__(self, rows=None, scalar=None):
        self._rows = rows or []
        self._scalar = scalar

    def fetchall(self):
        return self._rows

    def scalar(self):
        return self._scalar


class _FakeSession:
    def __init__(self, album_count=0, eligible=(), known=()):
        self.album_count = album_count
        self.eligible = list(eligible)   # artist spotify_ids, pre-sorted
        self.known = set(known)          # album spotify_ids already in DB
        self.executed = []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.executed.append((sql, params or {}))
        if "count(*) FROM albums" in sql:
            return _Result(scalar=self.album_count)
        if "FROM artists" in sql:
            assert (params or {}).get("pop_min") is not None
            return _Result(rows=[_Row(sid) for sid in self.eligible])
        if "FROM albums WHERE spotify_id = ANY" in sql:
            sids = params["sids"]
            return _Result(rows=[_Row(s) for s in sids if s in self.known])
        return _Result()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


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
        lambda: session, catalog, enqueued.extend, days_since_epoch=day
    )
    return counters, enqueued


@pytest.mark.unit
def test_happy_path_filters_known_and_gates_low_pop(monkeypatch):
    monkeypatch.setattr(ais.settings, "INGEST_SINCE", "2026-06-10")
    session = _FakeSession(
        album_count=900,
        eligible=["artA", "artB"],
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
    }
    # full-lengths only: every discography call pinned to include_groups=album
    assert all(g == "album" for _, g in catalog.artist_calls)
    # the popularity probe only multi-gets novel ids, never known ones
    assert catalog.albums_calls == [["new-hot", "new-flop-variant"]]


@pytest.mark.unit
def test_catalog_cap_short_circuits_without_spotify_calls(monkeypatch):
    monkeypatch.setattr(ais.settings, "MAX_CATALOG_ALBUMS", 100)
    session = _FakeSession(album_count=100, eligible=["artA"])
    catalog = _FakeCatalog({"artA": [{"id": "x", "release_date": "2026-07-01"}]}, {"x": 99})
    counters, enqueued = _run(session, catalog)

    assert enqueued == []
    assert counters["swept"] == 0
    assert catalog.artist_calls == []  # no Spotify traffic at all


@pytest.mark.unit
def test_release_date_precision_year_and_month(monkeypatch):
    monkeypatch.setattr(ais.settings, "INGEST_SINCE", "2026-01-01")
    # Spotify precision variants: bare year and year-month must compare sanely
    session = _FakeSession(album_count=0, eligible=["artA"])
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
    session = _FakeSession(album_count=0, eligible=["artA"])
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
    eligible = ["a1", "a2", "a3", "a4", "a5"]  # 3 buckets: [a1,a2] [a3,a4] [a5]

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
    assert sorted(swept_by_day[0] + swept_by_day[1] + swept_by_day[2]) == sorted(eligible)


@pytest.mark.unit
def test_collab_album_deduped_within_tick(monkeypatch):
    monkeypatch.setattr(ais.settings, "INGEST_SINCE", "2026-01-01")
    session = _FakeSession(album_count=0, eligible=["artA", "artB"])
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
