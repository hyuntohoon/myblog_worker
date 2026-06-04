# Unit tests for the Spotify listening cache sync (FEAT-member-dashboard Step 3).
#
# Pure-logic tests with a fake session that records executed SQL — they verify the
# distinct-window extraction, known/unknown split, prune, and enqueue wiring. They
# are deliberately blind to real SQL semantics (upsert/prune correctness against
# Postgres lives in tests/integration/test_listening_sync_db.py, gated on TEST_DB_URL —
# per feedback-sa-session-lifecycle-mock-blind).
from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest

from worker.service.listening_sync_service import (
    run_listening_sync,
    sync_now_playing,
    sync_recent_albums,
)


class _Row:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class _Result:
    def __init__(self, rows=None, rowcount=0):
        self._rows = rows or []
        self.rowcount = rowcount

    def fetchall(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None


class _FakeSession:
    """Records execute() calls; answers album lookups from `known_map`."""

    def __init__(self, known_map):
        self.known_map = known_map  # spotify_id -> uuid
        self.executed = []  # list[(sql, params)]

    def execute(self, stmt, params=None):
        sql = str(stmt)
        params = params or {}
        self.executed.append((sql, params))
        if "FROM albums WHERE spotify_id = ANY" in sql:
            sids = params["sids"]
            rows = [_Row(id=self.known_map[s], spotify_id=s) for s in sids if s in self.known_map]
            return _Result(rows=rows)
        if "FROM albums WHERE spotify_id = :sid" in sql:
            sid = params.get("sid")
            return _Result(rows=[_Row(id=self.known_map[sid])] if sid in self.known_map else [])
        if sql.strip().startswith("DELETE"):
            return _Result(rowcount=1)
        return _Result()

    # context-manager + transaction shims
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

    def sql_of(self, predicate):
        return [e for e in self.executed if predicate(e[0])]


class _FakeClient:
    def __init__(self, recent=None, now=None):
        self._recent = recent if recent is not None else []
        self._now = now

    def get_recently_played(self, limit=50):
        return self._recent

    def get_currently_playing(self):
        return self._now


def _play(album_id, played_at):
    return {"track": {"album": {"id": album_id}}, "played_at": played_at}


# ── sync_recent_albums ──────────────────────────────────────────────────────────

@pytest.mark.unit
def test_recent_distinct_known_unknown_split_and_prune():
    ua, ub = uuid.uuid4(), uuid.uuid4()
    known = {"A": ua, "B": ub}  # C is unknown
    session = _FakeSession(known)
    client = _FakeClient(recent=[
        _play("A", "2026-06-04T10:00:00Z"),
        _play("C", "2026-06-04T09:30:00Z"),  # unknown
        _play("B", "2026-06-04T09:00:00Z"),
        _play("A", "2026-06-04T08:00:00Z"),  # dup of A — older, must be ignored
    ])
    enqueue = MagicMock()

    res = sync_recent_albums(lambda: session, client, enqueue_unknown=enqueue)

    assert res == {"known": 2, "unknown": 1, "pruned": 1}
    # two upserts (A, B), one prune
    inserts = session.sql_of(lambda s: "INSERT INTO spotify_recent_albums" in s)
    assert len(inserts) == 2
    # A keeps its latest (10:00) play, not the 08:00 dup
    a_insert = next(p for s, p in inserts if p["album_id"] == ua)
    assert a_insert["last_played_at"] == "2026-06-04T10:00:00Z"
    assert session.sql_of(lambda s: s.strip().startswith("DELETE"))
    enqueue.assert_called_once()
    assert enqueue.call_args[0][0] == ["C"]


@pytest.mark.unit
def test_recent_empty_window_leaves_cache_untouched():
    session = _FakeSession({})
    res = sync_recent_albums(lambda: session, _FakeClient(recent=[]))
    assert res == {"known": 0, "unknown": 0, "pruned": 0}
    assert session.executed == []  # no DB writes when nothing came back


@pytest.mark.unit
def test_recent_all_unknown_prunes_whole_cache():
    session = _FakeSession({})  # nothing known
    client = _FakeClient(recent=[_play("X", "2026-06-04T10:00:00Z")])
    enqueue = MagicMock()
    res = sync_recent_albums(lambda: session, client, enqueue_unknown=enqueue)
    assert res["known"] == 0 and res["unknown"] == 1
    # full-table delete branch
    assert any("DELETE FROM spotify_recent_albums" in s and "ANY" not in s for s, _ in session.executed)
    enqueue.assert_called_once_with(["X"])


# ── sync_now_playing ────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_now_playing_active_upserts_track():
    ua = uuid.uuid4()
    session = _FakeSession({"A": ua})
    now = {
        "is_playing": True,
        "progress_ms": 42000,
        "item": {
            "name": "Airbag",
            "duration_ms": 284000,
            "album": {"id": "A", "name": "OK Computer"},
            "artists": [{"name": "Radiohead"}],
        },
    }
    res = sync_now_playing(lambda: session, _FakeClient(now=now))
    assert res == {"is_playing": True}
    ins = session.sql_of(lambda s: "INSERT INTO spotify_now_playing" in s)
    assert len(ins) == 1
    p = ins[0][1]
    assert p["is_playing"] is True
    assert p["track_name"] == "Airbag"
    assert p["artist_name"] == "Radiohead"
    assert p["album_id"] == ua


@pytest.mark.unit
def test_now_playing_nothing_playing_sets_false():
    session = _FakeSession({})
    res = sync_now_playing(lambda: session, _FakeClient(now=None))
    assert res == {"is_playing": False}
    ins = session.sql_of(lambda s: "INSERT INTO spotify_now_playing" in s)
    assert ins and ins[0][1]["is_playing"] is False


# ── run_listening_sync ──────────────────────────────────────────────────────────

@pytest.mark.unit
def test_run_listening_sync_isolates_now_playing_failure():
    """A now-playing failure must not lose the already-committed recent sync."""
    ua = uuid.uuid4()
    session = _FakeSession({"A": ua})

    class _BoomNow(_FakeClient):
        def get_currently_playing(self):
            raise RuntimeError("spotify 500")

    client = _BoomNow(recent=[_play("A", "2026-06-04T10:00:00Z")])
    res = run_listening_sync(lambda: session, client)
    assert res["recent"]["known"] == 1
    assert "error" in res["now_playing"]
