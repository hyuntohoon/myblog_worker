# Unit tests for the Last.fm recent-tracks poll (FEAT-multi-user Phase 3a).
#
# Pure-logic: the client parser is exercised with a stubbed HTTP helper; the service
# with a fake session that records executed SQL (blind to real Postgres semantics —
# the NOT-EXISTS dedup / partial-index interaction is validated by a prod dry-run,
# per feedback-sa-session-lifecycle-mock-blind).
from __future__ import annotations

import uuid

import pytest

from worker.clients import lastfm_client as lc
from worker.service.lastfm_sync_service import run_lastfm_sync


class _Resp:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def _recent(tracks):
    return {"recenttracks": {"track": tracks}}


class TestLastfmClientParse:
    def test_splits_scrobbles_and_nowplaying_picks_largest_image(self, monkeypatch):
        payload = _recent([
            {"@attr": {"nowplaying": "true"}, "name": "Live Song",
             "artist": {"#text": "RM", "mbid": "a1"}, "album": {"#text": "Indigo"},
             "image": [{"#text": "s.jpg", "size": "small"},
                       {"#text": "xl.jpg", "size": "extralarge"}]},
            {"name": "Done Song", "artist": {"#text": "IU"}, "album": {"#text": "LILAC"},
             "mbid": "t2", "date": {"uts": "1700000000"}, "image": []},
        ])
        monkeypatch.setattr(lc, "_request_with_retry", lambda *a, **k: _Resp(payload))
        scrobbles, nowplaying = lc.LastfmClient().get_recent_tracks("rj")
        assert nowplaying["track"] == "Live Song" and nowplaying["image"] == "xl.jpg"
        assert nowplaying["artist_mbid"] == "a1"
        assert len(scrobbles) == 1
        assert scrobbles[0]["track"] == "Done Song"
        assert scrobbles[0]["played_at_uts"] == 1700000000

    def test_user_not_found_raises(self, monkeypatch):
        monkeypatch.setattr(
            lc, "_request_with_retry",
            lambda *a, **k: _Resp({"error": 6, "message": "User not found"}, status=404),
        )
        with pytest.raises(lc.LastfmUserNotFound):
            lc.LastfmClient().get_recent_tracks("nope")

    def test_single_track_object_coerced_to_list(self, monkeypatch):
        payload = _recent({"name": "Solo", "artist": {"#text": "X"}, "date": {"uts": "1700000001"}})
        monkeypatch.setattr(lc, "_request_with_retry", lambda *a, **k: _Resp(payload))
        scrobbles, np = lc.LastfmClient().get_recent_tracks("rj")
        assert len(scrobbles) == 1 and np is None

    def test_empty_returns_empty(self, monkeypatch):
        monkeypatch.setattr(lc, "_request_with_retry",
                            lambda *a, **k: _Resp({"recenttracks": {"track": []}}))
        assert lc.LastfmClient().get_recent_tracks("rj") == ([], None)


class _Row:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class _Result:
    def __init__(self, rows=None, rowcount=0):
        self._rows = rows or []
        self.rowcount = rowcount

    def fetchall(self):
        return self._rows


class _FakeSession:
    def __init__(self, connected):
        self.connected = connected  # list[_Row(user_id, username, cursor_uts)]
        self.executed = []  # list[(sql, params)]

    def execute(self, stmt, params=None):
        sql = str(stmt)
        params = params or {}
        self.executed.append((sql, params))
        if "FROM user_integrations ui" in sql:
            return _Result(rows=self.connected)
        if sql.strip().startswith("INSERT INTO lastfm_recent_tracks"):
            return _Result(rowcount=1)
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


class _FakeClient:
    def __init__(self, scrobbles, nowplaying=None, raise_exc=None):
        self._s = scrobbles
        self._np = nowplaying
        self._exc = raise_exc
        self.calls = []

    def get_recent_tracks(self, username, from_uts=None):
        self.calls.append((username, from_uts))
        if self._exc:
            raise self._exc
        return self._s, self._np


def _scrobble(uts=1700000000):
    return {"artist": "IU", "track": "Done", "album": "LILAC", "artist_mbid": None,
            "track_mbid": None, "album_mbid": None, "image": None, "played_at_uts": uts}


class TestLastfmSyncService:
    def test_inserts_scrobbles_nowplaying_and_marks_connected(self):
        uid = uuid.uuid4()
        session = _FakeSession([_Row(user_id=uid, username="rj", cursor_uts=None)])
        client = _FakeClient(
            scrobbles=[_scrobble()],
            nowplaying={"artist": "RM", "track": "Live", "album": "Indigo", "artist_mbid": None,
                        "track_mbid": None, "album_mbid": None, "image": None},
        )
        res = run_lastfm_sync(lambda: session, client)
        assert res == {"users": 1, "scrobbles": 1}
        # scrobble insert = INSERT … SELECT … NOT EXISTS (dedup)
        assert session.sql_of(lambda s: "INSERT INTO lastfm_recent_tracks" in s and "NOT EXISTS" in s)
        # now-playing replaced (delete then insert VALUES … TRUE)
        assert session.sql_of(lambda s: "DELETE FROM lastfm_recent_tracks" in s and "is_now_playing" in s)
        assert session.sql_of(lambda s: "VALUES" in s and "TRUE" in s)
        upd = session.sql_of(lambda s: "UPDATE user_integrations" in s)
        assert upd and upd[0][1]["status"] == "connected"
        assert client.calls == [("rj", None)]  # no cursor → full fetch

    def test_incremental_cursor_becomes_from_plus_one(self):
        uid = uuid.uuid4()
        session = _FakeSession([_Row(user_id=uid, username="rj", cursor_uts=1700000000)])
        client = _FakeClient(scrobbles=[])
        run_lastfm_sync(lambda: session, client)
        assert client.calls == [("rj", 1700000001)]

    def test_user_not_found_sets_status_error(self):
        uid = uuid.uuid4()
        session = _FakeSession([_Row(user_id=uid, username="nope", cursor_uts=None)])
        client = _FakeClient(scrobbles=[], raise_exc=lc.LastfmUserNotFound("nope"))
        run_lastfm_sync(lambda: session, client)
        upd = session.sql_of(lambda s: "UPDATE user_integrations" in s)
        assert upd[0][1]["status"] == "error"

    def test_transient_error_keeps_connected(self):
        uid = uuid.uuid4()
        session = _FakeSession([_Row(user_id=uid, username="rj", cursor_uts=None)])
        client = _FakeClient(scrobbles=[], raise_exc=RuntimeError("network"))
        run_lastfm_sync(lambda: session, client)
        upd = session.sql_of(lambda s: "UPDATE user_integrations" in s)
        assert upd[0][1]["status"] == "connected"

    def test_no_connected_users_is_noop(self):
        session = _FakeSession([])
        assert run_lastfm_sync(lambda: session, _FakeClient(scrobbles=[])) == {"users": 0, "scrobbles": 0}
