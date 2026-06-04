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

import httpx
import pytest

from worker.clients import spotify_user_client as suc
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

    def __init__(self, known_map, debounce_age_s=None):
        self.known_map = known_map  # spotify_id -> uuid
        self.executed = []  # list[(sql, params)]
        # what the DB-side debounce age query returns: None = bootstrap (no cache
        # row yet), a float = seconds since last write.
        self.debounce_age_s = debounce_age_s

    def execute(self, stmt, params=None):
        sql = str(stmt)
        params = params or {}
        self.executed.append((sql, params))
        if "age_s" in sql:  # _debounce_age_seconds() probe
            return _Result(rows=[_Row(age_s=self.debounce_age_s)])
        if "INSERT INTO spotify_play_events" in sql:  # D29 append — pretend each lands
            return _Result(rowcount=1)
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

    assert res == {"known": 2, "unknown": 1, "pruned": 1, "events": 3}
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


# ── append-only play events (D29) ────────────────────────────────────────────────

@pytest.mark.unit
def test_recent_appends_every_play_to_events():
    """Every play of a catalog-known album is appended to spotify_play_events (one
    row per play, not deduped to latest); unknown albums are skipped. Idempotency on
    rolling-window re-read is covered by the integration test (real ON CONFLICT)."""
    ua, ub = uuid.uuid4(), uuid.uuid4()
    known = {"A": ua, "B": ub}  # C is unknown
    session = _FakeSession(known)
    client = _FakeClient(recent=[
        _play("A", "2026-06-04T10:00:00Z"),
        _play("C", "2026-06-04T09:30:00Z"),  # unknown → no event
        _play("B", "2026-06-04T09:00:00Z"),
        _play("A", "2026-06-04T08:00:00Z"),  # 2nd play of A → its own event (not collapsed)
    ])
    res = sync_recent_albums(lambda: session, client)

    assert res["events"] == 3  # A@10, B@9, A@8 (C skipped)
    inserts = session.sql_of(lambda s: "INSERT INTO spotify_play_events" in s)
    assert len(inserts) == 3
    # album A keeps BOTH plays as distinct events
    a_events = sorted(p["played_at"] for _, p in inserts if p["album_id"] == ua)
    assert a_events == ["2026-06-04T08:00:00Z", "2026-06-04T10:00:00Z"]
    b_events = [p["played_at"] for _, p in inserts if p["album_id"] == ub]
    assert b_events == ["2026-06-04T09:00:00Z"]
    # unknown album C is never recorded as an event
    assert all(p["album_id"] in (ua, ub) for _, p in inserts)


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


@pytest.mark.unit
def test_run_listening_sync_isolates_recent_failure():
    """Symmetric isolation: a recent-albums failure must NOT abort the now-playing
    read (it used to crash the whole tick). recent surfaces an error; now-playing
    still runs and succeeds."""
    session = _FakeSession({})

    class _BoomRecent(_FakeClient):
        def get_recently_played(self, limit=50):
            raise RuntimeError("spotify 500 on recent")

    client = _BoomRecent(recent=[], now=None)  # now-playing: nothing playing → ok
    res = run_listening_sync(lambda: session, client)
    assert "error" in res["recent"]
    assert res["now_playing"] == {"is_playing": False}


# ── manual-refresh debounce (D31) ────────────────────────────────────────────────

class _BoomClient:
    """Any Spotify read is a failure — proves the debounce returned before syncing."""

    def get_recently_played(self, limit=50):
        raise AssertionError("Spotify read despite debounce")

    def get_currently_playing(self):
        raise AssertionError("Spotify read despite debounce")


@pytest.mark.unit
def test_manual_refresh_debounced_when_cache_fresh():
    # cache written 10s ago (< 60s) → manual refresh skips Spotify entirely.
    session = _FakeSession({}, debounce_age_s=10.0)
    res = run_listening_sync(lambda: session, _BoomClient(), is_manual_refresh=True)
    assert res == {"skipped": "debounced"}
    assert not session.sql_of(lambda s: "INSERT INTO" in s)


@pytest.mark.unit
def test_manual_refresh_runs_when_cache_stale():
    # cache written 120s ago (> 60s) → manual refresh proceeds normally.
    ua = uuid.uuid4()
    session = _FakeSession({"A": ua}, debounce_age_s=120.0)
    client = _FakeClient(recent=[_play("A", "2026-06-04T10:00:00Z")], now=None)
    res = run_listening_sync(lambda: session, client, is_manual_refresh=True)
    assert res["recent"]["known"] == 1
    assert session.sql_of(lambda s: "INSERT INTO spotify_recent_albums" in s)


@pytest.mark.unit
def test_manual_refresh_runs_on_bootstrap_empty_cache():
    # no cache row yet (age None) → run (don't debounce the first ever sync).
    ua = uuid.uuid4()
    session = _FakeSession({"A": ua}, debounce_age_s=None)
    client = _FakeClient(recent=[_play("A", "2026-06-04T10:00:00Z")], now=None)
    res = run_listening_sync(lambda: session, client, is_manual_refresh=True)
    assert res["recent"]["known"] == 1


@pytest.mark.unit
def test_cron_never_debounced_even_when_cache_fresh():
    # cron path (is_manual_refresh=False) ignores a fresh cache and always syncs.
    ua = uuid.uuid4()
    session = _FakeSession({"A": ua}, debounce_age_s=5.0)
    client = _FakeClient(recent=[_play("A", "2026-06-04T10:00:00Z")], now=None)
    res = run_listening_sync(lambda: session, client)  # is_manual_refresh defaults False
    assert res["recent"]["known"] == 1
    # the debounce age probe must not even run for the cron path
    assert not session.sql_of(lambda s: "age_s" in s)


# ── transient-failure retry/backoff (RFC: 3 tries, honour Retry-After on 429) ─────

def _resp(status, headers=None, json_body=None):
    """Real httpx.Response so raise_for_status / .json() behave like production."""
    return httpx.Response(
        status,
        headers=headers or {},
        json=json_body,
        request=httpx.Request("GET", "https://api.spotify.com/v1/x"),
    )


def _patch_requests(monkeypatch, responses):
    """Feed `_request_with_retry` a fixed response sequence; record calls + sleeps.
    A response that is an Exception instance is raised (transport-error simulation)."""
    calls: list = []
    sleeps: list = []

    def fake_request(method, url, **kwargs):
        idx = len(calls)
        calls.append((method, url))
        item = responses[idx]
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(suc.httpx, "request", fake_request)
    monkeypatch.setattr(suc.time, "sleep", lambda s: sleeps.append(s))
    return calls, sleeps


@pytest.mark.unit
def test_retry_429_honours_retry_after_then_succeeds(monkeypatch):
    calls, sleeps = _patch_requests(monkeypatch, [
        _resp(429, headers={"Retry-After": "2"}),
        _resp(200, json_body={"ok": True}),
    ])
    r = suc._request_with_retry("GET", "https://api.spotify.com/v1/x")
    assert r.status_code == 200
    assert len(calls) == 2
    assert sleeps == [2.0]  # slept exactly the Retry-After, not a backoff guess


@pytest.mark.unit
def test_retry_5xx_uses_exponential_backoff_then_succeeds(monkeypatch):
    calls, sleeps = _patch_requests(monkeypatch, [
        _resp(503), _resp(502), _resp(200, json_body={"ok": True}),
    ])
    r = suc._request_with_retry("GET", "https://api.spotify.com/v1/x", max_tries=3)
    assert r.status_code == 200
    assert len(calls) == 3
    assert sleeps == [0.5, 1.0]  # BASE_BACKOFF * 2**attempt


@pytest.mark.unit
def test_retry_exhausts_returns_last_failing_response(monkeypatch):
    calls, sleeps = _patch_requests(monkeypatch, [_resp(500), _resp(500), _resp(500)])
    r = suc._request_with_retry("GET", "https://api.spotify.com/v1/x", max_tries=3)
    assert r.status_code == 500
    assert len(calls) == 3  # gave up after 3 tries
    with pytest.raises(httpx.HTTPStatusError):
        r.raise_for_status()  # the caller still sees the failure


@pytest.mark.unit
def test_retry_does_not_retry_non_transient_4xx(monkeypatch):
    # a 400 (e.g. invalid_grant) is permanent → returned on the first try, no sleep
    calls, sleeps = _patch_requests(monkeypatch, [_resp(400, json_body={"error": "invalid_grant"})])
    r = suc._request_with_retry("POST", "https://accounts.spotify.com/api/token")
    assert r.status_code == 400
    assert len(calls) == 1
    assert sleeps == []


@pytest.mark.unit
def test_retry_transport_error_retries_then_reraises(monkeypatch):
    calls, sleeps = _patch_requests(monkeypatch, [
        httpx.ConnectError("boom"), httpx.ConnectError("boom"), httpx.ConnectError("boom"),
    ])
    with pytest.raises(httpx.ConnectError):
        suc._request_with_retry("GET", "https://api.spotify.com/v1/x", max_tries=3)
    assert len(calls) == 3  # retried transport errors up to the cap before giving up


@pytest.mark.unit
def test_get_access_token_invalid_grant_is_not_retried(monkeypatch):
    """The token 400 invalid_grant path stays single-shot: flags re-auth and raises
    without retrying (it's permanent, not transient)."""
    calls, _ = _patch_requests(monkeypatch, [_resp(400, json_body={"error": "invalid_grant"})])
    persisted: dict = {}
    monkeypatch.setattr(suc, "_persist_token_state", lambda **kw: persisted.update(kw))
    client = suc.SpotifyUserClient(
        creds={"client_id": "a", "client_secret": "b", "refresh_token": "c"}
    )
    with pytest.raises(RuntimeError, match="invalid_grant"):
        client._get_access_token()
    assert len(calls) == 1  # not retried
    assert persisted == {"needs_reauth": True}
