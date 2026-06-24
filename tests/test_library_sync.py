# Unit tests for the Spotify Library two-way reconcile (FEAT-spotify-library-sync
# Step 2). Pure-logic tests with a stateful fake session that emulates the four
# tables the reconcile touches (review_buckets, review_bucket_items, albums,
# spotify_library_albums) closely enough to exercise ADD / REMOVE / PULL diffs, the
# never-delete-preexisting rule, plan-only gating, and idempotent re-runs. The
# Spotify client is fully MOCKED (no network). Real SQL/upsert semantics against
# Postgres are out of scope here (feedback-sa-session-lifecycle-mock-blind); the
# _insert_bucket_item ON CONFLICT/partial-index path is covered on a real engine by
# tests/integration/test_library_sync_db.py.
from __future__ import annotations

import copy
import uuid
from typing import Any, Dict, List, Optional

import pytest

from worker.service.library_sync_service import run_library_sync


class _Row:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class _Result:
    def __init__(self, rows=None):
        self._rows = rows or []

    def fetchall(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None


class _FakeStore:
    """In-memory mirror of the tables the reconcile reads/writes. Shared across the
    (possibly several) sessions a single run_library_sync opens, so writes persist
    like a real DB and a re-run sees them."""

    def __init__(self):
        # albums: spotify_id <-> uuid
        self.albums: Dict[str, Any] = {}          # spotify_id -> album uuid
        self.album_sid: Dict[Any, str] = {}       # album uuid -> spotify_id
        self.bucket_id: Optional[Any] = None      # the special bucket's id (None = absent)
        self.items: List[Dict[str, Any]] = []     # review_bucket_items rows
        self.side: Dict[Any, Dict[str, Any]] = {} # album uuid -> spotify_library_albums row

    def add_album(self, spotify_id: str) -> Any:
        aid = uuid.uuid4()
        self.albums[spotify_id] = aid
        self.album_sid[aid] = spotify_id
        return aid

    def add_bucket_item(self, album_uuid: Any, position: int = 0) -> None:
        self.items.append({"bucket_id": self.bucket_id, "album_id": album_uuid, "position": position})


class _FakeSession:
    def __init__(self, store: _FakeStore):
        self.store = store

    def execute(self, stmt, params=None):
        sql = " ".join(str(stmt).split())  # collapse whitespace for substring matching
        p = params or {}
        s = self.store

        if "FROM review_buckets WHERE kind = 'spotify_library'" in sql:
            return _Result([_Row(id=s.bucket_id)] if s.bucket_id is not None else [])

        if "FROM albums WHERE spotify_id = ANY" in sql:
            rows = [_Row(id=s.albums[sid], spotify_id=sid) for sid in p["sids"] if sid in s.albums]
            return _Result(rows)

        if "SELECT album_id FROM review_bucket_items WHERE bucket_id" in sql:
            rows = [_Row(album_id=it["album_id"]) for it in s.items if it["bucket_id"] == p["bid"]]
            return _Result(rows)

        if "FROM albums WHERE id = ANY" in sql:
            ids = set(p["ids"])
            rows = [_Row(id=aid, spotify_id=s.album_sid.get(aid)) for aid in ids if aid in s.album_sid]
            return _Result(rows)

        if "SELECT album_id, source FROM spotify_library_albums" in sql:
            return _Result([_Row(album_id=aid, source=row["source"]) for aid, row in s.side.items()])

        if "MAX(position)" in sql:
            positions = [it["position"] for it in s.items if it["bucket_id"] == p["bid"]]
            return _Result([_Row(pos=(max(positions) + 1) if positions else 0)])

        if "INSERT INTO spotify_library_albums" in sql:
            aid = p["album_id"]
            existing = s.side.get(aid)
            if existing is None:
                s.side[aid] = {
                    "spotify_id": p["spotify_id"],
                    "source": p["source"] or "myblog_added",  # COALESCE default
                    "state": p["state"] or "pending",
                    "in_bucket": p["in_bucket"] if p["in_bucket"] is not None else True,
                    "in_spotify": p["in_spotify"] if p["in_spotify"] is not None else False,
                    "last_error": p["last_error"],
                    "synced": bool(p["stamp_synced"]),
                }
            else:
                # ON CONFLICT DO UPDATE — source is IMMUTABLE (never overwritten).
                existing["spotify_id"] = p["spotify_id"]
                if p["state"] is not None:
                    existing["state"] = p["state"]
                if p["in_bucket"] is not None:
                    existing["in_bucket"] = p["in_bucket"]
                if p["in_spotify"] is not None:
                    existing["in_spotify"] = p["in_spotify"]
                existing["last_error"] = p["last_error"]
                if p["stamp_synced"]:
                    existing["synced"] = True
            return _Result()

        if "UPDATE spotify_library_albums SET state" in sql and "WHERE album_id" in sql:
            row = s.side.get(p["album_id"])
            if row is not None:
                row["state"] = p["state"]
                row["last_error"] = p["last_error"]
                row["synced"] = True
            return _Result()

        if "UPDATE spotify_library_albums SET state = 'needs_attention'" in sql and "WHERE state = 'pending'" in sql:
            for row in s.side.values():
                if row["state"] == "pending":
                    row["state"] = "needs_attention"
            return _Result()

        if "INSERT INTO review_bucket_items" in sql:
            # ON CONFLICT (bucket_id, album_id) DO NOTHING
            dup = any(it["bucket_id"] == p["bid"] and it["album_id"] == p["album_id"] for it in s.items)
            if not dup:
                s.items.append({"bucket_id": p["bid"], "album_id": p["album_id"], "position": p["position"]})
            return _Result()

        return _Result()

    # context-manager + transaction shims (mirror listening_sync tests)
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

    def begin_nested(self):
        # Emulate a Postgres SAVEPOINT: snapshot the mutable store on enter and,
        # if the block raises, restore it (savepoint rollback) before re-raising —
        # so per-item PULL isolation is actually exercised, not just stubbed.
        return _FakeSession._Savepoint(self.store)

    class _Savepoint:
        def __init__(self, store: "_FakeStore"):
            self._store = store
            self._items = None
            self._side = None

        def __enter__(self):
            self._items = copy.deepcopy(self._store.items)
            self._side = copy.deepcopy(self._store.side)
            return self

        def __exit__(self, exc_type, *a):
            if exc_type is not None:
                self._store.items = self._items
                self._store.side = self._side
            return False  # re-raise so the caller's per-item except records the failure


def _session_factory(store: _FakeStore):
    return lambda: _FakeSession(store)


class _FakeSpotify:
    """Mock SpotifyUser. Records every save/remove call so tests can assert the EXACT
    id sets passed to Spotify (esp. that a preexisting album never reaches remove)."""

    def __init__(self, saved: Optional[List[str]] = None, contains: Optional[Dict[str, bool]] = None):
        self._saved = list(saved or [])
        self._contains = contains or {}
        self.saved_calls: List[List[str]] = []
        self.removed_calls: List[List[str]] = []
        self.contains_calls: List[List[str]] = []

    def get_saved_albums(self):
        return [{"id": sid, "name": f"Album {sid}"} for sid in self._saved]

    def check_saved_albums(self, spotify_ids):
        self.contains_calls.append(list(spotify_ids))
        return {sid: self._contains.get(sid, sid in self._saved) for sid in spotify_ids}

    def save_albums(self, spotify_ids):
        self.saved_calls.append(list(spotify_ids))
        for sid in spotify_ids:
            if sid not in self._saved:
                self._saved.append(sid)

    def remove_albums(self, spotify_ids):
        self.removed_calls.append(list(spotify_ids))
        self._saved = [sid for sid in self._saved if sid not in spotify_ids]


def _all_removed(client: _FakeSpotify) -> List[str]:
    return [sid for call in client.removed_calls for sid in call]


def _all_saved(client: _FakeSpotify) -> List[str]:
    return [sid for call in client.saved_calls for sid in call]


# ── no special bucket ────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_absent_bucket_is_a_noop():
    store = _FakeStore()  # bucket_id stays None
    client = _FakeSpotify(saved=["s1"])
    res = run_library_sync(_session_factory(store), client, writes_enabled=True)
    assert res == {"bucket": "absent"}
    # never mutates Spotify; the worker does NOT create the bucket (backend's job)
    assert client.saved_calls == [] and client.removed_calls == []


# ── never delete a pre-existing album (req 5) ────────────────────────────────────

@pytest.mark.unit
def test_preexisting_album_is_never_removed():
    """An album that was already saved in Spotify BEFORE MyBlog touched it (source=
    'preexisting') and is no longer in the bucket must NEVER be passed to
    remove_albums — even with writes enabled."""
    store = _FakeStore()
    store.bucket_id = uuid.uuid4()
    pre = store.add_album("pre1")
    # pre-existing provenance, already pulled in earlier, now NOT in the bucket
    store.side[pre] = {
        "spotify_id": "pre1", "source": "preexisting", "state": "synced",
        "in_bucket": False, "in_spotify": True, "last_error": None, "synced": True,
    }
    # bucket is empty; Spotify still has the preexisting album saved
    client = _FakeSpotify(saved=["pre1"])

    res = run_library_sync(_session_factory(store), client, writes_enabled=True)

    assert "pre1" not in _all_removed(client)
    assert client.removed_calls == []  # nothing removed at all
    assert res["removed"] == 0
    # it stays preexisting forever (source immutable)
    assert store.side[pre]["source"] == "preexisting"


@pytest.mark.unit
def test_preexisting_not_in_bucket_is_pulled_in_not_removed():
    """A saved-in-Spotify album with NO side row yet, absent from the bucket, is
    PULLED into the bucket (stamped preexisting) — the never-delete rule's other half.
    It must not be removed."""
    store = _FakeStore()
    store.bucket_id = uuid.uuid4()
    aid = store.add_album("p9")
    client = _FakeSpotify(saved=["p9"])

    res = run_library_sync(_session_factory(store), client, writes_enabled=True)

    assert res["pulled"] == 1
    assert _all_removed(client) == []
    # now an item in the bucket + a preexisting side row
    assert any(it["album_id"] == aid for it in store.items)
    assert store.side[aid]["source"] == "preexisting"
    assert store.side[aid]["in_bucket"] is True


# ── ADD diff ─────────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_add_bucket_album_missing_from_spotify_when_writes_enabled():
    """A bucket album NOT saved in Spotify → save_albums (writes enabled). First-touch
    contains-check returns not-saved → source stamped myblog_added."""
    store = _FakeStore()
    store.bucket_id = uuid.uuid4()
    aid = store.add_album("new1")
    store.add_bucket_item(aid)
    client = _FakeSpotify(saved=[], contains={"new1": False})

    res = run_library_sync(_session_factory(store), client, writes_enabled=True)

    assert res["added"] == 1
    assert _all_saved(client) == ["new1"]
    assert store.side[aid]["source"] == "myblog_added"
    assert store.side[aid]["state"] == "synced"
    assert store.side[aid]["in_spotify"] is True
    assert _all_removed(client) == []


# ── REMOVE diff ──────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_remove_myblog_added_album_pulled_out_of_bucket():
    """An album MyBlog added (source='myblog_added'), still saved in Spotify but
    removed from the bucket → remove_albums (writes enabled)."""
    store = _FakeStore()
    store.bucket_id = uuid.uuid4()
    aid = store.add_album("m1")
    store.side[aid] = {
        "spotify_id": "m1", "source": "myblog_added", "state": "synced",
        "in_bucket": True, "in_spotify": True, "last_error": None, "synced": True,
    }
    # NOT added to the bucket items → user pulled it out
    client = _FakeSpotify(saved=["m1"])

    res = run_library_sync(_session_factory(store), client, writes_enabled=True)

    assert res["removed"] == 1
    assert _all_removed(client) == ["m1"]
    assert store.side[aid]["in_bucket"] is False
    assert store.side[aid]["in_spotify"] is False


# ── PULL diff ────────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_pull_saved_album_not_in_bucket_appends_item():
    store = _FakeStore()
    store.bucket_id = uuid.uuid4()
    existing = store.add_album("keep")
    store.add_bucket_item(existing, position=0)
    store.side[existing] = {
        "spotify_id": "keep", "source": "preexisting", "state": "synced",
        "in_bucket": True, "in_spotify": True, "last_error": None, "synced": True,
    }
    pulled = store.add_album("pull1")
    client = _FakeSpotify(saved=["keep", "pull1"])

    res = run_library_sync(_session_factory(store), client, writes_enabled=True)

    assert res["pulled"] == 1
    # appended AFTER the existing item (position 1)
    pull_item = next(it for it in store.items if it["album_id"] == pulled)
    assert pull_item["position"] == 1
    assert store.side[pulled]["source"] == "preexisting"
    # no Spotify mutation for a pure PULL
    assert _all_saved(client) == [] and _all_removed(client) == []


# ── plan-only (writes disabled) ──────────────────────────────────────────────────

@pytest.mark.unit
def test_plan_only_issues_no_spotify_writes_but_updates_db():
    """writes_enabled=False: an ADD candidate and a REMOVE candidate are computed and
    the DB rows advance to 'synced', but NO save/remove call is issued to Spotify."""
    store = _FakeStore()
    store.bucket_id = uuid.uuid4()
    # ADD candidate: bucket album not saved in Spotify, fresh (no side row)
    add_aid = store.add_album("addme")
    store.add_bucket_item(add_aid)
    # REMOVE candidate: myblog_added, saved in Spotify, not in bucket
    rem_aid = store.add_album("removeme")
    store.side[rem_aid] = {
        "spotify_id": "removeme", "source": "myblog_added", "state": "synced",
        "in_bucket": True, "in_spotify": True, "last_error": None, "synced": True,
    }
    client = _FakeSpotify(saved=["removeme"], contains={"addme": False})

    res = run_library_sync(_session_factory(store), client, writes_enabled=False)

    # NO real Spotify mutation
    assert client.saved_calls == []
    assert client.removed_calls == []
    # but the DB intent advanced + counts reported
    assert res["writes_enabled"] is False
    assert res["added"] == 1 and res["removed"] == 1
    assert store.side[add_aid]["state"] == "synced"
    assert store.side[rem_aid]["state"] == "synced"


@pytest.mark.unit
def test_plan_only_never_removes_preexisting_either():
    """Plan-only must apply the same never-delete rule: a preexisting album out of the
    bucket is never listed as a removal (it would be PULLED back instead)."""
    store = _FakeStore()
    store.bucket_id = uuid.uuid4()
    pre = store.add_album("preX")
    store.side[pre] = {
        "spotify_id": "preX", "source": "preexisting", "state": "synced",
        "in_bucket": False, "in_spotify": True, "last_error": None, "synced": True,
    }
    client = _FakeSpotify(saved=["preX"])
    res = run_library_sync(_session_factory(store), client, writes_enabled=False)
    assert res["removed"] == 0
    assert client.removed_calls == []


# ── idempotent re-run ────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_idempotent_rerun_is_a_noop():
    """When bucket intent already equals the Spotify Library, a second pass issues no
    save/remove and pulls nothing new."""
    store = _FakeStore()
    store.bucket_id = uuid.uuid4()
    # one myblog_added album that's both in the bucket and saved in Spotify
    aid = store.add_album("steady")
    store.add_bucket_item(aid)
    store.side[aid] = {
        "spotify_id": "steady", "source": "myblog_added", "state": "synced",
        "in_bucket": True, "in_spotify": True, "last_error": None, "synced": True,
    }
    client = _FakeSpotify(saved=["steady"])

    first = run_library_sync(_session_factory(store), client, writes_enabled=True)
    assert first == {  # nothing to do
        "added": 0, "removed": 0, "pulled": 0, "failed": 0, "skipped_unknown": 0,
        "writes_enabled": True, "needs_reauth": False,
    }
    saved_before = list(client.saved_calls)
    removed_before = list(client.removed_calls)
    items_before = len(store.items)

    second = run_library_sync(_session_factory(store), client, writes_enabled=True)
    assert second == first
    assert client.saved_calls == saved_before  # no new write
    assert client.removed_calls == removed_before
    assert len(store.items) == items_before     # no new PULL


# ── unknown saved album → enqueue + skip ─────────────────────────────────────────

@pytest.mark.unit
def test_unknown_saved_album_enqueued_and_skipped():
    """A saved album not yet in our catalog is enqueued for the candidates→SQS sync
    and skipped this pass (no side row, no PULL)."""
    store = _FakeStore()
    store.bucket_id = uuid.uuid4()
    # only "known" is in the catalog; "ghost" is not
    known = store.add_album("known")
    store.add_bucket_item(known)
    store.side[known] = {
        "spotify_id": "known", "source": "myblog_added", "state": "synced",
        "in_bucket": True, "in_spotify": True, "last_error": None, "synced": True,
    }
    client = _FakeSpotify(saved=["known", "ghost"])
    enqueued: List[List[str]] = []

    res = run_library_sync(
        _session_factory(store), client,
        enqueue_unknown=lambda ids: enqueued.append(list(ids)),
        writes_enabled=True,
    )

    assert res["skipped_unknown"] == 1
    assert enqueued == [["ghost"]]
    # ghost never got a side row or a bucket item
    assert all(it["album_id"] != "ghost" for it in store.items)


# ── token re-auth abort ──────────────────────────────────────────────────────────

@pytest.mark.unit
def test_invalid_grant_aborts_and_flags_pending_rows():
    store = _FakeStore()
    store.bucket_id = uuid.uuid4()
    aid = store.add_album("z")
    store.side[aid] = {
        "spotify_id": "z", "source": "myblog_added", "state": "pending",
        "in_bucket": True, "in_spotify": False, "last_error": None, "synced": False,
    }

    class _RevokedClient(_FakeSpotify):
        def get_saved_albums(self):
            raise RuntimeError("Spotify refresh token rejected (invalid_grant)")

    res = run_library_sync(_session_factory(store), _RevokedClient(), writes_enabled=True)
    assert res == {"needs_reauth": True}
    # the pending row was flipped to needs_attention
    assert store.side[aid]["state"] == "needs_attention"


@pytest.mark.unit
def test_missing_scope_on_read_returns_needs_reauth():
    from worker.clients.spotify_user_client import SpotifyScopeError

    store = _FakeStore()
    store.bucket_id = uuid.uuid4()

    class _NoScopeClient(_FakeSpotify):
        def get_saved_albums(self):
            raise SpotifyScopeError("missing user-library-read")

    res = run_library_sync(_session_factory(store), _NoScopeClient(), writes_enabled=True)
    assert res["needs_reauth"] is True
    assert res.get("reason") == "missing_scope"


# ── per-album write failure isolation ────────────────────────────────────────────

@pytest.mark.unit
def test_save_failure_marks_failed_without_aborting_pull():
    """A save_albums error marks the ADD albums 'failed' (+ last_error) but the pass
    continues — a PULL in the same run still lands."""
    store = _FakeStore()
    store.bucket_id = uuid.uuid4()
    add_aid = store.add_album("boom")
    store.add_bucket_item(add_aid)
    pull_aid = store.add_album("pullok")  # saved, not in bucket → PULL

    class _SaveBoom(_FakeSpotify):
        def save_albums(self, spotify_ids):
            self.saved_calls.append(list(spotify_ids))
            raise RuntimeError("spotify 500 on save")

    client = _SaveBoom(saved=["pullok"], contains={"boom": False})
    res = run_library_sync(_session_factory(store), client, writes_enabled=True)

    assert res["failed"] == 1
    assert store.side[add_aid]["state"] == "failed"
    assert store.side[add_aid]["last_error"] is not None
    # the PULL still happened despite the save failure
    assert res["pulled"] == 1
    assert any(it["album_id"] == pull_aid for it in store.items)
