# Unit tests for the Step 4 demand producers' pure logic and their wiring into the
# member poll (FEAT-lyrics-listening-experience Step 4).
#
# Scope boundary, stated so nobody mistakes a green run here for coverage of the
# producer: everything that touches lyrics_discovery_scopes / lyrics_album_demands is
# real SQL executed by the shared store, and a fake session is blind to all of it
# ([[feedback-sa-session-lifecycle-mock-blind]]). Those properties — reconciliation,
# the generation fence, member isolation — live in
# tests/integration/test_lyrics_member_demand_db.py against real Postgres. What is
# testable here is (a) how raw provider payloads become origin keys, (b) the
# /me/albums pagination contract, and (c) that the poll wires the producer up with the
# right member's token and isolates its failures.
from __future__ import annotations

import base64
import json
import uuid

import httpx
import pytest

from worker.clients.spotify_member_client import (
    SpotifyMemberClient,
    SpotifyMemberScopeError,
)
from worker.service.lyrics_member_demand_service import (
    RECENT_ORIGIN,
    SAVED_ORIGIN,
    _album_ids,
    _recent_album_ids,
)
from worker.service.spotify_member_sync_service import run_spotify_member_sync

CIPHERTEXT_B64 = base64.b64encode(b"kms-envelope-blob").decode()


class TestOriginKeys:
    def test_saved_album_ids_dedupe_and_keep_first_seen_order(self):
        albums = [{"id": "a"}, {"id": "b"}, {"id": "a"}, {"id": "c"}]
        assert _album_ids(albums) == ["a", "b", "c"]

    def test_malformed_album_ids_are_dropped_not_passed_to_the_store(self):
        # V57 CHECKs both the job's provider id and the demand's origin_key at
        # 1..128 chars. A bad id reaching the store aborts the whole transaction and
        # would take every good album in the pass down with it.
        albums = [
            {"id": "good"},
            {"id": "   "},              # whitespace only -> fails btrim length CHECK
            {"id": ""},
            {"id": None},
            {"id": 12345},              # provider returned a non-string
            {},                         # no id at all
            {"id": "x" * 129},          # over the 128-char CHECK
            {"id": "x" * 128},          # exactly at the boundary: kept
        ]
        assert _album_ids(albums) == ["good", "x" * 128]

    def test_album_ids_are_trimmed_before_use_as_an_origin_key(self):
        # The CHECK is on btrim(...), so " a " would pass validation but store a key
        # that a later exact-match removal could never address.
        assert _album_ids([{"id": " a "}, {"id": "a"}]) == ["a"]

    def test_recent_album_ids_skip_items_with_no_album_identity(self):
        items = [
            {"track": {"id": "t1", "album": {"id": "alb1"}}},
            {"track": {"id": "t2", "album": {}}},        # local file: no album id
            {"track": {"id": "t3"}},                      # no album at all
            {"track": None},                              # malformed item
            {},                                           # malformed item
            {"track": {"id": "t4", "album": {"id": "alb1"}}},  # same album again
            {"track": {"id": "t5", "album": {"id": "alb2"}}},
        ]
        assert _recent_album_ids(items) == ["alb1", "alb2"]

    def test_origins_are_distinct_constants(self):
        # They index separate rows in lyrics_discovery_scopes; collapsing them would
        # make an un-save delete the member's listening-derived demand as well.
        assert SAVED_ORIGIN != RECENT_ORIGIN


def _albums_page(ids, next_url, total):
    return {
        "items": [{"added_at": "2026-09-01T00:00:00Z", "album": {"id": i}} for i in ids],
        "next": next_url,
        "total": total,
    }


class TestMemberSavedAlbumsPagination:
    """The RFC requires COMPLETE pagination: a member with more than one page of saved
    albums must not silently contribute only their first 50."""

    def _client(self, handler):
        client = SpotifyMemberClient(creds={"client_id": "c", "client_secret": "s"})
        transport = httpx.MockTransport(handler)
        return client, transport

    def test_reads_every_page_until_next_is_null(self, monkeypatch):
        seen_offsets = []

        def handler(request):
            offset = int(request.url.params["offset"])
            seen_offsets.append(offset)
            assert request.url.params["limit"] == "50"
            if offset == 0:
                return httpx.Response(200, json=_albums_page(
                    [f"a{i}" for i in range(50)], "https://api/next", 120))
            if offset == 50:
                return httpx.Response(200, json=_albums_page(
                    [f"b{i}" for i in range(50)], "https://api/next", 120))
            return httpx.Response(200, json=_albums_page(
                [f"c{i}" for i in range(20)], None, 120))

        client, transport = self._client(handler)
        monkeypatch.setattr(
            "worker.clients.spotify_member_client._request_with_retry",
            lambda method, url, **kw: httpx.Client(transport=transport).request(
                method, url, **kw),
        )
        albums = client.get_saved_albums("at-1")
        assert seen_offsets == [0, 50, 100]
        assert len(albums) == 120
        assert albums[0]["id"] == "a0" and albums[-1]["id"] == "c19"

    def test_total_stops_a_never_null_next(self, monkeypatch):
        """`next` is authoritative but `total` guards a provider that never nulls it —
        without the guard this is an infinite loop, not a wrong answer."""
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            assert calls["n"] <= 5, "pagination did not terminate"
            offset = int(request.url.params["offset"])
            ids = [f"a{offset}"] if offset < 3 else []
            return httpx.Response(200, json=_albums_page(ids, "https://api/next", 3))

        client, transport = self._client(handler)
        monkeypatch.setattr(
            "worker.clients.spotify_member_client._request_with_retry",
            lambda method, url, **kw: httpx.Client(transport=transport).request(
                method, url, **kw),
        )
        assert [a["id"] for a in client.get_saved_albums("at-1")] == ["a0", "a1", "a2"]

    def test_403_is_a_scope_error_not_a_revoked_token(self, monkeypatch):
        client, transport = self._client(lambda r: httpx.Response(403, json={}))
        monkeypatch.setattr(
            "worker.clients.spotify_member_client._request_with_retry",
            lambda method, url, **kw: httpx.Client(transport=transport).request(
                method, url, **kw),
        )
        with pytest.raises(SpotifyMemberScopeError):
            client.get_saved_albums("at-1")


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
        self.connected = connected
        self.executed = []

    def execute(self, stmt, params=None):
        self.executed.append((str(stmt), params or {}))
        if "FROM user_integrations ui" in str(stmt):
            rows = self.connected
            only = (params or {}).get("only")
            if only is not None:
                rows = [r for r in rows if str(r.user_id) == str(only)]
            return _Result(rows=rows[: (params or {}).get("lim", 10)])
        return _Result(rowcount=1)

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


class _FakeKms:
    def __init__(self, by_blob):
        self.by_blob = by_blob

    def decrypt(self, CiphertextBlob):
        return {"Plaintext": self.by_blob[CiphertextBlob].encode()}

    def encrypt(self, KeyId, Plaintext):  # pragma: no cover - no rotation in these tests
        return {"CiphertextBlob": b"new"}


class _TokenScopedClient:
    """Returns DIFFERENT data per access token, so a test can tell whose library the
    producer actually read. Every method asserts it was handed a real token."""

    def __init__(self, by_token, saved_exc=None):
        self.by_token = by_token
        self.saved_exc = saved_exc
        self.saved_calls = []

    def refresh(self, refresh_token):
        return {"access_token": f"at-for-{refresh_token}", "expires_in": 3600}

    def get_player_state(self, access_token):
        return None

    def get_recently_played(self, access_token, limit=50):
        return self.by_token[access_token]["recent"]

    def get_saved_albums(self, access_token):
        self.saved_calls.append(access_token)
        if self.saved_exc is not None:
            raise self.saved_exc
        return self.by_token[access_token]["saved"]


def _payload(ciphertext):
    return json.dumps({
        "v": 1, "ciphertext": ciphertext, "scope": "user-library-read",
        "expires_in": 3600, "obtained_at": "2026-09-09T00:00:00+00:00",
    })


class TestPollWiring:
    """The producer's inputs must come from the member the poll is currently on."""

    def _two_members(self):
        uid_a, uid_b = uuid.uuid4(), uuid.uuid4()
        blob_a, blob_b = b"env-a", b"env-b"
        session = _FakeSession([
            _Row(user_id=uid_a, payload=_payload(base64.b64encode(blob_a).decode())),
            _Row(user_id=uid_b, payload=_payload(base64.b64encode(blob_b).decode())),
        ])
        kms = _FakeKms({blob_a: "refresh-a", blob_b: "refresh-b"})
        client = _TokenScopedClient({
            "at-for-refresh-a": {"saved": [{"id": "alb-a"}], "recent": []},
            "at-for-refresh-b": {"saved": [{"id": "alb-b"}], "recent": []},
        })
        return uid_a, uid_b, session, kms, client

    def test_each_member_library_is_read_with_that_members_own_token(self, monkeypatch):
        uid_a, uid_b, session, kms, client = self._two_members()
        seen = []
        monkeypatch.setattr(
            "worker.service.spotify_member_sync_service.sync_member_demand",
            lambda sf, user_id, *, saved_albums, recent_items: (
                seen.append((user_id, [a["id"] for a in (saved_albums or [])])) or {}),
        )
        run_spotify_member_sync(
            lambda: session, client, kms=kms, kms_key_id="k", demand_enabled=True,
        )
        # The pairing is the point: member A's id must never arrive with B's albums.
        assert seen == [(uid_a, ["alb-a"]), (uid_b, ["alb-b"])]
        assert client.saved_calls == ["at-for-refresh-a", "at-for-refresh-b"]

    def test_only_user_id_narrows_the_pass_to_one_member(self, monkeypatch):
        uid_a, uid_b, session, kms, client = self._two_members()
        seen = []
        monkeypatch.setattr(
            "worker.service.spotify_member_sync_service.sync_member_demand",
            lambda sf, user_id, **kw: (seen.append(user_id) or {}),
        )
        run_spotify_member_sync(
            lambda: session, client, kms=kms, kms_key_id="k", demand_enabled=True,
            only_user_id=str(uid_b), max_users=1,
        )
        assert seen == [uid_b]

    def test_producer_is_off_unless_enabled(self, monkeypatch):
        _, _, session, kms, client = self._two_members()
        calls = []
        monkeypatch.setattr(
            "worker.service.spotify_member_sync_service.sync_member_demand",
            lambda *a, **kw: (calls.append(1) or {}),
        )
        res = run_spotify_member_sync(
            lambda: session, client, kms=kms, kms_key_id="k", demand_enabled=False,
        )
        assert calls == [] and client.saved_calls == []
        assert res["saved_added"] == 0 and res["demand_failed"] == 0

    def test_kill_switch_default_is_read_from_settings(self, monkeypatch):
        """`demand_enabled=None` must resolve to LYRICS_MEMBER_DEMAND_ENABLED.

        Every other test in this class passes `demand_enabled` explicitly, so the seam
        that connects the owner's advertised fast override to the code was the one thing
        none of them touched: replacing the settings read with a bare ``True`` left the
        whole suite green ([[feedback-mutation-test-your-own-new-tests]]). A kill switch
        nothing asserts on is a kill switch nobody can trust to be wired.
        """
        from worker.core.config import settings

        for enabled, expected in ((False, []), (True, [1])):
            _, _, session, kms, client = self._two_members()
            calls = []
            monkeypatch.setattr(
                "worker.service.spotify_member_sync_service.sync_member_demand",
                lambda *a, **kw: (calls.append(1) or {}),
            )
            monkeypatch.setattr(settings, "LYRICS_MEMBER_DEMAND_ENABLED", enabled)
            # demand_enabled deliberately omitted — that is the path under test.
            run_spotify_member_sync(lambda: session, client, kms=kms, kms_key_id="k")
            assert calls[:1] == expected[:1], (
                f"LYRICS_MEMBER_DEMAND_ENABLED={enabled} was not honoured"
            )

    def test_missing_library_scope_skips_saved_but_still_produces_recent(self, monkeypatch):
        uid_a, _, session, kms, _ = self._two_members()
        client = _TokenScopedClient(
            {"at-for-refresh-a": {"saved": [], "recent": [{"track": {"id": "t", "album": {"id": "r1"}}}]},
             "at-for-refresh-b": {"saved": [], "recent": []}},
            saved_exc=SpotifyMemberScopeError("403"),
        )
        seen = []
        monkeypatch.setattr(
            "worker.service.spotify_member_sync_service.sync_member_demand",
            lambda sf, user_id, *, saved_albums, recent_items: (
                seen.append((saved_albums, len(recent_items))) or {}),
        )
        run_spotify_member_sync(
            lambda: session, client, kms=kms, kms_key_id="k", demand_enabled=True,
        )
        # saved_albums is None ("not observed"), NOT [] ("member saved nothing") —
        # the latter would reconcile every saved-origin demand away on a 403.
        assert seen[0][0] is None and seen[0][1] == 1

    def test_transient_library_failure_also_passes_none_not_empty(self, monkeypatch):
        _, _, session, kms, _ = self._two_members()
        client = _TokenScopedClient(
            {"at-for-refresh-a": {"saved": [], "recent": []},
             "at-for-refresh-b": {"saved": [], "recent": []}},
            saved_exc=httpx.ConnectTimeout("boom"),
        )
        seen = []
        monkeypatch.setattr(
            "worker.service.spotify_member_sync_service.sync_member_demand",
            lambda sf, user_id, *, saved_albums, recent_items: (
                seen.append(saved_albums) or {}),
        )
        run_spotify_member_sync(
            lambda: session, client, kms=kms, kms_key_id="k", demand_enabled=True,
        )
        assert seen == [None, None]

    def test_a_broken_producer_never_costs_the_member_their_listening_data(self, monkeypatch):
        """The listening poll predates Step 4 and must survive it."""
        _, _, session, kms, client = self._two_members()

        def boom(*a, **kw):
            raise RuntimeError("demand store unavailable")

        monkeypatch.setattr(
            "worker.service.spotify_member_sync_service.sync_member_demand", boom)
        res = run_spotify_member_sync(
            lambda: session, client, kms=kms, kms_key_id="k", demand_enabled=True,
        )
        assert res["users"] == 2 and res["reauth"] == 0 and res["skipped"] == 0
        assert res["demand_failed"] == 2
        # the listening writes still happened for both members
        touched = [e for e in session.executed if "UPDATE user_integrations" in e[0]
                   and "last_synced_at = now()" in e[0]]
        assert len(touched) == 2
        # and nothing was flipped to reauth over a translation-feature failure
        assert not [e for e in session.executed if "status = 'reauth'" in e[0]]


class TestBootstrapMessageHandling:
    """A message from the queue is input: a malformed one must select NOBODY."""

    def _handler(self, monkeypatch):
        import worker.handler as h
        calls = []
        monkeypatch.setattr(
            "worker.service.spotify_member_sync_service.run_spotify_member_sync",
            lambda *a, **kw: (calls.append(kw) or {}),
        )
        monkeypatch.setattr("worker.clients.spotify_member_client.spotify_member", object())
        return h, calls

    def test_a_valid_user_id_narrows_to_that_member(self, monkeypatch):
        h, calls = self._handler(monkeypatch)
        uid = str(uuid.uuid4())
        h._run_member_demand_bootstrap(uid)
        assert len(calls) == 1 and calls[0]["only_user_id"] == uid

    def test_a_missing_user_id_selects_nobody_rather_than_everybody(self, monkeypatch):
        # only_user_id=None is the "every member" selector — the dangerous default a
        # bare `str(user_id) if user_id else None` would fall into.
        h, calls = self._handler(monkeypatch)
        h._run_member_demand_bootstrap(None)
        assert calls == []

    def test_a_non_uuid_user_id_is_refused_before_it_reaches_sql(self, monkeypatch):
        h, calls = self._handler(monkeypatch)
        for bad in ("", "not-a-uuid", "'; DROP TABLE users; --", 12345, {"a": 1}):
            h._run_member_demand_bootstrap(bad)
        assert calls == []
