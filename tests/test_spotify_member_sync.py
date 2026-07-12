# Unit tests for the per-user Spotify listening poll (FEAT-multi-user Phase 3b-d).
#
# Pure-logic, mirroring test_lastfm_sync: a fake session records executed SQL
# (blind to real Postgres semantics — the ON CONFLICT interaction against the V45
# full unique is validated by a prod dry-run, per
# feedback-sa-session-lifecycle-mock-blind); KMS and the Spotify HTTP client are
# fakes. No token/ciphertext ever needs to leave the fixtures.
from __future__ import annotations

import base64
import json
import uuid

from worker.clients.spotify_member_client import SpotifyInvalidGrant
from worker.service.spotify_member_sync_service import run_spotify_member_sync

CIPHERTEXT_B64 = base64.b64encode(b"kms-envelope-blob").decode()
REFRESH_TOKEN = "member-refresh-token"
NEW_REFRESH_TOKEN = "rotated-refresh-token"
NEW_ENVELOPE = b"new-kms-envelope"


def _payload(ciphertext: str = CIPHERTEXT_B64) -> str:
    return json.dumps(
        {
            "v": 1,
            "ciphertext": ciphertext,
            "scope": "user-read-recently-played user-read-playback-state",
            "expires_in": 3600,
            "obtained_at": "2026-07-12T00:00:00+00:00",
        }
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


class _FakeSession:
    """Records every (sql, params); simulates the recent-tracks unique via a seen-set
    so a second identical insert reports rowcount=0 (ON CONFLICT DO NOTHING)."""

    def __init__(self, connected):
        self.connected = connected  # list[_Row(user_id, payload)]
        self.executed = []  # list[(sql, params)]
        self._recent_keys = set()

    def execute(self, stmt, params=None):
        sql = str(stmt)
        params = params or {}
        self.executed.append((sql, params))
        if "FROM user_integrations ui" in sql:
            return _Result(rows=self.connected)
        if "INSERT INTO spotify_member_recent_tracks" in sql:
            key = (params["user_id"], params["played_at"], params["spotify_track_id"])
            if key in self._recent_keys:
                return _Result(rowcount=0)
            self._recent_keys.add(key)
            return _Result(rowcount=1)
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

    def sql_of(self, pred):
        return [e for e in self.executed if pred(e[0])]


class _FakeKms:
    def __init__(self, fail_decrypt=False, fail_encrypt=False):
        self.fail_decrypt = fail_decrypt
        self.fail_encrypt = fail_encrypt
        self.decrypt_calls = []
        self.encrypt_calls = []

    def decrypt(self, CiphertextBlob):
        self.decrypt_calls.append(CiphertextBlob)
        if self.fail_decrypt:
            raise RuntimeError("KMS unavailable")
        return {"Plaintext": REFRESH_TOKEN.encode()}

    def encrypt(self, KeyId, Plaintext):
        self.encrypt_calls.append((KeyId, Plaintext))
        if self.fail_encrypt:
            raise RuntimeError("KMS unavailable")
        return {"CiphertextBlob": NEW_ENVELOPE}


def _recent_item(tid="t1", played_at="2026-07-12T01:00:00Z", name="Done"):
    return {
        "played_at": played_at,
        "track": {
            "id": tid,
            "name": name,
            "duration_ms": 200000,
            "artists": [{"name": "IU"}],
            "album": {"name": "LILAC", "images": [{"url": "xl.jpg"}, {"url": "s.jpg"}]},
        },
    }


def _player_state(is_playing=True, tid="t9"):
    return {
        "is_playing": is_playing,
        "progress_ms": 42000,
        "item": {
            "id": tid,
            "name": "Live",
            "duration_ms": 180000,
            "artists": [{"name": "RM"}],
            "album": {"name": "Indigo", "images": [{"url": "np.jpg"}]},
        },
    }


class _FakeClient:
    def __init__(self, refresh_body=None, refresh_exc=None, player=None, recent=None,
                 player_exc=None):
        self.refresh_body = refresh_body if refresh_body is not None else {
            "access_token": "at-1", "expires_in": 3600, "scope": "user-read-recently-played",
        }
        self.refresh_exc = refresh_exc
        self.player = player
        self.recent = recent if recent is not None else []
        self.player_exc = player_exc
        self.refresh_calls = []
        self.player_calls = 0
        self.recent_calls = 0

    def refresh(self, refresh_token):
        self.refresh_calls.append(refresh_token)
        if self.refresh_exc:
            raise self.refresh_exc
        return self.refresh_body

    def get_player_state(self, access_token):
        self.player_calls += 1
        if self.player_exc:
            raise self.player_exc
        return self.player

    def get_recently_played(self, access_token, limit=50):
        self.recent_calls += 1
        return self.recent


def _run(session, client, kms, **kw):
    return run_spotify_member_sync(
        lambda: session, client, kms=kms, kms_key_id=kw.pop("kms_key_id", "key-123"), **kw
    )


class TestSpotifyMemberSync:
    def test_happy_path_writes_recent_nowplaying_and_touches_row(self):
        uid = uuid.uuid4()
        session = _FakeSession([_Row(user_id=uid, payload=_payload())])
        kms = _FakeKms()
        client = _FakeClient(player=_player_state(), recent=[_recent_item()])
        res = _run(session, client, kms)
        assert res == {"users": 1, "recent": 1, "reauth": 0, "skipped": 0}
        # decrypt received the b64-decoded envelope
        assert kms.decrypt_calls == [b"kms-envelope-blob"]
        assert client.refresh_calls == [REFRESH_TOKEN]
        # recent insert = ON CONFLICT DO NOTHING on the full unique
        ins = session.sql_of(lambda s: "INSERT INTO spotify_member_recent_tracks" in s)
        assert ins and "ON CONFLICT (user_id, played_at, spotify_track_id) DO NOTHING" in ins[0][0]
        # now-playing upsert carries the track fields
        np = session.sql_of(lambda s: "INSERT INTO spotify_member_now_playing" in s)
        assert np and np[0][1]["spotify_track_id"] == "t9" and np[0][1]["is_playing"] is True
        # integration row touched, status untouched, no rotation (no new token)
        assert session.sql_of(lambda s: "SET last_synced_at = now()" in s)
        assert not session.sql_of(lambda s: "status = 'reauth'" in s)
        assert kms.encrypt_calls == []

    def test_invalid_grant_flips_reauth_and_never_encrypts_or_reads_player(self):
        uid = uuid.uuid4()
        session = _FakeSession([_Row(user_id=uid, payload=_payload())])
        kms = _FakeKms()
        client = _FakeClient(refresh_exc=SpotifyInvalidGrant("invalid_grant"))
        res = _run(session, client, kms)
        assert res == {"users": 0, "recent": 0, "reauth": 1, "skipped": 0}
        upd = session.sql_of(lambda s: "status = 'reauth'" in s)
        assert upd and upd[0][1] == {"user_id": uid}
        # payload kept: no payload UPDATE, no KMS Encrypt, no player reads
        assert not session.sql_of(lambda s: "SET payload" in s)
        assert kms.encrypt_calls == []
        assert client.player_calls == 0 and client.recent_calls == 0

    def test_transient_kms_decrypt_failure_skips_without_status_change(self):
        uid = uuid.uuid4()
        session = _FakeSession([_Row(user_id=uid, payload=_payload())])
        kms = _FakeKms(fail_decrypt=True)
        client = _FakeClient()
        res = _run(session, client, kms)
        assert res == {"users": 0, "recent": 0, "reauth": 0, "skipped": 1}
        assert client.refresh_calls == []
        assert not session.sql_of(lambda s: "UPDATE user_integrations" in s)

    def test_malformed_payload_skips_without_status_change(self):
        uid = uuid.uuid4()
        session = _FakeSession([_Row(user_id=uid, payload="not-json")])
        res = _run(session, _FakeClient(), _FakeKms())
        assert res["skipped"] == 1
        assert not session.sql_of(lambda s: "UPDATE user_integrations" in s)

    def test_rotation_reencrypts_and_updates_payload_same_shape(self):
        uid = uuid.uuid4()
        session = _FakeSession([_Row(user_id=uid, payload=_payload())])
        kms = _FakeKms()
        client = _FakeClient(
            refresh_body={"access_token": "at-1", "refresh_token": NEW_REFRESH_TOKEN,
                          "expires_in": 3600, "scope": "user-read-recently-played"},
            player=None, recent=[],
        )
        res = _run(session, client, kms)
        assert res["users"] == 1
        assert kms.encrypt_calls == [("key-123", NEW_REFRESH_TOKEN.encode())]
        upd = session.sql_of(lambda s: "SET payload" in s)
        assert len(upd) == 1
        doc = json.loads(upd[0][1]["payload"])
        assert doc["v"] == 1
        assert doc["ciphertext"] == base64.b64encode(NEW_ENVELOPE).decode()
        assert doc["scope"] == "user-read-recently-played"
        # never the plaintext token anywhere in the stored payload
        assert NEW_REFRESH_TOKEN not in upd[0][1]["payload"]

    def test_rotation_encrypt_failure_keeps_old_payload_and_still_syncs(self):
        uid = uuid.uuid4()
        session = _FakeSession([_Row(user_id=uid, payload=_payload())])
        kms = _FakeKms(fail_encrypt=True)
        client = _FakeClient(
            refresh_body={"access_token": "at-1", "refresh_token": NEW_REFRESH_TOKEN,
                          "expires_in": 3600},
            player=_player_state(), recent=[_recent_item()],
        )
        res = _run(session, client, kms)
        assert res == {"users": 1, "recent": 1, "reauth": 0, "skipped": 0}
        assert not session.sql_of(lambda s: "SET payload" in s)  # old row untouched
        assert session.sql_of(lambda s: "INSERT INTO spotify_member_now_playing" in s)

    def test_204_now_playing_writes_is_playing_false_keeping_track_fields(self):
        uid = uuid.uuid4()
        session = _FakeSession([_Row(user_id=uid, payload=_payload())])
        client = _FakeClient(player=None, recent=[])
        _run(session, client, _FakeKms())
        idle = session.sql_of(
            lambda s: "INSERT INTO spotify_member_now_playing" in s and "FALSE" in s
        )
        assert idle and idle[0][1] == {"user_id": uid}
        # the idle upsert must NOT overwrite the last track fields
        assert "spotify_track_id = EXCLUDED" not in idle[0][0]

    def test_recent_dedup_second_run_inserts_zero(self):
        uid = uuid.uuid4()
        session = _FakeSession([_Row(user_id=uid, payload=_payload())])
        kms = _FakeKms()
        client = _FakeClient(player=None, recent=[_recent_item(), _recent_item(tid="t2")])
        first = _run(session, client, kms)
        assert first["recent"] == 2
        second = _run(session, client, kms)
        assert second["recent"] == 0  # same unique keys → ON CONFLICT DO NOTHING

    def test_recent_rows_sorted_by_conflict_key(self):
        uid = uuid.uuid4()
        session = _FakeSession([_Row(user_id=uid, payload=_payload())])
        items = [  # arrives most-recent-first from Spotify (unsorted for the insert)
            _recent_item(tid="b", played_at="2026-07-12T03:00:00Z"),
            _recent_item(tid="a", played_at="2026-07-12T03:00:00Z"),
            _recent_item(tid="z", played_at="2026-07-12T01:00:00Z"),
        ]
        _run(session, _FakeClient(player=None, recent=items), _FakeKms())
        ins = session.sql_of(lambda s: "INSERT INTO spotify_member_recent_tracks" in s)
        keys = [(p["played_at"].isoformat(), p["spotify_track_id"]) for _, p in ins]
        assert keys == sorted(keys)

    def test_per_user_isolation_failure_in_a_does_not_stop_b(self):
        uid_a, uid_b = uuid.uuid4(), uuid.uuid4()
        session = _FakeSession([
            _Row(user_id=uid_a, payload=_payload()),
            _Row(user_id=uid_b, payload=_payload()),
        ])

        class _ClientAFails(_FakeClient):
            def get_recently_played(self, access_token, limit=50):
                self.recent_calls += 1
                if self.recent_calls == 1:
                    raise RuntimeError("boom on user A")
                return [_recent_item()]

        res = _run(session, _ClientAFails(player=None), _FakeKms())
        assert res == {"users": 1, "recent": 1, "reauth": 0, "skipped": 1}
        touched = session.sql_of(lambda s: "SET last_synced_at = now()" in s)
        assert [p["user_id"] for _, p in touched] == [uid_b]

    def test_no_connected_users_is_noop(self):
        session = _FakeSession([])
        res = _run(session, _FakeClient(), _FakeKms())
        assert res == {"users": 0, "recent": 0, "reauth": 0, "skipped": 0}
        assert not session.sql_of(lambda s: "INSERT" in s)
