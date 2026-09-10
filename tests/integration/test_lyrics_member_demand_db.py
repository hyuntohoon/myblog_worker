"""Automatic member demand production against real Postgres (FEAT-lyrics-listening-
experience Step 4).

Real engine, not mocks, for the same reason Step 3's suite is: everything that matters
here is a property of SQL the V57 store executes — the ``FOR UPDATE`` scope lock, the
generation fence on ``add_demand``, the cascade behaviour of ``remove_origin`` and the
``s.user_id = :member`` predicates that are the entire basis of member isolation. A fake
session sees none of that ([[feedback-sa-session-lifecycle-mock-blind]]).

The suite is built around the property Step 4's verification names first: **two members
must not be able to read or use one another's connection or library.** The first test
drives the whole poll — real credential-selection SQL, per-member KMS envelopes, a client
that answers differently per access token — and then asserts both halves of the
separation. Its control is the pair of albums each member saved but the other did not: if
the wiring ever crossed member and token, those would show up in the wrong scope, and a
test that only checked "member A has some demand" would pass anyway
([[feedback-measure-with-a-control]]).

Guarded by TEST_DB_URL; skipped when unset. CI loads the pinned canonical schema, so
these run in the deploy gate.
"""
from __future__ import annotations

import base64
import json
import os
import threading
import time
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from myblog_shared_db.lyrics_demand import LyricsDemandStore, StaleDiscovery

from worker.service.lyrics_member_demand_service import (
    DISCOVERY_ORIGINS,
    RECENT_ORIGIN,
    SAVED_ORIGIN,
    sync_member_demand,
)
from worker.clients.spotify_member_client import SpotifyInvalidGrant
from worker.service.spotify_member_sync_service import run_spotify_member_sync

_TEST_DB_URL = os.environ.get("TEST_DB_URL")

pytestmark = pytest.mark.skipif(
    not _TEST_DB_URL,
    reason="integration test requires TEST_DB_URL env var (Postgres test database)",
)

_PREFIX = "lyr_member_"
_HANDLE = _PREFIX.replace("_", "-")


@pytest.fixture(scope="module")
def factory():
    eng = create_engine(_TEST_DB_URL, pool_pre_ping=True, future=True)
    yield sessionmaker(bind=eng, future=True)
    eng.dispose()


def _cleanup(factory):
    with factory() as s, s.begin():
        # Jobs first (lyrics_album_demands cascades from them), then the scopes, then
        # the members whose FK the scopes hang off.
        s.execute(text("DELETE FROM lyrics_album_jobs WHERE spotify_album_id LIKE :p"),
                  {"p": f"{_PREFIX}%"})
        s.execute(text("DELETE FROM lyrics_discovery_scopes WHERE user_id IN "
                       "(SELECT id FROM users WHERE handle LIKE :p)"), {"p": f"{_HANDLE}%"})
        s.execute(text("DELETE FROM user_integrations WHERE user_id IN "
                       "(SELECT id FROM users WHERE handle LIKE :p)"), {"p": f"{_HANDLE}%"})
        s.execute(text("DELETE FROM spotify_member_recent_tracks WHERE user_id IN "
                       "(SELECT id FROM users WHERE handle LIKE :p)"), {"p": f"{_HANDLE}%"})
        s.execute(text("DELETE FROM spotify_member_now_playing WHERE user_id IN "
                       "(SELECT id FROM users WHERE handle LIKE :p)"), {"p": f"{_HANDLE}%"})
        s.execute(text("DELETE FROM albums WHERE spotify_id LIKE :p"), {"p": f"{_PREFIX}%"})
        s.execute(text("DELETE FROM users WHERE handle LIKE :p"), {"p": f"{_HANDLE}%"})


def _member(factory, tag):
    uid = uuid.uuid4()
    with factory() as s, s.begin():
        s.execute(
            text("INSERT INTO users (id, handle, display_name) "
                 "VALUES (:id, :h, :d)"),
            {"id": str(uid), "h": f"{_HANDLE}{tag}-{uid.hex[:8]}", "d": f"Member {tag}"},
        )
    return uid


def _connect(factory, user_id, envelope: bytes):
    """Idempotent: the `members` fixture connects both members, because the producer
    refuses to create demand for a member with no connection row and every test here
    except the disconnect ones models a connected member."""
    with factory() as s, s.begin():
        s.execute(
            text("INSERT INTO user_integrations (user_id, provider, payload, status) "
                 "VALUES (:u, 'spotify', :p, 'connected') "
                 "ON CONFLICT (user_id, provider) DO UPDATE SET payload = EXCLUDED.payload, "
                 "status = 'connected'"),
            {"u": str(user_id),
             "p": json.dumps({"v": 1,
                              "ciphertext": base64.b64encode(envelope).decode(),
                              "scope": "user-library-read user-read-recently-played",
                              "expires_in": 3600,
                              "obtained_at": "2026-09-09T00:00:00+00:00"})},
        )


@pytest.fixture
def members(factory):
    _cleanup(factory)
    ids = {"a": _member(factory, "a"), "b": _member(factory, "b"),
           "sfx": uuid.uuid4().hex[:8]}
    # Both members are CONNECTED. That is not scene-setting: the producer's authority to
    # create demand is the member's live connection row, so a test that skipped this
    # would exercise a state production never reaches (and, before the guard existed,
    # would have hidden the disconnect-resurrection defect).
    _connect(factory, ids["a"], b"env-a")
    _connect(factory, ids["b"], b"env-b")
    yield ids
    _cleanup(factory)


def _sid(ids, name):
    return f"{_PREFIX}{name}_{ids['sfx']}"


def _keys(factory, user_id, origin):
    """The origin keys currently demanded by ONE member on ONE origin."""
    with factory() as s:
        return set(s.execute(
            text("SELECT d.origin_key FROM lyrics_album_demands d "
                 "JOIN lyrics_discovery_scopes sc ON sc.id = d.scope_id "
                 "WHERE sc.user_id = :u AND sc.origin = :o"),
            {"u": str(user_id), "o": origin},
        ).scalars())


def _job_of(factory, spotify_album_id):
    with factory() as s:
        return s.execute(
            text("SELECT id, album_id, cancelled FROM lyrics_album_jobs "
                 "WHERE spotify_album_id = :sid"),
            {"sid": spotify_album_id},
        ).mappings().one_or_none()


def _saved(*sids):
    return [{"id": s, "name": "x"} for s in sids]


def _recent(*sids):
    return [{"played_at": "2026-09-09T01:00:00Z",
             "track": {"id": f"t-{s}", "name": "x", "artists": [{"name": "A"}],
                       "album": {"id": s, "name": "x", "images": []}}}
            for s in sids]


# ---------------------------------------------------------------------------
# The property Step 4 exists to guarantee.
# ---------------------------------------------------------------------------

class _FakeKms:
    """Maps each member's stored envelope to that member's refresh token. A wiring bug
    that used the wrong member's payload would decrypt to the wrong token and be caught
    by the per-token client below."""

    def __init__(self, by_blob):
        self.by_blob = by_blob

    def decrypt(self, CiphertextBlob):
        return {"Plaintext": self.by_blob[CiphertextBlob].encode()}

    def encrypt(self, KeyId, Plaintext):  # pragma: no cover - no rotation here
        return {"CiphertextBlob": b"unused"}


class _PerTokenClient:
    def __init__(self, by_token):
        self.by_token = by_token

    def refresh(self, refresh_token):
        return {"access_token": f"at:{refresh_token}", "expires_in": 3600}

    def get_player_state(self, access_token):
        return None

    def get_recently_played(self, access_token, limit=50):
        return self.by_token[access_token]["recent"]

    def get_saved_albums(self, access_token):
        return self.by_token[access_token]["saved"]


def test_two_members_cannot_read_or_use_each_others_connection_or_library(
        factory, members):
    a, b = members["a"], members["b"]
    a_only, b_only, shared = _sid(members, "aonly"), _sid(members, "bonly"), _sid(members, "shared")
    _connect(factory, a, b"env-a")
    _connect(factory, b, b"env-b")

    kms = _FakeKms({b"env-a": "refresh-a", b"env-b": "refresh-b"})
    client = _PerTokenClient({
        "at:refresh-a": {"saved": _saved(a_only, shared), "recent": _recent(a_only)},
        "at:refresh-b": {"saved": _saved(b_only, shared), "recent": _recent(b_only)},
    })

    res = run_spotify_member_sync(
        factory, client, kms=kms, kms_key_id="k", max_users=10, demand_enabled=True,
    )
    assert res["users"] == 2 and res["demand_failed"] == 0

    # 1. Each member's saved scope holds exactly their OWN library. The *-only albums
    #    are the control: a crossed wiring puts them in the wrong scope.
    assert _keys(factory, a, SAVED_ORIGIN) == {a_only, shared}
    assert _keys(factory, b, SAVED_ORIGIN) == {b_only, shared}
    assert b_only not in _keys(factory, a, SAVED_ORIGIN)
    assert a_only not in _keys(factory, b, SAVED_ORIGIN)
    assert _keys(factory, a, RECENT_ORIGIN) == {a_only}
    assert _keys(factory, b, RECENT_ORIGIN) == {b_only}

    # 2. Neither member can READ the other's demand through the member-scoped API,
    #    even for a job that exists and is perfectly live.
    job_b = _job_of(factory, b_only)["id"]
    job_shared = _job_of(factory, shared)["id"]
    with factory() as s:
        store = LyricsDemandStore(s.connection())
        assert store.album_progress(a, job_b) is None
        assert store.album_progress(b, _job_of(factory, a_only)["id"]) is None
        # ...while the album they BOTH saved is visible to both — proving the None
        # above is isolation and not simply a query that never returns anything.
        assert store.album_progress(a, job_shared) is not None
        assert store.album_progress(b, job_shared) is not None

    # 3. Neither member can WRITE into the other's scope: the store requires the
    #    member id to own the scope row.
    with factory() as s:
        scope_b = s.execute(
            text("SELECT id, generation FROM lyrics_discovery_scopes "
                 "WHERE user_id = :u AND origin = :o"),
            {"u": str(b), "o": SAVED_ORIGIN}).mappings().one()
    with factory() as s:
        with pytest.raises(StaleDiscovery):
            LyricsDemandStore(s.connection()).add_demand(
                a, scope_b["id"], scope_b["generation"], _sid(members, "evil"), "evil")


# ---------------------------------------------------------------------------
# Saved-library reconciliation (a SET) vs recent listening (append-only).
# ---------------------------------------------------------------------------

def test_saved_library_reconciles_an_unsave_but_recent_listening_never_does(
        factory, members):
    a = members["a"]
    x, y = _sid(members, "x"), _sid(members, "y")

    first = sync_member_demand(factory, a, saved_albums=_saved(x, y),
                               recent_items=_recent(x, y))
    assert first == {"saved_added": 2, "saved_removed": 0,
                     "recent_added": 2, "saved_skipped": 0}

    # The member un-saves y, and y has also scrolled out of the 50-item recent window.
    second = sync_member_demand(factory, a, saved_albums=_saved(x), recent_items=_recent(x))
    assert second["saved_removed"] == 1 and second["saved_added"] == 0

    assert _keys(factory, a, SAVED_ORIGIN) == {x}
    # OQ6: an observation ageing out of the provider's rolling window is NOT a removal.
    # A play happened; it cannot un-happen.
    assert _keys(factory, a, RECENT_ORIGIN) == {x, y}


def test_one_pass_both_adds_and_removes_without_losing_the_additions(factory, members):
    """The ordering hazard inside `_apply`, made visible.

    `remove_origin` rotates the scope generation and `add_demand` is fenced on it, so a
    pass that removed first would have its remaining additions rejected as stale — and
    the producer swallows StaleDiscovery by design (a real disconnect must stop it), so
    the albums would simply go missing with an INFO log and no error. Every other
    reconcile test here changes the library in only one direction and would stay green
    through that bug.
    """
    a = members["a"]
    keep, drop = _sid(members, "keep"), _sid(members, "drop")
    added = [_sid(members, f"new{i}") for i in range(3)]

    sync_member_demand(factory, a, saved_albums=_saved(keep, drop))
    assert _keys(factory, a, SAVED_ORIGIN) == {keep, drop}

    # One pass: drop one album and save three new ones.
    res = sync_member_demand(factory, a, saved_albums=_saved(keep, *added))
    assert res["saved_added"] == 3 and res["saved_removed"] == 1
    assert _keys(factory, a, SAVED_ORIGIN) == {keep, *added}


def test_an_unsave_leaves_another_members_demand_for_the_same_album_alive(
        factory, members):
    """OQ6: removal takes the affected origin's demand, never other members'."""
    a, b = members["a"], members["b"]
    shared = _sid(members, "shared")
    sync_member_demand(factory, a, saved_albums=_saved(shared))
    sync_member_demand(factory, b, saved_albums=_saved(shared))
    assert _job_of(factory, shared)["cancelled"] is False

    sync_member_demand(factory, a, saved_albums=_saved())  # a un-saves it
    assert _keys(factory, a, SAVED_ORIGIN) == set()
    assert _keys(factory, b, SAVED_ORIGIN) == {shared}
    # The job stays live because B still wants it — cancelling here would stop the
    # Step 3 collector for a member who never asked for anything to change.
    assert _job_of(factory, shared)["cancelled"] is False

    sync_member_demand(factory, b, saved_albums=_saved())  # ...and now the last one
    assert _job_of(factory, shared)["cancelled"] is True


def test_a_failed_library_read_cannot_delete_demand(factory, members):
    """None means 'not observed'; [] means 'the member saved nothing'. Conflating them
    would make one 403 or one 502 wipe a member's entire saved-origin demand."""
    a = members["a"]
    x = _sid(members, "x")
    sync_member_demand(factory, a, saved_albums=_saved(x))
    assert _keys(factory, a, SAVED_ORIGIN) == {x}

    res = sync_member_demand(factory, a, saved_albums=None, recent_items=_recent())
    assert res["saved_skipped"] == 1 and res["saved_removed"] == 0
    assert _keys(factory, a, SAVED_ORIGIN) == {x}

    # ...and the truthful empty observation still reconciles, so the guard above is
    # not simply disabling removal.
    sync_member_demand(factory, a, saved_albums=_saved())
    assert _keys(factory, a, SAVED_ORIGIN) == set()


def test_a_duplicate_recent_event_produces_no_second_demand(factory, members):
    a = members["a"]
    x = _sid(members, "x")
    assert sync_member_demand(factory, a, recent_items=_recent(x))["recent_added"] == 1
    # Same play observed again on the next 15-minute tick (the window overlaps).
    assert sync_member_demand(factory, a, recent_items=_recent(x))["recent_added"] == 0
    with factory() as s:
        n = s.execute(text(
            "SELECT count(*) FROM lyrics_album_demands d "
            "JOIN lyrics_discovery_scopes sc ON sc.id = d.scope_id "
            "WHERE sc.user_id = :u"), {"u": str(a)}).scalar()
    assert n == 1


def test_a_steady_state_pass_writes_nothing(factory, members):
    """The cron runs this every 15 minutes for every member. An unchanged library must
    cost a diff and no writes, or the producer becomes the system's busiest writer."""
    a = members["a"]
    sids = [_sid(members, f"s{i}") for i in range(10)]
    sync_member_demand(factory, a, saved_albums=_saved(*sids), recent_items=_recent(*sids))
    again = sync_member_demand(factory, a, saved_albums=_saved(*sids),
                               recent_items=_recent(*sids))
    assert again == {"saved_added": 0, "saved_removed": 0,
                     "recent_added": 0, "saved_skipped": 0}


def test_a_multi_page_library_lands_every_album(factory, members):
    """D5: no truncation of eligible scope. 120 albums is three /me/albums pages."""
    a = members["a"]
    sids = [_sid(members, f"p{i:03d}") for i in range(120)]
    res = sync_member_demand(factory, a, saved_albums=_saved(*sids))
    assert res["saved_added"] == 120
    assert _keys(factory, a, SAVED_ORIGIN) == set(sids)


def test_an_album_absent_from_the_catalog_still_becomes_demand(factory, members):
    """The RFC requires accepting a provider identity BEFORE a catalog UUID exists —
    resolution is the Step 3 collector's job, not a precondition for recording demand."""
    a = members["a"]
    unknown = _sid(members, "unknown")
    with factory() as s:
        assert s.execute(text("SELECT 1 FROM albums WHERE spotify_id = :s"),
                         {"s": unknown}).first() is None

    sync_member_demand(factory, a, saved_albums=_saved(unknown))
    job = _job_of(factory, unknown)
    assert job is not None
    assert job["album_id"] is None          # catalog_pending, not rejected
    assert job["cancelled"] is False


# ---------------------------------------------------------------------------
# Disconnect / reconnect (OQ6 generation fence).
# ---------------------------------------------------------------------------

def test_disconnect_fences_an_observation_that_was_already_in_flight(factory, members):
    """The race Step 4 has to survive: a worker tick reads a member's library, the
    member disconnects mid-pass, and the tick then tries to write what it read."""
    a = members["a"]
    x, late = _sid(members, "x"), _sid(members, "late")
    sync_member_demand(factory, a, saved_albums=_saved(x), recent_items=_recent(x))

    # A tick captures the scope it is about to write with...
    with factory() as s:
        scope = s.execute(
            text("SELECT id, generation FROM lyrics_discovery_scopes "
                 "WHERE user_id = :u AND origin = :o"),
            {"u": str(a), "o": SAVED_ORIGIN}).mappings().one()

    # ...the member disconnects (what IntegrationService.disconnect does)...
    with factory() as s:
        LyricsDemandStore(s.connection()).revoke_scopes(a, [SAVED_ORIGIN, RECENT_ORIGIN])
        s.commit()

    assert _keys(factory, a, SAVED_ORIGIN) == set()
    assert _keys(factory, a, RECENT_ORIGIN) == set()
    with factory() as s:
        actives = s.execute(
            text("SELECT origin, active FROM lyrics_discovery_scopes WHERE user_id = :u"),
            {"u": str(a)}).mappings().all()
    assert actives and all(r["active"] is False for r in actives)

    # ...and the in-flight write is rejected rather than resurrecting removed demand.
    with factory() as s:
        with pytest.raises(StaleDiscovery):
            LyricsDemandStore(s.connection()).add_demand(
                a, scope["id"], scope["generation"], late, late)
    assert _keys(factory, a, SAVED_ORIGIN) == set()


def test_reconnect_issues_a_fresh_generation_and_rebuilds_demand(factory, members):
    a = members["a"]
    x = _sid(members, "x")
    sync_member_demand(factory, a, saved_albums=_saved(x))
    with factory() as s:
        before = s.execute(
            text("SELECT generation FROM lyrics_discovery_scopes "
                 "WHERE user_id = :u AND origin = :o"),
            {"u": str(a), "o": SAVED_ORIGIN}).scalar()

    with factory() as s:
        LyricsDemandStore(s.connection()).revoke_scopes(a, [SAVED_ORIGIN, RECENT_ORIGIN])
        s.commit()

    # Reconnect: the connection row is still present (a reconnect writes one), so the
    # very next producer pass legitimately re-activates the scope and rebuilds. The
    # contrast is test_a_disconnect_mid_pass_...: identical revoke, no connection row,
    # and the producer must then rebuild nothing.
    res = sync_member_demand(factory, a, saved_albums=_saved(x))
    assert res["saved_added"] == 1
    assert _keys(factory, a, SAVED_ORIGIN) == {x}
    with factory() as s:
        row = s.execute(
            text("SELECT generation, active FROM lyrics_discovery_scopes "
                 "WHERE user_id = :u AND origin = :o"),
            {"u": str(a), "o": SAVED_ORIGIN}).mappings().one()
    assert row["active"] is True
    assert row["generation"] != before


def test_a_disconnected_member_is_not_polled_at_all(factory, members):
    """The credential-selection SQL is the first line of defence: a disconnected row
    must never reach the token exchange, let alone the producer."""
    a = members["a"]
    _connect(factory, a, b"env-a")
    with factory() as s, s.begin():
        s.execute(text("UPDATE user_integrations SET status = 'disconnected' "
                       "WHERE user_id = :u"), {"u": str(a)})

    class _Boom:
        def refresh(self, *a, **k):
            raise AssertionError("a disconnected member must never be refreshed")

    res = run_spotify_member_sync(
        factory, _Boom(), kms=_FakeKms({}), kms_key_id="k",
        only_user_id=str(a), demand_enabled=True,
    )
    assert res["users"] == 0
    assert _keys(factory, a, SAVED_ORIGIN) == set()


def test_revoking_the_app_at_spotify_revokes_the_demand_it_produced(factory, members):
    """A member who removes the app at spotify.com never touches our DELETE route.

    That is the strongest "stop using my library" signal a member can send, and it
    reaches us only as `invalid_grant` on the next refresh. Step 4's disconnect fence
    lives in the backend route this path never enters, so before the revoke was added
    here the poll flipped the member to `reauth`, dropped them out of
    `_SELECT_CONNECTED` — and left every demand row derived from their private library
    live and served, with no way for them to reach it short of reconnecting in order to
    disconnect. "Stops producing NEW demand" is not the same claim as "stops using the
    library" ([[feedback-rfc-verification-list-is-not-a-threat-model]]).

    The control is member B, who is untouched: a revoke that fired for every member
    rather than the one that lost its grant would pass a test that only looked at A.
    """
    a, b = members["a"], members["b"]
    a_alb, b_alb = _sid(members, "arevoke"), _sid(members, "brevoke")
    kms = _FakeKms({b"env-a": "refresh-a", b"env-b": "refresh-b"})

    # Both members have real, live demand before anything is revoked.
    client = _PerTokenClient({
        "at:refresh-a": {"saved": _saved(a_alb), "recent": []},
        "at:refresh-b": {"saved": _saved(b_alb), "recent": []},
    })
    run_spotify_member_sync(
        factory, client, kms=kms, kms_key_id="k", max_users=10, demand_enabled=True,
    )
    assert _keys(factory, a, SAVED_ORIGIN) == {a_alb}
    assert _keys(factory, b, SAVED_ORIGIN) == {b_alb}

    # Member A removes the app at Spotify: their refresh now fails with invalid_grant.
    class _RevokedForA:
        def refresh(self, refresh_token):
            if refresh_token == "refresh-a":
                raise SpotifyInvalidGrant("invalid_grant")
            return {"access_token": f"at:{refresh_token}", "expires_in": 3600}

        def get_player_state(self, access_token):
            return None

        def get_recently_played(self, access_token, limit=50):
            return []

        def get_saved_albums(self, access_token):
            return _saved(b_alb)

    # Narrowed to A on purpose. With B in the same pass, B's own `reset_scope` re-opens
    # whatever a too-broad revoke had just closed, and a mutant that revoked EVERY
    # member's scopes would pass this test — the control has to be a member the pass
    # never touches ([[feedback-measure-with-a-control]]).
    res = run_spotify_member_sync(
        factory, _RevokedForA(), kms=kms, kms_key_id="k", max_users=10,
        only_user_id=str(a), demand_enabled=True,
    )
    assert res["reauth"] == 1

    # A: status flipped AND the library-derived demand is gone, in the same transaction.
    with factory() as s:
        status = s.execute(
            text("SELECT status FROM user_integrations WHERE user_id = :u"),
            {"u": str(a)}).scalar()
        actives = s.execute(
            text("SELECT count(*) FROM lyrics_discovery_scopes "
                 "WHERE user_id = :u AND active"), {"u": str(a)}).scalar()
    assert status == "reauth"
    assert actives == 0, "the member revoked us at Spotify and the scope stayed active"
    assert _keys(factory, a, SAVED_ORIGIN) == set()

    # B is the control: untouched grant, untouched demand.
    assert _keys(factory, b, SAVED_ORIGIN) == {b_alb}
    with factory() as s:
        b_actives = s.execute(
            text("SELECT count(*) FROM lyrics_discovery_scopes "
                 "WHERE user_id = :u AND active"), {"u": str(b)}).scalar()
    # One live scope per origin — an empty `recent` page still opens its scope, so the
    # number to expect here is the origin count, not 1.
    assert b_actives == len(DISCOVERY_ORIGINS), (
        "the revoke fired for a member who never lost their grant"
    )


def test_a_failing_revoke_rolls_the_reauth_flip_back(factory, members, monkeypatch):
    """The revoke shares the status flip's transaction, and only a failure proves it.

    A sequential test cannot see atomicity: split the two into separate transactions and
    every assertion in the test above still passes. So this forces the revoke to fail. If
    the flip were committed separately it would already be durable, and the member would
    sit in `reauth` — out of `_SELECT_CONNECTED`, so nothing ever retries — with their
    library-derived demand live forever. Rolling back instead leaves them 'connected' so
    the next tick tries again; the refresh still fails first, so nothing is produced in
    the meantime.
    """
    a = members["a"]
    a_alb = _sid(members, "aatomic")
    kms = _FakeKms({b"env-a": "refresh-a"})

    run_spotify_member_sync(
        factory, _PerTokenClient({"at:refresh-a": {"saved": _saved(a_alb), "recent": []}}),
        kms=kms, kms_key_id="k", only_user_id=str(a), demand_enabled=True,
    )
    assert _keys(factory, a, SAVED_ORIGIN) == {a_alb}

    def _boom(self, member_id, origins):
        raise RuntimeError("revoke failed")

    monkeypatch.setattr(LyricsDemandStore, "revoke_scopes", _boom)

    class _RevokedForA:
        def refresh(self, refresh_token):
            raise SpotifyInvalidGrant("invalid_grant")

    res = run_spotify_member_sync(
        factory, _RevokedForA(), kms=kms, kms_key_id="k", only_user_id=str(a),
        demand_enabled=True,
    )
    # The poll isolates the failure per member rather than crashing the invocation.
    assert res["reauth"] == 0 and res["skipped"] == 1

    with factory() as s:
        status = s.execute(
            text("SELECT status FROM user_integrations WHERE user_id = :u"),
            {"u": str(a)}).scalar()
    assert status == "connected", (
        "the reauth flip committed without its revoke — they are not one transaction"
    )
    assert _keys(factory, a, SAVED_ORIGIN) == {a_alb}


def test_the_cron_recovers_a_bootstrap_message_that_was_never_delivered(factory, members):
    """The RFC requires that a broker failure after the connection commits is
    recoverable by reconciliation. It is recoverable because the cron and the bootstrap
    are the same code path — so this simulates the lost message by simply never sending
    it and letting the ordinary pass run."""
    a = members["a"]
    x = _sid(members, "x")
    _connect(factory, a, b"env-a")          # credentials committed, nothing enqueued
    assert _keys(factory, a, SAVED_ORIGIN) == set()

    client = _PerTokenClient({"at:refresh-a": {"saved": _saved(x), "recent": []}})
    res = run_spotify_member_sync(
        factory, client, kms=_FakeKms({b"env-a": "refresh-a"}), kms_key_id="k",
        demand_enabled=True,
    )
    assert res["users"] == 1 and res["saved_added"] == 1
    assert _keys(factory, a, SAVED_ORIGIN) == {x}


def test_a_disconnect_mid_pass_cannot_be_undone_by_the_producer(factory, members):
    """The fence's real failure mode, which the stale-generation test above does NOT
    reach.

    That test hands `add_demand` a generation captured before the revoke, and the store
    rejects it. But the producer never uses a stale generation: `_apply` opens with
    `reset_scope`, which mints a fresh one — and `reset_scope` also sets `active = true`.
    So a pass that starts before a disconnect and writes after it would re-activate the
    scope it had just had revoked and re-add the member's entire library, with every
    generation check passing.

    The window is real: the cron selects connected members once, then spends a token
    exchange and a paginated library read per member before writing. A member who
    disconnects during that window must not keep producing demand.
    """
    a = members["a"]
    x, y = _sid(members, "x"), _sid(members, "y")
    _connect(factory, a, b"env-a")
    sync_member_demand(factory, a, saved_albums=_saved(x), recent_items=_recent(x))
    assert _keys(factory, a, SAVED_ORIGIN) == {x}

    # The member disconnects — exactly what IntegrationService.disconnect commits.
    with factory() as s, s.begin():
        s.execute(text("DELETE FROM user_integrations WHERE user_id = :u"),
                  {"u": str(a)})
        LyricsDemandStore(s.connection()).revoke_scopes(
            a, [SAVED_ORIGIN, RECENT_ORIGIN])

    # ...and the in-flight pass, holding a library it read moments earlier, lands.
    res = sync_member_demand(factory, a, saved_albums=_saved(x, y),
                             recent_items=_recent(x, y))

    assert res == {"saved_added": 0, "saved_removed": 0,
                   "recent_added": 0, "saved_skipped": 0}
    assert _keys(factory, a, SAVED_ORIGIN) == set()
    assert _keys(factory, a, RECENT_ORIGIN) == set()
    with factory() as s:
        actives = s.execute(
            text("SELECT count(*) FROM lyrics_discovery_scopes "
                 "WHERE user_id = :u AND active"), {"u": str(a)}).scalar()
    assert actives == 0, "the producer re-activated a revoked scope"


def test_a_disconnect_committing_mid_guard_is_serialised_not_lost(factory, members,
                                                                  monkeypatch):
    """The concurrent half of the fence, which the sequential test above cannot reach.

    `_CONNECTION_EXISTS` takes the connection row `FOR UPDATE`. Drop that and the guard
    still reads "connected" correctly — and is still wrong: at READ COMMITTED a
    disconnect can commit in the gap between our guard SELECT and our `reset_scope`, and
    the reset then re-activates the scope the disconnect just revoked. Removing only the
    `FOR UPDATE` keeps every sequential test in this file green, so this is the test that
    holds the lock itself accountable.

    The interleaving is forced: a producer pass is paused exactly in that gap while a
    second thread runs what `IntegrationService.disconnect` commits. With the lock the
    disconnect blocks until the pass finishes and its revoke wins; without it, the pass
    resurrects the member's library.
    """
    import worker.service.lyrics_member_demand_service as mod

    a = members["a"]
    x = _sid(members, "x")
    in_gap = threading.Event()
    disconnect_done = threading.Event()

    real_store = mod.LyricsDemandStore

    class _PausingStore(real_store):
        def reset_scope(self, *args, **kwargs):
            # We are past the guard SELECT and still inside its transaction.
            in_gap.set()
            time.sleep(2.0)
            return super().reset_scope(*args, **kwargs)

    monkeypatch.setattr(mod, "LyricsDemandStore", _PausingStore)

    def disconnect():
        in_gap.wait(timeout=10)
        with factory() as s, s.begin():
            s.execute(text("DELETE FROM user_integrations WHERE user_id = :u"),
                      {"u": str(a)})
            real_store(s.connection()).revoke_scopes(a, [SAVED_ORIGIN, RECENT_ORIGIN])
        disconnect_done.set()

    worker = threading.Thread(target=disconnect, daemon=True)
    worker.start()
    sync_member_demand(factory, a, saved_albums=_saved(x))
    worker.join(timeout=30)

    assert disconnect_done.is_set(), "the disconnect thread never completed"
    assert _keys(factory, a, SAVED_ORIGIN) == set()
    with factory() as s:
        actives = s.execute(
            text("SELECT count(*) FROM lyrics_discovery_scopes "
                 "WHERE user_id = :u AND active"), {"u": str(a)}).scalar()
    assert actives == 0, "a disconnect that committed mid-guard was overwritten"
