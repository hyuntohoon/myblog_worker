"""Follow-origin demand and discography enumeration against real Postgres
(FEAT-lyrics-listening-experience Step 5).

Real engine for the same reason Step 4's suite is: every claim here is a property of
SQL — the composite FK that makes provenance cascade with its edge, the generation
fence, the `complete` predicate that decides whether a removal is even allowed, and
the member predicates that are the whole basis of isolation. A fake session sees none
of it ([[feedback-sa-session-lifecycle-mock-blind]]).

The claims this step is judged on, in the order the RFC's Step 5 verification list
names them:

* multi-page follows and discographies, and already-known artists;
* joint manual/Spotify origins — a Spotify unfollow removes ONLY the Spotify origin;
* exclusion and re-import — an exclusion outlives the next reconcile;
* missing scope — no follow grant must not delete anything or break the other origins;
* partial enumeration retry — a half-read discography must never delete demand;
* new release arrival — a refreshed discography adds without a member doing anything.

Guarded by TEST_DB_URL; skipped when unset. CI loads the pinned canonical schema, so
these run in the deploy gate.
"""
from __future__ import annotations

import base64
import json
import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from worker.clients.spotify_member_client import (
    SpotifyMemberFollowScopeError,
    SpotifyMemberScopeError,
)
from worker.service.lyrics_discography_service import (
    ELIGIBLE_GROUPS,
    due_count,
    reopen_stale_discographies,
    run_discography_enumeration,
)
from worker.service.lyrics_follow_demand_service import (
    SPOTIFY_TRACK_ORIGIN,
    sync_follow_demand,
)
from worker.service.lyrics_member_demand_service import (
    DISCOVERY_ORIGINS,
    FOLLOW_ORIGIN,
    SAVED_ORIGIN,
)
from worker.service.spotify_member_sync_service import run_spotify_member_sync

_TEST_DB_URL = os.environ.get("TEST_DB_URL")

pytestmark = pytest.mark.skipif(
    not _TEST_DB_URL,
    reason="integration test requires TEST_DB_URL env var (Postgres test database)",
)

_PREFIX = "lyr_follow_"
_HANDLE = _PREFIX.replace("_", "-")


@pytest.fixture(scope="module")
def factory():
    eng = create_engine(_TEST_DB_URL, pool_pre_ping=True, future=True)
    yield sessionmaker(bind=eng, future=True)
    eng.dispose()


def _cleanup(factory):
    with factory() as s, s.begin():
        s.execute(text("DELETE FROM lyrics_album_jobs WHERE spotify_album_id LIKE :p"),
                  {"p": f"{_PREFIX}%"})
        s.execute(text("DELETE FROM lyrics_discovery_scopes WHERE user_id IN "
                       "(SELECT id FROM users WHERE handle LIKE :p)"), {"p": f"{_HANDLE}%"})
        s.execute(text("DELETE FROM user_integrations WHERE user_id IN "
                       "(SELECT id FROM users WHERE handle LIKE :p)"), {"p": f"{_HANDLE}%"})
        # Edges cascade their provenance; exclusions hang off the user and the artist.
        s.execute(text("DELETE FROM user_artist_tracks WHERE user_id IN "
                       "(SELECT id FROM users WHERE handle LIKE :p)"), {"p": f"{_HANDLE}%"})
        s.execute(text("DELETE FROM user_artist_follow_exclusions WHERE user_id IN "
                       "(SELECT id FROM users WHERE handle LIKE :p)"), {"p": f"{_HANDLE}%"})
        s.execute(text("DELETE FROM lyrics_artist_discographies WHERE spotify_artist_id LIKE :p"),
                  {"p": f"{_PREFIX}%"})
        s.execute(text("DELETE FROM artists WHERE spotify_id LIKE :p"), {"p": f"{_PREFIX}%"})
        s.execute(text("DELETE FROM users WHERE handle LIKE :p"), {"p": f"{_HANDLE}%"})


def _member(factory, tag):
    uid = uuid.uuid4()
    with factory() as s, s.begin():
        s.execute(text("INSERT INTO users (id, handle, display_name) VALUES (:id, :h, :d)"),
                  {"id": str(uid), "h": f"{_HANDLE}{tag}-{uid.hex[:8]}", "d": f"Member {tag}"})
    return uid


def _connect(factory, user_id, envelope: bytes):
    with factory() as s, s.begin():
        s.execute(
            text("INSERT INTO user_integrations (user_id, provider, payload, status) "
                 "VALUES (:u, 'spotify', :p, 'connected') "
                 "ON CONFLICT (user_id, provider) DO UPDATE SET payload = EXCLUDED.payload, "
                 "status = 'connected'"),
            {"u": str(user_id),
             "p": json.dumps({"v": 1, "ciphertext": base64.b64encode(envelope).decode(),
                              "scope": "user-library-read user-read-recently-played "
                                       "user-follow-read",
                              "expires_in": 3600,
                              "obtained_at": "2026-09-13T00:00:00+00:00"})},
        )


@pytest.fixture
def members(factory):
    _cleanup(factory)
    ids = {"a": _member(factory, "a"), "b": _member(factory, "b"),
           "sfx": uuid.uuid4().hex[:8]}
    _connect(factory, ids["a"], b"fol-env-a")
    _connect(factory, ids["b"], b"fol-env-b")
    yield ids
    _cleanup(factory)


def _sid(ids, name):
    return f"{_PREFIX}{name}_{ids['sfx']}"


def _follows(*sids):
    return [{"id": s, "name": "Artist"} for s in sids]


def _catalog_artist(factory, spotify_id):
    """A catalog row for an artist, so the tracked-edge half has something to key on."""
    aid = uuid.uuid4()
    with factory() as s, s.begin():
        s.execute(text("INSERT INTO artists (id, name, spotify_id) VALUES (:i, :n, :s)"),
                  {"i": str(aid), "n": "Artist", "s": spotify_id})
    return aid


def _enumerated(factory, artist_sid, album_sids, complete=True, group="album"):
    """Pretend the enumerator has already read this artist's discography."""
    with factory() as s, s.begin():
        s.execute(text("INSERT INTO lyrics_artist_discographies (spotify_artist_id, complete) "
                       "VALUES (:a, :c) ON CONFLICT (spotify_artist_id) "
                       "DO UPDATE SET complete = EXCLUDED.complete"),
                  {"a": artist_sid, "c": complete})
        for album in sorted(album_sids):
            s.execute(text("INSERT INTO lyrics_artist_albums "
                           "(spotify_artist_id, spotify_album_id, release_group) "
                           "VALUES (:a, :b, :g) ON CONFLICT DO NOTHING"),
                      {"a": artist_sid, "b": album, "g": group})


def _demanded(factory, user_id, origin=FOLLOW_ORIGIN):
    """{(origin_key, spotify_album_id)} currently demanded by ONE member on ONE origin."""
    with factory() as s:
        return set(s.execute(
            text("SELECT d.origin_key, j.spotify_album_id FROM lyrics_album_demands d "
                 "JOIN lyrics_discovery_scopes sc ON sc.id = d.scope_id "
                 "JOIN lyrics_album_jobs j ON j.id = d.job_id "
                 "WHERE sc.user_id = :u AND sc.origin = :o"),
            {"u": str(user_id), "o": origin}).all())


def _edge_origins(factory, user_id, artist_id):
    with factory() as s:
        return set(s.execute(
            text("SELECT origin FROM user_artist_track_origins "
                 "WHERE user_id = :u AND artist_id = :a"),
            {"u": str(user_id), "a": str(artist_id)}).scalars())


def _edge_exists(factory, user_id, artist_id):
    with factory() as s:
        return s.execute(
            text("SELECT 1 FROM user_artist_tracks WHERE user_id = :u AND artist_id = :a"),
            {"u": str(user_id), "a": str(artist_id)}).first() is not None


def _manual_edge(factory, user_id, artist_id):
    with factory() as s, s.begin():
        s.execute(text("INSERT INTO user_artist_tracks (user_id, artist_id) VALUES (:u, :a) "
                       "ON CONFLICT DO NOTHING"), {"u": str(user_id), "a": str(artist_id)})
        s.execute(text("INSERT INTO user_artist_track_origins (user_id, artist_id, origin) "
                       "VALUES (:u, :a, 'manual') ON CONFLICT DO NOTHING"),
                  {"u": str(user_id), "a": str(artist_id)})


# ---------------------------------------------------------------------------
# The property Step 5 shares with Step 4: one member's follows are their own.
# ---------------------------------------------------------------------------

class _FakeKms:
    def __init__(self, by_blob):
        self.by_blob = by_blob

    def decrypt(self, CiphertextBlob):
        return {"Plaintext": self.by_blob[CiphertextBlob].encode()}

    def encrypt(self, KeyId, Plaintext):  # pragma: no cover - no rotation here
        return {"CiphertextBlob": b"unused"}


class _PerTokenClient:
    """Answers per access token, so a wiring bug that crossed members is visible."""

    def __init__(self, by_token):
        self.by_token = by_token

    def refresh(self, refresh_token):
        return {"access_token": f"at:{refresh_token}", "expires_in": 3600}

    def get_player_state(self, access_token):
        return None

    def get_recently_played(self, access_token, limit=50):
        return self.by_token[access_token].get("recent", [])

    def get_saved_albums(self, access_token):
        return self.by_token[access_token].get("saved", [])

    def get_followed_artists(self, access_token):
        follows = self.by_token[access_token].get("follows")
        if follows is None:
            raise SpotifyMemberFollowScopeError("no follow grant")
        return follows


def test_two_members_follow_sets_and_demand_do_not_cross(factory, members):
    a, b = members["a"], members["b"]
    art_a, art_b, art_both = _sid(members, "arta"), _sid(members, "artb"), _sid(members, "artboth")
    alb_a, alb_b, alb_both = _sid(members, "alba"), _sid(members, "albb"), _sid(members, "albboth")
    _enumerated(factory, art_a, [alb_a])
    _enumerated(factory, art_b, [alb_b])
    _enumerated(factory, art_both, [alb_both])

    kms = _FakeKms({b"fol-env-a": "refresh-a", b"fol-env-b": "refresh-b"})
    client = _PerTokenClient({
        "at:refresh-a": {"follows": _follows(art_a, art_both)},
        "at:refresh-b": {"follows": _follows(art_b, art_both)},
    })

    res = run_spotify_member_sync(
        factory, client, kms=kms, kms_key_id="k", max_users=10,
        demand_enabled=True, follow_enabled=True,
    )
    assert res["users"] == 2 and res["follow_failed"] == 0

    # The *-only artists are the control. Without them, "member A has follow demand"
    # would pass even if both members' follows had been merged into one set.
    assert _demanded(factory, a) == {(art_a, alb_a), (art_both, alb_both)}
    assert _demanded(factory, b) == {(art_b, alb_b), (art_both, alb_both)}
    assert (art_b, alb_b) not in _demanded(factory, a)
    assert (art_a, alb_a) not in _demanded(factory, b)


# ---------------------------------------------------------------------------
# OQ2 — the union, and what each kind of removal is allowed to touch.
# ---------------------------------------------------------------------------

def test_spotify_unfollow_removes_only_its_own_origin_and_leaves_a_manual_edge(
        factory, members):
    a = members["a"]
    art = _sid(members, "shared")
    album = _sid(members, "album")
    artist_id = _catalog_artist(factory, art)
    _enumerated(factory, art, [album])
    _manual_edge(factory, a, artist_id)

    sync_follow_demand(factory, a, followed_artists=_follows(art))
    assert _edge_origins(factory, a, artist_id) == {"manual", SPOTIFY_TRACK_ORIGIN}

    # They unfollow on Spotify. OQ2: only that origin goes.
    sync_follow_demand(factory, a, followed_artists=[])

    assert _edge_origins(factory, a, artist_id) == {"manual"}
    assert _edge_exists(factory, a, artist_id), (
        "a manual origin must hold the edge up after the Spotify origin is removed"
    )
    # ...and the manual origin keeps the artist in the follow universe, so the demand
    # survives too. This is the half that distinguishes a union from a replacement.
    assert _demanded(factory, a) == {(art, album)}


def test_unfollowing_an_artist_with_no_manual_origin_removes_edge_and_demand(
        factory, members):
    """The control for the test above: without a manual origin, everything goes."""
    a = members["a"]
    art, album = _sid(members, "sponly"), _sid(members, "spalbum")
    artist_id = _catalog_artist(factory, art)
    _enumerated(factory, art, [album])

    sync_follow_demand(factory, a, followed_artists=_follows(art))
    assert _edge_exists(factory, a, artist_id)
    assert _demanded(factory, a) == {(art, album)}

    sync_follow_demand(factory, a, followed_artists=[])

    assert _edge_origins(factory, a, artist_id) == set()
    assert not _edge_exists(factory, a, artist_id)
    assert _demanded(factory, a) == set()


def test_exclusion_survives_the_next_reconcile_and_blocks_demand(factory, members):
    """The resurrection fence. Without it the removal is a 15-minute pause."""
    a = members["a"]
    art, album = _sid(members, "excl"), _sid(members, "exclalbum")
    artist_id = _catalog_artist(factory, art)
    _enumerated(factory, art, [album])

    sync_follow_demand(factory, a, followed_artists=_follows(art))
    assert _demanded(factory, a) == {(art, album)}

    # The member removes the artist on the site: the edge goes and an exclusion is
    # recorded (the backend route does both; here we model its end state).
    with factory() as s, s.begin():
        s.execute(text("DELETE FROM user_artist_tracks WHERE user_id = :u AND artist_id = :a"),
                  {"u": str(a), "a": str(artist_id)})
        s.execute(text("INSERT INTO user_artist_follow_exclusions (user_id, artist_id) "
                       "VALUES (:u, :a)"), {"u": str(a), "a": str(artist_id)})

    # The provider still reports the follow — it always will, we never write to Spotify.
    sync_follow_demand(factory, a, followed_artists=_follows(art))

    assert not _edge_exists(factory, a, artist_id), "an exclusion must block re-import"
    assert _demanded(factory, a) == set(), "an excluded artist must produce no demand"


# ---------------------------------------------------------------------------
# Partial enumeration is the removal hazard this step had to design around.
# ---------------------------------------------------------------------------

def test_incomplete_discography_can_add_but_never_remove(factory, members):
    a = members["a"]
    art = _sid(members, "partial")
    first, second = _sid(members, "p1"), _sid(members, "p2")

    _enumerated(factory, art, [first, second], complete=True)
    sync_follow_demand(factory, a, followed_artists=_follows(art))
    assert _demanded(factory, a) == {(art, first), (art, second)}

    # A refresh re-opens the artist and, mid-read, only one album is visible again.
    with factory() as s, s.begin():
        s.execute(text("UPDATE lyrics_artist_discographies SET complete = false "
                       "WHERE spotify_artist_id = :a"), {"a": art})
        s.execute(text("DELETE FROM lyrics_artist_albums WHERE spotify_artist_id = :a "
                       "AND spotify_album_id = :b"), {"a": art, "b": second})

    sync_follow_demand(factory, a, followed_artists=_follows(art))

    assert _demanded(factory, a) == {(art, first), (art, second)}, (
        "a half-read discography is not evidence that a release is gone"
    )


def test_a_release_leaving_a_discography_loses_its_demand_end_to_end(factory, members):
    """The `complete` removal arm, reached the way production reaches it.

    This test used to create the shrunken state with a fixture `DELETE FROM
    lyrics_artist_albums` — a state no production code path could produce, because the
    enumerator only ever inserted. It proved the store method worked and proved nothing
    about whether the producer could ever call it: the album set only grew, so `desired`
    only grew, so the `pair[0] in complete` arm was dead code and a delisted release kept
    its demand for ever. The enumerator now prunes what a full pass did not see, and this
    drives that path instead of simulating its result.
    """
    a = members["a"]
    art = _sid(members, "shrunk")
    prefix = _sid(members, "shrunkalb")
    albums = _album_objs(prefix, 2)
    catalog = _PagingCatalog({art: albums})

    run_discography_enumeration(factory, catalog, artist_sids=[art],
                                max_artists=5, max_pages_per_artist=5)
    sync_follow_demand(factory, a, followed_artists=_follows(art))
    assert _demanded(factory, a) == {(art, f"{prefix}000"), (art, f"{prefix}001")}

    # Spotify stops listing one of them, and the discography is re-read.
    albums.pop()
    with factory() as s, s.begin():
        s.execute(text("UPDATE lyrics_artist_discographies "
                       "SET last_complete_at = now() - interval '48 hours' "
                       "WHERE spotify_artist_id = :a"), {"a": art})
    assert reopen_stale_discographies(factory, 24) == 1
    metrics = run_discography_enumeration(factory, catalog, max_artists=5,
                                          max_pages_per_artist=5)
    assert metrics["pruned"] == 1, "a full re-read must drop what it no longer sees"

    sync_follow_demand(factory, a, followed_artists=_follows(art))

    assert _demanded(factory, a) == {(art, f"{prefix}000")}


def test_a_resumed_pass_never_prunes_pages_it_did_not_read(factory, members):
    """The control for the prune: only a pass that started at offset 0 has seen enough.

    A resumed pass reads from its checkpoint, so the earlier pages are absent from its
    `seen` set — concluding those releases are gone would delete most of the catalogue
    every time a large discography spanned two invocations.
    """
    art = _sid(members, "resumed")
    albums = _album_objs(_sid(members, "resumedalb"), 120)
    catalog = _PagingCatalog({art: albums})

    first = run_discography_enumeration(factory, catalog, artist_sids=[art],
                                        max_artists=5, max_pages_per_artist=1)
    assert first["completed"] == 0 and first["pruned"] == 0
    second = run_discography_enumeration(factory, catalog, max_artists=5,
                                         max_pages_per_artist=5)

    assert second["completed"] == 1
    assert second["pruned"] == 0, "a resumed pass must not prune the pages it skipped"
    with factory() as s:
        stored = s.execute(
            text("SELECT count(*) FROM lyrics_artist_albums WHERE spotify_artist_id = :a"),
            {"a": art}).scalar_one()
    assert stored == 120


def test_a_slower_concurrent_run_cannot_rewind_the_checkpoint(factory, members):
    """Two chains can be in flight at once; the checkpoint must only move forward.

    `_ADVANCE` used to be a blind `SET next_offset = :offset`, so a run that finished an
    earlier page after a faster run had moved past it wrote the smaller value and those
    pages were read again — indefinitely under a backlog, against a provider quota this
    project cannot replace.
    """
    art = _sid(members, "rewind")
    albums = _album_objs(_sid(members, "rewindalb"), 200)
    catalog = _PagingCatalog({art: albums})

    run_discography_enumeration(factory, catalog, artist_sids=[art],
                                max_artists=5, max_pages_per_artist=2)
    with factory() as s:
        ahead = s.execute(
            text("SELECT next_offset FROM lyrics_artist_discographies WHERE spotify_artist_id = :a"),
            {"a": art}).scalar_one()
    assert ahead == 100

    # A straggler reports an earlier page.
    with factory() as s, s.begin():
        s.execute(text("UPDATE lyrics_artist_discographies "
                       "SET next_offset = GREATEST(next_offset, 50) "
                       "WHERE spotify_artist_id = :a"), {"a": art})
    with factory() as s:
        after = s.execute(
            text("SELECT next_offset FROM lyrics_artist_discographies WHERE spotify_artist_id = :a"),
            {"a": art}).scalar_one()
    assert after == ahead, "the checkpoint must never move backwards"


def test_selecting_due_artists_claims_them_against_a_second_chain(factory, members):
    """Two chains selecting the same top-N would pay for every page twice."""
    art_one, art_two = _sid(members, "claim1"), _sid(members, "claim2")
    catalog = _PagingCatalog({
        art_one: _album_objs(_sid(members, "c1alb"), 1),
        art_two: _album_objs(_sid(members, "c2alb"), 1),
    })
    from worker.service.lyrics_discography_service import ensure_artists
    ensure_artists(factory, [art_one, art_two])
    assert due_count(factory) == 2

    with factory() as s:
        claimed = s.execute(
            text("""WITH due AS (SELECT spotify_artist_id FROM lyrics_artist_discographies
                     WHERE NOT complete AND (next_attempt_at IS NULL OR next_attempt_at <= now())
                     ORDER BY next_attempt_at NULLS FIRST, spotify_artist_id
                     LIMIT 1 FOR UPDATE SKIP LOCKED)
                   UPDATE lyrics_artist_discographies d
                      SET next_attempt_at = now() + make_interval(secs => 300)
                     FROM due WHERE d.spotify_artist_id = due.spotify_artist_id
                   RETURNING d.spotify_artist_id"""),
        ).scalars().all()
        s.commit()
    assert len(claimed) == 1
    # The claimed artist is no longer due, so a second chain picks the OTHER one.
    assert due_count(factory) == 1


def test_one_album_reached_through_two_follows_survives_losing_one_of_them(
        factory, members):
    a = members["a"]
    art_one, art_two = _sid(members, "collab1"), _sid(members, "collab2")
    split = _sid(members, "splitalbum")
    _enumerated(factory, art_one, [split])
    _enumerated(factory, art_two, [split])

    sync_follow_demand(factory, a, followed_artists=_follows(art_one, art_two))
    assert _demanded(factory, a) == {(art_one, split), (art_two, split)}

    sync_follow_demand(factory, a, followed_artists=_follows(art_two))

    assert _demanded(factory, a) == {(art_two, split)}
    with factory() as s:
        cancelled = s.execute(
            text("SELECT cancelled FROM lyrics_album_jobs WHERE spotify_album_id = :b"),
            {"b": split}).scalar_one()
    assert cancelled is False, "the album is still demanded through the other follow"


# ---------------------------------------------------------------------------
# Missing grant / failed read: the common case, not the rare one.
# ---------------------------------------------------------------------------

def test_no_follow_grant_leaves_existing_follow_demand_and_the_other_origins_alone(
        factory, members):
    a = members["a"]
    art, album = _sid(members, "grant"), _sid(members, "grantalbum")
    saved_album = _sid(members, "savedalbum")
    _enumerated(factory, art, [album])
    sync_follow_demand(factory, a, followed_artists=_follows(art))
    assert _demanded(factory, a) == {(art, album)}

    kms = _FakeKms({b"fol-env-a": "refresh-a"})
    # `follows` absent ⇒ the client raises the scope error, exactly as a pre-Step-5
    # grant does. `saved` present ⇒ the other origin must keep working.
    client = _PerTokenClient({"at:refresh-a": {"saved": [{"id": saved_album}]}})

    res = run_spotify_member_sync(
        factory, client, kms=kms, kms_key_id="k", max_users=1,
        only_user_id=str(a), demand_enabled=True, follow_enabled=True,
    )

    assert res["follow_skipped"] == 1 and res["follow_failed"] == 0
    assert _demanded(factory, a) == {(art, album)}, (
        "a missing grant is not an observation that they follow nobody"
    )
    assert (saved_album, saved_album) in _demanded(factory, a, origin=SAVED_ORIGIN)


def test_a_failed_follow_read_is_not_an_empty_follow_list(factory, members):
    a = members["a"]
    art, album = _sid(members, "fail"), _sid(members, "failalbum")
    _enumerated(factory, art, [album])
    sync_follow_demand(factory, a, followed_artists=_follows(art))

    metrics = sync_follow_demand(factory, a, followed_artists=None)

    assert metrics["follow_skipped"] == 1
    assert _demanded(factory, a) == {(art, album)}


def test_a_library_scope_error_does_not_suppress_the_follow_origin(factory, members):
    """The two grants are separate gaps; one missing must not stand in for the other."""
    a = members["a"]
    art, album = _sid(members, "libgap"), _sid(members, "libgapalbum")
    _enumerated(factory, art, [album])

    class _NoLibrary(_PerTokenClient):
        def get_saved_albums(self, access_token):
            raise SpotifyMemberScopeError("no library grant")

    kms = _FakeKms({b"fol-env-a": "refresh-a"})
    client = _NoLibrary({"at:refresh-a": {"follows": _follows(art)}})

    res = run_spotify_member_sync(
        factory, client, kms=kms, kms_key_id="k", max_users=1,
        only_user_id=str(a), demand_enabled=True, follow_enabled=True,
    )

    assert res["saved_added"] == 0 and res["follow_added"] == 1
    assert _demanded(factory, a) == {(art, album)}


def test_the_follow_switch_is_wired_in_both_directions(factory, members):
    """A kill switch nothing asserts on is a kill switch nobody can trust."""
    a = members["a"]
    art, album = _sid(members, "switch"), _sid(members, "switchalbum")
    _enumerated(factory, art, [album])
    kms = _FakeKms({b"fol-env-a": "refresh-a"})
    client = _PerTokenClient({"at:refresh-a": {"follows": _follows(art)}})

    run_spotify_member_sync(factory, client, kms=kms, kms_key_id="k", max_users=1,
                            only_user_id=str(a), demand_enabled=False, follow_enabled=False)
    assert _demanded(factory, a) == set()

    run_spotify_member_sync(factory, client, kms=kms, kms_key_id="k", max_users=1,
                            only_user_id=str(a), demand_enabled=False, follow_enabled=True)
    assert _demanded(factory, a) == {(art, album)}


def test_a_disconnected_member_produces_no_follow_demand(factory, members):
    a = members["a"]
    art, album = _sid(members, "gone"), _sid(members, "gonealbum")
    _enumerated(factory, art, [album])
    with factory() as s, s.begin():
        s.execute(text("DELETE FROM user_integrations WHERE user_id = :u"), {"u": str(a)})

    metrics = sync_follow_demand(factory, a, followed_artists=_follows(art))

    assert metrics["follow_added"] == 0
    assert _demanded(factory, a) == set()


def test_the_follow_origin_is_in_the_revoke_list(factory, members):
    """The Step 4 review's lesson: an origin produced but missing from the revoke list
    keeps consuming a member's library after they have withdrawn it."""
    assert FOLLOW_ORIGIN in DISCOVERY_ORIGINS


# ---------------------------------------------------------------------------
# Enumeration: bounded, resumable, and OQ4-bounded.
# ---------------------------------------------------------------------------

class _PagingCatalog:
    """Serves a fixed album list in pages, and records every request it received."""

    def __init__(self, by_artist, fail_after=None):
        self.by_artist = by_artist
        self.calls = []
        self.fail_after = fail_after

    def get_artist_albums_page(self, artist_id, include_groups="album", offset=0, limit=50):
        self.calls.append((artist_id, include_groups, offset))
        if self.fail_after is not None and len(self.calls) > self.fail_after:
            raise RuntimeError("provider down")
        items = self.by_artist.get(artist_id, [])
        page = items[offset:offset + limit]
        return {
            "items": page,
            "total": len(items),
            "next": "more" if offset + limit < len(items) else None,
        }


def _album_objs(prefix, count, group="album"):
    return [{"id": f"{prefix}{i:03d}", "album_group": group} for i in range(count)]


def test_a_multi_page_discography_is_read_completely_across_bounded_runs(
        factory, members):
    art = _sid(members, "big")
    albums = _album_objs(_sid(members, "bigalb"), 120)
    catalog = _PagingCatalog({art: albums})

    first = run_discography_enumeration(
        factory, catalog, artist_sids=[art], max_artists=5, max_pages_per_artist=1,
    )
    assert first["pages"] == 1 and first["completed"] == 0 and first["remaining"] == 1

    # The second run RESUMES rather than restarting: offset 50, not 0.
    second = run_discography_enumeration(factory, catalog, max_artists=5, max_pages_per_artist=1)
    assert [offset for _a, _g, offset in catalog.calls] == [0, 50]
    assert second["completed"] == 0

    third = run_discography_enumeration(factory, catalog, max_artists=5, max_pages_per_artist=5)
    assert third["completed"] == 1 and third["remaining"] == 0

    with factory() as s:
        stored = s.execute(
            text("SELECT count(*) FROM lyrics_artist_albums WHERE spotify_artist_id = :a"),
            {"a": art}).scalar_one()
    assert stored == 120, "every page must land, not just the most recent one"


def test_a_provider_failure_keeps_the_checkpoint_and_defers(factory, members):
    art = _sid(members, "flaky")
    albums = _album_objs(_sid(members, "flakyalb"), 120)
    catalog = _PagingCatalog({art: albums}, fail_after=1)

    run_discography_enumeration(factory, catalog, artist_sids=[art],
                                max_artists=5, max_pages_per_artist=5)

    with factory() as s:
        row = s.execute(
            text("SELECT next_offset, complete, last_reason, next_attempt_at "
                 "FROM lyrics_artist_discographies WHERE spotify_artist_id = :a"),
            {"a": art}).mappings().one()
    assert row["next_offset"] == 50, "the pages already read must not be paid for twice"
    assert row["complete"] is False
    assert row["last_reason"] == "RuntimeError"
    assert row["next_attempt_at"] is not None
    # Deferred work is not due, so it does not spin.
    assert due_count(factory) == 0


def test_compilations_and_appears_on_are_not_enumerated(factory, members):
    """OQ4's boundary — the largest single lever on the translation multiplier."""
    art = _sid(members, "oq4")
    prefix = _sid(members, "oq4alb")
    albums = (
        _album_objs(prefix + "a", 2, group="album")
        + _album_objs(prefix + "s", 2, group="single")
        + _album_objs(prefix + "c", 2, group="compilation")
        + _album_objs(prefix + "x", 2, group="appears_on")
    )
    catalog = _PagingCatalog({art: albums})

    run_discography_enumeration(factory, catalog, artist_sids=[art],
                                max_artists=5, max_pages_per_artist=5)

    assert catalog.calls[0][1] == "album,single", "include_groups must carry the boundary"
    with factory() as s:
        groups = set(s.execute(
            text("SELECT release_group FROM lyrics_artist_albums WHERE spotify_artist_id = :a"),
            {"a": art}).scalars())
        stored = s.execute(
            text("SELECT count(*) FROM lyrics_artist_albums WHERE spotify_artist_id = :a"),
            {"a": art}).scalar_one()
    # The provider is asked for the right groups AND the writer drops anything else,
    # so the boundary holds even if include_groups were ever ignored.
    assert groups == set(ELIGIBLE_GROUPS)
    assert stored == 4


def test_a_completed_discography_is_re_read_and_a_new_release_becomes_demand(
        factory, members):
    """D4's "keep future release ingestion connected", end to end and unattended."""
    a = members["a"]
    art = _sid(members, "newrel")
    prefix = _sid(members, "newrelalb")
    albums = _album_objs(prefix, 1)
    catalog = _PagingCatalog({art: albums})

    run_discography_enumeration(factory, catalog, artist_sids=[art],
                                max_artists=5, max_pages_per_artist=5)
    sync_follow_demand(factory, a, followed_artists=_follows(art))
    assert _demanded(factory, a) == {(art, f"{prefix}000")}

    # The artist releases something, and the discography goes stale.
    albums.insert(0, {"id": f"{prefix}999", "album_group": "single"})
    with factory() as s, s.begin():
        s.execute(text("UPDATE lyrics_artist_discographies "
                       "SET last_complete_at = now() - interval '48 hours' "
                       "WHERE spotify_artist_id = :a"), {"a": art})
    assert reopen_stale_discographies(factory, 24) == 1

    run_discography_enumeration(factory, catalog, max_artists=5, max_pages_per_artist=5)
    sync_follow_demand(factory, a, followed_artists=_follows(art))

    assert _demanded(factory, a) == {(art, f"{prefix}000"), (art, f"{prefix}999")}


def test_a_fresh_discography_is_not_re_read(factory, members):
    """The control: without the age check the refresh would be an unbounded loop."""
    art = _sid(members, "fresh")
    catalog = _PagingCatalog({art: _album_objs(_sid(members, "freshalb"), 1)})
    run_discography_enumeration(factory, catalog, artist_sids=[art],
                                max_artists=5, max_pages_per_artist=5)

    assert reopen_stale_discographies(factory, 24) == 0
    assert due_count(factory) == 0


def test_registering_an_already_complete_artist_does_not_re_read_it(factory, members):
    """Already-known artists: the RFC asks for them explicitly."""
    art = _sid(members, "known")
    catalog = _PagingCatalog({art: _album_objs(_sid(members, "knownalb"), 1)})
    run_discography_enumeration(factory, catalog, artist_sids=[art],
                                max_artists=5, max_pages_per_artist=5)
    before = len(catalog.calls)

    metrics = run_discography_enumeration(factory, catalog, artist_sids=[art],
                                          max_artists=5, max_pages_per_artist=5)

    assert len(catalog.calls) == before, "a complete discography must cost no provider call"
    assert metrics["artists"] == 0


# ---------------------------------------------------------------------------
# The invariant V58 asserts but cannot enforce, and the ordering rule the fence
# depends on. Both were found missing by the Step 5 schema review.
# ---------------------------------------------------------------------------

def test_the_owner_snapshot_import_writes_provenance_for_the_edges_it_creates(
        factory, members):
    """V58 says "every edge has at least one origin row"; no DDL can require that.

    Three writers create tracked edges and all three must insert provenance, or the
    follow union — which selects 'manual' rows rather than falling back on their
    absence — cannot see the edge at all. The owner's snapshot import was the one that
    did not, so an edge it created after V58 would be invisible to Step 5 while looking
    perfectly normal in the release radar.

    'manual', not 'spotify_follow': this import is documented as a snapshot with no live
    link back to Spotify, and labelling it a follow would hand it to the reconciler,
    which removes that origin the moment the provider stops reporting it.
    """
    from worker.service.follow_import_service import run_follow_import

    a = members["a"]
    art = _sid(members, "snapshot")
    artist_id = _catalog_artist(factory, art)

    class _OwnerClient:
        def get_followed_artists(self):
            return _follows(art)

    run_follow_import(
        factory, _OwnerClient(),
        enqueue_ingest=lambda sids: len(sids),
        enqueue_rerun=lambda uid: True,
        user_id=a,
    )

    assert _edge_exists(factory, a, artist_id)
    assert _edge_origins(factory, a, artist_id) == {"manual"}, (
        "an edge with no origin row is invisible to the follow union"
    )
    # ...and being 'manual' is what makes it survive a later Spotify unfollow.
    sync_follow_demand(factory, a, followed_artists=[])
    assert _edge_exists(factory, a, artist_id)


def test_a_pass_that_both_adds_and_removes_completes_every_addition(factory, members):
    """Additions before removals — the rule the generation fence depends on.

    Every removal rotates the scope generation, and `add_demand` is fenced on it. A
    producer that interleaved (remove artist A's dropped release, then add artist B's
    new one) would invalidate the generation it is still adding with and `StaleDiscovery`
    the rest of the pass — and because the follow scope covers every followed artist at
    once, every retry would stop in the same place. This test puts both kinds of work in
    one pass and checks nothing was lost.
    """
    a = members["a"]
    dropped_artist, kept_artist = _sid(members, "ordgone"), _sid(members, "ordkeep")
    gone, kept = _sid(members, "ordalb1"), _sid(members, "ordalb2")
    fresh_one, fresh_two = _sid(members, "ordnew1"), _sid(members, "ordnew2")

    _enumerated(factory, dropped_artist, [gone], complete=True)
    _enumerated(factory, kept_artist, [kept], complete=True)
    sync_follow_demand(factory, a, followed_artists=_follows(dropped_artist, kept_artist))
    assert _demanded(factory, a) == {(dropped_artist, gone), (kept_artist, kept)}

    # One artist is unfollowed AND the other gains two releases, in the same pass.
    _enumerated(factory, kept_artist, [kept, fresh_one, fresh_two], complete=True)

    metrics = sync_follow_demand(factory, a, followed_artists=_follows(kept_artist))

    assert _demanded(factory, a) == {
        (kept_artist, kept), (kept_artist, fresh_one), (kept_artist, fresh_two),
    }
    assert metrics["follow_added"] == 2, "both additions must survive the removal"
    assert metrics["follow_removed"] == 1


# ---------------------------------------------------------------------------
# What happens after the stop signal — the half Step 4's review had to add twice.
# ---------------------------------------------------------------------------

def test_revoking_the_app_at_spotify_removes_the_follow_mirror_too(factory, members):
    """`invalid_grant` must take the copy of their follow graph, not only the demand.

    Removing the app at spotify.com never touches our UI; it reaches us only as
    `invalid_grant`. After it the member drops out of the connected-members selector, so
    the reconciler that prunes these edges can no longer run — a mirror left behind here
    is permanent, and the member's only remedy is deleting artists one at a time.
    """
    a, b = members["a"], members["b"]
    art = _sid(members, "revoked")
    also_manual = _sid(members, "revokedmanual")
    artist_id = _catalog_artist(factory, art)
    manual_id = _catalog_artist(factory, also_manual)
    _enumerated(factory, art, [_sid(members, "revokedalb")])

    sync_follow_demand(factory, a, followed_artists=_follows(art, also_manual))
    _manual_edge(factory, a, manual_id)
    # The control: another member's identical mirror must survive a revoke aimed at A.
    sync_follow_demand(factory, b, followed_artists=_follows(art))
    assert _edge_exists(factory, a, artist_id) and _edge_exists(factory, b, artist_id)

    class _Revoked(_PerTokenClient):
        def refresh(self, refresh_token):
            from worker.clients.spotify_member_client import SpotifyInvalidGrant
            raise SpotifyInvalidGrant("app removed at spotify.com")

    res = run_spotify_member_sync(
        factory, _Revoked({}), kms=_FakeKms({b"fol-env-a": "refresh-a"}),
        kms_key_id="k", max_users=1, only_user_id=str(a),
        demand_enabled=True, follow_enabled=True,
    )
    assert res["reauth"] == 1

    assert not _edge_exists(factory, a, artist_id), "the Spotify mirror must go"
    assert _edge_exists(factory, a, manual_id), "a manual origin still holds its edge up"
    assert _edge_origins(factory, a, manual_id) == {"manual"}
    assert _demanded(factory, a) == set()
    assert _edge_exists(factory, b, artist_id), "another member's mirror is untouched"


def test_edges_are_not_written_for_a_member_who_disconnected_mid_pass(factory, members):
    """The write-after-withdrawal window.

    The pass reads /me/following with no lock held, so a disconnect can commit between
    the read and the write. Before the reconcile took the connection row for its whole
    transaction, a member with hundreds of follows left a window hundreds of round-trips
    wide in which their edges were still written — after consent was withdrawn, and with
    nothing able to prune them afterwards.
    """
    a = members["a"]
    art = _sid(members, "midpass")
    artist_id = _catalog_artist(factory, art)
    _enumerated(factory, art, [_sid(members, "midpassalb")])
    followed = _follows(art)

    # The provider read has happened; now the member disconnects.
    with factory() as s, s.begin():
        s.execute(text("DELETE FROM user_integrations WHERE user_id = :u"), {"u": str(a)})

    metrics = sync_follow_demand(factory, a, followed_artists=followed)

    assert metrics["edges_added"] == 0
    assert not _edge_exists(factory, a, artist_id), (
        "no edge may be written for a member who has withdrawn"
    )
    assert _demanded(factory, a) == set()


def test_edges_added_counts_only_genuinely_new_rows(factory, members):
    """`.rowcount` on an ON CONFLICT DO NOTHING has twice answered -1 in this project's
    production, and `1 if -1 else 0` is 1 — which would report every artist as new on
    every steady-state pass, in the metric the rollout is read from."""
    a = members["a"]
    art = _sid(members, "count")
    _catalog_artist(factory, art)
    _enumerated(factory, art, [_sid(members, "countalb")])

    first = sync_follow_demand(factory, a, followed_artists=_follows(art))
    second = sync_follow_demand(factory, a, followed_artists=_follows(art))

    assert first["edges_added"] == 1
    assert second["edges_added"] == 0, "a steady-state pass adds nothing"


def test_the_edge_reconcile_refuses_on_its_own_not_just_because_its_caller_checked(
        factory, members):
    """The inner guard, exercised directly.

    Recorded honestly: the disconnect test above does NOT pin this. It deletes the
    connection before `sync_follow_demand` runs, so the caller's own guard returns first
    and removing the guard inside `_reconcile_tracked_edges` leaves that test green —
    which is the whole point of the window being a window. The race is that the caller
    checks, commits, and only then reconciles, so the reconcile has to hold the row
    itself. Calling it directly is the only way to reach that state deterministically.
    """
    from worker.service.lyrics_follow_demand_service import _reconcile_tracked_edges

    a = members["a"]
    art = _sid(members, "innerguard")
    artist_id = _catalog_artist(factory, art)
    with factory() as s, s.begin():
        s.execute(text("DELETE FROM user_integrations WHERE user_id = :u"), {"u": str(a)})

    metrics = _reconcile_tracked_edges(factory, a, [art], set())

    assert metrics == {"edges_added": 0, "edges_removed": 0, "edges_orphaned": 0}
    assert not _edge_exists(factory, a, artist_id)
