# worker/service/lyrics_follow_demand_service.py
"""FEAT-lyrics-listening-experience Step 5 — the follow origin.

Step 4 produced demand from what a member *saved* and *played*. D4 adds the third
source the owner confirmed: artists they follow, "including previous releases". So
this producer turns a follow into demand for that artist's whole eligible catalogue,
enumerated globally by `lyrics_discography_service`.

**OQ2, resolved 2026-09-13 — the follow set is a union, and removal is per origin.**
A member can arrive at an artist two ways: they added them on the site, or they follow
them on Spotify. Those are different facts with different removal semantics, so V58
stores them as separate rows in `user_artist_track_origins` and the tracked edge is
their union. Unfollowing on Spotify deletes the `spotify_follow` row and nothing else;
a manual edge underneath it survives untouched. Removing the artist on the site is the
stronger statement and writes an **exclusion**, because without one the next reconcile
15 minutes later would see the follow still live at the provider and put the artist
straight back — a removal that only ever pauses is not a removal. We never write to
Spotify: unfollowing there is the member's business, not ours.

**Three origin keys, one scope.** `lyrics_album_demands` is keyed
(scope, job, origin_key) and the follow scope keys by the **artist**, which is what the
V57 model always anticipated. One album demanded through two followed artists (a
collaboration, a split release) is two demand rows over one job, so losing one of the
two follows leaves the album demanded by the other — the same convergence `saved` and
`recent` already have per album.

**Partial enumeration must not delete anything.** Follow demand is reconciled as a set,
and an artist whose discography is still being read looks like a *smaller* set. So the
removal rule is narrower than the addition rule: an album is removed only when its
artist is no longer followed at all, or when that artist's enumeration is `complete`
and the album is genuinely no longer in it. An artist mid-enumeration can only gain.

**Session discipline.** Every provider read happens before any session opens; each
store call takes its own short transaction, additions before removals, exactly as
Step 4's producer documents — `remove_origin`/`remove_demand` rotate the generation
that `add_demand` is fenced on.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from sqlalchemy import text

from myblog_shared_db.lyrics_demand import LyricsDemandStore, StaleDiscovery

from worker.service.lyrics_discography_service import ensure_artists
from worker.service.lyrics_member_demand_service import (
    FOLLOW_ORIGIN,
    _CONNECTION_EXISTS,
)

logger = logging.getLogger(__name__)

# `user_artist_track_origins.origin` values (V58). 'manual' is an explicit site add
# (or a pre-V58 edge, which the migration backfilled as manual); 'spotify_follow' is
# this reconciler's own row and the ONLY one it is allowed to delete.
MANUAL_TRACK_ORIGIN = "manual"
SPOTIFY_TRACK_ORIGIN = "spotify_follow"

_MAX_PROVIDER_ID = 128


# ── provider id hygiene ──────────────────────────────────────────────────────

def artist_ids(artists: Iterable[Dict[str, Any]]) -> List[str]:
    """Provider artist ids from /me/following objects, de-duped, first-seen order."""
    out: List[str] = []
    seen: Set[str] = set()
    for artist in artists or []:
        sid = (artist or {}).get("id")
        if not isinstance(sid, str):
            continue
        sid = sid.strip()
        # V58 CHECKs the provider id at 1..128 chars; a malformed id would abort the
        # whole transaction, so drop it here rather than at the constraint.
        if not sid or len(sid) > _MAX_PROVIDER_ID or sid in seen:
            continue
        seen.add(sid)
        out.append(sid)
    return out


# ── tracked-artist edge reconciliation (site follows) ────────────────────────

# The member's own exclusions, as provider ids. An excluded artist is skipped by every
# part of this pass: no edge, no demand, no enumeration. The join to `artists` is what
# turns the catalog-keyed exclusion into something comparable with a Spotify follow.
_SELECT_EXCLUDED = text(
    """
    SELECT a.spotify_id
      FROM user_artist_follow_exclusions e
      JOIN artists a ON a.id = e.artist_id
     WHERE e.user_id = :member AND a.spotify_id IS NOT NULL
    """
)

# Artists the member tracks on the site whose edge has a 'manual' origin. These stay in
# the follow universe whatever Spotify says — that is the ∪ in OQ2.
_SELECT_MANUAL_TRACKED = text(
    """
    SELECT a.spotify_id
      FROM user_artist_tracks t
      JOIN user_artist_track_origins o
        ON o.user_id = t.user_id AND o.artist_id = t.artist_id AND o.origin = 'manual'
      JOIN artists a ON a.id = t.artist_id
     WHERE t.user_id = :member AND a.spotify_id IS NOT NULL
    """
)

_MATCH_ARTISTS = text(
    "SELECT id, spotify_id FROM artists WHERE spotify_id = ANY(:sids) ORDER BY id"
)

# Edge first, then its provenance: the origin row's composite FK requires the edge.
_UPSERT_EDGE = text(
    """
    INSERT INTO user_artist_tracks (user_id, artist_id)
    VALUES (:member, :artist)
    ON CONFLICT (user_id, artist_id) DO NOTHING
    """
)

_UPSERT_EDGE_ORIGIN = text(
    """
    INSERT INTO user_artist_track_origins (user_id, artist_id, origin)
    VALUES (:member, :artist, :origin)
    ON CONFLICT (user_id, artist_id, origin) DO NOTHING
    """
)

# Spotify-followed artists this member currently has an imported edge for.
_SELECT_SPOTIFY_EDGES = text(
    """
    SELECT o.artist_id, a.spotify_id
      FROM user_artist_track_origins o
      JOIN artists a ON a.id = o.artist_id
     WHERE o.user_id = :member AND o.origin = 'spotify_follow'
     ORDER BY o.artist_id
    """
)

_DELETE_EDGE_ORIGIN = text(
    """
    DELETE FROM user_artist_track_origins
     WHERE user_id = :member AND artist_id = :artist AND origin = 'spotify_follow'
    """
)

# An edge is the union of its origins, so it survives exactly as long as one remains.
# NOT EXISTS rather than "delete the edge too": a manual origin must hold it up.
_DELETE_ORPHAN_EDGE = text(
    """
    DELETE FROM user_artist_tracks t
     WHERE t.user_id = :member AND t.artist_id = :artist
       AND NOT EXISTS (
             SELECT 1 FROM user_artist_track_origins o
              WHERE o.user_id = t.user_id AND o.artist_id = t.artist_id
           )
    """
)


def _reconcile_tracked_edges(
    session_factory: Callable[[], Any],
    user_id: Any,
    followed_sids: Sequence[str],
    excluded_sids: Set[str],
) -> Dict[str, int]:
    """Bring `user_artist_tracks` + provenance in line with the member's Spotify follows.

    Only ever touches the `spotify_follow` origin. A manual edge is never created,
    deleted or relabelled here, and an excluded artist is never imported.

    Uncatalogued followed artists have no `artists.id`, so they get no edge — but they
    still produce demand, because demand is keyed on provider ids all the way through.
    The catalog catches up separately (see `unmatched` in the caller).
    """
    metrics = {"edges_added": 0, "edges_removed": 0, "edges_orphaned": 0}
    wanted = [sid for sid in followed_sids if sid not in excluded_sids]

    with session_factory() as session:
        matched = session.execute(_MATCH_ARTISTS, {"sids": wanted}).all() if wanted else []
        existing = session.execute(_SELECT_SPOTIFY_EDGES, {"member": user_id}).all()
    by_sid = {sid: artist_id for artist_id, sid in matched}
    # Sorted by the conflict key so two concurrent passes take rows in one order.
    for artist_id in sorted(by_sid.values()):
        with session_factory() as session:
            session.execute(_UPSERT_EDGE, {"member": user_id, "artist": artist_id})
            added = session.execute(
                _UPSERT_EDGE_ORIGIN,
                {"member": user_id, "artist": artist_id, "origin": SPOTIFY_TRACK_ORIGIN},
            ).rowcount
            session.commit()
        metrics["edges_added"] += 1 if added else 0

    keep = set(by_sid.values())
    for artist_id, _sid in existing:
        if artist_id in keep:
            continue
        # Unfollowed at Spotify, or newly excluded. Either way only OUR origin goes.
        with session_factory() as session:
            session.execute(_DELETE_EDGE_ORIGIN, {"member": user_id, "artist": artist_id})
            orphaned = session.execute(
                _DELETE_ORPHAN_EDGE, {"member": user_id, "artist": artist_id}
            ).rowcount
            session.commit()
        metrics["edges_removed"] += 1
        metrics["edges_orphaned"] += 1 if orphaned else 0
    return metrics


# ── follow demand ────────────────────────────────────────────────────────────

_SELECT_ENUMERATED = text(
    """
    SELECT spotify_artist_id, spotify_album_id
      FROM lyrics_artist_albums
     WHERE spotify_artist_id = ANY(:sids)
    """
)

_SELECT_COMPLETE = text(
    """
    SELECT spotify_artist_id
      FROM lyrics_artist_discographies
     WHERE spotify_artist_id = ANY(:sids) AND complete
    """
)

# Existing demand as (artist, album) pairs. `_existing_keys` in the Step 4 producer
# reads origin_key alone, which is enough when the key IS the album; here the album
# lives on the job, so the diff needs the join.
_SELECT_EXISTING_PAIRS = text(
    """
    SELECT d.origin_key, j.spotify_album_id
      FROM lyrics_album_demands d
      JOIN lyrics_album_jobs j ON j.id = d.job_id
     WHERE d.scope_id = :scope
    """
)


def _apply_follow(
    session_factory: Callable[[], Any],
    user_id: Any,
    followed_sids: Sequence[str],
) -> Dict[str, int]:
    """Reconcile the member's whole `follow` scope. Returns {'added','removed'}."""
    result = {"added": 0, "removed": 0}
    sids = list(dict.fromkeys(followed_sids))

    # --- guard + scope + diff inputs, one short transaction, then CLOSE -----
    with session_factory() as session:
        # The producer's authority to create demand for a member is that member's live
        # connection row — the same fence, and the same reason, as Step 4's producer.
        if session.execute(_CONNECTION_EXISTS, {"member": user_id}).first() is None:
            logger.info(
                "follow demand: no Spotify connection for user_id=%s — producing nothing",
                user_id,
            )
            session.rollback()
            return result
        store = LyricsDemandStore(session.connection())
        scope = store.reset_scope(user_id, FOLLOW_ORIGIN)
        existing: Set[Tuple[str, str]] = {
            (key, album)
            for key, album in session.execute(
                _SELECT_EXISTING_PAIRS, {"scope": scope["id"]}
            ).all()
        }
        enumerated = session.execute(_SELECT_ENUMERATED, {"sids": sids}).all() if sids else []
        complete = set(
            session.execute(_SELECT_COMPLETE, {"sids": sids}).scalars()
        ) if sids else set()
        session.commit()
    scope_id, generation = scope["id"], scope["generation"]

    desired: Set[Tuple[str, str]] = {(artist, album) for artist, album in enumerated}
    followed = set(sids)

    additions = sorted(desired - existing)
    # The asymmetry is deliberate (see the module docstring): an artist still being
    # enumerated can gain albums but never lose them, because a partial read is not
    # evidence that a release is gone.
    removals = sorted(
        pair for pair in existing - desired
        if pair[0] not in followed or pair[0] in complete
    )

    # --- additions first: every removal below rotates the generation --------
    for artist_sid, album_sid in additions:
        try:
            with session_factory() as session:
                LyricsDemandStore(session.connection()).add_demand(
                    user_id, scope_id, generation, album_sid, artist_sid
                )
                session.commit()
        except StaleDiscovery:
            logger.info(
                "follow demand: scope superseded mid-pass (user_id=%s) — stopping "
                "after %d addition(s)", user_id, result["added"],
            )
            return result
        result["added"] += 1

    # An unfollowed artist loses every album at once (one call, one generation bump);
    # a still-followed artist that dropped one release loses exactly that album.
    dropped_artists = sorted({a for a, _ in removals if a not in followed})
    for artist_sid in dropped_artists:
        with session_factory() as session:
            LyricsDemandStore(session.connection()).remove_origin(
                user_id, FOLLOW_ORIGIN, origin_key=artist_sid
            )
            session.commit()
        result["removed"] += sum(1 for a, _ in removals if a == artist_sid)

    for artist_sid, album_sid in removals:
        if artist_sid in set(dropped_artists):
            continue
        with session_factory() as session:
            LyricsDemandStore(session.connection()).remove_demand(
                user_id, FOLLOW_ORIGIN, artist_sid, album_sid
            )
            session.commit()
        result["removed"] += 1

    return result


def sync_follow_demand(
    session_factory: Callable[[], Any],
    user_id: Any,
    *,
    followed_artists: Optional[Iterable[Dict[str, Any]]] = None,
) -> Dict[str, int]:
    """Reconcile one member's follows into tracked edges and durable album demand.

    `followed_artists` is the fully paginated /me/following result, or **None** when it
    could not be read this pass (missing `user-follow-read` grant, provider failure).
    None means "no observation", which is not the same as "they follow nobody": an
    empty list is a truthful observation and reconciles every Spotify-origin edge and
    every follow demand away, so a failed read must never be passed as `[]`. This is
    the same rule Step 4 wrote for saved albums, and it matters more here — every
    member who connected before Step 5 has no follow grant at all until they
    re-consent, so the failed-read path is the *common* one, not the rare one.
    """
    metrics = {
        "follow_skipped": 0, "followed": 0, "excluded": 0, "unmatched": 0,
        "edges_added": 0, "edges_removed": 0, "edges_orphaned": 0,
        "follow_added": 0, "follow_removed": 0, "follow_artists": 0,
    }
    if followed_artists is None:
        metrics["follow_skipped"] = 1
        return metrics

    followed_sids = artist_ids(followed_artists)
    metrics["followed"] = len(followed_sids)

    with session_factory() as session:
        if session.execute(_CONNECTION_EXISTS, {"member": user_id}).first() is None:
            logger.info(
                "follow sync: no Spotify connection for user_id=%s — producing nothing",
                user_id,
            )
            session.rollback()
            return metrics
        excluded = {
            sid for sid in session.execute(_SELECT_EXCLUDED, {"member": user_id}).scalars()
        }
        manual = {
            sid for sid in session.execute(_SELECT_MANUAL_TRACKED, {"member": user_id}).scalars()
        }
        session.commit()
    metrics["excluded"] = len(excluded)

    metrics.update(
        _reconcile_tracked_edges(session_factory, user_id, followed_sids, excluded)
    )

    # OQ2's union, minus the member's exclusions. A manually tracked artist stays in
    # scope even when Spotify does not list them, which is the whole point of keeping
    # the two origins apart.
    universe = sorted(({sid for sid in followed_sids} | manual) - excluded)
    metrics["follow_artists"] = len(universe)

    # Register before producing: an artist with no enumeration yet simply contributes
    # nothing this pass and everything once the enumerator reaches them. Demand is
    # never blocked on the catalog knowing the artist.
    ensure_artists(session_factory, universe)

    applied = _apply_follow(session_factory, user_id, universe)
    metrics["follow_added"], metrics["follow_removed"] = applied["added"], applied["removed"]
    return metrics
