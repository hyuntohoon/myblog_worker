# worker/service/lyrics_member_demand_service.py
"""FEAT-lyrics-listening-experience Step 4 — the first automatic demand producers.

Steps 2 and 3 built the durable demand store (V57) and the collector that serves it,
but nothing ever *created* demand: every row in production so far came from a manual
request or a test fixture. This module is the producer. It turns two things a member
already does — saving an album on Spotify, and simply listening — into durable album
translation demand, with no owner involvement.

Two origins, deliberately kept separate rather than flattened into one "library" event
(the RFC is explicit that the player's saved-**track** heart, saved albums and site
follows are different facts):

    'saved'   GET /me/albums, fully paginated. A reconciled SET: an album the member
              un-saves loses its demand from this origin (OQ6).
    'recent'  the 50-item recently-played window the member poll already reads.
              APPEND-ONLY: OQ6 says an observation ageing out of Spotify's rolling
              window must NOT remove demand, so this origin never diffs for removals.
              A play is a fact that happened; un-playing is not a thing.

OQ1 (adopted 2026-09-09, owner delegation): saved **albums** only. Liked tracks
expanded to their albums are a separate explicit extension and are not produced here.

**Member isolation is the property this step has to prove**, so the shape of the API
enforces it instead of documenting it: `sync_member_demand` takes one `user_id` and the
observations that were read with *that member's own* access token, in one call. There is
no path that pairs one member's id with another member's albums, and every store call
below is member-scoped by construction (`LyricsDemandStore` requires the member id to
match the scope row's `user_id`). Nothing in this module reads the owner's
`spotify_library_albums` — that table stays owner-global and is not the member model.

**Transaction discipline.** `LyricsDemandStore` documents a lock order of
scope -> album jobs (UUID order) -> work (UUID order), and "one store call per
transaction". `add_demand` locks the scope and then exactly one job, so batching N of
them into one transaction would take job locks in *observation* order — not UUID order —
and deadlock against a concurrent `remove_origin`, which takes them in UUID order. So
each store call gets its own short transaction. That is also why we diff first and write
only the delta: a steady-state pass over an unchanged library issues zero writes.

**Additions run before removals**, because `remove_origin` rotates the scope generation
and `add_demand` is fenced on it. Doing removals first would invalidate the generation we
are mid-way through adding with. See `_apply`.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

from sqlalchemy import text

from myblog_shared_db.lyrics_demand import LyricsDemandStore, StaleDiscovery

logger = logging.getLogger(__name__)

SAVED_ORIGIN = "saved"
RECENT_ORIGIN = "recent"

# Both origins key a demand row by the provider album id. It is the only identity we
# have at production time (the catalog UUID may not exist yet — that is exactly what
# `add_demand` accepts and the Step 3 collector resolves later), and it makes
# `remove_origin(..., origin_key=sid)` address precisely one album.
_MAX_ORIGIN_KEY = 128


def _album_ids(albums: Iterable[Dict[str, Any]]) -> List[str]:
    """Provider album ids from /me/albums objects, de-duped, first-seen order."""
    out: List[str] = []
    seen: set = set()
    for album in albums or []:
        sid = (album or {}).get("id")
        if not isinstance(sid, str):
            continue
        sid = sid.strip()
        # V57 CHECKs the origin_key and the job's provider id at 1..128 chars; a
        # malformed id would abort the whole transaction, so drop it here instead.
        if not sid or len(sid) > _MAX_ORIGIN_KEY or sid in seen:
            continue
        seen.add(sid)
        out.append(sid)
    return out


def _recent_album_ids(items: Iterable[Dict[str, Any]]) -> List[str]:
    """Provider album ids observed in a recently-played page.

    Local files and podcast episodes carry no stable album identity and are skipped —
    the same items `_recent_rows` already drops for having no track id.
    """
    albums = []
    for item in items or []:
        track = (item or {}).get("track") or {}
        albums.append(track.get("album") or {})
    return _album_ids(albums)


# The producer's authority to create demand for a member IS that member's live
# connection row, and this is the statement that checks it.
#
# `reset_scope` sets `active = true` unconditionally, so without this guard the OQ6
# disconnect fence has a hole that the generation check cannot close: the cron selects
# connected members once, then spends a token exchange and a paginated library read per
# member before it writes. A member who disconnects inside that window would have their
# scope re-activated and their whole library re-added by the pass already in flight —
# with every generation check passing, because the pass mints a fresh generation itself
# rather than carrying a stale one.
#
# FOR UPDATE is what makes the two orderings both safe, by serialising against
# `IntegrationService.disconnect` (which deletes this row and revokes the scopes in one
# transaction):
#   * disconnect commits first  -> we find no row and produce nothing;
#   * disconnect commits second -> it rotates the generation we are already using, and
#     every later `add_demand` in this pass fails StaleDiscovery and stops it.
# Both take the integration row before the scope rows, so the lock order matches and
# they cannot deadlock.
#
# Existence, not `status = 'connected'`: disconnect is what removes the row, and OQ6 ties
# the fence to disconnect rather than to token health. A member in `reauth` still holds a
# connection, and the poll already declines to select them.
_CONNECTION_EXISTS = text(
    "SELECT 1 FROM user_integrations "
    "WHERE user_id = :member AND provider = 'spotify' "
    "FOR UPDATE"
)


def _existing_keys(session, scope_id) -> set:
    rows = session.execute(
        text("SELECT origin_key FROM lyrics_album_demands WHERE scope_id = :scope"),
        {"scope": scope_id},
    ).scalars()
    return set(rows)


def _apply(
    session_factory: Callable[[], Any],
    user_id: Any,
    origin: str,
    observed: Sequence[str],
    *,
    reconcile_removals: bool,
) -> Dict[str, int]:
    """Reconcile one origin's demand for one member. Returns {'added','removed'}.

    Refuses to do anything for a member with no Spotify connection row (see
    `_CONNECTION_EXISTS`), then opens the scope (creating it on first sight, rotating its
    generation), diffs the observation against what is already stored, and writes only
    the delta — each store call in its own short transaction, additions before removals.
    """
    result = {"added": 0, "removed": 0}

    # --- guard + scope + diff, one short transaction, then CLOSE -------------
    with session_factory() as session:
        if session.execute(_CONNECTION_EXISTS, {"member": user_id}).first() is None:
            logger.info(
                "member demand: no Spotify connection for user_id=%s — producing "
                "nothing for origin %s", user_id, origin,
            )
            session.rollback()
            return result
        store = LyricsDemandStore(session.connection())
        scope = store.reset_scope(user_id, origin)
        existing = _existing_keys(session, scope["id"])
        session.commit()
    scope_id, generation = scope["id"], scope["generation"]

    desired = list(dict.fromkeys(observed))
    additions = [sid for sid in desired if sid not in existing]
    removals = sorted(existing - set(desired)) if reconcile_removals else []

    # --- additions first: `remove_origin` below rotates the generation --------
    for sid in additions:
        try:
            with session_factory() as session:
                LyricsDemandStore(session.connection()).add_demand(
                    user_id, scope_id, generation, sid, sid
                )
                session.commit()
        except StaleDiscovery:
            # The member disconnected (or another tick reconciled the same origin)
            # while this pass was mid-flight. The fence did its job: stop adding
            # rather than resurrect demand the owner of the scope has revoked.
            logger.info(
                "member demand: %s scope superseded mid-pass (user_id=%s) — "
                "stopping after %d addition(s)", origin, user_id, result["added"],
            )
            return result
        result["added"] += 1

    for sid in removals:
        with session_factory() as session:
            LyricsDemandStore(session.connection()).remove_origin(
                user_id, origin, origin_key=sid
            )
            session.commit()
        result["removed"] += 1

    return result


def sync_member_demand(
    session_factory: Callable[[], Any],
    user_id: Any,
    *,
    saved_albums: Optional[Iterable[Dict[str, Any]]] = None,
    recent_items: Optional[Iterable[Dict[str, Any]]] = None,
) -> Dict[str, int]:
    """Produce durable album demand for ONE member from that member's own reads.

    `saved_albums` is the fully paginated /me/albums result, or None when the library
    could not be read this pass (missing `user-library-read` grant / provider failure).
    None means "no observation", which is NOT the same as "the library is empty": an
    empty list reconciles every saved-origin demand away, so a failed read must never
    be passed as `[]`. `recent_items` is the raw recently-played page.

    Both origins are optional and independent — a member whose library read fails still
    gets recent-listening demand, and vice versa.
    """
    metrics = {
        "saved_added": 0, "saved_removed": 0,
        "recent_added": 0, "saved_skipped": 0,
    }

    if saved_albums is None:
        metrics["saved_skipped"] = 1
    else:
        saved = _apply(
            session_factory, user_id, SAVED_ORIGIN, _album_ids(saved_albums),
            reconcile_removals=True,
        )
        metrics["saved_added"], metrics["saved_removed"] = saved["added"], saved["removed"]

    if recent_items is not None:
        recent = _apply(
            session_factory, user_id, RECENT_ORIGIN, _recent_album_ids(recent_items),
            reconcile_removals=False,
        )
        metrics["recent_added"] = recent["added"]

    return metrics
