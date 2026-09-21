# worker/service/lyrics_discography_service.py
"""FEAT-lyrics-listening-experience Step 5 — complete discography enumeration.

Step 4 turned a member's saved albums and plays into demand. This module supplies
the other half of D4: an artist a member follows contributes their **whole** back
catalogue, not the most recent page of it. The RFC is explicit that "latest album
+ single" or a first page is not a complete catalogue, and D5 forbids capping the
scope to make it cheaper — so the work is bounded per invocation and *resumed*,
never truncated.

**Global, not member-scoped.** `/artists/{id}/albums` is public catalog data read
with the app's client-credentials token, so this table is shared: two members who
follow the same artist pay for one enumeration. Nothing member-identifying is
written here, and no member token is used, which is also why an enumeration can
outlive the member who first asked for it.

**`complete` is the load-bearing column, and it is about deletion.** Follow demand
is reconciled as a set. A half-enumerated artist looks like a *smaller* set, and a
producer that reconciled removals against it would delete demand nobody withdrew.
So the producer only ever removes an album for an artist whose enumeration has
finished; an artist mid-enumeration can gain albums but cannot lose them.

**OQ4 (2026-09-13).** `include_groups=album,single` — the artist's own records, and
not every compilation they were pressed onto or record they appear on. Spotify
delivers EPs inside the `single` group, so the two values cover album/single/EP as
the decision requires. `release_group` stores what came back rather than assuming
it, so widening OQ4 later is a producer change and not a migration.

**A registration is demand-driven, and so is the decision to read it** (2026-09-21).
`ensure_artists` is the only writer and the follow reconciler the only caller, so the
table means "some member's follow universe contains this artist" — but nothing used to
ask the reverse question, so an artist who was unfollowed or excluded on the site kept
being paged for ever on the refresh timer with nothing consuming the result, against a
quota this project cannot replace. `_CONSUMED` is that question, and it fences every
path that spends a provider call. Nothing is deleted: the enumeration already read is
the expensive artefact, and keeping it means a re-follow, a reconnect or a lifted
exclusion costs zero further reads.

**Session discipline** (the recurring bug class): every Spotify page is fetched with
NO DB session open. Fetch page -> close -> short write session -> next page.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

from sqlalchemy import text

from worker.service.lyrics_member_demand_service import FOLLOW_ORIGIN

logger = logging.getLogger(__name__)

# OQ4's boundary, in one place. Compilations and `appears_on` are deliberately out:
# the measurement that authorised this step found the release-type boundary to be the
# largest single lever on the translation multiplier, and "every record they appear
# on" is not what following an artist means.
ELIGIBLE_GROUPS = ("album", "single")
_INCLUDE_GROUPS = ",".join(ELIGIBLE_GROUPS)

# V58 CHECKs both provider ids at 1..128 chars.
_MAX_PROVIDER_ID = 128

_PAGE_LIMIT = 50


# ── is anybody still consuming this artist? ──────────────────────────────────
#
# One spelling, interpolated into every statement that can lead to a provider call:
# `_SELECT_DUE`, `_COUNT_DUE` (which must agree with it, or the nudge enqueues runs that
# find nothing) and `_REOPEN_STALE`. A second spelling would be a leak nobody sees.
#
# **Why a fence and not a delete.** The obvious remedy for an unfollowed artist is to
# delete the registration, and it was written that way first. It is strictly more
# destructive than the defect requires: `invalid_grant` — a member's token expiring, or
# them removing the app at spotify.com — flips them to 'reauth' and, in the SAME
# transaction, revokes both their follow demand and their `spotify_follow` edges. Every
# registration only that member held would lose both signals at once, and deleting them
# cascades `lyrics_artist_albums` and the page checkpoints, so the recoverable state the
# 3b-e reconnect badge exists for would cost a full re-enumeration out of the quota.
# Fencing the reads stops the spend and keeps the corpus, so a reconnect, a re-follow or
# a lifted exclusion resumes at zero provider cost. (Owner decision, 2026-09-21.)
#
# Two signals, because neither covers the table alone:
#
#   * the tracked edge, via `user_artist_track_origins` minus that member's own
#     exclusions — the ONLY signal for an artist whose discography turned out empty,
#     since no albums means no demand;
#   * follow demand, via `lyrics_album_demands` on the 'follow' scope, whose origin_key
#     IS the artist id — the ONLY signal for a followed artist the catalog does not
#     have, because an uncatalogued artist has no `artists.id` and therefore no edge at
#     all. That is why this step keys on provider ids end to end.
#
# The member fence is `status = 'connected'`, a deliberate TWIN of
# `spotify_member_sync_service._SELECT_CONNECTED` and not the producer's
# `_CONNECTION_EXISTS` (which ignores status): a follow universe only exists for members
# that selector returns, so anyone it skips has no universe and their edges must not keep
# an enumeration running. A change to that selector belongs here in the same commit.
#
# The third arm is not a consumer, it is the DEADLOCK BREAK. A registration nobody has
# read yet has no albums, therefore no demand, and if the artist is uncatalogued it has
# no edge either — so the two signals cannot see it, and a fence without this arm would
# starve exactly the artists Step 5 keyed on provider ids to support: no read, no albums,
# no demand, no read.
#
# It is `last_complete_at IS NULL` — "has never finished a pass" — and NOT "has never read
# a page". A first pass over a large discography spans many runs, and demand for the
# albums it has already found is only written by the next member poll, so the narrower
# version stalls every multi-page uncatalogued artist after its first page and makes
# progress depend on a 15-minute tick. A row the refresh re-opened keeps its
# `last_complete_at`, so it does not qualify — and cannot, because `_REOPEN_STALE` is
# fenced too. The cost of the wider arm is that an artist unfollowed DURING their first
# enumeration finishes it: bounded by one pass, and harmless because nothing is deleted.
#
# Known gap, accepted: an uncatalogued artist whose completed pass found NO eligible
# release has neither signal and no longer qualifies here, so a release they put out later
# is never picked up. They cost nothing while that is true, and a catalog entry or one
# album brings them back.
_CONSUMED = """
    (
      EXISTS (
        SELECT 1
          FROM user_artist_track_origins o
          JOIN artists a ON a.id = o.artist_id
         WHERE a.spotify_id = {d}.spotify_artist_id
           AND EXISTS (
                 SELECT 1 FROM user_integrations ui
                  WHERE ui.user_id = o.user_id AND ui.provider = 'spotify'
                    AND ui.status = 'connected' AND ui.payload IS NOT NULL)
           AND NOT EXISTS (
                 SELECT 1 FROM user_artist_follow_exclusions e
                  WHERE e.user_id = o.user_id AND e.artist_id = o.artist_id))
      OR EXISTS (
        SELECT 1
          FROM lyrics_album_demands dm
          JOIN lyrics_discovery_scopes s ON s.id = dm.scope_id
         WHERE s.origin = :follow AND dm.origin_key = {d}.spotify_artist_id)
      OR {d}.last_complete_at IS NULL
    )
"""


# Artists that still owe pages: never enumerated, interrupted mid-way, or a completed
# discography whose refresh has come due (the producer resets `complete` for those, so
# "not complete and due" is the single selector for all three).
# Selecting and CLAIMING in one statement. `next_attempt_at` doubles as the lease: a
# claimed artist stops being due, so a second chain's `_SELECT_DUE` skips it instead of
# issuing the same provider pages. Without this the poll's chain and a connect-time
# chain select the same top-N and pay for every page twice.
#
# The lease is short and self-healing: a run that dies without advancing leaves the
# artist due again after it expires, which is the same recovery a deferred failure gets.
_SELECT_DUE = text(
    """
    WITH due AS (
        SELECT dd.spotify_artist_id
          FROM lyrics_artist_discographies dd
         WHERE NOT dd.complete
           AND (dd.next_attempt_at IS NULL OR dd.next_attempt_at <= now())
           AND """ + _CONSUMED.format(d="dd").strip() + """
         ORDER BY dd.next_attempt_at NULLS FIRST, dd.spotify_artist_id
         LIMIT :lim
         FOR UPDATE OF dd SKIP LOCKED
    )
    UPDATE lyrics_artist_discographies d
       SET next_attempt_at = now() + make_interval(secs => :lease), updated_at = now()
      FROM due
     WHERE d.spotify_artist_id = due.spotify_artist_id
    RETURNING d.spotify_artist_id, d.next_offset
    """
)

# Deliberately the same predicate as `_SELECT_DUE`. The nudge enqueues on this count, so
# a looser one would produce a message every 15 minutes for work the run then declines —
# and, worse, `remaining` feeds the self-chain, which would hop to its cap doing nothing.
_COUNT_DUE = text(
    """
    SELECT count(*) FROM lyrics_artist_discographies dd
     WHERE NOT dd.complete
       AND (dd.next_attempt_at IS NULL OR dd.next_attempt_at <= now())
       AND """ + _CONSUMED.format(d="dd").strip() + """
    """
)

# Registering an artist must never disturb an enumeration already in progress, so the
# conflict arm touches nothing. `ensure_artists` is called on every poll tick.
_ENSURE_ARTIST = text(
    """
    INSERT INTO lyrics_artist_discographies (spotify_artist_id)
    VALUES (:artist)
    ON CONFLICT (spotify_artist_id) DO NOTHING
    """
)

# Sorted by the conflict key in the caller (bulk-upsert deadlock rule).
_INSERT_ALBUM = text(
    """
    INSERT INTO lyrics_artist_albums (spotify_artist_id, spotify_album_id, release_group)
    VALUES (:artist, :album, :group)
    ON CONFLICT (spotify_artist_id, spotify_album_id) DO NOTHING
    """
)

# GREATEST, not a blind SET: two chains can be in flight at once (the 15-minute poll
# and a member connecting), and a slower one finishing an EARLIER page would otherwise
# write a smaller offset and rewind the checkpoint, so those pages get read again — for
# ever, under a sustained backlog, against a provider quota this project cannot replace.
_ADVANCE = text(
    """
    UPDATE lyrics_artist_discographies
       SET next_offset = GREATEST(next_offset, :offset), album_total = :total,
           last_reason = NULL, next_attempt_at = NULL, updated_at = now()
     WHERE spotify_artist_id = :artist
    """
)

_FINISH = text(
    """
    UPDATE lyrics_artist_discographies
       SET next_offset = 0, complete = true, album_total = :total, last_reason = NULL,
           next_attempt_at = NULL, last_complete_at = now(), updated_at = now()
     WHERE spotify_artist_id = :artist
    """
)

# Releases that were in this artist's discography and are not any more — delisted, or
# re-issued under a new id. Without this the table only ever grows, and `complete` never
# authorises a removal because the desired set never shrinks: the producer could add but
# never subtract, which is the one-way ratchet the store's `remove_demand` was added to
# break. Run ONLY after a pass that read the discography from offset 0 in one invocation;
# a resumed pass has not seen the earlier pages and must not conclude they are gone.
_PRUNE_UNSEEN = text(
    """
    DELETE FROM lyrics_artist_albums
     WHERE spotify_artist_id = :artist AND NOT (spotify_album_id = ANY(:seen))
    """
)

# A failure keeps `next_offset` exactly where it was: the point of the checkpoint is
# that a provider outage costs the pages it interrupted, not the ones already read.
_DEFER = text(
    """
    UPDATE lyrics_artist_discographies
       SET last_reason = :reason, next_attempt_at = :due, updated_at = now()
     WHERE spotify_artist_id = :artist
    """
)

# Re-open a completed discography so the enumerator reads it again. New releases are
# the reason: an artist a member follows keeps releasing, and D4 wants those linked.
# `complete` goes false but the rows stay, so the producer keeps every album it has
# while it declines to reconcile removals for this artist until the pass finishes.
# Fenced on `_CONSUMED`, because THIS is the statement that turned a stale registration
# into recurring provider traffic: it re-opened every completed artist on a timer with no
# reference to whether anybody still followed them.
_REOPEN_STALE = text(
    """
    UPDATE lyrics_artist_discographies dd
       SET complete = false, next_offset = 0, next_attempt_at = NULL, updated_at = now()
     WHERE dd.complete
       AND (dd.last_complete_at IS NULL
            OR dd.last_complete_at < now() - CAST(:age AS interval))
       AND """ + _CONSUMED.format(d="dd").strip() + """
    """
)


def _clean_ids(values: Iterable[Any]) -> List[str]:
    """Provider ids that V58's CHECK will accept, de-duped, first-seen order."""
    out: List[str] = []
    seen: set = set()
    for value in values or []:
        if not isinstance(value, str):
            continue
        value = value.strip()
        if not value or len(value) > _MAX_PROVIDER_ID or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def ensure_artists(session_factory: Callable[[], Any], artist_sids: Iterable[str]) -> int:
    """Register artists for enumeration. Idempotent; returns how many were requested.

    Sorted before insert so two concurrent ticks registering overlapping artist sets
    take the same row order (bulk-upsert deadlock rule).
    """
    sids = sorted(_clean_ids(artist_sids))
    if not sids:
        return 0
    with session_factory() as session:
        for sid in sids:
            session.execute(_ENSURE_ARTIST, {"artist": sid})
        session.commit()
    return len(sids)


def reopen_stale_discographies(session_factory: Callable[[], Any], max_age_hours: int) -> int:
    """Re-open completed discographies older than `max_age_hours` so new releases land.

    Only for artists somebody still consumes (`_CONSUMED`). A non-positive age disables
    the refresh entirely, which is the honest way to turn it off — not a silent no-op
    default.

    Returns the number re-opened.
    """
    if max_age_hours <= 0:
        return 0
    with session_factory() as session:
        result = session.execute(
            _REOPEN_STALE,
            {"age": f"{int(max_age_hours)} hours", "follow": FOLLOW_ORIGIN},
        )
        session.commit()
    return result.rowcount or 0


def due_count(session_factory: Callable[[], Any]) -> int:
    """How many artists still owe pages right now (0 ⇒ nothing to enqueue)."""
    with session_factory() as session:
        return int(
            session.execute(_COUNT_DUE, {"follow": FOLLOW_ORIGIN}).scalar_one()
        )


def _page(catalog_client, artist_sid: str, offset: int) -> Dict[str, Any]:
    return catalog_client.get_artist_albums_page(
        artist_sid, include_groups=_INCLUDE_GROUPS, offset=offset, limit=_PAGE_LIMIT
    )


def _enumerate_one(
    session_factory: Callable[[], Any],
    catalog_client,
    artist_sid: str,
    start_offset: int,
    *,
    max_pages: int,
    retry_seconds: int,
) -> Dict[str, int]:
    """Read up to `max_pages` pages of one artist, checkpointing after each.

    Returns {'pages', 'albums', 'complete', 'deferred'}. Never raises for a provider
    failure: one dead artist must not cost the rest of the run its progress.
    """
    result = {"pages": 0, "albums": 0, "complete": 0, "deferred": 0, "pruned": 0}
    offset = max(0, int(start_offset))
    # Only a pass that starts at the beginning sees the whole discography, so only that
    # pass may conclude a release has disappeared. A resumed pass skips the prune; the
    # next refresh resets the checkpoint to 0 and picks it up.
    full_pass = offset == 0
    seen: List[str] = []

    for _ in range(max_pages):
        # --- provider read, NO session held -------------------------------
        try:
            payload = _page(catalog_client, artist_sid, offset) or {}
        except Exception as e:
            due = datetime.now(timezone.utc) + timedelta(seconds=retry_seconds)
            with session_factory() as session:
                session.execute(
                    _DEFER,
                    {"artist": artist_sid, "reason": type(e).__name__, "due": due},
                )
                session.commit()
            logger.warning(
                "discography page failed (checkpoint kept at offset=%d) for artist=%s: %s",
                offset, artist_sid, type(e).__name__,
            )
            result["deferred"] = 1
            return result

        items = payload.get("items") or []
        total = payload.get("total")
        rows = []
        for item in items:
            album_id = (item or {}).get("id")
            if not isinstance(album_id, str):
                continue
            album_id = album_id.strip()
            if not album_id or len(album_id) > _MAX_PROVIDER_ID:
                continue
            # `album_group` is what this release is *to this artist*; `album_type` is
            # what it is in general. OQ4 is a statement about the former, and only the
            # former distinguishes "their single" from "a compilation they are on".
            group = (item or {}).get("album_group") or (item or {}).get("album_type")
            if group not in ELIGIBLE_GROUPS:
                # Spotify honours include_groups, so this is a provider surprise
                # rather than an expected filter. Dropping it keeps the OQ4 boundary
                # true even if the parameter is ever ignored.
                continue
            rows.append({"artist": artist_sid, "album": album_id, "group": group})

        # Sorted by the conflict key before insert (bulk-upsert deadlock rule).
        rows.sort(key=lambda r: r["album"])
        seen.extend(row["album"] for row in rows)
        offset += len(items)
        # `next` is the authoritative paginator, exactly as in the library clients;
        # `total` only guards a never-null `next`.
        finished = not items or not payload.get("next")
        if isinstance(total, int) and offset >= total:
            finished = True

        # --- one short write session per page -----------------------------
        with session_factory() as session:
            for row in rows:
                session.execute(_INSERT_ALBUM, row)
            if finished:
                if full_pass:
                    result["pruned"] = session.execute(
                        _PRUNE_UNSEEN, {"artist": artist_sid, "seen": seen or [""]}
                    ).rowcount or 0
                session.execute(_FINISH, {"artist": artist_sid, "total": total})
            else:
                session.execute(
                    _ADVANCE, {"artist": artist_sid, "offset": offset, "total": total}
                )
            session.commit()

        result["pages"] += 1
        result["albums"] += len(rows)
        if finished:
            result["complete"] = 1
            return result

    # Out of page budget with the checkpoint advanced — the next run resumes here.
    return result


def run_discography_enumeration(
    session_factory: Callable[[], Any],
    catalog_client,
    *,
    artist_sids: Optional[Sequence[str]] = None,
    max_artists: int = 10,
    max_pages_per_artist: int = 10,
    retry_seconds: int = 900,
    lease_seconds: int = 300,
) -> Dict[str, int]:
    """Enumerate due artists' discographies, bounded and resumable.

    `artist_sids` registers those artists first (the follow reconciler passes the
    member's followed set); the run itself always selects from the due queue, so a
    message naming an artist who is already complete costs one no-op upsert rather
    than a re-read.

    Returns a metrics dict including `remaining`, which the caller uses to decide
    whether to chain another invocation.
    """
    metrics = {
        "requested": 0, "artists": 0, "pages": 0, "albums": 0,
        "completed": 0, "deferred": 0, "pruned": 0, "remaining": 0,
    }
    if artist_sids:
        metrics["requested"] = ensure_artists(session_factory, artist_sids)

    with session_factory() as session:
        due = session.execute(
            _SELECT_DUE,
            {"lim": max_artists, "lease": lease_seconds, "follow": FOLLOW_ORIGIN},
        ).fetchall()
        session.commit()
    if not due:
        logger.info("discography enumeration: nothing due")
        return metrics

    for artist_sid, next_offset in due:
        one = _enumerate_one(
            session_factory, catalog_client, artist_sid, next_offset,
            max_pages=max_pages_per_artist, retry_seconds=retry_seconds,
        )
        metrics["artists"] += 1
        metrics["pages"] += one["pages"]
        metrics["albums"] += one["albums"]
        metrics["completed"] += one["complete"]
        metrics["deferred"] += one["deferred"]
        metrics["pruned"] += one["pruned"]

    metrics["remaining"] = due_count(session_factory)
    logger.info("discography enumeration metrics: %s", metrics)
    return metrics
