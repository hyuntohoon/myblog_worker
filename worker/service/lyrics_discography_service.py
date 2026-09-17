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

**Session discipline** (the recurring bug class): every Spotify page is fetched with
NO DB session open. Fetch page -> close -> short write session -> next page.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

from sqlalchemy import text

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


# Artists that still owe pages: never enumerated, interrupted mid-way, or a completed
# discography whose refresh has come due (the producer resets `complete` for those, so
# "not complete and due" is the single selector for all three).
_SELECT_DUE = text(
    """
    SELECT spotify_artist_id, next_offset
      FROM lyrics_artist_discographies
     WHERE NOT complete
       AND (next_attempt_at IS NULL OR next_attempt_at <= now())
     ORDER BY next_attempt_at NULLS FIRST, spotify_artist_id
     LIMIT :lim
    """
)

_COUNT_DUE = text(
    """
    SELECT count(*) FROM lyrics_artist_discographies
     WHERE NOT complete AND (next_attempt_at IS NULL OR next_attempt_at <= now())
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

_ADVANCE = text(
    """
    UPDATE lyrics_artist_discographies
       SET next_offset = :offset, album_total = :total, last_reason = NULL,
           next_attempt_at = NULL, updated_at = now()
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
_REOPEN_STALE = text(
    """
    UPDATE lyrics_artist_discographies
       SET complete = false, next_offset = 0, next_attempt_at = NULL, updated_at = now()
     WHERE complete
       AND (last_complete_at IS NULL OR last_complete_at < now() - CAST(:age AS interval))
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

    Returns the number re-opened. A non-positive age disables the refresh entirely,
    which is the honest way to turn it off — not a silent no-op default.
    """
    if max_age_hours <= 0:
        return 0
    with session_factory() as session:
        result = session.execute(_REOPEN_STALE, {"age": f"{int(max_age_hours)} hours"})
        session.commit()
    return result.rowcount or 0


def due_count(session_factory: Callable[[], Any]) -> int:
    """How many artists still owe pages right now (0 ⇒ nothing to enqueue)."""
    with session_factory() as session:
        return int(session.execute(_COUNT_DUE).scalar_one())


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
    result = {"pages": 0, "albums": 0, "complete": 0, "deferred": 0}
    offset = max(0, int(start_offset))

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
        "completed": 0, "deferred": 0, "remaining": 0,
    }
    if artist_sids:
        metrics["requested"] = ensure_artists(session_factory, artist_sids)

    with session_factory() as session:
        due = session.execute(_SELECT_DUE, {"lim": max_artists}).fetchall()
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

    metrics["remaining"] = due_count(session_factory)
    logger.info("discography enumeration metrics: %s", metrics)
    return metrics
