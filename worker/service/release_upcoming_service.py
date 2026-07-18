# FEAT-release-calendar Step 4 — multi-source upcoming-release poller (announced path).
#
# Two source passes, each its own EventBridge schedule (worker/handler.py routes
# {"job":"release_upcoming_poll","mode":...}); separate schedules so one source
# lagging or failing never delays the other (RFC Step 4), and neither touches the
# blogSQS album-sync queue — a MB/iTunes outage must not clog album sync (the same
# boundary that keeps the alias fill on EventBridge).
#
# Fan-out design: STATELESS TIME-BUCKET ROTATION (album_ingest precedent) instead
# of SQS chunk fan-out or a DB cursor row. The watchlist (stable spotify_id order)
# is partitioned into ceil(n / per_tick) buckets; the tick's bucket is
# ticks-since-epoch modulo bucket count. No cursor state to migrate or corrupt; a
# failed tick's bucket is simply revisited next cycle (announcement lead times are
# weeks — probe median 43.5 d — so a missed tick costs nothing). Each tick is
# bounded by a per-tick artist cap AND a wall-clock budget for the 120 s Lambda.
#
# Session discipline (reference-db-session-across-long-external-loop): the
# watchlist is fetched → materialized → session CLOSED before the external loop;
# writes happen in a fresh short session after the loop. Upserts are sorted by the
# conflict key (deadlock rule) and NEVER touch status / spotify_album_id — a
# Step-5 'released' confirmation can never be downgraded back to 'announced'.
#
# Raw text() SQL — the worker needs NO shared_db pin bump for the V44 tables
# (lastfm_sync_service precedent); only the migration must be applied first.
from __future__ import annotations

import logging
import math
import time
from datetime import date, timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text

from worker.core.config import settings

logger = logging.getLogger(__name__)

# Tick-index divisors — MUST match the eventbridge.tf schedule rates
# (worker-release-upcoming-mb rate(1 hour) / worker-release-upcoming-itunes
# rate(30 minutes)). A mismatch only stretches/shrinks the coverage cycle;
# correctness (which artists a tick queries) is unaffected.
MB_TICK_SECONDS = 3600
ITUNES_TICK_SECONDS = 1800

# artist_source_ids sentinel for a failed iTunes resolution (MBID_NOT_FOUND
# precedent — prevents re-querying iTunes for the same artist every cycle).
ITUNES_ID_NOT_FOUND = "not_found"

# resolved_via for a sentinel written because the artist had no UPC-bearing
# catalog album (vs a lookup miss, resolved_via='not_found'). no_upc sentinels
# bypass the retry_days gate: re-checking costs only the already-batched UPC
# prefetch SELECT (zero HTTP unless a UPC has appeared), and after OQ5 widens
# the catalog to singles/EPs a newly-ingested UPC-bearing release makes the
# artist resolvable at its next bucket visit instead of ≤30 d later. Legacy
# rows (both kinds written as 'not_found' pre-Step-5) keep the 30 d gate; a
# one-time ops DELETE clears the no-UPC ones (Step 5 PR body).
RESOLVED_VIA_NO_UPC = "no_upc"

# Poll scope = popularity watchlist ∪ user-tracked artists (FEAT-personal-
# release-tracking Step 4a): an artist a member tracks must get upcoming
# discovery even below the popularity floor. Both source passes share this rule.
_SELECT_MB_WATCHLIST = text(
    """
    SELECT id AS artist_id, musicbrainz_id
      FROM artists
     WHERE (popularity >= :pop_min
            OR EXISTS (SELECT 1 FROM user_artist_tracks t WHERE t.artist_id = artists.id))
       AND musicbrainz_id IS NOT NULL
       AND musicbrainz_id <> 'not_found'
     ORDER BY spotify_id
    """
)

_SELECT_ITUNES_WATCHLIST = text(
    """
    SELECT a.id AS artist_id,
           asi.source_artist_id AS itunes_id,
           (asi.resolved_at < now() - make_interval(days => :retry_days)) AS sentinel_stale,
           asi.resolved_via AS resolved_via
      FROM artists a
      LEFT JOIN artist_source_ids asi
        ON asi.artist_id = a.id AND asi.source = 'itunes'
     WHERE (a.popularity >= :pop_min
            OR EXISTS (SELECT 1 FROM user_artist_tracks t WHERE t.artist_id = a.id))
     ORDER BY a.spotify_id
    """
)

# Newest-first UPC-bearing catalog albums for the resolution pre-pass (the
# lookup?upc= chain tries the newest, then falls back to the next-newest —
# probe: 61% resolve on the newest alone, +fallback recovers stored-UPC misses).
_SELECT_UPCS = text(
    """
    SELECT aa.artist_id, al.ext_refs->>'upc' AS upc
      FROM albums al
      JOIN album_artists aa ON aa.album_id = al.id
     WHERE aa.artist_id = ANY(:artist_ids)
       AND al.ext_refs->>'upc' IS NOT NULL
     ORDER BY aa.artist_id, al.release_date DESC NULLS LAST
    """
)

_UPSERT_SOURCE_ID = text(
    """
    INSERT INTO artist_source_ids (artist_id, source, source_artist_id, resolved_via)
    VALUES (:artist_id, 'itunes', :source_artist_id, :resolved_via)
    ON CONFLICT (artist_id, source)
    DO UPDATE SET source_artist_id = EXCLUDED.source_artist_id,
                  resolved_via     = EXCLUDED.resolved_via,
                  resolved_at      = now()
    """
)

# Deliberately does NOT set status or spotify_album_id on conflict: those belong
# to the Step-5 release-day confirmation and must survive re-announcement upserts.
_UPSERT_EVENT = text(
    """
    INSERT INTO artist_release_events
        (artist_id, source, source_key, title, release_type, release_date, status)
    VALUES (:artist_id, :source, :source_key, :title, :release_type, :release_date, 'announced')
    ON CONFLICT (source, source_key)
    DO UPDATE SET title        = EXCLUDED.title,
                  release_type = EXCLUDED.release_type,
                  release_date = EXCLUDED.release_date,
                  updated_at   = now()
    """
)


def _bucket(items: Sequence, per_tick: int, tick_index: int) -> List:
    """The tick's slice of the stable-ordered watchlist (stateless rotation).

    Intra-bucket fairness (Step 5 부수 픽스): the 90 s budget guard can stop a
    tick partway through its bucket, and a fixed start-at-0 order made the SAME
    tail artists the casualty every cycle (live: budget_stop on 8/14 MB ticks,
    stopping at 35–64 of 70). The bucket's start therefore rotates by the cycle
    index (tick_index // bucket count) — still fully stateless (derived from
    the tick clock, no cursor row) and bucket MEMBERSHIP is unchanged, only the
    visit order inside the bucket shifts each cycle. With observed progress of
    ≥ half the bucket per tick, every artist is reached within ~2 cycles; any
    progress ≥ 1 artist/tick still guarantees full coverage within
    len(bucket) cycles.
    """
    buckets = max(1, math.ceil(len(items) / per_tick))
    start = (tick_index % buckets) * per_tick
    sweep = list(items[start : start + per_tick])
    if len(sweep) > 1:
        offset = (tick_index // buckets) % len(sweep)
        sweep = sweep[offset:] + sweep[:offset]
    return sweep


def _is_full_date(raw: Optional[str]) -> bool:
    return bool(raw) and len(raw) == 10


def _in_window(iso_day: str, today: date, horizon_days: int) -> bool:
    try:
        d = date.fromisoformat(iso_day)
    except ValueError:
        return False
    return today <= d <= today + timedelta(days=horizon_days)


def _mb_release_type(primary_type: Optional[str]) -> Optional[str]:
    if not primary_type:
        return None
    return {"album": "album", "ep": "ep", "single": "single"}.get(
        primary_type.lower(), "other"
    )


def _itunes_release_type(collection_name: str) -> str:
    # iTunes has no explicit type field; the storefront convention suffixes
    # singles/EPs on the collection name.
    if collection_name.endswith(" - Single"):
        return "single"
    if collection_name.endswith(" - EP"):
        return "ep"
    return "album"


def _dedup_sorted(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Dedup on the conflict key (a collab release can surface under several
    swept artists in one tick), then sort by it (deadlock rule)."""
    seen: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in rows:
        seen.setdefault((r["source"], r["source_key"]), r)
    return [seen[k] for k in sorted(seen)]


def _mb_pass(
    session_factory,
    mb_search: Callable[[str, str, str], List[dict]],
    *,
    tick_index: int,
    today: date,
    counters: Dict[str, int],
) -> List[Dict[str, Any]]:
    with session_factory() as session:
        rows = session.execute(
            _SELECT_MB_WATCHLIST, {"pop_min": settings.RELEASE_POLL_POP_MIN}
        ).fetchall()
    watchlist = [(r.artist_id, r.musicbrainz_id) for r in rows]
    counters["eligible"] = len(watchlist)

    sweep = _bucket(watchlist, settings.RELEASE_POLL_MB_ARTISTS_PER_TICK, tick_index)
    counters["swept"] = len(sweep)

    date_from = today.isoformat()
    date_to = (today + timedelta(days=settings.RELEASE_POLL_HORIZON_DAYS)).isoformat()

    events: List[Dict[str, Any]] = []
    started = time.monotonic()
    for i, (artist_id, mbid) in enumerate(sweep):
        if i and time.monotonic() - started > settings.RELEASE_POLL_TIME_BUDGET_SEC:
            counters["budget_stop"] = 1
            logger.warning("release poll (mb): budget stop after %d/%d artists", i, len(sweep))
            break
        try:
            rgs = mb_search(mbid, date_from, date_to)
        except Exception as exc:  # one slow/failed artist never fails the tick
            counters["errors"] += 1
            logger.warning("release poll (mb): artist %s failed: %s", mbid, exc)
            continue
        counters["polled"] += 1
        for rg in rgs:
            first_date = rg.get("first-release-date", "")
            # full-date-only v1 (OQ3): partial dates seen in the probe were all
            # bare-year placeholders; the search window can also return edge
            # rows — re-check the window locally.
            if not _is_full_date(first_date):
                continue
            if not _in_window(first_date, today, settings.RELEASE_POLL_HORIZON_DAYS):
                continue
            if not rg.get("id") or not rg.get("title"):
                continue
            events.append(
                {
                    "artist_id": artist_id,
                    "source": "musicbrainz",
                    "source_key": rg["id"],
                    "title": rg["title"],
                    "release_type": _mb_release_type(rg.get("primary-type")),
                    "release_date": first_date,
                }
            )
    return events


def _resolve_itunes_ids(
    itunes_client,
    to_resolve: List[Any],
    upcs_by_artist: Dict[Any, List[str]],
    *,
    started: float,
    counters: Dict[str, int],
) -> Dict[Any, Tuple[Optional[str], str]]:
    """UPC hard-ID chain per unresolved artist: newest UPC → next-newest → sentinel.
    Returns {artist_id: (itunes_id | None, resolved_via)} where resolved_via ∈
    'upc' (resolved) | 'no_upc' (no UPC-bearing album, zero HTTP) |
    'not_found' (all UPCs missed in iTunes — 30 d retry sentinel)."""
    resolutions: Dict[Any, Tuple[Optional[str], str]] = {}
    for artist_id in to_resolve:
        if time.monotonic() - started > settings.RELEASE_POLL_TIME_BUDGET_SEC:
            counters["budget_stop"] = 1
            break
        upcs = upcs_by_artist.get(artist_id, [])[:2]
        if not upcs:
            resolutions[artist_id] = (None, RESOLVED_VIA_NO_UPC)
            counters["no_upc"] += 1
            continue
        resolved: Optional[str] = None
        for upc in upcs:
            try:
                resolved = itunes_client.lookup_artist_by_upc(upc)
            except Exception as exc:
                counters["errors"] += 1
                logger.warning("release poll (itunes): upc %s lookup failed: %s", upc, exc)
                break  # transient — leave unresolved WITHOUT a sentinel (retried next cycle)
            if resolved:
                break
        else:
            resolutions[artist_id] = (None, ITUNES_ID_NOT_FOUND)  # all UPCs missed
            counters["resolve_miss"] += 1
            continue
        if resolved:
            resolutions[artist_id] = (resolved, "upc")
            counters["resolved"] += 1
    return resolutions


def _itunes_pass(
    session_factory,
    itunes_client,
    *,
    tick_index: int,
    today: date,
    counters: Dict[str, int],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    with session_factory() as session:
        rows = session.execute(
            _SELECT_ITUNES_WATCHLIST,
            {
                "pop_min": settings.RELEASE_POLL_POP_MIN,
                "retry_days": settings.RELEASE_POLL_RESOLVE_RETRY_DAYS,
            },
        ).fetchall()
    watchlist = [
        (r.artist_id, r.itunes_id, r.sentinel_stale, r.resolved_via) for r in rows
    ]
    counters["eligible"] = len(watchlist)

    sweep = list(
        _bucket(watchlist, settings.RELEASE_POLL_ITUNES_ARTISTS_PER_TICK, tick_index)
    )
    counters["swept"] = len(sweep)

    lookups: List[Tuple[Any, str]] = []  # (artist_id, itunes_id) to query
    to_resolve: List[Any] = []
    prior_no_upc: set = set()  # skip rewriting an unchanged no_upc sentinel
    for artist_id, itunes_id, sentinel_stale, resolved_via in sweep:
        if itunes_id is None or (
            itunes_id == ITUNES_ID_NOT_FOUND
            # no_upc sentinels bypass the retry gate (DB-only recheck, see
            # RESOLVED_VIA_NO_UPC); lookup-miss sentinels wait out retry_days.
            and (sentinel_stale or resolved_via == RESOLVED_VIA_NO_UPC)
        ):
            to_resolve.append(artist_id)
            if resolved_via == RESOLVED_VIA_NO_UPC:
                prior_no_upc.add(artist_id)
        elif itunes_id == ITUNES_ID_NOT_FOUND:
            counters["sentinel_skip"] += 1
        else:
            lookups.append((artist_id, itunes_id))

    # Prefetch the resolution pre-pass' UPC candidates in ONE short session,
    # closed before any HTTP call.
    upcs_by_artist: Dict[Any, List[str]] = {}
    if to_resolve:
        with session_factory() as session:
            upc_rows = session.execute(
                _SELECT_UPCS, {"artist_ids": to_resolve}
            ).fetchall()
        for r in upc_rows:
            upcs_by_artist.setdefault(r.artist_id, []).append(r.upc)

    started = time.monotonic()
    resolutions = _resolve_itunes_ids(
        itunes_client, to_resolve, upcs_by_artist, started=started, counters=counters
    )
    source_id_rows = [
        {
            "artist_id": artist_id,
            "source_artist_id": itunes_id or ITUNES_ID_NOT_FOUND,
            "resolved_via": via,
        }
        for artist_id, (itunes_id, via) in resolutions.items()
        # still-no-UPC recheck of an existing no_upc sentinel → no write churn
        if not (via == RESOLVED_VIA_NO_UPC and artist_id in prior_no_upc)
    ]
    lookups.extend(
        (artist_id, itunes_id)
        for artist_id, (itunes_id, _via) in resolutions.items()
        if itunes_id
    )

    events: List[Dict[str, Any]] = []
    for artist_id, itunes_id in lookups:
        if time.monotonic() - started > settings.RELEASE_POLL_TIME_BUDGET_SEC:
            counters["budget_stop"] = 1
            logger.warning("release poll (itunes): budget stop before artistId %s", itunes_id)
            break
        try:
            collections = itunes_client.get_artist_albums(itunes_id)
        except Exception as exc:
            counters["errors"] += 1
            logger.warning("release poll (itunes): artistId %s failed: %s", itunes_id, exc)
            continue
        counters["polled"] += 1
        for col in collections:
            iso_day = (col.get("releaseDate") or "")[:10]
            if not _is_full_date(iso_day):
                continue
            if not _in_window(iso_day, today, settings.RELEASE_POLL_HORIZON_DAYS):
                continue
            name = col.get("collectionName") or ""
            if not name:
                continue
            events.append(
                {
                    "artist_id": artist_id,
                    "source": "itunes",
                    "source_key": str(col["collectionId"]),
                    "title": name,
                    "release_type": _itunes_release_type(name),
                    "release_date": iso_day,
                }
            )
    return events, source_id_rows


def run_release_upcoming_poll(
    session_factory,
    *,
    mode: str,
    mb_search: Optional[Callable[[str, str, str], List[dict]]] = None,
    itunes_client=None,
    tick_index: Optional[int] = None,
    today: Optional[date] = None,
) -> Dict[str, int]:
    """One poller tick for one source. Returns the counter summary (also logged
    at WARNING so it lands in CloudWatch under the prod LOG_LEVEL=WARNING)."""
    if today is None:
        today = date.today()

    counters: Dict[str, int] = {
        "eligible": 0, "swept": 0, "polled": 0, "found": 0, "upserted": 0,
        "resolved": 0, "resolve_miss": 0, "no_upc": 0, "sentinel_skip": 0,
        "errors": 0, "budget_stop": 0,
    }

    if mode == "musicbrainz":
        if tick_index is None:
            tick_index = int(time.time() // MB_TICK_SECONDS)
        events = _mb_pass(
            session_factory, mb_search, tick_index=tick_index, today=today,
            counters=counters,
        )
        source_id_rows: List[Dict[str, Any]] = []
    elif mode == "itunes":
        if tick_index is None:
            tick_index = int(time.time() // ITUNES_TICK_SECONDS)
        events, source_id_rows = _itunes_pass(
            session_factory, itunes_client, tick_index=tick_index, today=today,
            counters=counters,
        )
    else:
        raise ValueError(f"unknown release poll mode: {mode}")

    events = _dedup_sorted(events)
    counters["found"] = len(events)

    # Fresh short write session after the external loop; rows already sorted by
    # the conflict key. Resolution rows sort by artist_id (their conflict key).
    if events or source_id_rows:
        with session_factory() as session, session.begin():
            for row in sorted(source_id_rows, key=lambda r: str(r["artist_id"])):
                session.execute(_UPSERT_SOURCE_ID, row)
            for row in events:
                counters["upserted"] += session.execute(_UPSERT_EVENT, row).rowcount

    logger.warning(
        "release poll (%s) summary: eligible=%d swept=%d polled=%d found=%d "
        "upserted=%d resolved=%d resolve_miss=%d no_upc=%d sentinel_skip=%d "
        "errors=%d budget_stop=%d",
        mode, counters["eligible"], counters["swept"], counters["polled"],
        counters["found"], counters["upserted"], counters["resolved"],
        counters["resolve_miss"], counters["no_upc"], counters["sentinel_skip"],
        counters["errors"], counters["budget_stop"],
    )
    return counters
