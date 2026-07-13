# FEAT-release-calendar Step 5 — release-day confirmation (released path).
#
# Called from run_album_ingest with the tick's confirm candidates: albums the
# catalog sweep encountered for WATCHLIST artists whose full release_date falls
# inside the calendar-relevant window. For each candidate the matching
# artist_release_events rows — same artist, normalized-title match, release_date
# within ± RELEASE_CONFIRM_DATE_PROXIMITY_DAYS, across ALL sources — flip to
# status='released' + spotify_album_id (the cross-source collapse point of the
# RFC's hard-key-merge design: a confirm-time match, not a data merge). A
# release never announced by any source gets a spotify-source 'released' row
# instead, so day-0 discoveries still appear on the calendar.
#
# STEP-4 INVARIANT: the regular observation upsert in release_upcoming_service
# deliberately never touches status / spotify_album_id — this module is the
# ONLY status-transition path, so a 'released' row can never be downgraded.
#
# Session discipline: the caller's external-API loop is already over when this
# runs; reads happen in one short session, writes in a fresh short transaction
# (reference-db-session-across-long-external-loop). Raw text() SQL — no
# shared_db pin bump (Step 4 precedent; only the V44 migration must exist).
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Dict, List

from sqlalchemy import text

from worker.core.config import settings

logger = logging.getLogger(__name__)

# TWIN: myblog_music/app/services/release_calendar_service.py normalize_title()
# — the Step 6 display soft-grouping key. The confirm match must collapse the
# same groups the calendar renders, so the two copies must stay in sync (worker
# never imports across repos; cross-repo twin rule — sweep both on change).
_ITUNES_SUFFIXES = (" - single", " - ep")


def normalize_title(title: str) -> str:
    """Confirm-match key component (NOT stored): casefold + collapse whitespace
    + strip iTunes storefront ' - Single'/' - EP' suffixes."""
    norm = " ".join((title or "").split()).casefold()
    for suffix in _ITUNES_SUFFIXES:
        if norm.endswith(suffix):
            norm = norm[: -len(suffix)].rstrip()
            break
    return norm


_SELECT_EVENTS = text(
    """
    SELECT id, artist_id, title, release_date, status, spotify_album_id
      FROM artist_release_events
     WHERE artist_id = ANY(:artist_ids)
       AND release_date BETWEEN :date_min AND :date_max
    """
)

# Flip guard: rowcount only counts real transitions (the sweep re-encounters
# the same albums every rotation cycle — re-confirms must be no-op writes).
_FLIP_EVENTS = text(
    """
    UPDATE artist_release_events
       SET status = 'released',
           spotify_album_id = :spotify_album_id,
           updated_at = now()
     WHERE id = ANY(:ids)
       AND (status IS DISTINCT FROM 'released'
            OR spotify_album_id IS DISTINCT FROM :spotify_album_id)
    """
)

# Never-announced path. Unlike the Step-4 observation upsert, this IS the
# confirm path, so setting status/spotify_album_id on conflict is correct; the
# WHERE keeps re-encounters rowcount-silent (idempotent counters).
_INSERT_RELEASED = text(
    """
    INSERT INTO artist_release_events
        (artist_id, source, source_key, title, release_type, release_date,
         status, spotify_album_id)
    VALUES (:artist_id, 'spotify', :source_key, :title, :release_type,
            :release_date, 'released', :spotify_album_id)
    ON CONFLICT (source, source_key)
    DO UPDATE SET status           = 'released',
                  spotify_album_id = EXCLUDED.spotify_album_id,
                  title            = EXCLUDED.title,
                  release_type     = EXCLUDED.release_type,
                  release_date     = EXCLUDED.release_date,
                  updated_at       = now()
    WHERE artist_release_events.status IS DISTINCT FROM 'released'
       OR artist_release_events.spotify_album_id
          IS DISTINCT FROM EXCLUDED.spotify_album_id
    """
)


def match_events(
    candidate: Dict[str, Any],
    events: List[Dict[str, Any]],
    proximity_days: int,
) -> List[Dict[str, Any]]:
    """Announced rows the candidate confirms: same artist + normalized-title
    equality + release_date within ±proximity_days. Pure function (unit-tested
    directly); under-merge bias — a non-match inserts rather than fuzzy-merges."""
    cand_date = date.fromisoformat(candidate["release_date"])
    cand_norm = normalize_title(candidate["title"])
    matched = []
    for ev in events:
        if str(ev["artist_id"]) != str(candidate["artist_id"]):
            continue
        if abs((ev["release_date"] - cand_date).days) > proximity_days:
            continue
        if normalize_title(ev["title"]) != cand_norm:
            continue
        matched.append(ev)
    return matched


def confirm_release_events(
    session_factory, candidates: List[Dict[str, Any]], counters: Dict[str, int]
) -> None:
    """Flip matching announced rows to released / insert never-announced rows.

    ``candidates``: dicts with artist_id, spotify_album_id, title, release_type,
    release_date (ISO day string). Mutates ``counters`` in place
    (confirm_candidates / confirm_flipped / confirm_inserted).
    """
    # Dedup on (artist_id, album) — a collab album surfaces under each swept
    # artist; each artist still confirms its own event rows.
    seen: Dict[tuple, Dict[str, Any]] = {}
    for c in candidates:
        seen.setdefault((str(c["artist_id"]), c["spotify_album_id"]), c)
    cands = [seen[k] for k in sorted(seen)]
    counters["confirm_candidates"] = len(cands)
    if not cands:
        return

    proximity = settings.RELEASE_CONFIRM_DATE_PROXIMITY_DAYS
    cand_dates = [date.fromisoformat(c["release_date"]) for c in cands]
    artist_ids = sorted({c["artist_id"] for c in cands}, key=str)

    # One short read session — closed before any further work.
    with session_factory() as session:
        rows = session.execute(
            _SELECT_EVENTS,
            {
                "artist_ids": artist_ids,
                "date_min": min(cand_dates) - timedelta(days=proximity),
                "date_max": max(cand_dates) + timedelta(days=proximity),
            },
        ).fetchall()
    events = [
        {
            "id": r.id,
            "artist_id": r.artist_id,
            "title": r.title,
            "release_date": r.release_date,
            "status": r.status,
            "spotify_album_id": r.spotify_album_id,
        }
        for r in rows
    ]

    flips: List[Dict[str, Any]] = []
    inserts: List[Dict[str, Any]] = []
    for cand in cands:  # already sorted by (artist_id, album id) — deterministic
        matched = match_events(cand, events, proximity)
        if matched:
            flips.append(
                {
                    "ids": sorted((m["id"] for m in matched), key=str),
                    "spotify_album_id": cand["spotify_album_id"],
                }
            )
        elif not cand.get("passes_gate", False):
            # Never-announced AND below the catalog quality bar — do not put it
            # on the calendar (owner 2026-07-13; fail-closed when the caller
            # didn't annotate). Flips above are deliberately exempt.
            counters["confirm_gate_skipped"] = (
                counters.get("confirm_gate_skipped", 0) + 1
            )
        else:
            inserts.append(
                {
                    "artist_id": cand["artist_id"],
                    "source_key": cand["spotify_album_id"],
                    "title": cand["title"],
                    "release_type": cand.get("release_type"),
                    "release_date": cand["release_date"],
                    "spotify_album_id": cand["spotify_album_id"],
                }
            )

    if not flips and not inserts:
        return

    # Fresh short write transaction; inserts sorted by the conflict key and
    # flip id-lists pre-sorted (deadlock rule).
    with session_factory() as session, session.begin():
        for flip in flips:
            counters["confirm_flipped"] += session.execute(_FLIP_EVENTS, flip).rowcount
        for row in sorted(inserts, key=lambda r: r["source_key"]):
            counters["confirm_inserted"] += session.execute(
                _INSERT_RELEASED, row
            ).rowcount
