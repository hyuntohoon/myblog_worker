# Scheduled album-catalog ingest (FEAT-album-catalog-ingest Step 2).
#
# New-releases-only sweep: rotate through catalog artists whose popularity clears
# ARTIST_POP_MIN, pull their discography page, keep releases newer than
# INGEST_SINCE, drop already-known and low-popularity albums, and enqueue the rest
# onto the existing candidates→SQS→sync_albums_batch pipeline. This job only
# DISCOVERS + ENQUEUES — the heavy upsert runs in the SQS consumer, so a tick stays
# far under the 120 s Lambda budget.
#
# FEAT-release-calendar Step 5 (OQ5, owner-decided 2026-07-12): watchlist artists
# (popularity ≥ RELEASE_POLL_POP_MIN) sweep include_groups="album,single" so
# announced singles/EPs can confirm; non-watchlist artists keep the album-only
# sweep — the added volume stays bounded to watchlist scope. The same sweep feeds
# the release-day confirm path (release_confirm_service): watchlist albums whose
# full release_date falls in the calendar window flip matching
# artist_release_events rows to 'released' (or insert a spotify-source row).
#
# The artist rotation is stateless: eligible artists (stable spotify_id order) are
# partitioned into ceil(n / SWEEP_ARTISTS_PER_TICK) buckets and the tick's bucket is
# days-since-epoch modulo bucket count. No cursor row to migrate or corrupt; adding
# or removing artists shifts partitions slightly, but every artist is still visited
# once per cycle. NOTE: at the OQ5 floor (≥50 ≈ 1,530 eligible / 30 per tick /
# rate(1 day)) a full cycle is ~51 days — so a "release-day" confirm can lag the
# actual release by up to a cycle for a non-tracked artist; the per-tick knob is
# owner curation policy and was deliberately NOT changed here (flagged in the
# Step 5 report).
#
# FEAT-personal-release-tracking gap fix (found via a live "카탈로그 ID 없음"
# 나를 위한 새 앨범 report): a user_artist_tracks artist is pulled OUT of that
# rotation and swept every tick instead (still deduped against the day's
# regular bucket). Confirmation is exclusively driven by this sweep — the
# MB/iTunes announced-pollers (release_upcoming_service.py) already excused
# tracked artists from the popularity floor (2026-07-18), but this sweep
# didn't, so a tracked artist's own release could ride the full ~51-day
# rotation before ever getting a spotify_album_id / status='released', even
# once the album already existed in the catalog via an unrelated ingest path.
from __future__ import annotations

import logging
import math
import time
from datetime import date, timedelta
from typing import Any, Callable, Dict, List, Optional

from sqlalchemy import text

from worker.core.config import settings
from worker.service.release_confirm_service import confirm_release_events

logger = logging.getLogger(__name__)

# Watchlist flag rides along so the OQ5 widening + confirm path stay
# watchlist-scoped even if the two floors ever diverge again.
# The NOT IN list is a decoy exclusion: '클래식' is a substring of '클래식 록'
# (classic rock), '클래식 소울', '클래식 컨트리', '바로크 팝', and
# '클래식 크로스오버'. A bare substring rule would nuke AC/DC, Led Zeppelin,
# the Rolling Stones, etc.; the exclusions therefore use exact JSONB element
# matches only. This key reads ARTIST genres because tracks do not exist yet at
# ingest time. hashtext supplies a deterministic 1-in-:holdout_mod holdout so
# the misclassification rate stays measurable forever.
#
# `tracked` (FEAT-personal-release-tracking gap fix): a user_artist_tracks row
# both (a) admits a sub-:pop_min artist to `eligible` at all and (b) forces
# `watch` true for them, mirroring release_upcoming_service.py's MB/iTunes
# watchlist queries — those two already carve tracked artists out of the
# popularity floor (shipped 2026-07-18); this sweep, which is the ONLY path
# that ever confirms an artist_release_events row (spotify_album_id +
# status='released'), never got the same carve-out. `tracked` is also read
# below to pull these artists out of the day-bucket rotation.
_SELECT_ELIGIBLE = text(
    """
    SELECT spotify_id, id AS artist_id,
           (popularity >= :watch_min OR t.artist_id IS NOT NULL) AS watch,
           (t.artist_id IS NOT NULL) AS tracked,
           EXISTS (
             SELECT 1 FROM jsonb_array_elements_text(a.genres) g
              WHERE g NOT IN ('클래식 록','클래식 소울','클래식 컨트리','바로크 팝','클래식 크로스오버')
                AND ( g ~ '클래식|바로크|오페라|오케스트라|협주곡|체임버|인상주의|낭만주의'
                      OR g IN ('발레','합창') )
           ) AS classical,
           (abs(hashtext(spotify_id)) % :holdout_mod) = 0 AS holdout
      FROM artists a
      LEFT JOIN (SELECT DISTINCT artist_id FROM user_artist_tracks) t
        ON t.artist_id = a.id
     WHERE popularity >= :pop_min OR t.artist_id IS NOT NULL
     ORDER BY spotify_id
    """
)


def _release_date_key(raw: str) -> str:
    """Normalize Spotify's variable-precision release_date for string comparison.
    Precision is 'day' ('2026-03-27'), 'month' ('2026-03') or 'year' ('2026');
    padding with -01-01 maps coarser precisions to their earliest day."""
    return (raw + "-01-01")[:10] if raw else "0000-01-01"


def _spotify_release_type(album_type: Optional[str]) -> Optional[str]:
    # Spotify album_type ∈ album|single|compilation; EPs surface as 'single'
    # (matching the calendar's soft-grouping suffix handling on display).
    if not album_type:
        return None
    return {"album": "album", "single": "single", "ep": "ep"}.get(
        album_type.lower(), "other"
    )


def _confirm_candidate(
    artist_id: Any, alb: Dict[str, Any], win_lo: date, win_hi: date
) -> Optional[Dict[str, Any]]:
    """A watchlist artist's swept album qualifies for the Step-5 confirm when it
    has an id + name and a FULL release_date inside the calendar window
    [today − lookback, today + horizon]; partial dates can't anchor a calendar
    day (full-date-only v1, OQ3)."""
    raw = alb.get("release_date") or ""
    if len(raw) != 10 or not alb.get("id") or not alb.get("name"):
        return None
    try:
        d = date.fromisoformat(raw)
    except ValueError:
        return None
    if not (win_lo <= d <= win_hi):
        return None
    return {
        "artist_id": artist_id,
        "spotify_album_id": alb["id"],
        "title": alb["name"],
        "release_type": _spotify_release_type(alb.get("album_type")),
        "release_date": raw,
    }


def run_album_ingest(
    session_factory,
    catalog_client,
    enqueue: Callable[[List[str]], None],
    *,
    days_since_epoch: int | None = None,
    today: date | None = None,
) -> Dict[str, int]:
    """One ingest tick. Returns the counter summary (also logged at WARNING so it
    lands in CloudWatch under the prod LOG_LEVEL=WARNING)."""
    if days_since_epoch is None:
        days_since_epoch = int(time.time() // 86400)
    if today is None:
        today = date.today()

    counters = {
        "eligible": 0, "swept": 0, "tracked_swept": 0, "discovered": 0, "fresh": 0,
        "novel": 0, "passed_gate": 0, "enqueued": 0,
        "classical_excluded": 0, "classical_holdout": 0,
        "confirm_candidates": 0, "confirm_flipped": 0, "confirm_inserted": 0,
        "confirm_gate_skipped": 0,
    }

    # Read the catalog cap + eligible-artist list in a short session, then CLOSE
    # before the slow per-artist get_artist_albums loop. Neon's pooler drops a
    # connection left idle-in-transaction across a minutes-long external loop
    # (reference-db-session-across-long-external-loop).
    with session_factory() as session:
        album_count = session.execute(text("SELECT count(*) FROM albums")).scalar()
        if album_count is not None and album_count >= settings.MAX_CATALOG_ALBUMS:
            logger.warning(
                "album_ingest: catalog cap reached (%d >= %d) — skipping tick",
                album_count, settings.MAX_CATALOG_ALBUMS,
            )
            return counters

        rows = session.execute(
            _SELECT_ELIGIBLE,
            {
                "pop_min": settings.ARTIST_POP_MIN,
                "watch_min": settings.RELEASE_POLL_POP_MIN,
                "holdout_mod": settings.INGEST_CLASSICAL_HOLDOUT_MOD,
            },
        ).fetchall()

    classical_allowlist = set(settings.INGEST_CLASSICAL_ALLOWLIST)
    eligible = []
    for spotify_id, artist_id, watch, tracked, classical, holdout in rows:
        if (
            settings.INGEST_EXCLUDE_CLASSICAL
            and classical
            and spotify_id not in classical_allowlist
        ):
            if holdout:
                counters["classical_holdout"] += 1
            else:
                counters["classical_excluded"] += 1
                continue
        eligible.append((spotify_id, artist_id, bool(watch), bool(tracked)))
    counters["eligible"] = len(eligible)
    if not eligible:
        logger.warning("album_ingest: no artists clear ARTIST_POP_MIN=%d", settings.ARTIST_POP_MIN)
        return counters

    buckets = max(1, math.ceil(len(eligible) / settings.SWEEP_ARTISTS_PER_TICK))
    bucket = days_since_epoch % buckets
    start = bucket * settings.SWEEP_ARTISTS_PER_TICK
    bucketed = eligible[start : start + settings.SWEEP_ARTISTS_PER_TICK]

    # Tracked artists ride EVERY tick, outside the day-bucket rotation — a
    # ~1,530-artist pool at 30/tick means a bucket-rotated artist is only
    # revisited every ~51 days, which is also how long a personally-tracked
    # release could sit unconfirmed (see _SELECT_ELIGIBLE comment). Dedup
    # against this tick's bucket so a tracked artist who's also due for their
    # regular popularity-bucket turn isn't swept twice.
    tracked_artists = [e for e in eligible if e[3]]
    counters["tracked_swept"] = len(tracked_artists)
    tracked_ids = {e[1] for e in tracked_artists}
    merged = tracked_artists + [e for e in bucketed if e[1] not in tracked_ids]
    sweep = [(spotify_id, artist_id, watch) for spotify_id, artist_id, watch, _tracked in merged]
    counters["swept"] = len(sweep)

    # Slow external discovery loop — no DB session held open here.
    since = _release_date_key(settings.INGEST_SINCE)
    win_lo = today - timedelta(days=settings.RELEASE_CONFIRM_LOOKBACK_DAYS)
    win_hi = today + timedelta(days=settings.RELEASE_POLL_HORIZON_DAYS)
    fresh_ids: List[str] = []
    confirm_candidates: List[Dict[str, Any]] = []
    for artist_sid, artist_id, watch in sweep:
        items = catalog_client.get_artist_albums(
            artist_sid, include_groups="album,single" if watch else "album"
        )
        counters["discovered"] += len(items)
        for alb in items:
            if _release_date_key(alb.get("release_date", "")) >= since and alb.get("id"):
                fresh_ids.append(alb["id"])
            if watch:
                cand = _confirm_candidate(artist_id, alb, win_lo, win_hi)
                if cand:
                    confirm_candidates.append(cand)
    # collab albums can surface under several swept artists in one tick
    fresh_ids = list(dict.fromkeys(fresh_ids))
    counters["fresh"] = len(fresh_ids)

    # Fresh short session to resolve which discovered ids are novel.
    novel_ids: List[str] = []
    if fresh_ids:
        with session_factory() as session:
            known = session.execute(
                text("SELECT spotify_id FROM albums WHERE spotify_id = ANY(:sids)"),
                {"sids": fresh_ids},
            ).fetchall()
        known_set = {r[0] for r in known}
        novel_ids = [i for i in fresh_ids if i not in known_set]
    counters["novel"] = len(novel_ids)

    passed: List[str] = []
    pop_by_id: Dict[str, int] = {}
    if novel_ids:
        full_albums: List[Dict[str, Any]] = catalog_client.get_albums(novel_ids)
        for a in full_albums:
            if a and a.get("id"):
                pop_by_id[a["id"]] = a.get("popularity") or 0
        passed = [
            a["id"]
            for a in full_albums
            if a and a.get("id") and (a.get("popularity") or 0) >= settings.ALBUM_POP_MIN
        ]
    counters["passed_gate"] = len(passed)

    to_enqueue = passed[: settings.MAX_ENQUEUE_PER_TICK]
    if to_enqueue:
        enqueue(to_enqueue)
    counters["enqueued"] = len(to_enqueue)

    # Confirm-insert gate (owner 2026-07-13, first-live-tick finding): a
    # never-announced insert must clear the same ALBUM_POP_MIN bar as the
    # catalog enqueue — without it the confirm path floods the calendar with
    # credit-farm compilations that list a watchlist artist as primary
    # (Schubert-class noise). Flips stay ungated: an announced row was already
    # curated by an MB/iTunes observation. Missing popularity ⇒ gate fails
    # (fail-closed; the artist re-sweeps next rotation). Ids already fetched
    # for the novel gate are reused, so the extra Spotify cost is only the
    # already-known candidate albums.
    if confirm_candidates:
        need = sorted(
            {c["spotify_album_id"] for c in confirm_candidates} - set(pop_by_id)
        )
        if need:
            for a in catalog_client.get_albums(need):
                if a and a.get("id"):
                    pop_by_id[a["id"]] = a.get("popularity") or 0
        for c in confirm_candidates:
            c["passes_gate"] = (
                pop_by_id.get(c["spotify_album_id"], 0) >= settings.ALBUM_POP_MIN
            )

    # Step-5 release-day confirm (DB-only; external loop already over).
    confirm_release_events(session_factory, confirm_candidates, counters)

    logger.warning(
        "album_ingest summary: eligible=%(eligible)d swept=%(swept)d "
        "tracked_swept=%(tracked_swept)d "
        "discovered=%(discovered)d fresh=%(fresh)d novel=%(novel)d "
        "passed_gate=%(passed_gate)d enqueued=%(enqueued)d "
        "confirm_candidates=%(confirm_candidates)d "
        "confirm_flipped=%(confirm_flipped)d "
        "confirm_inserted=%(confirm_inserted)d "
        "confirm_gate_skipped=%(confirm_gate_skipped)d "
        "classical_excluded=%(classical_excluded)d "
        "classical_holdout=%(classical_holdout)d",
        counters,
    )
    return counters
