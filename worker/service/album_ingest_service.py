# Scheduled album-catalog ingest (FEAT-album-catalog-ingest Step 2).
#
# New-releases-only sweep: rotate through catalog artists whose popularity clears
# ARTIST_POP_MIN, pull their full-length discography page, keep releases newer than
# INGEST_SINCE, drop already-known and low-popularity albums, and enqueue the rest
# onto the existing candidates→SQS→sync_albums_batch pipeline. This job only
# DISCOVERS + ENQUEUES — the heavy upsert runs in the SQS consumer, so a tick stays
# far under the 120 s Lambda budget.
#
# The artist rotation is stateless: eligible artists (stable spotify_id order) are
# partitioned into ceil(n / SWEEP_ARTISTS_PER_TICK) buckets and the tick's bucket is
# days-since-epoch modulo bucket count. No cursor row to migrate or corrupt; adding
# or removing artists shifts partitions slightly, but every artist is still visited
# about once per cycle (~10 days at 305 eligible / 30 per tick).
from __future__ import annotations

import logging
import math
import time
from typing import Any, Callable, Dict, List

from sqlalchemy import text

from worker.core.config import settings

logger = logging.getLogger(__name__)


def _release_date_key(raw: str) -> str:
    """Normalize Spotify's variable-precision release_date for string comparison.
    Precision is 'day' ('2026-03-27'), 'month' ('2026-03') or 'year' ('2026');
    padding with -01-01 maps coarser precisions to their earliest day."""
    return (raw + "-01-01")[:10] if raw else "0000-01-01"


def run_album_ingest(
    session_factory,
    catalog_client,
    enqueue: Callable[[List[str]], None],
    *,
    days_since_epoch: int | None = None,
) -> Dict[str, int]:
    """One ingest tick. Returns the counter summary (also logged at WARNING so it
    lands in CloudWatch under the prod LOG_LEVEL=WARNING)."""
    if days_since_epoch is None:
        days_since_epoch = int(time.time() // 86400)

    counters = {
        "eligible": 0, "swept": 0, "discovered": 0, "fresh": 0,
        "novel": 0, "passed_gate": 0, "enqueued": 0,
    }

    with session_factory() as session:
        album_count = session.execute(text("SELECT count(*) FROM albums")).scalar()
        if album_count is not None and album_count >= settings.MAX_CATALOG_ALBUMS:
            logger.warning(
                "album_ingest: catalog cap reached (%d >= %d) — skipping tick",
                album_count, settings.MAX_CATALOG_ALBUMS,
            )
            return counters

        rows = session.execute(
            text("""
                SELECT spotify_id
                FROM artists
                WHERE popularity >= :pop_min
                ORDER BY spotify_id
            """),
            {"pop_min": settings.ARTIST_POP_MIN},
        ).fetchall()
        eligible = [r[0] for r in rows]
        counters["eligible"] = len(eligible)
        if not eligible:
            logger.warning("album_ingest: no artists clear ARTIST_POP_MIN=%d", settings.ARTIST_POP_MIN)
            return counters

        buckets = max(1, math.ceil(len(eligible) / settings.SWEEP_ARTISTS_PER_TICK))
        bucket = days_since_epoch % buckets
        start = bucket * settings.SWEEP_ARTISTS_PER_TICK
        sweep = eligible[start : start + settings.SWEEP_ARTISTS_PER_TICK]
        counters["swept"] = len(sweep)

        since = _release_date_key(settings.INGEST_SINCE)
        fresh_ids: List[str] = []
        for artist_sid in sweep:
            items = catalog_client.get_artist_albums(artist_sid, include_groups="album")
            counters["discovered"] += len(items)
            for alb in items:
                if _release_date_key(alb.get("release_date", "")) >= since and alb.get("id"):
                    fresh_ids.append(alb["id"])
        # collab albums can surface under several swept artists in one tick
        fresh_ids = list(dict.fromkeys(fresh_ids))
        counters["fresh"] = len(fresh_ids)

        novel_ids: List[str] = []
        if fresh_ids:
            known = session.execute(
                text("SELECT spotify_id FROM albums WHERE spotify_id = ANY(:sids)"),
                {"sids": fresh_ids},
            ).fetchall()
            known_set = {r[0] for r in known}
            novel_ids = [i for i in fresh_ids if i not in known_set]
        counters["novel"] = len(novel_ids)

    passed: List[str] = []
    if novel_ids:
        full_albums: List[Dict[str, Any]] = catalog_client.get_albums(novel_ids)
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

    logger.warning(
        "album_ingest summary: eligible=%(eligible)d swept=%(swept)d "
        "discovered=%(discovered)d fresh=%(fresh)d novel=%(novel)d "
        "passed_gate=%(passed_gate)d enqueued=%(enqueued)d",
        counters,
    )
    return counters
