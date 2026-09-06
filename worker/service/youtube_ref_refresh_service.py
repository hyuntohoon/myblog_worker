"""Refresh and expire stored YouTube mappings.

FEAT-youtube-playback-provider Step A5.

THIS JOB IS A COMPLIANCE MECHANISM, NOT A CACHE WARMER. YouTube Developer
Policy III.E.4.c/.d caps stored API data at 30 calendar days, and III.E.4.d
extends that ceiling to a videoId obtained from a plain public `search.list` —
there is no resource-id exception. So "once mapped, never look again" is not
implementable; the mapping survives only as a REFRESHED one, and a row this job
cannot refresh is DELETED rather than kept.

Two passes, in this order, and the order is load-bearing:

  1. EXPIRE — delete every row past the retention window, whatever its
     verify_state. Runs FIRST so a quota failure in pass 2 can never postpone a
     deletion the policy requires. This is also why `idx_tpr_stale` is
     unconditional: a 'gone' row still STORES API-derived data (the videoId,
     privacy_status, made_for_kids), so the clock applies to it in full, and
     marking a row 'gone' must not become a way to opt it out of retention.

  2. REFRESH — take the oldest 'live' rows and re-verify them in batches of 50.

WHY PASS 2 REFRESHES ONLY 'live' ROWS. A row already known 'gone' or
'not_embeddable' is a settled fact the member has to act on (re-pick), not
something to poll. Refreshing it would move `last_verified_at` forward every
day and keep a dead row alive forever — technically compliant, permanently
useless, and it would spend an id slot every single day. Left unrefreshed it
ages out and pass 1 deletes it at 30 days, which gives the member up to 30 days
of the useful `410 Gone` "your video died, pick another" signal and then
reclaims the row. If the video's owner ever re-enables embedding, the member
re-picks and the A3 write path re-verifies — no polling needed.

SESSION LIFECYCLE. fetch ids -> materialise -> CLOSE -> HTTP -> fresh short
write session. Holding a transaction across the API loop is the recurring bug
class that produced the Neon `ProtocolViolation` (workspace CLAUDE.md).
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

#: `videos.list` takes at most 50 ids per 1-unit call. That ratio is what makes
#: this job affordable and it is the API's limit, not a tuning choice.
BATCH_SIZE = 50

# Twin of the music and backend parsers. The `T` section is OPTIONAL because a
# live stream reports a bare `P0D` — measured against the live API 2026-09-06.
_ISO_DURATION = re.compile(
    r"^P(?:(?P<d>\d+)D)?(?:T(?:(?P<h>\d+)H)?(?:(?P<m>\d+)M)?(?:(?P<s>\d+)S)?)?$"
)


def parse_iso8601_duration(value: Optional[str]) -> Optional[int]:
    """`PT4M13S` -> 253. None for anything unparseable, and for zero.

    isinstance rather than truthiness: `re.match` raises TypeError on a truthy
    non-string, which would turn one odd payload into a failed run.
    """
    if not isinstance(value, str) or not value:
        return None
    m = _ISO_DURATION.match(value)
    if not m:
        return None
    d, h, mi, sec = (int(m.group(k) or 0) for k in ("d", "h", "m", "s"))
    return (d * 86400 + h * 3600 + mi * 60 + sec) or None


_DELETE_EXPIRED = text(
    """
    DELETE FROM track_provider_refs
    WHERE provider = 'youtube'
      AND last_verified_at < now() - make_interval(days => :days)
    RETURNING id
    """
)

_SELECT_STALE = text(
    """
    SELECT external_id
    FROM track_provider_refs
    WHERE provider = 'youtube'
      AND verify_state = 'live'
    ORDER BY last_verified_at ASC
    LIMIT :limit
    """
)

_MARK_GONE = text(
    """
    UPDATE track_provider_refs
    SET verify_state = 'gone', updated_at = now()
    WHERE provider = 'youtube' AND external_id = ANY(:ids)
    """
)

_REFRESH_ONE = text(
    """
    UPDATE track_provider_refs
    SET embeddable       = :embeddable,
        privacy_status   = :privacy_status,
        made_for_kids    = :made_for_kids,
        duration_sec     = :duration_sec,
        verify_state     = :verify_state,
        last_verified_at = now(),
        updated_at       = now()
    WHERE provider = 'youtube' AND external_id = :external_id
    """
)


class YouTubeRefRefreshService:
    def __init__(self, session_factory, client, *, retention_days: int) -> None:
        self._session_factory = session_factory
        self._client = client
        self._retention_days = retention_days

    # ── pass 1: expire ──────────────────────────────────────────────────────

    def expire_stale(self) -> int:
        """Delete every row past the retention window. Runs BEFORE any API call.

        Unconditional on verify_state, deliberately: a 'gone' row still stores
        API-derived data, so III.E.4's clock applies to it in full.

        Ordered first so that a quota exhaustion, a network failure or a bad
        deploy in pass 2 cannot postpone a deletion the policy requires. A
        compliance sweep that only runs when the enrichment succeeds is not a
        compliance sweep.
        """
        session: Session = self._session_factory()
        try:
            deleted = session.execute(
                _DELETE_EXPIRED, {"days": self._retention_days}
            ).fetchall()
            session.commit()
        finally:
            session.close()
        if deleted:
            logger.info(
                "youtube_ref_refresh: deleted %d row(s) past the %d-day retention window",
                len(deleted), self._retention_days,
            )
        return len(deleted)

    # ── pass 2: refresh ─────────────────────────────────────────────────────

    def _claim_work(self, limit: int) -> List[str]:
        """Read the oldest live ids and CLOSE before the first API call."""
        session: Session = self._session_factory()
        try:
            return [r[0] for r in session.execute(_SELECT_STALE, {"limit": limit}).fetchall()]
        finally:
            session.close()

    def _write_batch(self, details: Dict[str, Dict[str, Any]], asked: List[str]) -> Dict[str, int]:
        """One short transaction per batch, opened AFTER the API call returned."""
        missing = [vid for vid in asked if vid not in details]
        session: Session = self._session_factory()
        try:
            if missing:
                # Absent from the response = deleted, private, or never existed.
                # The API reports this by OMISSION, never as an error.
                session.execute(_MARK_GONE, {"ids": missing})
            for vid, item in details.items():
                status = item.get("status") or {}
                embeddable = status.get("embeddable") is True
                privacy = status.get("privacyStatus")
                # A row that is no longer embeddable, or no longer public, is
                # recorded as such rather than deleted: `resolve` answers 410 and
                # the member gets the "pick another" affordance. The retention
                # sweep reclaims it if it is never re-picked.
                verify_state = "live" if (embeddable and privacy == "public") else "not_embeddable"
                session.execute(
                    _REFRESH_ONE,
                    {
                        "external_id": vid,
                        "embeddable": embeddable,
                        "privacy_status": privacy,
                        "made_for_kids": status.get("madeForKids"),
                        "duration_sec": parse_iso8601_duration(
                            (item.get("contentDetails") or {}).get("duration")
                        ),
                        "verify_state": verify_state,
                    },
                )
            session.commit()
        finally:
            session.close()
        return {"refreshed": len(details), "gone": len(missing)}

    def run(self, limit: int) -> Dict[str, int]:
        expired = self.expire_stale()

        ids = self._claim_work(limit)
        totals = {"expired": expired, "refreshed": 0, "gone": 0, "batches": 0}
        if not ids:
            logger.info("youtube_ref_refresh: nothing to refresh (expired=%d)", expired)
            return totals

        for start in range(0, len(ids), BATCH_SIZE):
            batch = ids[start:start + BATCH_SIZE]
            # No session is open here — the HTTP call happens between two short
            # transactions, never inside one.
            details = self._client.list_videos(batch)
            counts = self._write_batch(details, batch)
            totals["refreshed"] += counts["refreshed"]
            totals["gone"] += counts["gone"]
            totals["batches"] += 1

        logger.info(
            "youtube_ref_refresh: expired=%d refreshed=%d gone=%d batches=%d",
            totals["expired"], totals["refreshed"], totals["gone"], totals["batches"],
        )
        return totals


def run_youtube_ref_refresh(session_factory, client, *, limit: int, retention_days: int) -> Dict[str, int]:
    return YouTubeRefRefreshService(
        session_factory, client, retention_days=retention_days
    ).run(limit)
