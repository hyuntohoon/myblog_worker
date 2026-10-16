# worker/service/automation_control.py
"""Owner pause and run-now stamps for worker runs (FEAT-automation-admin 3b, 3c).

The owner's automation page writes `automation_controls.paused` (V60). Before a
top-level invocation runs (an EventBridge cron or a direct invoke), the handler
maps the event to the job's catalog key and asks here whether that job is
paused. A paused job is skipped, and the skip stamps `honored_at`, which is how
the page tells "paused and confirmed by the executor" from "paused, not yet
seen".

Scope, by design:

* Only top-level events are checked. SQS messages of the same job type are user
  or owner actions (e.g. "지금 새로고침") and are never paused.
* A run already executing finishes; nothing is cancelled.
* A read failure means RUN (fail-open), logged at ERROR. Pause is an operational
  control, not an auth boundary: failing closed would let a DB blip or a missing
  V60 silently stop every cron. A pause that did not apply stays visible on the
  page, because `honored_at` does not advance.

The keys mirror the backend catalog (`app/services/automation_catalog.py`).
`tests/test_automation_control.py` pins the full set, so a key added on one side
only fails a test instead of silently never pausing.

Run now (3c): the owner's page invokes this Lambda with the cron's own constant
input (`myblog_shared_db.automation.WORKER_CRON_PAYLOADS`) plus
`automation_run_id`, the audit row's request_id. The handler stamps that row's
`started_at` when the run begins and `finished_at` + `run_outcome` when it ends.
A stamp that fails is logged at ERROR and never blocks the run (fail-open, as
the pause check): the page then shows an invoked run with no start, which is the
truthful reading.
"""
from __future__ import annotations

import logging
import uuid
from typing import Any, Callable, Dict, Optional

from myblog_shared_db.automation import RUN_ID_FIELD
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Jobs whose `event.job` is the catalog key as-is.
_PLAIN_JOBS = frozenset({
    "spotify_listening",
    "spotify_member_poll",
    "lastfm_recent_tracks",
    "album_ingest",
    "lyrics_incremental",
    "lyrics_demand_source",
    "lyrics_reassessment",
    "genius_fetch",
    "artist_photo_backfill",
    "isrc_backfill",
    "youtube_ref_refresh",
    # Run-now only (3c): the alias cron itself sends the default scheduled event
    # (source=aws.events, mapped below); the owner's run-now sends this instead,
    # so the backend never forges an aws.events event.
    "musicbrainz_alias_fill",
})

# Jobs split into one catalog row per EventBridge rule, by `event.mode`. The
# default mode is the one the handler itself falls back to.
_MODE_JOBS = {
    "release_upcoming_poll": ("release_upcoming_poll_", "musicbrainz", frozenset({"musicbrainz", "itunes"})),
    "spotify_saved_tracks_sync": ("spotify_saved_tracks_", "incremental", frozenset({"incremental", "full"})),
}

ALIAS_FILL_KEY = "musicbrainz_alias_fill"


def control_key(event: Dict[str, Any]) -> Optional[str]:
    """The catalog key a top-level event runs as, or None when it has no control."""
    job = event.get("job")
    if job is None:
        # The alias cron sends the default scheduled event (no constant input).
        return ALIAS_FILL_KEY if event.get("source") == "aws.events" else None
    if not isinstance(job, str):
        return None
    if job in _PLAIN_JOBS:
        return job
    if job in _MODE_JOBS:
        prefix, default, modes = _MODE_JOBS[job]
        mode = event.get("mode", default)
        return prefix + mode if mode in modes else None
    return None


def all_control_keys() -> frozenset:
    keys = set(_PLAIN_JOBS)
    for prefix, _default, modes in _MODE_JOBS.values():
        keys |= {prefix + mode for mode in modes}
    return frozenset(keys)


# One statement checks the flag and stamps the skip, so "paused" and "honoured"
# cannot disagree. READ WRITE first: a pooler session can carry a leaked
# read-only default, which would turn every check into a fail-open error.
# clock_timestamp(), not now(): a stamp taken after waiting on the row lock must
# not predate the owner's changed_at, or a real skip reads as "not yet confirmed".
_CLAIM_SKIP = text(
    "UPDATE automation_controls SET honored_at = clock_timestamp() "
    "WHERE job_key = :key AND paused RETURNING job_key"
)


def skip_if_paused(event: Dict[str, Any], session_factory: Callable[[], Session]) -> bool:
    """True when the owner has paused this event's job; the caller must not run it."""
    key = control_key(event)
    if key is None:
        return False
    try:
        with session_factory() as db:
            db.execute(text("SET TRANSACTION READ WRITE"))
            db.execute(text("SET LOCAL statement_timeout = '5s'"))
            paused = db.execute(_CLAIM_SKIP, {"key": key}).first() is not None
            db.commit()
    except Exception as error:  # noqa: BLE001 — fail-open is the documented contract
        # Only the exception type: driver messages can carry connection details.
        logger.error(
            "automation pause check failed for %s (%s); running anyway",
            key, type(error).__name__,
        )
        return False
    if paused:
        logger.info("automation: %s is paused by the owner; skipping this run", key)
    return paused


# ── run-now stamps (3c) ─────────────────────────────────────────────────────────

# A retry (Lambda async retries a raised run) restamps the start and clears the end,
# so the row reads "running" again instead of keeping the failed attempt's end.
# `action = 'run'` keeps a stray id from touching a pause/resume row (V62 would
# reject it anyway).
_STAMP_START = text(
    "UPDATE automation_control_events "
    "SET started_at = clock_timestamp(), finished_at = NULL, run_outcome = NULL "
    "WHERE request_id = :rid AND action = 'run'"
)
_STAMP_END = text(
    "UPDATE automation_control_events "
    "SET finished_at = clock_timestamp(), run_outcome = :outcome "
    "WHERE request_id = :rid AND action = 'run'"
)

RUN_OUTCOMES = frozenset({"ok", "error", "skipped_paused"})


def run_id_of(event: Dict[str, Any]) -> Optional[uuid.UUID]:
    """The owner run-now id a top-level event carries, or None."""
    raw = event.get(RUN_ID_FIELD)
    if raw is None:
        return None
    try:
        return uuid.UUID(str(raw))
    except ValueError:
        logger.warning("automation: ignoring a malformed %s", RUN_ID_FIELD)
        return None


def _stamp(statement, params: Dict[str, Any], session_factory: Callable[[], Session]) -> None:
    try:
        with session_factory() as db:
            db.execute(text("SET TRANSACTION READ WRITE"))
            db.execute(text("SET LOCAL statement_timeout = '5s'"))
            db.execute(statement, params)
            db.commit()
    except Exception as error:  # noqa: BLE001 — a stamp never blocks the run
        logger.error("automation run stamp failed for %s (%s)", params["rid"], type(error).__name__)


def stamp_run_start(run_id: uuid.UUID, session_factory: Callable[[], Session]) -> None:
    _stamp(_STAMP_START, {"rid": run_id}, session_factory)


def stamp_run_end(run_id: uuid.UUID, outcome: str, session_factory: Callable[[], Session]) -> None:
    if outcome not in RUN_OUTCOMES:
        raise ValueError(outcome)
    _stamp(_STAMP_END, {"rid": run_id, "outcome": outcome}, session_factory)
