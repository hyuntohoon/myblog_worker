# worker/service/lyrics_suspicion_service.py
"""FEAT-lyrics-match-correction Step 4: suspicion sweep (worker).

Three signals over ``matched`` rows, measured on prod 2026-10-17 before this was built
(RFC § Step 4 result):

* ``duration_overrun`` — the last LRC timestamp runs past the track by more than 5 s;
* ``early_end`` — the sync stops before 40 % of the track;
* ``shared_lrclib_id`` — one LRCLIB id serves tracks with different base titles (a
  language version collapsed into the original, BTS ``Go Go``). Kept as a tripwire: its
  population was one group, which consensus cannot judge.

Neither timing signal found a wrong lyric (content-wrong 0/26 determinable). About a third
of the timing rows serve the right words with a sync from another upload, while a
same-words upload whose sync fits exists. So the sweep fixes first and reports the rest:

1. **Re-check** each timing row with the running matcher (which now prefers a fitting sync
   among same-support bodies) under the reassessment guard plus one extra lateral path that
   exists only here: same basis, same words, the stored sync misfits and the new one fits,
   and the new body is not a stub of the stored one (``timing_upgrade``). It is monotone —
   a fitting sync never trips the path again — so it cannot churn. A row with a ``done``
   translation is held, not fixed: a different upload's line breaks would turn the
   translation ``stale``, and the owner's pick re-queues it (Step 3a), the automatic path
   does not.
2. **Report** what is left: a row that still trips a timing signal opens a system report
   (``user_id`` NULL, ``reason`` = the signal) unless today's candidates vouch for its
   timing — two or more same-words uploads, none of which fits (``_timing_vouch``). A
   ``shared_lrclib_id`` row is reported without a re-check.

A report is never opened twice for the same served body: any earlier report with that
signal and body hash, open or closed, settles the row (an owner's ``keep`` stays kept). The
insert takes the same per-track advisory lock as the backend's member report
(``lyrics_report_service._advisory_key("track", …)``), so a member's and a signal's first
reports are serialised. A system report enqueues nothing: the sweep has just re-checked.

**Shadow mode is the default** (``shadow: false`` in the message runs it for real): the
re-check, the gate and the report decision all run, nothing is written, and the would-be
fixes and reports are logged at WARNING with their track ids. Manual SQS job, no schedule.
"""
from __future__ import annotations

import hashlib
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from worker.clients.lrclib_client import LrclibClient
from worker.core.config import settings
from worker.service.lyrics_eval_core import PRIMARY_ARTIST_NAMES_LATERAL, Verdict, run_eval_batch
from worker.service.lyrics_matcher import (
    CONTENT_AGREE_THRESHOLD,
    MATCHER_VERSION,
    STATUS_MATCHED,
    TIMING_PEER_FRACTION,
    Candidate,
    MatchOutcome,
    body_shingles,
    canonical_base_title,
    content_similarity,
    extract_version_tokens,
    locked_row_sql,
    lrc_timestamps,
    plausible_candidates,
    sync_fits,
    timing_signal,
)
from worker.service.lyrics_reassessment_service import (
    _EXISTING_ROW_COLUMNS,
    LyricsReassessmentService,
    _basis_strength,
    _rows,
    replacement_verdict,
)

logger = logging.getLogger(__name__)

SIGNAL_SHARED_ID = "shared_lrclib_id"
DEFAULT_LIMIT = 150
# The worker Lambda has 120 s. Unlike reassessment, the sweep also scans every synced row,
# groups shared ids and writes one short transaction per report on the same clock, so its
# LRCLIB loop gets less of it and the report phase is capped. What is left waits for the
# next pass (a cut-off row is not settled, so it is selected again).
DEFAULT_TIME_BUDGET_SEC = 60.0
MAX_REPORTS_PER_PASS = 100

# The lrclib id a row serves, wherever its evidence keeps it: top level (exact-title rows,
# best-of since Step 1) or the promotion block of an older best-of row.
_SERVED_LRCLIB_ID_SQL = (
    "COALESCE(tl.evidence ->> 'lrclib_id', "
    "tl.evidence #>> '{promotion,chosen,lrclib_id}', "
    "tl.evidence #>> '{promotion,lrclib_id}')"
)


def body_sha(synced: Optional[str], plain: Optional[str]) -> str:
    """The backend's ``lyrics_report_service.body_sha`` for a ``matched`` row — byte for
    byte, so a system report's snapshot reads like a member's in the owner queue."""
    return hashlib.sha256(f"{synced or ''}\x00{plain or ''}".encode("utf-8")).hexdigest()


def report_advisory_key(track_id: Any) -> int:
    """The backend's ``_advisory_key("track", track_id)`` (``lyrics_report_service``)."""
    canonical = uuid.UUID(str(track_id))
    digest = hashlib.sha256(f"lyrics_report:track:{canonical}".encode()).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


def timing_upgrade(row: Dict[str, Any], outcome: MatchOutcome) -> bool:
    """The sweep's one extra lateral path: same words, a sync that fits instead of one that
    does not. Same basis rung only (a different rung is the ladder's call), never a lock,
    and the new body must hold at least half the stored one's words and timed lines."""
    if row.get("existing_locked") or row.get("existing_status") != STATUS_MATCHED:
        return False
    if outcome.match_status != STATUS_MATCHED:
        return False
    if _basis_strength(outcome.match_basis) != _basis_strength(row.get("existing_basis")):
        return False
    duration = row.get("duration_sec")
    stored_synced = row.get("existing_lyric_synced")
    if timing_signal(stored_synced, duration) is None:
        return False
    if not sync_fits(outcome.lyric_synced, duration):
        return False
    stored = body_shingles(stored_synced, row.get("existing_lyric_plain"))
    picked = body_shingles(outcome.lyric_synced, outcome.lyric_plain)
    if content_similarity(stored, picked) < CONTENT_AGREE_THRESHOLD:
        return False
    return (
        len(picked or ()) >= TIMING_PEER_FRACTION * len(stored or ())
        and len(lrc_timestamps(outcome.lyric_synced))
        >= TIMING_PEER_FRACTION * len(lrc_timestamps(stored_synced))
    )


@dataclass(frozen=True)
class TimingVouch:
    """What today's candidates say about the stored body's timing."""

    same_words: int  # same-version synced uploads sharing the stored body (stored id excluded)
    fitting: int  # of those, how many fit the track

    @property
    def vouched(self) -> bool:
        """The recording's own timing: several uploads of these words, none fits."""
        return self.same_words >= 2 and self.fitting == 0


def _timing_vouch(row: Dict[str, Any], candidates: List[Candidate]) -> TimingVouch:
    stored = body_shingles(row.get("existing_lyric_synced"), row.get("existing_lyric_plain"))
    title = row.get("title") or ""
    track_tokens = extract_version_tokens(title)
    existing_id = row.get("existing_lrclib_id")
    same = fitting = 0
    for cand, *_ in plausible_candidates(
        title, list(row.get("artist_names") or []), list(row.get("aliases") or []),
        row.get("duration_sec"), candidates,
    ):
        if existing_id is not None and str(cand.id) == str(existing_id):
            continue
        if not cand.synced_lyrics or extract_version_tokens(cand.title) != track_tokens:
            continue
        if content_similarity(stored, body_shingles(cand.synced_lyrics, cand.plain_lyrics)) \
                < CONTENT_AGREE_THRESHOLD:
            continue
        same += 1
        if sync_fits(cand.synced_lyrics, row.get("duration_sec")):
            fitting += 1
    return TimingVouch(same, fitting)


class LyricsSuspicionService:
    """Fix what the timing signals can fix; report the rest to the owner's queue."""

    def __init__(
        self,
        session: Session,
        client: Optional[LrclibClient] = None,
        *,
        concurrency: Optional[int] = None,
        time_budget_sec: Optional[float] = None,
    ) -> None:
        self.session = session
        self._client = client
        self._scope: Optional[List[str]] = None
        self.concurrency = concurrency or settings.LYRICS_REASSESS_CONCURRENCY
        self.time_budget_sec = (
            time_budget_sec if time_budget_sec is not None
            else min(DEFAULT_TIME_BUDGET_SEC, settings.LYRICS_REASSESS_TIME_BUDGET_SEC)
        )

    def sweep(
        self, *, shadow: bool = True, limit: Optional[int] = None,
        track_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """One pass: re-check up to ``limit`` timing rows (stalest first), then report.
        ``track_ids`` narrows both signals to those tracks (a by-hand re-run of a few)."""
        limit = DEFAULT_LIMIT if limit is None else max(0, int(limit))
        self._scope = [str(uuid.UUID(str(t))) for t in track_ids] if track_ids else None
        timing_rows = self._unsettled(self._fetch_timing_rows())[:limit]
        shared_rows = self._unsettled(self._fetch_shared_id_rows())
        held = self._translated({str(r["id"]) for r in timing_rows})
        for row in timing_rows:
            row["has_done_translation"] = str(row["id"]) in held
        # The selections open a read transaction; release it before the LRCLIB loop.
        self.session.commit()

        vouches: Dict[str, TimingVouch] = {}
        fixes: List[str] = []

        def gate(row: Dict[str, Any], outcome: MatchOutcome, candidates):
            vouches[str(row["id"])] = _timing_vouch(row, candidates or [])
            base = replacement_verdict(row, outcome, candidates)
            if base:
                # The reassessment guard's own write (a stronger basis). Shadow writes nothing.
                return Verdict(False, counter="would_replace") if shadow else base
            if timing_upgrade(row, outcome):
                if row.get("has_done_translation"):
                    return Verdict(False, counter="timing_fix_held_translation")
                fixes.append(str(row["id"]))
                if shadow:
                    return Verdict(False, counter="would_timing_fix")
                patch = dict(base.evidence or {})
                for key, stamp in patch.items():
                    # A re-sweep stamp says what the reassessment guard alone decided
                    # (unchanged / kept); this row is rewritten, so say that instead.
                    patch[key] = {**stamp, "verdict": "timing_fixed",
                                  "replaced_lrclib_id": row.get("existing_lrclib_id")}
                return Verdict(True, counter="timing_fixed", evidence={
                    **patch,
                    "timing_fix": {
                        "matcher_version": MATCHER_VERSION,
                        "replaced_lrclib_id": row.get("existing_lrclib_id"),
                        "signal": timing_signal(row.get("existing_lyric_synced"), row.get("duration_sec")),
                    },
                })
            return Verdict(False) if shadow else base

        metrics = run_eval_batch(
            self.session, timing_rows,
            concurrency=self.concurrency,
            time_budget_sec=self.time_budget_sec,
            client=self._client,
            should_write=gate,
            touch_on_guard_kept=not shadow,
            touch_on_consistency_error=not shadow,
            log_prefix="Lyrics suspicion sweep",
        )
        metrics["shadow"] = shadow
        metrics["selected_timing"] = len(timing_rows)
        metrics["selected_shared_id"] = len(shared_rows)

        to_report: List[tuple] = []
        for row in timing_rows:
            vouch = vouches.get(str(row["id"]))
            if vouch is None:
                continue  # not judged this run (LRCLIB transient, budget): next run
            if shadow and str(row["id"]) in fixes:
                continue  # would be fixed. A real fix is caught by _open_report's re-read.
            if vouch.vouched:
                metrics["timing_vouched"] = metrics.get("timing_vouched", 0) + 1
                continue
            to_report.append((row, row["signal"]))
        to_report.extend((row, SIGNAL_SHARED_ID) for row in shared_rows)

        opened: List[Dict[str, str]] = []
        if len(to_report) > MAX_REPORTS_PER_PASS:
            metrics["reports_deferred"] = len(to_report) - MAX_REPORTS_PER_PASS
            to_report = to_report[:MAX_REPORTS_PER_PASS]
        for row, reason in to_report:
            if shadow:
                opened.append({"track_id": str(row["id"]), "reason": reason})
                continue
            if self._open_report(row, reason):
                opened.append({"track_id": str(row["id"]), "reason": reason})
        metrics["would_report" if shadow else "reports_opened"] = len(opened)
        # WARNING: prod Lambdas run LOG_LEVEL=WARNING and this is the run's only record of
        # which rows it would fix and report.
        logger.warning(
            "Lyrics suspicion sweep (shadow=%s): %s fixes=%s reports=%s",
            shadow, {k: v for k, v in metrics.items() if k != "promotion_criteria"},
            fixes, opened,
        )
        return metrics

    # ── selection ──────────────────────────────────────────────────────────────────

    def _select(self, where: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows = self.session.execute(
            text(
                f"""
                SELECT t.id, t.title, t.duration_sec,
                       primary_artists.artist_names                     AS artist_names,
                       ARRAY_REMOVE(ARRAY_AGG(DISTINCT al.alias), NULL) AS aliases,
{_EXISTING_ROW_COLUMNS}
                FROM track_lyrics tl
                JOIN tracks t         ON t.id = tl.track_id
                JOIN track_artists ta ON ta.track_id = t.id
                JOIN artists a        ON a.id = ta.artist_id
                LEFT JOIN LATERAL jsonb_array_elements_text(a.aliases) AS al(alias) ON true
{PRIMARY_ARTIST_NAMES_LATERAL}
                WHERE tl.match_status = 'matched'
                  AND NOT {locked_row_sql("tl")}
                  AND (CAST(:scope AS uuid[]) IS NULL OR t.id = ANY(CAST(:scope AS uuid[])))
                  AND {where}
                GROUP BY tl.track_id, t.id, t.title, t.duration_sec,
                         primary_artists.artist_names
                ORDER BY tl.updated_at ASC
                """
            ),
            {**params, "scope": self._scope},
        ).fetchall()
        tracks = _rows(rows)
        for row in tracks:
            # _EXISTING_ROW_COLUMNS reads the promotion block at a key older best-of rows
            # never had; the sweep needs the served id for both the vouch and the snapshot.
            row["existing_lrclib_id"] = None
        ids = [row["id"] for row in tracks]
        if ids:
            served = dict(
                self.session.execute(
                    text(f"SELECT tl.track_id, {_SERVED_LRCLIB_ID_SQL} FROM track_lyrics tl "
                         "WHERE tl.track_id = ANY(:ids)"),
                    {"ids": ids},
                ).fetchall()
            )
            for row in tracks:
                row["existing_lrclib_id"] = served.get(row["id"])
        return LyricsReassessmentService(self.session)._attach_stored_bodies(tracks)

    def _fetch_timing_rows(self) -> List[Dict[str, Any]]:
        """Synced ``matched`` rows whose last timestamp trips a timing signal, stalest first.

        The SQL reads whole seconds only, so it pre-filters with a one-second margin and
        ``timing_signal`` decides on the exact value."""
        rows = self._select(
            """tl.lyric_synced IS NOT NULL AND t.duration_sec IS NOT NULL
                  AND EXISTS (
                      SELECT 1 FROM (
                          SELECT max(m[1]::int * 60 + m[2]::int) AS last_sec
                          FROM regexp_matches(tl.lyric_synced, '\\[(\\d{1,3}):(\\d{2})', 'g') m
                      ) s
                      WHERE s.last_sec + 1 > t.duration_sec + :overrun
                         OR s.last_sec < :early * t.duration_sec
                  )""",
            {"overrun": 5.0, "early": 0.4},
        )
        out = []
        for row in rows:
            signal = timing_signal(row.get("existing_lyric_synced"), row.get("duration_sec"))
            if signal is not None:
                row["signal"] = signal
                out.append(row)
        return out

    def _fetch_shared_id_rows(self) -> List[Dict[str, Any]]:
        """Rows whose served lrclib id also serves a track with a different base title."""
        pairs = self.session.execute(
            text(
                f"""
                SELECT tl.track_id, t.title, {_SERVED_LRCLIB_ID_SQL} AS lrclib_id
                FROM track_lyrics tl JOIN tracks t ON t.id = tl.track_id
                WHERE tl.match_status = 'matched'
                  AND {_SERVED_LRCLIB_ID_SQL} IN (
                      SELECT {_SERVED_LRCLIB_ID_SQL} FROM track_lyrics tl
                      WHERE tl.match_status = 'matched'
                      GROUP BY 1 HAVING count(*) > 1
                  )
                """
            )
        ).fetchall()
        groups: Dict[str, List[tuple]] = {}
        for track_id, title, lrclib_id in pairs:
            groups.setdefault(str(lrclib_id), []).append((track_id, canonical_base_title(title or "")))
        flagged = [
            track_id for members in groups.values()
            if len({base for _, base in members}) > 1
            for track_id, _ in members
        ]
        if not flagged:
            return []
        rows = self._select("t.id = ANY(:ids)", {"ids": flagged})
        for row in rows:
            row["signal"] = SIGNAL_SHARED_ID
        return rows

    def _unsettled(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Drop rows that already had a report for this signal on this exact body."""
        if not rows:
            return rows
        seen = {
            (str(r[0]), r[1], r[2])
            for r in self.session.execute(
                text(
                    "SELECT track_id, reason, reported_body_sha FROM track_lyrics_reports "
                    "WHERE user_id IS NULL AND track_id = ANY(:ids)"
                ),
                {"ids": [row["id"] for row in rows]},
            ).fetchall()
        }
        out = []
        for row in rows:
            row["body_sha"] = body_sha(row.get("existing_lyric_synced"), row.get("existing_lyric_plain"))
            if (str(row["id"]), row["signal"], row["body_sha"]) not in seen:
                out.append(row)
        return out

    def _translated(self, track_ids: set) -> set:
        if not track_ids:
            return set()
        return {
            str(r[0])
            for r in self.session.execute(
                text("SELECT track_id FROM track_lyrics_translations "
                     "WHERE status = 'done' AND track_id = ANY(:ids)"),
                {"ids": list(track_ids)},
            ).fetchall()
        }

    # ── report ─────────────────────────────────────────────────────────────────────

    def _open_report(self, row: Dict[str, Any], reason: str) -> bool:
        """One short transaction: lock like the backend does, re-read what is served now, and
        open the report only if the row still shows the signal and no report for this signal
        has seen this body. True when a row was inserted."""
        track_id = row["id"]
        try:
            self.session.execute(
                text("SELECT pg_advisory_xact_lock(:k)"), {"k": report_advisory_key(track_id)}
            )
            current = self.session.execute(
                text(
                    f"""
                    SELECT tl.match_status, tl.lyric_synced, tl.lyric_plain,
                           {locked_row_sql("tl")} AS locked, {_SERVED_LRCLIB_ID_SQL} AS lrclib_id
                    FROM track_lyrics tl WHERE tl.track_id = :t
                    """
                ),
                {"t": track_id},
            ).first()
            if current is None or current[0] != STATUS_MATCHED or current[3]:
                self.session.rollback()
                return False
            synced, plain = current[1], current[2]
            if reason != SIGNAL_SHARED_ID and timing_signal(synced, row.get("duration_sec")) is None:
                self.session.rollback()
                return False
            sha = body_sha(synced, plain)
            inserted = self.session.execute(
                text(
                    """
                    INSERT INTO track_lyrics_reports
                        (track_id, user_id, reason, reported_status, reported_lrclib_id,
                         reported_body_sha)
                    SELECT :t, NULL, :reason, 'matched', :lrclib_id, :sha
                    WHERE NOT EXISTS (
                        SELECT 1 FROM track_lyrics_reports r
                        WHERE r.track_id = :t AND r.user_id IS NULL AND r.reason = :reason
                          AND (r.status = 'open' OR r.reported_body_sha = :sha)
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """
                ),
                {"t": track_id, "reason": reason, "sha": sha,
                 "lrclib_id": _int(current[4])},
            ).first()
            self.session.commit()
            return inserted is not None
        except Exception:
            self.session.rollback()
            logger.exception("Lyrics suspicion sweep: report not opened for %s (%s)", track_id, reason)
            return False


def _int(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
