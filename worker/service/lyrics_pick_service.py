# worker/service/lyrics_pick_service.py
"""FEAT-lyrics-match-correction Step 3a: write the owner's pick, locked.

The backend's ``POST /api/admin/lyrics/{id}/decision {action: pick}`` only validates and
enqueues ``{"job": "lyrics_pick", "track_id", "lrclib_id", "actor"}`` — a user-facing
request never calls LRCLIB. This job:

1. reads the stored row and checks the pick is still a candidate the owner may choose
   (in the stored set, not rejected) — one short read, committed before any external call;
2. re-fetches the body from LRCLIB by id (``/api/get/{id}``) — never from the panel's
   preview, which is not the lyric;
3. writes it with ``match_basis='manual'`` through ``TrackLyricsWriter.write_manual``, the
   only writer path that may replace a locked row, so the pick is locked in turn;
4. in the same transaction, re-queues the track's Korean translation when the body changed
   and a non-manual translation already existed (owner decision 2026-10-16, RFC OQ2).

A transient LRCLIB failure raises, so SQS redelivers the message; a vanished record or a
refused pick is logged and dropped (redelivery cannot fix either).
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from worker.clients.lrclib_client import LrclibClient
from worker.service.lyrics_matcher import (
    BASIS_MANUAL,
    MANUAL_MATCHER_VERSION,
    STATUS_MATCHED,
    STATUS_NO_LYRICS,
    MatchOutcome,
    TrackLyricsWriter,
)

logger = logging.getLogger(__name__)

# Re-queue a translation the poller (or an earlier request) produced. A hand-written
# ('manual' origin) translation is left as it is: it reads `stale` against the new body
# and the owner re-requests it if they want the LLM's version over their own.
_REQUEUE_TRANSLATION_SQL = """
    UPDATE track_lyrics_translations
       SET status = 'requested', claimed_at = NULL, error = NULL,
           requested_at = NOW(), updated_at = NOW()
     WHERE track_id = :tid
       AND status IN ('done', 'failed')
       AND origin IS DISTINCT FROM 'manual'
"""


class LyricsPickService:
    def __init__(self, session: Session, client: Optional[LrclibClient] = None) -> None:
        self.session = session
        self._client = client

    def pick(self, track_id: str, lrclib_id: Any, actor: Optional[str]) -> Dict[str, Any]:
        """Attach LRCLIB ``lrclib_id`` to ``track_id`` as the owner's locked pick."""
        try:
            tid = uuid.UUID(str(track_id))
            lid = int(lrclib_id)
        except (TypeError, ValueError):
            logger.error("Lyrics pick refused: malformed message (track %r, id %r)", track_id, lrclib_id)
            return {"outcome": "refused", "reason": "malformed"}
        actor = (actor or "").strip()[:128] or "owner"

        state = self.session.execute(
            text(
                """
                SELECT EXISTS (SELECT 1 FROM tracks WHERE id = :tid)                      AS has_track,
                       EXISTS (SELECT 1 FROM track_lyrics_candidates
                                WHERE track_id = :tid AND lrclib_id = :lid)              AS in_set,
                       EXISTS (SELECT 1 FROM track_lyrics_rejections
                                WHERE track_id = :tid AND lrclib_id = :lid)              AS rejected,
                       tl.match_status, tl.evidence ->> 'match_basis' AS basis,
                       COALESCE(tl.evidence ->> 'lrclib_id',
                                tl.evidence #>> '{promotion,lrclib_id}') AS current_id,
                       tl.matcher_version, tl.lyric_plain, tl.lyric_synced
                FROM (SELECT 1) one
                LEFT JOIN track_lyrics tl ON tl.track_id = :tid
                """
            ),
            {"tid": tid, "lid": lid},
        ).mappings().one()
        self.session.commit()  # close the read before the LRCLIB call

        refusal = None
        if not state["has_track"]:
            refusal = "unknown_track"
        elif state["rejected"]:
            refusal = "rejected"
        elif not state["in_set"]:
            refusal = "not_a_candidate"
        if refusal:
            logger.warning("Lyrics pick refused for track %s, id %s: %s", tid, lid, refusal)
            return {"outcome": "refused", "reason": refusal}

        owns_client = self._client is None
        client = self._client or LrclibClient(max_connections=2)
        try:
            cand = client.get_by_id(lid)  # LrclibTransientError propagates → SQS redelivers
        finally:
            if owns_client:
                client.close()
        if cand is None:
            logger.error("Lyrics pick: LRCLIB record %s is gone (track %s)", lid, tid)
            return {"outcome": "gone"}

        has_body = bool(cand.plain_lyrics) or bool(cand.synced_lyrics)
        status = STATUS_MATCHED if has_body and not cand.instrumental else STATUS_NO_LYRICS
        outcome = MatchOutcome(
            track_id=tid,
            match_status=status,
            evidence={
                "lrclib_id": lid,
                "lrclib_title": cand.title,
                "lrclib_artist": cand.artist,
                "lrclib_album": cand.album,
                "lrclib_duration_sec": cand.duration_sec,
                "manual": {
                    "action": "pick",
                    "by": actor,
                    "at": datetime.now(timezone.utc).isoformat(),
                    "previous": {
                        "status": state["match_status"],
                        "basis": state["basis"],
                        "lrclib_id": state["current_id"],
                        "matcher_version": state["matcher_version"],
                    },
                },
            },
            lyric_plain=cand.plain_lyrics if status == STATUS_MATCHED else "",
            lyric_synced=cand.synced_lyrics if status == STATUS_MATCHED else None,
            matcher_version=MANUAL_MATCHER_VERSION,
            match_basis=BASIS_MANUAL,
        )

        new_body = (outcome.lyric_plain or "", outcome.lyric_synced or "")
        old_body = (state["lyric_plain"] or "", state["lyric_synced"] or "")
        try:
            TrackLyricsWriter(self.session).write_manual(outcome)
            requeued = 0
            if status == STATUS_MATCHED and new_body != old_body:
                requeued = self.session.execute(
                    text(_REQUEUE_TRANSLATION_SQL), {"tid": tid}
                ).rowcount
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise
        result = {"outcome": "picked", "status": status, "translation_requeued": requeued}
        # WARNING: prod Lambdas run LOG_LEVEL=WARNING, and an owner action deserves a trace.
        logger.warning("Lyrics pick track %s -> LRCLIB %s by %s: %s", tid, lid, actor, result)
        return result
