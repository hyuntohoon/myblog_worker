# worker/service/genius_fetch_service.py
"""
FEAT-lyrics-annotations Thread 1 — the writer.

Fills ``track_genius_songs`` / ``track_genius_annotations`` (V49) so the lyrics
sheet has something to render. Everything downstream already exists: the read path
anchors fragments per request, and the sheet draws them.

Three rules this job exists inside, each a recurring bug class in this repo:

1. **Never hold a DB session across the external-API loop.** The selection SELECT
   autobegins a transaction that would otherwise sit idle across ~3 HTTP round trips
   per track; Neon drops those connections (ProtocolViolation). So: fetch the work
   list, materialize it, close — then loop — then open a fresh short write session
   per track.
2. **Bulk ``ON CONFLICT`` upserts sort by the conflict key.** Annotations are sorted
   by ``genius_annotation_id`` before the statement, for row-lock deadlock avoidance.
3. **Explicit HTTP timeout** — set on the client.

And one invariant this job owns, which the read path depends on:

    **The songs row is written BEFORE its annotations.**

``lyrics_service.get_normalized`` gates the annotation query on the parent row, to
avoid a second cross-region round trip for the ~26,000 tracks with no Genius data.
Annotations written without a parent row are therefore invisible — not wrong, not
logged, simply never read. Both writes share one transaction here so the order
cannot be observed broken.

**Scope.** Tracks that have matched lyrics and no Genius row yet, oldest first. The
store is deliberately independent of lyrics (2 of 15 LUX tracks carry annotations
and no synced lyrics), so this scope is a *bounded starting pool*, not a statement
about the schema — widening it is a query change and nothing else.

Korean tracks are not filtered out. Genius prose coverage for Korean measured 1/8
descriptions and 0/8 annotation sets, so they mostly resolve to a matched row with
no annotations, which is the honest outcome and is recorded in ``annotation_count``.
Filtering would need a language signal we do not have — Genius's own ``language`` is
one tag per song and unreliable.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from worker.clients.genius_client import (
    GeniusAnnotation,
    GeniusAuthError,
    GeniusClient,
    GeniusSong,
    GeniusTransientError,
)
from worker.core.config import settings

logger = logging.getLogger(__name__)

FETCHER_VERSION = "genius-fetch/1"

MATCH_MATCHED = "matched"
MATCH_AMBIGUOUS = "ambiguous"
MATCH_NOT_FOUND = "not_found"

_SELECT_WORK = text("""
    SELECT t.id, t.title, COALESCE(a.title, '') AS album,
           COALESCE((
             SELECT array_agg(ar.name ORDER BY ar.popularity DESC NULLS LAST, ar.name)
               FROM track_artists ta
               JOIN artists ar ON ar.id = ta.artist_id
              WHERE ta.track_id = t.id
           ), ARRAY[]::text[]) AS artists
      FROM tracks t
      JOIN track_lyrics tl ON tl.track_id = t.id
      LEFT JOIN albums a ON a.id = t.album_id
      LEFT JOIN track_genius_songs g ON g.track_id = t.id
     WHERE tl.match_status = 'matched'
       AND g.track_id IS NULL
     ORDER BY t.id
     LIMIT :limit
""")

_UPSERT_SONG = text("""
    INSERT INTO track_genius_songs (
        track_id, genius_song_id, genius_url, match_status, match_confidence,
        genius_title, genius_artist, description, annotation_count,
        credits, relationships, language, fetched_at, fetcher_version, updated_at
    ) VALUES (
        :track_id, :genius_song_id, :genius_url, :match_status, :match_confidence,
        :genius_title, :genius_artist, :description, :annotation_count,
        CAST(:credits AS jsonb), CAST(:relationships AS jsonb), :language,
        now(), :fetcher_version, now()
    )
    ON CONFLICT (track_id) DO UPDATE SET
        genius_song_id   = EXCLUDED.genius_song_id,
        genius_url       = EXCLUDED.genius_url,
        match_status     = EXCLUDED.match_status,
        match_confidence = EXCLUDED.match_confidence,
        genius_title     = EXCLUDED.genius_title,
        genius_artist    = EXCLUDED.genius_artist,
        description      = EXCLUDED.description,
        annotation_count = EXCLUDED.annotation_count,
        credits          = EXCLUDED.credits,
        relationships    = EXCLUDED.relationships,
        language         = EXCLUDED.language,
        fetched_at       = now(),
        fetcher_version  = EXCLUDED.fetcher_version,
        updated_at       = now()
""")

# body_ko / translation_status are NOT touched on conflict: a re-fetch must not
# discard a Korean body that a later translation pass produced. Staleness is the
# read path's job — it compares body_source_fingerprint at read time.
_UPSERT_ANNOTATION = text("""
    INSERT INTO track_genius_annotations (
        genius_annotation_id, track_id, fragment, referent_ordinal,
        body_source, body_source_lang, votes_total, is_verified, state,
        fetched_at, updated_at
    ) VALUES (
        :genius_annotation_id, :track_id, :fragment, :referent_ordinal,
        :body_source, :body_source_lang, :votes_total, :is_verified, :state,
        now(), now()
    )
    ON CONFLICT (genius_annotation_id) DO UPDATE SET
        track_id         = EXCLUDED.track_id,
        fragment         = EXCLUDED.fragment,
        referent_ordinal = EXCLUDED.referent_ordinal,
        body_source      = EXCLUDED.body_source,
        body_source_lang = EXCLUDED.body_source_lang,
        votes_total      = EXCLUDED.votes_total,
        is_verified      = EXCLUDED.is_verified,
        state            = EXCLUDED.state,
        fetched_at       = now(),
        updated_at       = now()
""")


class GeniusFetchService:
    def __init__(self, session_factory, client: GeniusClient) -> None:
        self._session_factory = session_factory
        self._client = client

    # ── work list ───────────────────────────────────────────────────────────

    def _claim_work(self, limit: int) -> List[Dict[str, Any]]:
        """Read the pool and CLOSE before the first Genius call. See rule 1."""
        session: Session = self._session_factory()
        try:
            rows = session.execute(_SELECT_WORK, {"limit": limit}).fetchall()
            return [
                {"track_id": str(r[0]), "title": r[1] or "", "album": r[2],
                 "artists": list(r[3] or [])}
                for r in rows
            ]
        finally:
            session.close()

    # ── write ───────────────────────────────────────────────────────────────

    def _write(
        self,
        track_id: str,
        song: Optional[GeniusSong],
        status: str,
        annotations: List[GeniusAnnotation],
    ) -> int:
        """One short transaction per track: the songs row, then its annotations.

        Both in the same transaction so the read path can never observe annotations
        without their parent — the ordering invariant is enforced by the boundary,
        not by hoping the calls stay in this order.
        """
        import json

        session: Session = self._session_factory()
        try:
            session.execute(_UPSERT_SONG, {
                "track_id": track_id,
                "genius_song_id": song.song_id if song else 0,
                "genius_url": song.url if song else None,
                "match_status": status,
                "match_confidence": song.confidence if song else None,
                "genius_title": song.title if song else None,
                "genius_artist": song.artist if song else None,
                "description": song.description if song else None,
                "annotation_count": song.annotation_count if song else None,
                "credits": json.dumps(song.credits if song else {}, ensure_ascii=False),
                "relationships": json.dumps(song.relationships if song else {}, ensure_ascii=False),
                "language": song.language if song else None,
                "fetcher_version": FETCHER_VERSION,
            })

            written = 0
            if annotations:
                # Sorted by the conflict key — row-lock deadlock avoidance (rule 2).
                for a in sorted(annotations, key=lambda x: x.annotation_id):
                    session.execute(_UPSERT_ANNOTATION, {
                        "genius_annotation_id": a.annotation_id,
                        "track_id": track_id,
                        "fragment": a.fragment,
                        "referent_ordinal": a.referent_ordinal,
                        "body_source": a.body,
                        "body_source_lang": song.language if song else None,
                        "votes_total": a.votes_total,
                        "is_verified": a.is_verified,
                        "state": a.state,
                    })
                    written += 1
            session.commit()
            return written
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # ── the job ─────────────────────────────────────────────────────────────

    def run(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """Bounded pass. Returns metrics; never raises for a single bad track."""
        metrics = {
            "considered": 0, "matched": 0, "ambiguous": 0,
            "not_found": 0, "annotations": 0, "errors": 0,
        }
        if not self._client.enabled:
            logger.warning("genius_fetch: GENIUS_ACCESS_TOKEN unset — no-op")
            return metrics

        # `limit or settings.X` would turn an explicit limit=0 into a full batch —
        # the opposite of what "0" reads as, and a trap this repo has already hit.
        limit = settings.GENIUS_FETCH_BATCH_LIMIT if limit is None else limit
        if limit <= 0:
            logger.warning("genius_fetch: limit=%s — nothing to do", limit)
            return metrics
        work = self._claim_work(limit)          # session closed before any HTTP
        metrics["considered"] = len(work)
        if not work:
            return metrics

        try:
            for item in work:
                try:
                    song, status, annotations = self._fetch_one(item)
                    written = self._write(item["track_id"], song, status, annotations)
                    metrics[status] = metrics.get(status, 0) + 1
                    metrics["annotations"] += written
                except GeniusAuthError:
                    # The owner rotates the token themselves; hammering wastes the batch.
                    logger.error("genius_fetch: token rejected — stopping this run")
                    metrics["errors"] += 1
                    break
                except GeniusTransientError as exc:
                    # Park nothing: leaving the row unwritten means it is retried,
                    # whereas a not_found row would remove it from the pool for good.
                    logger.warning("genius_fetch: transient failure on %s: %s", item["track_id"], exc)
                    metrics["errors"] += 1
                except Exception:
                    logger.exception("genius_fetch: unexpected failure on %s", item["track_id"])
                    metrics["errors"] += 1
        finally:
            self._client.close()

        logger.warning("genius_fetch done: %s", metrics)   # prod LOG_LEVEL is WARNING
        return metrics

    def _fetch_one(
        self, item: Dict[str, Any]
    ) -> Tuple[Optional[GeniusSong], str, List[GeniusAnnotation]]:
        # EVERY credited artist, not just the most popular one — Genius names a
        # collaboration by its own idea of the primary credit, which is routinely
        # not ours. See GeniusClient.find_song.
        song = self._client.find_song(item["title"], item["artists"])
        if song is None:
            return None, MATCH_NOT_FOUND, []
        # TWO gates, not one. The blend leans on the artist, so it cannot catch the
        # quiet failure — right artist, wrong song — which is what a shared-artist
        # search hit looks like. Measured live: "GUIZ CORLEONE" resolved to Freeze
        # Corleone's "Braquage" at title 0.19 / artist 1.0 / blend 0.676, cleared the
        # blended gate, and wrote 18 annotations from a different song.
        if (
            song.confidence < settings.GENIUS_MIN_CONFIDENCE
            or song.title_score < settings.GENIUS_MIN_TITLE_SIMILARITY
        ):
            # Recorded, with its scores and what Genius thought it was, so a bad
            # match is visible in a query — but its annotations are NOT written.
            logger.warning(
                "genius_fetch: ambiguous match for %r — got %r by %r (title=%.2f artist=%.2f)",
                item["title"], song.title, song.artist, song.title_score, song.artist_score,
            )
            return song, MATCH_AMBIGUOUS, []
        song = self._client.load_song(song)
        annotations = self._client.load_annotations(song.song_id)
        return song, MATCH_MATCHED, annotations


def run_genius_fetch(session_factory, client: GeniusClient, *, limit: Optional[int] = None) -> Dict[str, Any]:
    return GeniusFetchService(session_factory, client).run(limit=limit)
