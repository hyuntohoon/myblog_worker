# worker/handler.py
from __future__ import annotations
import json
import logging
from typing import Any, Dict, List

from worker.core.config import settings
from worker.clients.spotify_client import spotify

from worker.infra.db import SessionLocal
from worker.service.sync_service import AlbumSyncService, generate_and_save_aliases
from worker.service.listening_sync_service import run_listening_sync

logger = logging.getLogger(__name__)


def _run_listening_sync(is_manual_refresh: bool = False) -> None:
    """Spotify listening cache sync (recently-played + now-playing). Triggered by
    the EventBridge 1h cron and the manual '지금 새로고침' SQS message. Manual
    refreshes are debounced server-side (D31); the cron never is."""
    from worker.clients.spotify_user_client import spotify_user
    from worker.clients.sqs_producer import enqueue_album_sync

    run_listening_sync(
        SessionLocal,
        spotify_user,
        enqueue_unknown=enqueue_album_sync,
        is_manual_refresh=is_manual_refresh,
    )


def _run_library_sync() -> None:
    """Spotify saved-albums two-way reconcile (FEAT-spotify-library-sync). Triggered
    by the {"job": "spotify_library_sync"} SQS message the backend enqueues. Whether
    real Spotify PUT/DELETE writes execute is read from the worker's OWN setting
    (SPOTIFY_LIBRARY_WRITES_ENABLED) — NOT the message — so a stray/replayed message
    can never force a write. Plan-only by default."""
    from worker.clients.spotify_user_client import spotify_user
    from worker.clients.sqs_producer import enqueue_album_sync
    from worker.service.library_sync_service import run_library_sync

    run_library_sync(
        SessionLocal,
        spotify_user,
        enqueue_unknown=enqueue_album_sync,
        writes_enabled=settings.SPOTIFY_LIBRARY_WRITES_ENABLED,
    )


def _run_saved_tracks_sync(mode: str = "incremental") -> None:
    """Spotify saved-tracks (좋아요) cache sync for the /profile 분석 버킷
    (FEAT-genre-artist-distribution). Triggered by EventBridge (daily incremental +
    weekly full) and the manual {"job": "spotify_saved_tracks_sync", "mode": …} SQS
    message. mode ∈ {incremental, full}; full reconciles + prunes un-likes. Cache
    only — no Spotify write-back — so a message-sourced mode is safe (rule #9: the
    cron/endpoint only triggers; the worker does the Spotify read)."""
    from worker.clients.spotify_user_client import spotify_user
    from worker.service.saved_tracks_sync_service import run_saved_tracks_sync

    run_saved_tracks_sync(SessionLocal, spotify_user, mode=mode)


def _run_lastfm_sync() -> None:
    """Per-user Last.fm recent-tracks poll (FEAT-multi-user Phase 3a). Triggered by
    the EventBridge cron {"job":"lastfm_recent_tracks"}. Fetch→close per user; never
    holds a DB session across the Last.fm HTTP calls (rule #9 principle: the cron
    triggers, the worker reads Last.fm). No-op when LASTFM_API_KEY is unset."""
    if not settings.LASTFM_API_KEY:
        logger.info("lastfm sync skipped: LASTFM_API_KEY unset")
        return
    from worker.clients.lastfm_client import lastfm
    from worker.service.lastfm_sync_service import run_lastfm_sync

    run_lastfm_sync(SessionLocal, lastfm, max_users=settings.LASTFM_MAX_USERS_PER_TICK)


def _run_spotify_member_poll() -> None:
    """Per-user Spotify listening poll (FEAT-multi-user Phase 3b-d). Triggered by
    the EventBridge cron {"job":"spotify_member_poll"}. Per member: KMS-decrypt the
    3b-c refresh token → refresh → rotate/re-encrypt → write the V45 member
    listening tables. invalid_grant ⇒ status='reauth' (never retried); infra/KMS
    failures skip the user without a status change. Fetch→materialize→close; no DB
    session is ever held across the KMS/Spotify calls (rule #9: the cron pulls, the
    API only reads the cached rows). No connected members ⇒ natural no-op."""
    from worker.clients.spotify_member_client import spotify_member
    from worker.service.spotify_member_sync_service import run_spotify_member_sync

    run_spotify_member_sync(
        SessionLocal,
        spotify_member,
        max_users=settings.SPOTIFY_MEMBER_MAX_USERS_PER_TICK,
    )


def _run_release_upcoming_poll(mode: str) -> None:
    """Multi-source upcoming-release poller (FEAT-release-calendar Step 4).
    Triggered by two EventBridge schedules, one per source ({"job":
    "release_upcoming_poll","mode":"musicbrainz"|"itunes"}) so one source
    lagging never delays the other. Stateless time-bucket rotation over the
    pop≥50 watchlist; upserts 'announced' rows on UNIQUE(source, source_key).
    Deliberately EventBridge-only — never the blogSQS queue (an MB/iTunes
    outage must not clog album sync, same boundary as the alias fill)."""
    from worker.service.release_upcoming_service import run_release_upcoming_poll

    if mode == "musicbrainz":
        from worker.clients.musicbrainz_client import search_upcoming_release_groups

        run_release_upcoming_poll(
            SessionLocal, mode="musicbrainz", mb_search=search_upcoming_release_groups
        )
        return
    if mode == "itunes":
        from worker.clients.itunes_client import itunes

        run_release_upcoming_poll(SessionLocal, mode="itunes", itunes_client=itunes)
        return
    logger.warning("release_upcoming_poll: unknown mode %r — skipping", mode)


def _process_single(album_id: str, market: str) -> None:
    logger.info("Processing single album_id=%s market=%s DRY_RUN=%s", album_id, market, settings.DRY_RUN)
    if settings.DRY_RUN:
        # SpotifyClient has no single-get; the batch call with one id is equivalent
        # (spotify.get_album never existed — this path raised AttributeError).
        albums = spotify.get_albums([album_id], market=market)
        logger.info("[DRY_RUN] album='%s'", albums[0].get("name") if albums else None)
        return

    with SessionLocal() as session, session.begin():
        svc = AlbumSyncService(session.connection())
        svc.sync_albums_batch([album_id], market)
        logger.info("Album synced to DB: %s", album_id)


def _process_batch(album_ids: List[str], market: str) -> None:
    album_ids = [aid for aid in (album_ids or []) if aid]
    if not album_ids:
        logger.info("Skipping empty album_ids in batch")
        return

    logger.info("Processing batch albums=%d market=%s DRY_RUN=%s", len(album_ids), market, settings.DRY_RUN)
    if settings.DRY_RUN:
        albums = spotify.get_albums(album_ids, market=market)
        logger.info("[DRY_RUN] fetched=%d (batch)", len(albums))
        return

    with SessionLocal() as session, session.begin():
        svc = AlbumSyncService(session.connection())
        svc.sync_albums_batch(album_ids, market)
        logger.info("Batch synced to DB: %d albums", len(album_ids))


def _run_isrc_backfill(limit: int = 1000) -> None:
    """Bounded ISRC backfill for FEAT-lyrics-corpus Step 1b. Fetches up to `limit`
    tracks lacking ISRC from the DB, enriches from Spotify GET /v1/tracks, writes
    ISRC or sentinel. Follows alias-fill failure-isolation pattern (one batch failure
    doesn't block the job)."""
    from worker.service.isrc_backfill_service import IsrcBackfillService

    # No handler-owned session.begin(): the service commits per batch via the
    # session (and rolls back a failed batch), following the alias-fill /
    # lyrics-incremental pattern. Wrapping this in session.begin() would
    # deassociate the transaction the moment the service commits.
    with SessionLocal() as session:
        svc = IsrcBackfillService(session)
        metrics = svc.backfill_isrc(limit=limit)
        logger.info("ISRC backfill metrics: %s", metrics)


def _run_lyrics_incremental(limit: int | None = None) -> None:
    """Periodic incremental lyrics collection (FEAT-lyrics-corpus Step 3). Alias-fill
    pattern: select recently-added tracks lacking a track_lyrics row, evaluate each via
    the LRCLIB /api/search API with the Step 2 canonical matcher, write the match outcome
    + sentinel per row. Failure-isolated — a lyrics-source outage skips rows and never
    blocks album sync (this is a separate invocation from the SQS album path)."""
    from worker.service.lyrics_incremental_service import LyricsIncrementalService

    with SessionLocal() as session:
        svc = LyricsIncrementalService(session)
        metrics = svc.collect(limit=limit)
        logger.info("Lyrics incremental metrics: %s", metrics)


def _run_lyrics_reassessment(limit: int | None = None) -> None:
    """Periodic reassessment of unresolved lyrics rows (FEAT-lyrics-corpus Step 4). Re-checks
    not_found / ambiguous / review_required tracks (stalest first) against current LRCLIB
    coverage with the Step 2 canonical matcher: promotes on new evidence, refreshes otherwise,
    and NEVER overwrites a good match (replacement guard). Separate invocation from album sync;
    bounded to the 120s Lambda (shared eval loop)."""
    from worker.service.lyrics_reassessment_service import LyricsReassessmentService

    with SessionLocal() as session:
        svc = LyricsReassessmentService(session)
        metrics = svc.reassess(limit=limit)
        logger.info("Lyrics reassessment metrics: %s", metrics)


def _run_alias_generation() -> None:
    """Called by the EventBridge scheduled trigger (not the SQS sync path)."""
    try:
        generate_and_save_aliases(SessionLocal)
    except Exception as e:
        logger.error("Alias generation failed: %s", e, exc_info=True)
        raise


def _run_album_ingest() -> None:
    """Scheduled album-catalog ingest (FEAT-album-catalog-ingest). Discovers
    gate-passing new releases by catalog artists and enqueues them onto the same
    SQS album-sync pipeline this handler consumes (the consumer never re-enqueues,
    so there is no feedback loop)."""
    from worker.clients.sqs_producer import enqueue_album_sync
    from worker.service.album_ingest_service import run_album_ingest

    run_album_ingest(SessionLocal, spotify, enqueue_album_sync)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    # EventBridge 1h cron — Spotify listening cache sync. This rule's target sends a
    # constant input {"job": "spotify_listening"} (no "source"), so check job first.
    if event.get("job") == "spotify_listening":
        logger.info("EventBridge trigger: running Spotify listening sync")
        _run_listening_sync()
        return {}

    # EventBridge daily cron — album-catalog ingest (constant input, no "source").
    if event.get("job") == "album_ingest":
        logger.info("EventBridge trigger: running album-catalog ingest")
        _run_album_ingest()
        return {}

    # EventBridge crons — Spotify saved-tracks (좋아요) sync (constant input, no
    # "source"). The daily rule sends mode=incremental, the weekly rule mode=full.
    if event.get("job") == "spotify_saved_tracks_sync":
        mode = event.get("mode", "incremental")
        logger.info("EventBridge trigger: running saved-tracks sync (mode=%s)", mode)
        _run_saved_tracks_sync(mode)
        return {}

    # EventBridge/SQS trigger — ISRC backfill (FEAT-lyrics-corpus Step 1b).
    # Bounded backfill: fetch tracks without ISRC, enrich from Spotify, write ISRC or sentinel.
    if event.get("job") == "isrc_backfill":
        limit = event.get("limit", 1000)
        logger.info("EventBridge/SQS trigger: running ISRC backfill (limit=%s)", limit)
        _run_isrc_backfill(limit=limit)
        return {}

    # EventBridge/SQS trigger — incremental lyrics collection (FEAT-lyrics-corpus Step 3).
    # Constant input {"job": "lyrics_incremental"} (routed before the alias source check,
    # same pattern as isrc_backfill / album_ingest). Bounded per invocation; optional
    # "limit" overrides settings.LYRICS_INCR_BATCH_LIMIT.
    if event.get("job") == "lyrics_incremental":
        limit = event.get("limit")
        logger.info("EventBridge/SQS trigger: running lyrics incremental collection (limit=%s)", limit)
        _run_lyrics_incremental(limit=limit)
        return {}

    # EventBridge/SQS trigger — periodic reassessment of unresolved lyrics rows
    # (FEAT-lyrics-corpus Step 4). Constant input {"job":"lyrics_reassessment"} (routed before
    # the alias source check). Bounded per invocation; optional "limit" overrides the setting.
    if event.get("job") == "lyrics_reassessment":
        limit = event.get("limit")
        logger.info("EventBridge/SQS trigger: running lyrics reassessment (limit=%s)", limit)
        _run_lyrics_reassessment(limit=limit)
        return {}

    # EventBridge cron — per-user Last.fm recent-tracks poll (constant input, no
    # "source"; FEAT-multi-user Phase 3a). Routed before the alias source check.
    if event.get("job") == "lastfm_recent_tracks":
        logger.info("EventBridge trigger: running Last.fm recent-tracks sync")
        _run_lastfm_sync()
        return {}

    # EventBridge crons — multi-source upcoming-release poller (constant input,
    # no "source"; FEAT-release-calendar Step 4). One rule per source, routed on
    # event["mode"] (saved_tracks pattern), before the alias source check.
    if event.get("job") == "release_upcoming_poll":
        mode = event.get("mode", "musicbrainz")
        logger.info("EventBridge trigger: running upcoming-release poll (mode=%s)", mode)
        _run_release_upcoming_poll(mode)
        return {}

    # EventBridge cron — per-user Spotify listening poll (constant input, no
    # "source"; FEAT-multi-user Phase 3b-d). Routed before the alias source check.
    if event.get("job") == "spotify_member_poll":
        logger.info("EventBridge trigger: running Spotify member listening poll")
        _run_spotify_member_poll()
        return {}

    # EventBridge scheduled rule (alias cron) — full event carries source=aws.events
    if event.get("source") == "aws.events":
        logger.info("EventBridge trigger: running alias generation")
        _run_alias_generation()
        return {}

    # SQS trigger — album sync / manual listening refresh
    records = event.get("Records") or []
    logger.info("Received %d records", len(records))

    failed: List[str] = []
    album_synced = False  # any album-sync record landed → chain a lyrics pass below

    for i, record in enumerate(records, start=1):
        try:
            body = json.loads(record["body"])
            logger.info("[%d/%d] Processing record body=%s", i, len(records), body)

            # Manual "지금 새로고침" button → async listening sync (rule #9).
            # Debounced server-side (D31) so button spam can't burst Spotify.
            if body.get("job") == "spotify_refresh":
                _run_listening_sync(is_manual_refresh=True)
                continue

            # Spotify saved-albums two-way reconcile (FEAT-spotify-library-sync).
            # Enqueued by the backend POST /api/buckets/spotify-library/sync (rule #9:
            # the endpoint only enqueues). Real writes gated on the worker's own
            # setting, not this message.
            if body.get("job") == "spotify_library_sync":
                _run_library_sync()
                continue

            # Spotify saved-tracks (좋아요) cache sync — manual refresh / backfill.
            # mode ∈ {incremental, full}; full reconciles + prunes un-likes.
            if body.get("job") == "spotify_saved_tracks_sync":
                _run_saved_tracks_sync(body.get("mode", "incremental"))
                continue

            # Lyrics jobs via SQS. The EventBridge constant-input path hits the
            # event["job"] checks at the top of the handler; an SQS-delivered message
            # arrives wrapped in Records, so it must ALSO be routed here (the
            # eventbridge.tf comments promise the manual blogSQS path, and the
            # near-real-time chain below relies on it).
            if body.get("job") == "lyrics_incremental":
                _run_lyrics_incremental(limit=body.get("limit"))
                continue

            if body.get("job") == "lyrics_reassessment":
                _run_lyrics_reassessment(limit=body.get("limit"))
                continue

            market = body.get("market", settings.SPOTIFY_DEFAULT_MARKET)

            if "album_ids" in body and isinstance(body["album_ids"], list):
                _process_batch(body["album_ids"], market)
                album_synced = True
                continue

            if "spotify_album_id" in body:
                _process_single(body["spotify_album_id"], market)
                album_synced = True
                continue

            logger.warning("Unknown message format: %s", body)

        except Exception as e:
            logger.error("[%d/%d] Record failed: %s", i, len(records), e, exc_info=True)
            failed.append(record.get("messageId", str(i)))

    # Near-real-time lyrics chaining: an album sync just landed new tracks, so kick
    # the incremental collector now instead of waiting for the 15-min cron. One send
    # per invocation regardless of record count; DRY_RUN wrote nothing so there is
    # nothing to chain. Best-effort — a failed send must not fail the album records
    # (the cron is the safety net).
    if album_synced and not settings.DRY_RUN:
        try:
            from worker.clients.sqs_producer import enqueue_lyrics_incremental

            enqueue_lyrics_incremental()
        except Exception:
            logger.warning("lyrics-incremental chain enqueue failed; 15-min cron covers", exc_info=True)

    return {"batchItemFailures": [{"itemIdentifier": mid} for mid in failed]}
