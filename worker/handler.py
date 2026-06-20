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

    # EventBridge scheduled rule (alias cron) — full event carries source=aws.events
    if event.get("source") == "aws.events":
        logger.info("EventBridge trigger: running alias generation")
        _run_alias_generation()
        return {}

    # SQS trigger — album sync / manual listening refresh
    records = event.get("Records") or []
    logger.info("Received %d records", len(records))

    failed: List[str] = []

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

            market = body.get("market", settings.SPOTIFY_DEFAULT_MARKET)

            if "album_ids" in body and isinstance(body["album_ids"], list):
                _process_batch(body["album_ids"], market)
                continue

            if "spotify_album_id" in body:
                _process_single(body["spotify_album_id"], market)
                continue

            logger.warning("Unknown message format: %s", body)

        except Exception as e:
            logger.error("[%d/%d] Record failed: %s", i, len(records), e, exc_info=True)
            failed.append(record.get("messageId", str(i)))

    return {"batchItemFailures": [{"itemIdentifier": mid} for mid in failed]}
