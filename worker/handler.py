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


def _process_single(album_id: str, market: str) -> None:
    logger.info("Processing single album_id=%s market=%s DRY_RUN=%s", album_id, market, settings.DRY_RUN)
    if settings.DRY_RUN:
        alb = spotify.get_album(album_id, market=market)
        logger.info("[DRY_RUN] album='%s'", alb.get("name"))
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


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    # EventBridge 1h cron — Spotify listening cache sync. This rule's target sends a
    # constant input {"job": "spotify_listening"} (no "source"), so check job first.
    if event.get("job") == "spotify_listening":
        logger.info("EventBridge trigger: running Spotify listening sync")
        _run_listening_sync()
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
