# worker/handler.py
from __future__ import annotations
import json
import logging
from typing import Any, Dict, List

from worker.core.config import settings
from worker.clients.spotify_client import spotify

from worker.infra.db import SessionLocal
from worker.service.sync_service import AlbumSyncService, generate_and_save_aliases

logger = logging.getLogger(__name__)


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

    # --- 1) Main sync (transaction 1) ---
    with SessionLocal() as session, session.begin():
        svc = AlbumSyncService(session.connection())
        svc.sync_albums_batch(album_ids, market)
        logger.info("Batch synced to DB: %d albums", len(album_ids))

    # --- 2) AI alias generation (transaction 2, separate) ---
    try:
        generate_and_save_aliases(SessionLocal)
    except Exception as e:
        logger.warning("Alias generation failed (non-critical): %s", e)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    records = event.get("Records") or []
    logger.info("Received %d records", len(records))

    failed: List[str] = []

    for i, record in enumerate(records, start=1):
        try:
            body = json.loads(record["body"])
            logger.info("[%d/%d] Processing record body=%s", i, len(records), body)

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
            raise

    return {"batchItemFailures": [{"itemIdentifier": mid} for mid in failed]}
