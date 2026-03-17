# worker/handler.py
from __future__ import annotations
import json
from typing import Any, Dict, List

from worker.core.config import settings
from worker.clients.spotify_client import spotify

from worker.infra.db import SessionLocal
from worker.service.sync_service import AlbumSyncService, generate_and_save_aliases


def _process_single(album_id: str, market: str) -> None:
    print(f"[START] Processing album_id={album_id}, market={market}, DRY_RUN={settings.DRY_RUN}")
    if settings.DRY_RUN:
        alb = spotify.get_album(album_id, market=market)
        print(f"[DRY_RUN] album='{alb.get('name')}'")
        return

    with SessionLocal() as session, session.begin():
        svc = AlbumSyncService(session.connection())
        svc.sync_album_by_spotify(album_id, market)
        print(f"[OK] Album synced to DB: {album_id}")


def _process_batch(album_ids: List[str], market: str) -> None:
    album_ids = [aid for aid in (album_ids or []) if aid]
    if not album_ids:
        print("[SKIP] empty album_ids in batch")
        return

    print(f"[START-BATCH] albums={len(album_ids)}, market={market}, DRY_RUN={settings.DRY_RUN}")
    if settings.DRY_RUN:
        albums = spotify.get_albums(album_ids, market=market)
        print(f"[DRY_RUN] fetched={len(albums)} (batch)")
        return

    # --- 1) 메인 동기화 (트랜잭션 1) ---
    with SessionLocal() as session, session.begin():
        svc = AlbumSyncService(session.connection())
        svc.sync_albums_batch(album_ids, market)
        print(f"[OK] Batch synced to DB: {len(album_ids)} albums")
    # COMMIT 완료

    # --- 2) AI 별칭 생성 (트랜잭션 2, 별도) ---
    try:
        generate_and_save_aliases(SessionLocal)
    except Exception as e:
        # 별칭 생성 실패해도 메인 데이터는 이미 저장됨
        print(f"[WARN] Alias generation failed (non-critical): {e}")


def lambda_handler(event: Dict[str, Any], context: Any) -> List[bool]:
    records = event.get("Records") or []
    print(f"[EVENT] Received {len(records)} records")

    results: List[bool] = []

    for i, record in enumerate(records, start=1):
        try:
            body = json.loads(record["body"])
            print(f"[{i}/{len(records)}] Record body={body}")

            market = body.get("market", settings.SPOTIFY_DEFAULT_MARKET)

            if "album_ids" in body and isinstance(body["album_ids"], list):
                _process_batch(body["album_ids"], market)
                results.append(True)
                continue

            if "spotify_album_id" in body:
                _process_single(body["spotify_album_id"], market)
                results.append(True)
                continue

            print(f"[SKIP] Unknown message format: {body}")
            results.append(True)

        except Exception as e:
            print(f"[ERROR] Record {i} failed: {e}")
            results.append(True)

    return results