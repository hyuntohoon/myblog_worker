# worker/handler.py
from __future__ import annotations
import json
from typing import Any, Dict, List

from worker.core.config import settings
from worker.clients.spotify_client import spotify

# ↓ DRY_RUN=False일 때만 사용
from worker.infra.db import SessionLocal
from worker.service.sync_service import AlbumSyncService


def _process_single(album_id: str, market: str) -> None:
    print(f"[START] Processing album_id={album_id}, market={market}, DRY_RUN={settings.DRY_RUN}")
    if settings.DRY_RUN:
        # 호출량 최소화: 앨범만 확인
        alb = spotify.get_album(album_id, market=market)
        print(f"[DRY_RUN] album='{alb.get('name')}'")
        return

    with SessionLocal() as session, session.begin():
        svc = AlbumSyncService(session.connection())
        # 기존 단건 처리 로직 유지(호환)
        svc.sync_album_by_spotify(album_id, market)
        print(f"[OK] Album synced to DB: {album_id}")


def _process_batch(album_ids: List[str], market: str) -> None:
    # 배치 입력 정리
    album_ids = [aid for aid in (album_ids or []) if aid]
    if not album_ids:
        print("[SKIP] empty album_ids in batch")
        return

    print(f"[START-BATCH] albums={len(album_ids)}, market={market}, DRY_RUN={settings.DRY_RUN}")
    if settings.DRY_RUN:
        # 배치로 앨범 확인 (/v1/albums?ids=...)
        albums = spotify.get_albums(album_ids, market=market)
        print(f"[DRY_RUN] fetched={len(albums)} (batch)")
        return

    with SessionLocal() as session, session.begin():
        svc = AlbumSyncService(session.connection())
        # 새로 추가한 배치 동기화 사용
        svc.sync_albums_batch(album_ids, market)
        print(f"[OK] Batch synced to DB: {len(album_ids)} albums")


def lambda_handler(event: Dict[str, Any], context: Any) -> List[bool]:
    """
    반환: 레코드별 성공 여부.
    현재 폴러(run_local)는 성공/실패와 무관하게 삭제하도록 설정되어 있으므로
    여기서는 기본적으로 True를 반환한다.
    """
    records = event.get("Records") or []
    print(f"[EVENT] Received {len(records)} records")

    results: List[bool] = []

    for i, record in enumerate(records, start=1):
        try:
            body = json.loads(record["body"])
            print(f"[{i}/{len(records)}] Record body={body}")

            market = body.get("market", settings.SPOTIFY_DEFAULT_MARKET)

            # 권장 포맷: {"album_ids": [...], "market": "KR"}
            if "album_ids" in body and isinstance(body["album_ids"], list):
                _process_batch(body["album_ids"], market)
                results.append(True)
                continue

            # 하위 호환: {"spotify_album_id": "...", "market": "KR"}
            if "spotify_album_id" in body:
                _process_single(body["spotify_album_id"], market)
                results.append(True)
                continue

            # 알 수 없는 포맷
            print(f"[SKIP] Unknown message format: {body}")
            results.append(True)

        except Exception as e:
            # 재시도 비활성 정책: 로깅 후 True 반환(즉시 삭제)
            print(f"[ERROR] Record {i} failed: {e}")
            results.append(True)

    return results