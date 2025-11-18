# worker/service/sync_service.py
from __future__ import annotations
from typing import Dict, Any, List, Optional, Set
import json
from sqlalchemy import text
from sqlalchemy.engine import Connection
from worker.clients.spotify_client import spotify


def normalize_release_date(date: Optional[str]) -> Optional[str]:
    if not date:
        return None

    # 연도만 있는 경우
    if len(date) == 4:
        # "0000" 같은 말도 안 되는 연도는 버리자
        if not date.isdigit() or int(date) <= 0:
            return None
        return f"{date}-01-01"

    # "YYYY-MM" 형태
    if len(date) == 7:
        year, month = date.split("-", 1)
        if not (year.isdigit() and month.isdigit()):
            return None
        if int(year) <= 0 or int(month) <= 0:
            return None
        return f"{year}-{month}-01"

    # "YYYY-MM-DD" 같은 풀 포맷
    try:
        y, m, d = date.split("-")
        if int(y) <= 0 or int(m) <= 0 or int(d) <= 0:
            return None
        return date
    except Exception:
        # 이상한 포맷은 그냥 None
        return None


class AlbumSyncService:
    """배치로 앨범/트랙/아티스트를 수집하여 upsert"""

    def __init__(self, conn: Connection) -> None:
        self.conn = conn

    def sync_albums_batch(self, album_ids: List[str], market: Optional[str]) -> None:
        if not album_ids:
            return

        mkt = market or "KR"

        # 1) 앨범 배치 조회 (/v1/albums?ids=...)
        albums: List[Dict[str, Any]] = spotify.get_albums(album_ids, market=mkt)

        # 2) 트랙/아티스트 ID 수집 (간략 객체에서)
        album_artist_ids_map: Dict[str, List[str]] = {}
        track_artist_ids_map: Dict[str, List[str]] = {}

        for alb in albums:
            alb_id = alb["id"]

            # album artists
            alb_artist_ids = [a["id"] for a in (alb.get("artists") or []) if a.get("id")]
            album_artist_ids_map[alb_id] = alb_artist_ids

            # tracks (간략)
            tr_items = ((alb.get("tracks") or {}).get("items") or [])

            # track artists
            for t in tr_items:
                tid = t.get("id")
                if not tid:
                    continue
                artist_ids = [a["id"] for a in (t.get("artists") or []) if a.get("id")]
                track_artist_ids_map[tid] = artist_ids

        # 3) upsert (트랜잭션은 caller(SessionLocal().begin())에서 관리한다고 가정)

        # 3-1) artists (앨범/트랙에 등장하는 아티스트 이름만 우선 저장)
        # 앨범 아티스트
        for alb in albums:
            for a in (alb.get("artists") or []):
                sid = a.get("id")
                if not sid:
                    continue
                self.conn.execute(
                    text(
                        """
                        INSERT INTO artists (spotify_id, name)
                        VALUES (:sid, :name)
                        ON CONFLICT (spotify_id) DO UPDATE
                           SET name = EXCLUDED.name
                        """
                    ),
                    dict(sid=sid, name=a.get("name") or ""),
                )

        # 트랙 참여 아티스트
        for alb in albums:
            for t in ((alb.get("tracks") or {}).get("items") or []):
                for a in (t.get("artists") or []):
                    sid = a.get("id")
                    if not sid:
                        continue
                    self.conn.execute(
                        text(
                            """
                            INSERT INTO artists (spotify_id, name)
                            VALUES (:sid, :name)
                            ON CONFLICT (spotify_id) DO UPDATE
                               SET name = EXCLUDED.name
                            """
                        ),
                        dict(sid=sid, name=a.get("name") or ""),
                    )

        # 3-2) albums
        for alb in albums:
            cover = (alb.get("images") or [{}])[0].get("url")
            rdate = normalize_release_date(alb.get("release_date"))
            self.conn.execute(
                text(
                    """
                    INSERT INTO albums (
                        spotify_id,
                        title,
                        release_date,
                        cover_url,
                        album_type,
                        total_tracks,
                        label,
                        popularity,
                        ext_refs
                    )
                    VALUES (
                        :sid,
                        :title,
                        :rdate,
                        :cover,
                        :atype,
                        :total_tracks,
                        :label,
                        :popularity,
                        jsonb_build_object('spotify_url', CAST(:url AS text))
                    )
                    ON CONFLICT (spotify_id) DO UPDATE
                       SET title        = EXCLUDED.title,
                           release_date = EXCLUDED.release_date,
                           cover_url    = EXCLUDED.cover_url,
                           album_type   = EXCLUDED.album_type,
                           total_tracks = EXCLUDED.total_tracks,
                           label        = EXCLUDED.label,
                           popularity   = EXCLUDED.popularity
                    """
                ),
                dict(
                    sid=alb["id"],
                    title=alb.get("name") or "",
                    rdate=rdate,
                    cover=cover,
                    atype=alb.get("album_type"),
                    total_tracks=alb.get("total_tracks"),
                    label=alb.get("label"),
                    popularity=alb.get("popularity"),
                    url=(alb.get("external_urls") or {}).get("spotify"),
                ),
            )

        # 3-3) album_artists
        for alb in albums:
            alb_sid = alb["id"]
            for art_sid in album_artist_ids_map.get(alb_sid, []):
                self.conn.execute(
                    text(
                        """
                        INSERT INTO album_artists (album_id, artist_id)
                        VALUES (
                            (SELECT id FROM albums  WHERE spotify_id = :alb_sid),
                            (SELECT id FROM artists WHERE spotify_id = :art_sid)
                        )
                        ON CONFLICT DO NOTHING
                        """
                    ),
                    dict(alb_sid=alb_sid, art_sid=art_sid),
                )

        # 3-4) tracks
        for alb in albums:
            alb_sid = alb["id"]
            alb_tracks = ((alb.get("tracks") or {}).get("items") or [])
            for t in alb_tracks:
                tid = t.get("id")
                if not tid:
                    continue
                self.conn.execute(
                    text(
                        """
                        INSERT INTO tracks (spotify_id, album_id, title, track_no, duration_sec)
                        VALUES (
                            :sid,
                            (SELECT id FROM albums WHERE spotify_id = :alb_sid),
                            :title,
                            :no,
                            :dur
                        )
                        ON CONFLICT (spotify_id) DO UPDATE
                           SET title       = EXCLUDED.title,
                               album_id    = EXCLUDED.album_id,
                               track_no    = EXCLUDED.track_no,
                               duration_sec= EXCLUDED.duration_sec
                        """
                    ),
                    dict(
                        alb_sid=alb_sid,
                        sid=tid,
                        title=t.get("name") or "",
                        no=t.get("track_number"),
                        dur=(t.get("duration_ms") or 0) // 1000,
                    ),
                )

        # 3-5) track_artists
        for tid, aids in track_artist_ids_map.items():
            for aid in aids:
                self.conn.execute(
                    text(
                        """
                        INSERT INTO track_artists (track_id, artist_id)
                        VALUES (
                          (SELECT id FROM tracks  WHERE spotify_id = :tid),
                          (SELECT id FROM artists WHERE spotify_id = :aid)
                        )
                        ON CONFLICT DO NOTHING
                        """
                    ),
                    dict(tid=tid, aid=aid),
                )
         # ----------------------------------------------
        # 5) 추가: 사진 없는 아티스트만 상세 조회 후 enrich
        # ----------------------------------------------
        all_artist_ids: Set[str] = set()
        for ids in album_artist_ids_map.values():
            all_artist_ids.update(ids)
        for ids in track_artist_ids_map.values():
            all_artist_ids.update(ids)

        if all_artist_ids:
            rows = self.conn.execute(
                text("""
                    SELECT spotify_id
                    FROM artists
                    WHERE spotify_id = ANY(:ids)
                    AND (photo_url IS NULL OR photo_url = '')
                """),
                dict(ids=list(all_artist_ids)),
            ).fetchall()

            missing_ids = [r[0] for r in rows]

            if missing_ids:
                for chunk in [missing_ids[i:i+50] for i in range(0, len(missing_ids), 50)]:
                    detail_list = spotify.get_artists_batch(chunk)
                    for art in detail_list:
                        imgs = art.get("images") or []
                        photo = imgs[0].get("url") if imgs else None

                        followers = art.get("followers") or {}
                        if isinstance(followers, dict):
                            followers = followers.get("total")

                        self.conn.execute(
                        text("""
                            UPDATE artists SET
                                photo_url  = :photo,
                                genres     = CAST(:genres AS jsonb),
                                followers  = :followers,
                                popularity = :popularity
                            WHERE spotify_id = :sid
                        """),
                        dict(
                            sid=art["id"],
                            photo=photo,
                            # ✅ 여기서 JSON 문자열로 변환
                            genres=json.dumps(art.get("genres") or []),
                            followers=followers,
                            popularity=art.get("popularity"),
                        ),
                    )