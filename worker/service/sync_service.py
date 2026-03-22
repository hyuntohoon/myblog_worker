# worker/service/sync_service.py
from __future__ import annotations
from typing import Dict, Any, List, Optional, Set
import json
import requests
from sqlalchemy import text
from sqlalchemy.engine import Connection
from worker.clients.spotify_client import spotify
from worker.core.config import settings


def normalize_release_date(date: Optional[str]) -> Optional[str]:
    if not date:
        return None

    if len(date) == 4:
        if not date.isdigit() or int(date) <= 0:
            return None
        return f"{date}-01-01"

    if len(date) == 7:
        year, month = date.split("-", 1)
        if not (year.isdigit() and month.isdigit()):
            return None
        if int(year) <= 0 or int(month) <= 0:
            return None
        return f"{year}-{month}-01"

    try:
        y, m, d = date.split("-")
        if len(y) != 4:
            return None
        if int(y) <= 0 or int(m) <= 0 or int(d) <= 0:
            return None
        return date
    except Exception:
        return None


def generate_artist_aliases(name: str, genres: List[str]) -> List[str]:
    """Gemini API로 아티스트 별칭 생성. 실패하면 빈 리스트 반환."""
    api_key = getattr(settings, "GEMINI_API_KEY", None)
    if not api_key:
        return []

    try:
        response = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}",
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{
                    "parts": [{
                        "text": (
                            f"아티스트 이름: {name}\n"
                            f"장르: {', '.join(genres) if genres else '알 수 없음'}\n\n"
                            "이 아티스트를 검색할 때 사용할 수 있는 한국어 별칭, 다른 표기, 줄임말을 JSON 배열로 반환해.\n"
                            "규칙:\n"
                            "- 원래 이름은 제외\n"
                            "- 실제로 한국에서 사용되는 별칭만\n"
                            "- 한글 발음 표기 포함 (예: C JAMM → 씨잼)\n"
                            "- 붙여쓰기/띄어쓰기 변형 포함 (예: C JAMM → CJAMM)\n"
                            "- 없으면 빈 배열 []\n"
                            "- JSON 배열만 반환하고 다른 텍스트는 쓰지 마\n"
                            '예시: ["씨잼", "CJAMM"]'
                        )
                    }]
                }]
            },
            timeout=10,
        )

        if response.status_code != 200:
            print(f"  [ALIAS] Gemini API error: {response.status_code}")
            return []

        data = response.json()
        text_content = data["candidates"][0]["content"]["parts"][0]["text"]

        # JSON 파싱 (마크다운 코드블록 제거)
        cleaned = text_content.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[-1]
            cleaned = cleaned.rsplit("```", 1)[0]
        cleaned = cleaned.strip()

        aliases = json.loads(cleaned)
        if isinstance(aliases, list):
            aliases = [a.strip() for a in aliases if isinstance(a, str) and a.strip()]
            aliases = [a for a in aliases if a.lower() != name.lower()]
            print(f"  [ALIAS] {name} → {aliases}")
            return aliases

        return []

    except Exception as e:
        print(f"  [ALIAS] Failed for {name}: {e}")
        return []


class AlbumSyncService:
    """배치로 앨범/트랙/아티스트를 수집하여 upsert (bulk 최적화)"""

    def __init__(self, conn: Connection) -> None:
        self.conn = conn

    def sync_albums_batch(self, album_ids: List[str], market: Optional[str]) -> None:
        """메인 동기화. 호출자의 트랜잭션 안에서 실행된다."""
        if not album_ids:
            return

        mkt = market or "KR"

        # 1) 앨범 배치 조회
        albums: List[Dict[str, Any]] = spotify.get_albums(album_ids, market=mkt)

        # 2) 데이터 수집
        all_artists: Dict[str, str] = {}
        album_data: List[Dict] = []
        album_artist_pairs: List[Dict] = []
        track_data: List[Dict] = []
        track_artist_pairs: List[Dict] = []

        for alb in albums:
            alb_sid = alb["id"]

            alb_artist_ids = []
            for a in (alb.get("artists") or []):
                sid = a.get("id")
                if not sid:
                    continue
                all_artists[sid] = a.get("name") or ""
                alb_artist_ids.append(sid)

            cover = (alb.get("images") or [{}])[0].get("url")
            rdate = normalize_release_date(alb.get("release_date"))
            album_data.append(dict(
                sid=alb_sid,
                title=alb.get("name") or "",
                rdate=rdate,
                cover=cover,
                atype=alb.get("album_type"),
                total_tracks=alb.get("total_tracks"),
                label=alb.get("label"),
                popularity=alb.get("popularity"),
                url=(alb.get("external_urls") or {}).get("spotify"),
            ))

            for art_sid in alb_artist_ids:
                album_artist_pairs.append(dict(alb_sid=alb_sid, art_sid=art_sid))

            for t in ((alb.get("tracks") or {}).get("items") or []):
                tid = t.get("id")
                if not tid:
                    continue
                track_data.append(dict(
                    sid=tid,
                    alb_sid=alb_sid,
                    title=t.get("name") or "",
                    no=t.get("track_number"),
                    dur=(t.get("duration_ms") or 0) // 1000,
                ))
                for a in (t.get("artists") or []):
                    a_sid = a.get("id")
                    if not a_sid:
                        continue
                    all_artists[a_sid] = a.get("name") or ""
                    track_artist_pairs.append(dict(tid=tid, aid=a_sid))

        # 3) Bulk upsert

        if all_artists:
            artists_list = [dict(sid=sid, name=name) for sid, name in all_artists.items()]
            self.conn.execute(
                text("""
                    INSERT INTO artists (spotify_id, name)
                    VALUES (:sid, :name)
                    ON CONFLICT (spotify_id) DO UPDATE SET name = EXCLUDED.name
                """),
                artists_list,
            )
            print(f"  [BULK] artists upserted: {len(artists_list)}")

        if album_data:
            self.conn.execute(
                text("""
                    INSERT INTO albums (
                        spotify_id, title, release_date, cover_url,
                        album_type, total_tracks, label, popularity, ext_refs
                    )
                    VALUES (
                        :sid, :title, :rdate, :cover,
                        :atype, :total_tracks, :label, :popularity,
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
                """),
                album_data,
            )
            print(f"  [BULK] albums upserted: {len(album_data)}")

        if album_artist_pairs:
            self.conn.execute(
                text("""
                    INSERT INTO album_artists (album_id, artist_id)
                    VALUES (
                        (SELECT id FROM albums  WHERE spotify_id = :alb_sid),
                        (SELECT id FROM artists WHERE spotify_id = :art_sid)
                    )
                    ON CONFLICT DO NOTHING
                """),
                album_artist_pairs,
            )
            print(f"  [BULK] album_artists linked: {len(album_artist_pairs)}")

        if track_data:
            self.conn.execute(
                text("""
                    INSERT INTO tracks (spotify_id, album_id, title, track_no, duration_sec)
                    VALUES (
                        :sid,
                        (SELECT id FROM albums WHERE spotify_id = :alb_sid),
                        :title, :no, :dur
                    )
                    ON CONFLICT (spotify_id) DO UPDATE
                       SET title        = EXCLUDED.title,
                           album_id     = EXCLUDED.album_id,
                           track_no     = EXCLUDED.track_no,
                           duration_sec = EXCLUDED.duration_sec
                """),
                track_data,
            )
            print(f"  [BULK] tracks upserted: {len(track_data)}")

        if track_artist_pairs:
            unique_pairs = list({(p["tid"], p["aid"]): p for p in track_artist_pairs}.values())
            self.conn.execute(
                text("""
                    INSERT INTO track_artists (track_id, artist_id)
                    VALUES (
                        (SELECT id FROM tracks  WHERE spotify_id = :tid),
                        (SELECT id FROM artists WHERE spotify_id = :aid)
                    )
                    ON CONFLICT DO NOTHING
                """),
                unique_pairs,
            )
            print(f"  [BULK] track_artists linked: {len(unique_pairs)}")

        # 4) 사진 없는 아티스트 enrich
        all_artist_ids: Set[str] = set(all_artists.keys())

        if all_artist_ids:
            rows = self.conn.execute(
                text("""
                    SELECT spotify_id FROM artists
                    WHERE spotify_id = ANY(:ids)
                      AND (photo_url IS NULL OR photo_url = '')
                """),
                dict(ids=list(all_artist_ids)),
            ).fetchall()

            missing_ids = [r[0] for r in rows]

            if missing_ids:
                for chunk in [missing_ids[i:i + 50] for i in range(0, len(missing_ids), 50)]:
                    detail_list = spotify.get_artists_batch(chunk)

                    enrich_data = []
                    for art in detail_list:
                        imgs = art.get("images") or []
                        photo = imgs[0].get("url") if imgs else None
                        followers = art.get("followers") or {}
                        if isinstance(followers, dict):
                            followers = followers.get("total")

                        enrich_data.append(dict(
                            sid=art["id"],
                            photo=photo,
                            genres=json.dumps(art.get("genres") or []),
                            followers=followers,
                            popularity=art.get("popularity"),
                        ))

                    if enrich_data:
                        self.conn.execute(
                            text("""
                                UPDATE artists SET
                                    photo_url  = :photo,
                                    genres     = CAST(:genres AS jsonb),
                                    followers  = :followers,
                                    popularity = :popularity
                                WHERE spotify_id = :sid
                            """),
                            enrich_data,
                        )
                        print(f"  [BULK] artists enriched: {len(enrich_data)}")


def generate_and_save_aliases(session_factory) -> None:
    """
    트랜잭션 밖에서 별도로 호출.
    aliases가 비어있는 아티스트에 대해 Gemini로 별칭 생성 후 저장.

    handler.py에서 메인 트랜잭션 COMMIT 이후에 호출한다.
    """
    api_key = getattr(settings, "GEMINI_API_KEY", None)
    if not api_key:
        print("  [ALIAS] GEMINI_API_KEY not set, skipping")
        return

    try:
        with session_factory() as session:
            conn = session.connection()

            rows = conn.execute(
                text("""
                    SELECT spotify_id, name, genres
                    FROM artists
                    WHERE aliases = '[]'::jsonb OR aliases IS NULL
                    LIMIT 20
                """)
            ).fetchall()

            if not rows:
                print("  [ALIAS] No artists need aliases")
                return

            print(f"  [ALIAS] Generating aliases for {len(rows)} artists")

            update_data = []
            for row in rows:
                sid, name, genres_json = row[0], row[1], row[2]

                genres = []
                if genres_json:
                    try:
                        genres = json.loads(genres_json) if isinstance(genres_json, str) else genres_json
                    except Exception:
                        genres = []

                aliases = generate_artist_aliases(name, genres)
                update_data.append(dict(
                    sid=sid,
                    aliases=json.dumps(aliases, ensure_ascii=False),
                ))

            if update_data:
                conn.execute(
                    text("""
                        UPDATE artists
                        SET aliases = CAST(:aliases AS jsonb)
                        WHERE spotify_id = :sid
                    """),
                    update_data,
                )
                session.commit()
                print(f"  [ALIAS] Updated aliases for {len(update_data)} artists")

    except Exception as e:
        print(f"  [ALIAS] Error: {e}")