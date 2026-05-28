# worker/service/sync_service.py
from __future__ import annotations
from typing import Dict, Any, List, Optional, Set
import json
import logging
from sqlalchemy import text
from sqlalchemy.engine import Connection
from worker.clients.spotify_client import spotify
from worker.clients.musicbrainz_client import fetch_artist_mbid_and_aliases

logger = logging.getLogger(__name__)


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
            logger.info("artists upserted: %d", len(artists_list))

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
                           popularity   = EXCLUDED.popularity,
                           ext_refs     = EXCLUDED.ext_refs
                """),
                album_data,
            )
            logger.info("albums upserted: %d", len(album_data))

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
            logger.info("album_artists linked: %d", len(album_artist_pairs))

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
            logger.info("tracks upserted: %d", len(track_data))

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
            logger.info("track_artists linked: %d", len(unique_pairs))

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
                        logger.info("artists enriched: %d", len(enrich_data))


def generate_and_save_aliases(session_factory) -> None:
    """Called by the EventBridge scheduled trigger after the SQS sync COMMIT.

    Fetches aliases from MusicBrainz for artists that have not yet been looked up
    (musicbrainz_id IS NULL).  Writes musicbrainz_id + aliases in one UPDATE per
    artist.  If no MB match is found, musicbrainz_id is set to MBID_NOT_FOUND so
    the artist is skipped on the next scheduled run.
    """
    try:
        with session_factory() as session:
            conn = session.connection()

            rows = conn.execute(
                text("""
                    SELECT spotify_id, name, genres
                    FROM artists
                    WHERE musicbrainz_id IS NULL
                    LIMIT 10
                """)
            ).fetchall()

            if not rows:
                logger.debug("No artists pending MB lookup")
                return

            logger.info("Looking up %d artists on MusicBrainz", len(rows))

            update_data = []
            for row in rows:
                sid, name, genres = row[0], row[1], row[2]
                mbid, aliases = fetch_artist_mbid_and_aliases(
                    name, spotify_genres=genres or []
                )
                update_data.append(dict(
                    sid=sid,
                    mbid=mbid,
                    aliases=json.dumps(aliases, ensure_ascii=False),
                ))

            conn.execute(
                text("""
                    UPDATE artists
                    SET musicbrainz_id = :mbid,
                        aliases        = CAST(:aliases AS jsonb)
                    WHERE spotify_id = :sid
                """),
                update_data,
            )
            session.commit()
            logger.info("MB lookup done for %d artists", len(update_data))

    except Exception as e:
        logger.error("Alias update failed: %s", e, exc_info=True)
