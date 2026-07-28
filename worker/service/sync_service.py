# worker/service/sync_service.py
from __future__ import annotations
from typing import Dict, Any, List, NamedTuple, Optional, Set
import json
import logging
from sqlalchemy import bindparam, text
from sqlalchemy.exc import IntegrityError
from myblog_shared_db.genre_mapping import attachable_slugs
from worker.clients.spotify_client import spotify
from worker.clients.musicbrainz_client import fetch_artist_mbid_and_aliases
from worker.service.artist_enrich_service import enrich_artists

logger = logging.getLogger(__name__)

# Expanding IN (not `= ANY(:ids)`): identical semantics and plan on Postgres,
# but dialect-portable, so the transaction-boundary regression test can drive
# this path on a real engine instead of a mock connection.
_MISSING_PHOTO_SQL = text("""
    SELECT spotify_id FROM artists
    WHERE spotify_id IN :ids
      AND photo_url IS NULL
""").bindparams(bindparam("ids", expanding=True))

_ALBUM_GENRE_STRINGS_SQL = text("""
    SELECT al.spotify_id, ar.genres
    FROM albums al
    JOIN album_artists aa ON aa.album_id = al.id
    JOIN artists ar ON ar.id = aa.artist_id
    WHERE al.spotify_id IN :sids
""").bindparams(bindparam("sids", expanding=True))


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


class _Collected(NamedTuple):
    """Materialized rows for one batch — built with no DB session open."""
    all_artists: Dict[str, str]
    album_data: List[Dict]
    album_artist_pairs: List[Dict]
    track_data: List[Dict]
    track_artist_pairs: List[Dict]


class AlbumSyncService:
    """배치로 앨범/트랙/아티스트를 수집하여 upsert (bulk 최적화).

    Session contract (FIX-worker-txn-across-http): the service takes a
    ``session_factory`` — NOT an open connection — and owns its own short write
    transactions, so no DB transaction is ever held across a Spotify call.

    The batch runs as: Spotify fetch → build rows in memory → one short write
    transaction for the catalog → Spotify enrich loop (no transaction) → one
    short transaction for genre mapping. Handing this service an already-open
    transaction (the old ``AlbumSyncService(session.connection())``) parked a
    Neon connection idle-in-transaction — holding artists/albums/tracks row
    locks — for the whole enrich loop, which under a Spotify 403/404/410 fans
    out to up to 50 retried single GETs. Neon drops such a connection, the batch
    rolls back, SQS redelivers, and the 10-way concurrent invocations block on
    the held locks. Same reasoning and shape as ``run_artist_photo_backfill``.
    """

    def __init__(self, session_factory) -> None:
        self.session_factory = session_factory

    def sync_albums_batch(self, album_ids: List[str], market: Optional[str]) -> None:
        """메인 동기화. 서비스가 자신의 짧은 트랜잭션들을 직접 연다."""
        if not album_ids:
            return

        mkt = market or "KR"

        # 1) 앨범 배치 조회 — runs BEFORE any session is opened.
        albums: List[Dict[str, Any]] = spotify.get_albums(album_ids, market=mkt)

        # 2) 데이터 수집 (순수 계산, DB 접속 없음)
        collected = self._collect(albums)

        # 3) Bulk upsert + 사진 없는 아티스트 조회 — one short transaction.
        missing_ids = self._write_catalog(collected)

        # 4) 사진 없는 아티스트 enrich — Spotify loop with NO transaction held.
        if missing_ids:
            enriched = enrich_artists(self.session_factory, missing_ids)
            if enriched:
                logger.info("artists enriched: %d", enriched)

        # 5) S1 genre mapping — a second short transaction, after enrich.
        self._map_genres(collected.album_data)

    @staticmethod
    def _collect(albums: List[Dict[str, Any]]) -> _Collected:
        """Normalize the Spotify payload into bulk-upsert rows (no I/O)."""
        all_artists: Dict[str, str] = {}
        album_data: List[Dict] = []
        album_artist_pairs: List[Dict] = []
        track_data: List[Dict] = []
        track_artist_pairs: List[Dict] = []

        for alb in albums:
            # Spotify returns a null array element for any unknown/invalid id in
            # a batch GET /v1/albums?ids=. Skip it so one bad id can't poison the
            # whole SQS record (otherwise the entire batch fails → DLQ).
            if not alb:
                continue
            alb_sid = alb.get("id")
            if not alb_sid:
                continue

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
                # UPC rides the same GET /albums response (zero extra calls) —
                # the iTunes anchor key for genre labeling (FEAT-genre-system).
                # ISRC does NOT: album-nested tracks are SimplifiedTrackObject
                # without external_ids; track ISRCs come from the backfill
                # script's --keys mode instead (Step 3).
                upc=(alb.get("external_ids") or {}).get("upc"),
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

        return _Collected(
            all_artists=all_artists,
            album_data=album_data,
            album_artist_pairs=album_artist_pairs,
            track_data=track_data,
            track_artist_pairs=track_artist_pairs,
        )

    def _write_catalog(self, collected: _Collected) -> List[str]:
        """One short transaction: bulk-upsert the batch, then materialize the
        artists still missing a photo.

        Returns those IDs so the caller's Spotify enrich loop runs with this
        transaction already committed and its connection back in the pool.
        """
        all_artists = collected.all_artists
        album_data = collected.album_data
        album_artist_pairs = collected.album_artist_pairs
        track_data = collected.track_data
        track_artist_pairs = collected.track_artist_pairs

        with self.session_factory() as session:
            if all_artists:
                # Sort by spotify_id so concurrent invocations (e.g. a 분석 버킷 분류하기 burst
                # that fans out many album-sync messages) lock the SHARED artist rows in a
                # consistent order. Without this, two batches that share artists in different
                # insertion orders deadlock on the artists index (ON CONFLICT DO UPDATE takes
                # a row lock) → SQS retry livelock under the account's 10-way concurrency.
                artists_list = [dict(sid=sid, name=name) for sid, name in sorted(all_artists.items())]
                session.execute(
                    text("""
                        INSERT INTO artists (spotify_id, name)
                        VALUES (:sid, :name)
                        ON CONFLICT (spotify_id) DO UPDATE SET name = EXCLUDED.name
                    """),
                    artists_list,
                )
                logger.info("artists upserted: %d", len(artists_list))

            if album_data:
                # Sort by spotify_id (the ON CONFLICT key) so concurrent album-sync
                # invocations that share albums take the albums row locks in the SAME
                # order — same deadlock-avoidance rationale as the artists upsert above
                # (ON CONFLICT DO UPDATE takes a row lock; unsorted batches that overlap
                # deadlock under the account's 10-way SQS concurrency).
                album_data.sort(key=lambda a: a["sid"])
                session.execute(
                    text("""
                        INSERT INTO albums (
                            spotify_id, title, release_date, cover_url,
                            album_type, total_tracks, label, popularity, ext_refs
                        )
                        VALUES (
                            :sid, :title, :rdate, :cover,
                            :atype, :total_tracks, :label, :popularity,
                            jsonb_strip_nulls(jsonb_build_object(
                                'spotify_url', CAST(:url AS text),
                                'upc',         CAST(:upc AS text)
                            ))
                        )
                        ON CONFLICT (spotify_id) DO UPDATE
                           SET title        = EXCLUDED.title,
                               release_date = EXCLUDED.release_date,
                               cover_url    = EXCLUDED.cover_url,
                               album_type   = EXCLUDED.album_type,
                               total_tracks = EXCLUDED.total_tracks,
                               label        = EXCLUDED.label,
                               popularity   = EXCLUDED.popularity,
                               -- merge, don't replace: a response missing
                               -- external_ids must not clobber a backfilled upc,
                               -- and keys written by other writers must survive
                               ext_refs     = albums.ext_refs || EXCLUDED.ext_refs
                    """),
                    album_data,
                )
                logger.info("albums upserted: %d", len(album_data))

            if album_artist_pairs:
                # Sort by the pair key for the same reason the artists/albums/tracks
                # upserts sort: two concurrent batches that share an album or artist
                # must take these row locks in the SAME order (audit §9 C-9 — this
                # was the one bulk insert in this function that still went unsorted).
                album_artist_pairs.sort(key=lambda p: (p["alb_sid"], p["art_sid"]))
                session.execute(
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
                # Sort by spotify_id (the ON CONFLICT key) so overlapping concurrent
                # batches lock the shared tracks rows in a consistent order (deadlock
                # avoidance under 10-way SQS concurrency — see the artists/albums sorts).
                track_data.sort(key=lambda t: t["sid"])
                session.execute(
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
                # Dedupe, then sort on the pair key — same lock-ordering rationale as
                # album_artists above (audit §9 C-9).
                unique_pairs = list({(p["tid"], p["aid"]): p for p in track_artist_pairs}.values())
                unique_pairs.sort(key=lambda p: (p["tid"], p["aid"]))
                session.execute(
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

            # Materialize the artists still missing a photo BEFORE closing, so the
            # Spotify enrich loop runs with no transaction held.
            missing_ids: List[str] = []
            if all_artists:
                rows = session.execute(
                    _MISSING_PHOTO_SQL, dict(ids=sorted(all_artists.keys()))
                ).fetchall()
                missing_ids = [r[0] for r in rows]

            session.commit()

        return missing_ids

    def _map_genres(self, album_data: List[Dict]) -> None:
        """S1 genre mapping (FEAT-genre-system Step 2): deterministic
        artists.genres → tier-0 attach for the batch's albums.

        Its own short transaction, and it runs after the enrich step so
        first-seen artists already carry genre strings. K-Pop is
        arbitration-only — attachable_slugs strips it; the incremental
        enrichment pass (Step 3 poller) re-derives the flag from
        artists.genres, so it is not persisted here. ON CONFLICT DO NOTHING
        keeps re-syncs and backfill overlap idempotent.
        """
        if not album_data:
            return

        with self.session_factory() as session:
            rows = session.execute(
                _ALBUM_GENRE_STRINGS_SQL, dict(sids=[a["sid"] for a in album_data])
            ).fetchall()

            strings_by_album: Dict[str, Set[str]] = {}
            for alb_sid, genres in rows:
                strings_by_album.setdefault(alb_sid, set()).update(genres or [])

            genre_rows = []
            for alb_sid, strings in strings_by_album.items():
                slugs, _needs_arbitration = attachable_slugs(strings)
                genre_rows.extend(dict(alb_sid=alb_sid, slug=slug) for slug in slugs)

            if genre_rows:
                genre_rows.sort(key=lambda r: (r["alb_sid"], r["slug"]))
                session.execute(
                    text("""
                        INSERT INTO album_genres (album_id, genre_id, source, confidence)
                        SELECT al.id, g.id, 'mapping', 'low'
                        FROM albums al
                        JOIN genres g ON g.slug = :slug
                        WHERE al.spotify_id = :alb_sid
                        ON CONFLICT DO NOTHING
                    """),
                    genre_rows,
                )
                logger.info("album_genres mapped (S1): %d", len(genre_rows))

            session.commit()


def generate_and_save_aliases(session_factory) -> None:
    """Called by the EventBridge scheduled trigger after the SQS sync COMMIT.

    Fetches aliases from MusicBrainz for artists that have not yet been looked up
    (musicbrainz_id IS NULL). Writes musicbrainz_id + aliases in one UPDATE per
    artist, committed per row so a UNIQUE collision (BUG-17) on one MBID does not
    roll back the rest of the batch. If no MB match is found, musicbrainz_id is
    set to MBID_NOT_FOUND so the artist is skipped on the next scheduled run.
    """
    try:
        with session_factory() as session:
            # Use session.execute (not a cached conn) so each statement
            # acquires a live connection from the session — caching
            # session.connection() across session.commit() returns it to
            # the pool, leaving a stale handle (BUG-17 hotfix).
            rows = session.execute(
                text("""
                    SELECT spotify_id, name, genres
                    FROM artists
                    WHERE musicbrainz_id IS NULL
                    ORDER BY spotify_id
                    LIMIT 10
                """)
            ).fetchall()
            session.commit()

            if not rows:
                logger.debug("No artists pending MB lookup")
                return

            logger.info("Looking up %d artists on MusicBrainz", len(rows))

            update_stmt = text("""
                UPDATE artists
                SET musicbrainz_id = :mbid,
                    aliases        = CAST(:aliases AS jsonb)
                WHERE spotify_id = :sid
            """)

            # BUG-18 pre-check: reject MB candidates whose MBID already lives
            # in another artists row. Uses session.execute (not a cached conn)
            # so each SELECT acquires a live connection like the surrounding
            # UPDATEs (BUG-17 lesson). UNIQUE (BUG-13) + IntegrityError catch
            # remain the safety net; this is best-effort eviction.
            def _is_mbid_taken(mbid: str) -> bool:
                row = session.execute(
                    text("SELECT 1 FROM artists WHERE musicbrainz_id = :mbid LIMIT 1"),
                    {"mbid": mbid},
                ).first()
                return row is not None

            succeeded = 0
            skipped_collision = 0
            skipped_precheck = 0
            for row in rows:
                sid, name, genres = row[0], row[1], row[2]
                mbid, aliases = fetch_artist_mbid_and_aliases(
                    name,
                    spotify_genres=genres or [],
                    is_mbid_taken=_is_mbid_taken,
                )
                # MBID_NOT_FOUND result (no MB hit, or all candidates evicted
                # by pre-check) still gets written so the partial UNIQUE
                # (BUG-13) lets the row leave the IS NULL pool on the next
                # cycle (BUG-18 §Goal — NULL→NOT NULL eviction). The counter
                # bucket is "sentinel"; we don't distinguish "no hit at all"
                # from "all pre-check rejected" here because both have the
                # same operational effect.
                try:
                    session.execute(
                        update_stmt,
                        dict(
                            sid=sid,
                            mbid=mbid,
                            aliases=json.dumps(aliases, ensure_ascii=False),
                        ),
                    )
                    session.commit()
                    if mbid == "not_found":
                        skipped_precheck += 1
                    else:
                        succeeded += 1
                except IntegrityError as exc:
                    session.rollback()
                    logger.warning(
                        "alias_fill: skipped sid=%s name=%s mbid=%s — UNIQUE collision: %s",
                        sid, name, mbid, exc.orig,
                    )
                    skipped_collision += 1

            logger.info(
                "MB lookup done: %d ok, %d sentinel (no MB match or pre-check evicted), "
                "%d skipped (UNIQUE collision), of %d looked up",
                succeeded, skipped_precheck, skipped_collision, len(rows),
            )

    except Exception as e:
        # Re-raise so the EventBridge invocation is marked failed and the Lambda
        # Errors alarm fires. Per-artist MB misses are already absorbed inside
        # fetch_artist_mbid_and_aliases (→ MBID_NOT_FOUND) and per-row UNIQUE
        # collisions are caught above, so this only triggers on catastrophic
        # failures (DB down, session_factory error) — which must be visible, not
        # swallowed into a silent "success". This is the alias entry point only;
        # SQS album sync is a separate invocation, so surfacing here cannot block
        # album sync (service-boundary rule holds).
        logger.error("Alias update failed: %s", e, exc_info=True)
        raise
