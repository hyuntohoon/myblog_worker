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


def _run_follow_import(user_id: Any, rerun: bool = False) -> None:
    """Owner followed-artists snapshot import (FEAT-for-you-releases Step 2).
    Triggered by the backend's owner-gated POST /api/me/tracked-artists/spotify-import
    enqueue ({"job": "spotify_follow_import", "user_id": ...}) plus one self-chained
    delayed rerun (rule #9: the endpoint only enqueues; the worker reads Spotify)."""
    from worker.clients.spotify_user_client import spotify_user
    from worker.clients.sqs_producer import (
        enqueue_follow_import_rerun,
        enqueue_follow_ingest,
    )
    from worker.service.follow_import_service import run_follow_import

    run_follow_import(
        SessionLocal,
        spotify_user,
        enqueue_ingest=enqueue_follow_ingest,
        enqueue_rerun=enqueue_follow_import_rerun,
        user_id=user_id,
        rerun=rerun,
    )


def _run_follow_ingest(artist_sids: List[str]) -> None:
    """Expand uncatalogued followed artists onto the album-sync SQS pipeline
    (FEAT-for-you-releases Step 2, OQ1 = catalog-ingest). Fan-out messages are
    produced by _run_follow_import; artists are created downstream by album sync."""
    from worker.clients.sqs_producer import enqueue_album_sync
    from worker.service.follow_import_service import run_follow_ingest

    run_follow_ingest(spotify, enqueue_album_sync, artist_sids)


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

    # Pass the factory, not an open session: AlbumSyncService opens its own short
    # write transactions around the Spotify calls (FIX-worker-txn-across-http).
    # Handing it `session.connection()` inside `session.begin()` held artists/
    # albums/tracks row locks across the enrich loop's outbound HTTP.
    svc = AlbumSyncService(SessionLocal)
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

    # Factory, not an open session — see _process_single.
    svc = AlbumSyncService(SessionLocal)
    svc.sync_albums_batch(album_ids, market)
    logger.info("Batch synced to DB: %d albums", len(album_ids))


def _run_genius_fetch(limit: int | None = None, album_id: str | None = None) -> None:
    """Bounded Genius annotation fetch (FEAT-lyrics-annotations Thread 1).

    Passes ``SessionLocal`` itself, not a session: the service opens the read,
    closes it BEFORE the first HTTP call, then opens one short write transaction
    per track. Handing it an open session would put a Neon connection
    idle-in-transaction across ~3 API round trips per track — the failure this
    codebase has already hit (ProtocolViolation).

    ``limit`` defaults to ``settings.GENIUS_FETCH_BATCH_LIMIT``. With ``album_id``
    the pass is scoped to one album — the research poller's readiness nudge.
    Unset token ⇒ the service no-ops and says so; it is never a boot failure.
    """
    from worker.clients.genius_client import genius
    from worker.service.genius_fetch_service import run_genius_fetch

    metrics = run_genius_fetch(SessionLocal, genius, limit=limit, album_id=album_id)
    # WARNING, not INFO — prod Lambdas run LOG_LEVEL=WARNING, so an INFO line never
    # reaches CloudWatch and a scheduled run would be unobservable.
    logger.warning("Genius fetch metrics: %s", metrics)


def _run_isrc_backfill(limit: int | None = None) -> None:
    """Bounded ISRC backfill for FEAT-lyrics-corpus Step 1b. Fetches up to `limit`
    tracks lacking ISRC from the DB, enriches from Spotify GET /v1/tracks, writes the
    ISRC to the column or a miss marker to ``ext_refs.isrc_status``. Follows alias-fill
    failure-isolation pattern (one batch failure doesn't block the job). ``limit``
    defaults to ``settings.ISRC_BACKFILL_BATCH_LIMIT``."""
    from worker.service.isrc_backfill_service import IsrcBackfillService

    # No handler-owned session.begin(): the service commits per batch via the
    # session (and rolls back a failed batch), following the alias-fill /
    # lyrics-incremental pattern. Wrapping this in session.begin() would
    # deassociate the transaction the moment the service commits.
    with SessionLocal() as session:
        svc = IsrcBackfillService(session)
        metrics = svc.backfill_isrc(limit=limit)
        # WARNING, not INFO: prod Lambdas run LOG_LEVEL=WARNING, so an INFO line never
        # reaches CloudWatch and a scheduled run would be unobservable (album_ingest
        # logs its counters at WARNING for the same reason).
        logger.warning("ISRC backfill metrics: %s", metrics)


def _run_disc_no_backfill(limit: int | None = None) -> None:
    """One-off `disc_no` backfill for DATA-multidisc-track-order Step 2b. Re-fetches
    each currently-colliding album from Spotify and sets `tracks.disc_no`, matched by
    spotify_id. Bounded to the ~78-album collision population — not a recurring job;
    every future sync captures `disc_no` naturally via Step 2a's `AlbumSyncService`
    change. Same failure-isolation shape as `_run_isrc_backfill`."""
    from worker.service.disc_no_backfill_service import DiscNoBackfillService

    with SessionLocal() as session:
        svc = DiscNoBackfillService(session)
        metrics = svc.backfill_disc_no(limit=limit)
        # WARNING, not INFO: prod Lambdas run LOG_LEVEL=WARNING.
        logger.warning("disc_no backfill metrics: %s", metrics)


def _run_artist_photo_backfill(limit: int | None = None) -> None:
    """One-shot backlog + weekly EventBridge sweep (BUG-artist-image-backfill).
    EventBridge/SQS triggered and failure-isolated so it must not block album sync.
    """
    from worker.service.artist_enrich_service import run_artist_photo_backfill

    metrics = run_artist_photo_backfill(SessionLocal, limit=limit)
    logger.info("Artist photo backfill metrics: %s", metrics)


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


def _run_lyrics_reassessment(
    limit: int | None = None,
    album_id: str | None = None,
    cooldown_sec: float | None = None,
) -> None:
    """Periodic reassessment of unresolved lyrics rows (FEAT-lyrics-corpus Step 4). Re-checks
    not_found / ambiguous / review_required tracks (stalest first) against current LRCLIB
    coverage with the Step 2 canonical matcher: promotes on new evidence, refreshes otherwise,
    and NEVER overwrites a good match (replacement guard). Separate invocation from album sync;
    bounded to the 120s Lambda (shared eval loop).

    With "album_id" the same evaluation runs album-scoped and out of turn (DATA-catalog-noise
    Step 4 expedite) — same matcher, same guard, but the label-yield exclusion is bypassed and
    a cooldown makes an SQS redelivery cheap."""
    from worker.service.lyrics_reassessment_service import LyricsReassessmentService

    with SessionLocal() as session:
        svc = LyricsReassessmentService(session)
        if album_id:
            metrics = svc.reassess_album(album_id, limit=limit, cooldown_sec=cooldown_sec)
            logger.info("Lyrics expedite metrics: %s", metrics)
            return
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


def _run_youtube_ref_refresh(limit: int | None = None) -> None:
    """III.E.4 retention sweep + re-verification for stored YouTube mappings.

    A COMPLIANCE MECHANISM, not a cache warmer. Two passes, expire first, so a
    quota failure in the refresh can never postpone a deletion the policy
    requires. Fails LOUDLY when the key is unconfigured rather than no-opping
    quietly: a retention sweep that silently does nothing is the failure mode
    the policy exists to prevent, and it would look identical to a healthy run.
    """
    from worker.clients.youtube_client import YouTubeNotConfigured, youtube
    from worker.service.youtube_ref_refresh_service import run_youtube_ref_refresh

    try:
        run_youtube_ref_refresh(
            SessionLocal,
            youtube,
            limit=limit if limit is not None else settings.YOUTUBE_REFRESH_BATCH_LIMIT,
            retention_days=settings.YOUTUBE_RETENTION_DAYS,
        )
    except YouTubeNotConfigured:
        logger.error(
            "youtube_ref_refresh: YOUTUBE_API_KEY is unset — the retention sweep did "
            "NOT run. Stored mappings will age past the III.E.4 30-day ceiling until "
            "this is fixed."
        )
        raise


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    # EventBridge daily cron — YouTube mapping retention sweep + re-verification
    # (FEAT-youtube-playback-provider Step A5). Constant input
    # {"job": "youtube_ref_refresh"}, routed with the other job-keyed branches.
    if event.get("job") == "youtube_ref_refresh":
        limit = event.get("limit")  # None ⇒ settings.YOUTUBE_REFRESH_BATCH_LIMIT
        logger.info("EventBridge trigger: running YouTube ref refresh (limit=%s)", limit)
        _run_youtube_ref_refresh(limit=limit)
        return {}

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
    # Bounded backfill: fetch tracks without an ISRC and without a prior attempt, enrich
    # from Spotify, write the ISRC to tracks.isrc or a miss marker to ext_refs.isrc_status.
    if event.get("job") == "isrc_backfill":
        limit = event.get("limit")  # None ⇒ settings.ISRC_BACKFILL_BATCH_LIMIT
        logger.info("EventBridge/SQS trigger: running ISRC backfill (limit=%s)", limit)
        _run_isrc_backfill(limit=limit)
        return {}

    # Manual-invoke only — DATA-multidisc-track-order Step 2b one-off backfill.
    # No EventBridge rule: the ~78-album collision population doesn't recur (every
    # future sync captures disc_no via Step 2a), so this is triggered once by hand
    # via `aws lambda invoke` and never scheduled.
    if event.get("job") == "disc_no_backfill":
        limit = event.get("limit")  # None ⇒ all colliding albums
        logger.info("Manual trigger: running disc_no backfill (limit=%s)", limit)
        _run_disc_no_backfill(limit=limit)
        return {}

    # EventBridge/SQS trigger — Genius annotation fetch (FEAT-lyrics-annotations).
    # Bounded: tracks of research-requested albums with no Genius row yet
    # (lyrics status is deliberately not a condition). "album_id" scopes the pass
    # to one album — the research poller's readiness nudge. Writes the songs
    # row and its annotations in ONE transaction — the read path gates the
    # annotation query on the parent row, so the reverse order would make the
    # annotations invisible rather than merely late.
    if event.get("job") == "genius_fetch":
        limit = event.get("limit")  # None ⇒ settings.GENIUS_FETCH_BATCH_LIMIT
        album_id = event.get("album_id")
        logger.info(
            "EventBridge/SQS trigger: running Genius fetch (limit=%s, album_id=%s)",
            limit, album_id,
        )
        _run_genius_fetch(limit=limit, album_id=album_id)
        return {}

    # EventBridge/SQS trigger — one-shot backlog + weekly artist-photo sweep.
    if event.get("job") == "artist_photo_backfill":
        limit = event.get("limit")
        logger.info("EventBridge/SQS trigger: running artist photo backfill (limit=%s)", limit)
        _run_artist_photo_backfill(limit=limit)
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
    # An optional "album_id" switches the same job to the album-scoped expedite (Step 4 of
    # DATA-catalog-noise); "cooldown_sec": 0 forces a re-run inside the cooldown window.
    if event.get("job") == "lyrics_reassessment":
        limit = event.get("limit")
        album_id = event.get("album_id")
        logger.info(
            "EventBridge/SQS trigger: running lyrics reassessment (limit=%s, album_id=%s)",
            limit, album_id,
        )
        _run_lyrics_reassessment(
            limit=limit, album_id=album_id, cooldown_sec=event.get("cooldown_sec")
        )
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

            # Owner followed-artists snapshot import (FEAT-for-you-releases Step 2).
            # Enqueued by the backend POST /api/me/tracked-artists/spotify-import
            # (owner-gated; rule #9: the endpoint only enqueues) — plus one
            # self-chained delayed rerun (rerun=true never fans out again).
            if body.get("job") == "spotify_follow_import":
                _run_follow_import(body.get("user_id"), rerun=bool(body.get("rerun")))
                continue

            # Follow-import fan-out: catalog-ingest a chunk of followed artists
            # missing from the catalog (albums ride the normal album-sync path).
            if body.get("job") == "spotify_follow_ingest":
                _run_follow_ingest(body.get("artist_sids") or [])
                continue

            # Lyrics jobs via SQS. The EventBridge constant-input path hits the
            # event["job"] checks at the top of the handler; an SQS-delivered message
            # arrives wrapped in Records, so it must ALSO be routed here (the
            # eventbridge.tf comments promise the manual blogSQS path, and the
            # near-real-time chain below relies on it).
            if body.get("job") == "lyrics_incremental":
                _run_lyrics_incremental(limit=body.get("limit"))
                continue

            if body.get("job") == "artist_photo_backfill":
                _run_artist_photo_backfill(limit=body.get("limit"))
                continue

            if body.get("job") == "lyrics_reassessment":
                # "album_id" present ⇒ album-scoped expedite. This is the fire path the RFC
                # documents (one `aws sqs send-message`), so it must accept the same keys
                # as the EventBridge branch above.
                _run_lyrics_reassessment(
                    limit=body.get("limit"),
                    album_id=body.get("album_id"),
                    cooldown_sec=body.get("cooldown_sec"),
                )
                continue

            # Genius fetch via SQS — the research poller's readiness nudge sends
            # {"job":"genius_fetch","album_id":...} here, NOT through EventBridge,
            # so this Records-loop branch is the nudge's only route. Without it the
            # message hits "Unknown message format" and is ACKed away silently.
            if body.get("job") == "genius_fetch":
                _run_genius_fetch(limit=body.get("limit"), album_id=body.get("album_id"))
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
