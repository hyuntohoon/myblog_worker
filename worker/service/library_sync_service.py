# FEAT-spotify-library-sync Step 2 — Spotify saved-albums two-way reconcile.
#
# Reconciles the owner's Spotify saved-albums Library (GET/PUT/DELETE /me/albums,
# album granularity) with the SPECIAL review_buckets.kind='spotify_library' bucket.
# Triggered by the {"job": "spotify_library_sync"} SQS message (worker/handler.py),
# which the backend enqueues. Never called from a user-facing endpoint (hard rule #9).
#
# Raw text() SQL (mirrors listening_sync_service / generate_and_save_aliases) so the
# worker needs no ORM pin bump for the new tables — only the V15 migration must be
# applied first.
#
# Algorithm (RFC). B = album_ids that are items in the special bucket. L = Spotify
# saved set. The get-or-create of the special bucket is the BACKEND's job: if the
# worker finds no spotify_library bucket it creates none and no-ops.
#
#   1. Read L (get_saved_albums). Token invalid_grant / needs_reauth → abort, mark
#      pending rows needs_attention, return {"needs_reauth": True}.
#   2. Map L spotify_ids → albums.id; unknown → enqueue_unknown (reuse the
#      candidates→SQS album-sync pipeline) + skip this pass.
#   3. First-touch source stamp: for each B-album lacking a spotify_library_albums
#      row, contains-check → source='preexisting' if already saved else 'myblog_added';
#      upsert the side row.
#   4. Diffs (catalog-known only):
#        ADD    = B not in_spotify             → save_albums   (gated on writes_enabled)
#        REMOVE = in_spotify & not in_bucket
#                 & source='myblog_added'      → remove_albums (gated; NEVER preexisting)
#        PULL   = L not in_bucket (not myblog) → insert bucket item + upsert side row
#                                                (source='preexisting', synced)
#   5. Per-album state: 'synced' on success / plan-only, 'failed' on a per-chunk write
#      error (+ last_error), 'needs_attention' on scope/reauth. A per-album failure
#      must NOT abort the pass.
#   6. Stamp last_synced_at=now() on touched rows. Return a counts summary.
#
# Idempotent: a re-run where bucket intent already equals the Library is a no-op
# (no save/remove issued, no PULL inserted). PLAN-ONLY (writes_enabled=False): all DB
# writes + PULLs + state stamps still happen and the intended PUT/DELETE sets are
# LOGGED, but no real /me/albums mutation is issued.
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

from sqlalchemy.sql import text

from worker.clients.spotify_user_client import SpotifyScopeError

logger = logging.getLogger(__name__)

# FEAT-spotify-library-autosync — run bookkeeping (V61 spotify_library_sync_runs).
# A delivery is skipped when the run row says the request was already serviced within
# this window (SQS at-least-once / a duplicate POST that slipped through). Mirrors the
# backend's LIBRARY_SYNC_DEBOUNCE_SECONDS (myblog_backend bucket_service).
SYNC_DEBOUNCE_SECONDS = 30
# A started pass that never finished (Lambda timeout / crash) stops blocking a new
# claim after this long, measured from started_at. Tied to the worker Lambda timeout
# (infra/lambda.tf, 120 s): longer, so a live pass is never overlapped; shorter than
# the backend's 300 s stale-request window (measured from requested_at), so the
# backend's re-request after a crash is always claimable when it arrives.
STALE_RUN_SECONDS = 150
# Passes per message. A request that lands WHILE a pass runs is refused by the claim
# (the pass is running) and its message acked — so the running pass checks on finish
# and goes once more. One extra is enough: anything later is a new message.
MAX_PASSES = 2
# Prompt version for research rows this worker enqueues. Third copy of the constant
# (myblog_backend research_service.PROMPT_VERSION, scripts/research_poller.py). It can
# never create a SECOND note: the enqueue skips any album that already has a research
# row of ANY version, so a drift here only labels a brand-new row.
RESEARCH_PROMPT_VERSION = "v2"


def _is_needs_reauth(exc: Exception) -> bool:
    """The token refresh raises a RuntimeError carrying 'invalid_grant' when the
    stored refresh token was revoked/expired (re-auth needed). Distinguish it from a
    transient read failure by message so the reconcile can abort cleanly + flag the
    rows rather than marking individual albums 'failed'."""
    return "invalid_grant" in str(exc)


def _find_special_bucket_id(session) -> Optional[Any]:
    row = session.execute(
        text("SELECT id FROM review_buckets WHERE kind = 'spotify_library' LIMIT 1")
    ).first()
    return row.id if row else None


def _bucket_album_ids(session, bucket_id) -> List[Any]:
    """B = album_ids of the items currently in the special bucket."""
    rows = session.execute(
        text("SELECT album_id FROM review_bucket_items WHERE bucket_id = :bid"),
        {"bid": bucket_id},
    ).fetchall()
    return [r.album_id for r in rows]


def _upsert_side_row(
    session,
    *,
    album_id,
    spotify_id: str,
    source: Optional[str] = None,
    state: Optional[str] = None,
    in_bucket: Optional[bool] = None,
    in_spotify: Optional[bool] = None,
    last_error: Optional[str] = None,
    stamp_synced: bool = False,
) -> None:
    """Upsert one spotify_library_albums row. `source` is IMMUTABLE once stamped, so
    ON CONFLICT NEVER overwrites it (req 5 — the 'preexisting' fact must outlive a
    bucket-remove). On insert `source` is required; on a later touch pass source=None
    and only the mutable fields update. COALESCE keeps unspecified fields unchanged;
    last_error is always written (NULL clears a stale error on a now-clean sync)."""
    session.execute(
        text(
            """
            INSERT INTO spotify_library_albums
                (album_id, spotify_id, source, state, in_bucket, in_spotify,
                 last_error, last_synced_at, created_at, updated_at)
            VALUES
                (:album_id, :spotify_id,
                 CAST(COALESCE(:source, 'myblog_added') AS spotify_library_source),
                 CAST(COALESCE(:state, 'pending') AS spotify_library_state),
                 COALESCE(:in_bucket, TRUE), COALESCE(:in_spotify, FALSE),
                 :last_error,
                 CASE WHEN :stamp_synced THEN now() ELSE NULL END,
                 now(), now())
            ON CONFLICT (album_id) DO UPDATE SET
                spotify_id     = EXCLUDED.spotify_id,
                -- source is immutable: keep the originally-stamped provenance
                state          = COALESCE(EXCLUDED.state, spotify_library_albums.state),
                in_bucket      = COALESCE(:in_bucket, spotify_library_albums.in_bucket),
                in_spotify     = COALESCE(:in_spotify, spotify_library_albums.in_spotify),
                last_error     = :last_error,
                last_synced_at = CASE WHEN :stamp_synced THEN now()
                                      ELSE spotify_library_albums.last_synced_at END,
                updated_at     = now()
            """
        ),
        {
            "album_id": album_id,
            "spotify_id": spotify_id,
            "source": source,
            "state": state,
            "in_bucket": in_bucket,
            "in_spotify": in_spotify,
            "last_error": last_error,
            "stamp_synced": stamp_synced,
        },
    )


def _set_state(session, album_id, state: str, last_error: Optional[str] = None) -> None:
    """Stamp an existing side row's terminal state (+ last_synced_at) after the write
    decision is known (synced/failed/needs_attention). Leaves source/in_* intact."""
    session.execute(
        text(
            """
            UPDATE spotify_library_albums
               SET state          = CAST(:state AS spotify_library_state),
                   last_error     = :last_error,
                   last_synced_at = now(),
                   updated_at     = now()
             WHERE album_id = :album_id
            """
        ),
        {"album_id": album_id, "state": state, "last_error": last_error},
    )


def _next_bucket_position(session, bucket_id) -> int:
    row = session.execute(
        text(
            "SELECT COALESCE(MAX(position), -1) + 1 AS pos "
            "FROM review_bucket_items WHERE bucket_id = :bid"
        ),
        {"bid": bucket_id},
    ).first()
    return int(row.pos) if row and row.pos is not None else 0


def _insert_bucket_item(session, bucket_id, album_id, position: int) -> None:
    """Append an album to the special bucket. Idempotent via a NOT-EXISTS guard scoped to
    item_type='album' (a re-PULL is a no-op).

    Schema-agnostic on purpose: FEAT-pocket-buckit V30 replaces the table-wide
    UNIQUE(bucket_id, album_id) with a PARTIAL album index (WHERE item_type='album'). A bare
    ``ON CONFLICT (bucket_id, album_id)`` cannot infer a partial index and raises "no unique
    or exclusion constraint matching the ON CONFLICT specification", so we dedup with an
    explicit guard instead. This guard works BOTH before and after V30; the album partial
    unique (post-V30) backstops any concurrent-writer race. The inserted row's item_type
    defaults to 'album' (the column default), matching the guard predicate."""
    session.execute(
        text(
            """
            INSERT INTO review_bucket_items (bucket_id, album_id, position, status)
            SELECT :bid, :album_id, :position, 'candidate'
            WHERE NOT EXISTS (
                SELECT 1 FROM review_bucket_items
                 WHERE bucket_id = :bid AND item_type = 'album' AND album_id = :album_id
            )
            """
        ),
        {"bid": bucket_id, "album_id": album_id, "position": position},
    )


def _mark_pending_needs_attention(session_factory) -> None:
    """Token re-auth abort: flip any still-pending rows to needs_attention so the
    /profile banner reflects that the last sync couldn't run. Best-effort."""
    try:
        with session_factory() as session, session.begin():
            session.execute(
                text(
                    """
                    UPDATE spotify_library_albums
                       SET state          = 'needs_attention',
                           last_synced_at = now(),
                           updated_at     = now()
                     WHERE state = 'pending'
                    """
                )
            )
    except Exception as e:  # pragma: no cover - best-effort
        logger.warning("failed to mark pending rows needs_attention on reauth: %s", e)


def _reconcile(
    session_factory,
    spotify_user,
    enqueue_unknown: Optional[Callable[[List[str]], None]] = None,
    writes_enabled: bool = False,
) -> Dict[str, Any]:
    """Reconcile the Spotify saved-albums Library with the special bucket. See the
    module docstring for the algorithm. `writes_enabled` gates ONLY the real
    PUT/DELETE /me/albums calls (the worker reads this from its OWN settings, never
    from the SQS message). Mirrors the raw-SQL + per-item try/except isolation of
    listening_sync_service.

    Returns a summary dict. Special cases:
      - no special bucket               → {"bucket": "absent"}
      - token revoked (invalid_grant)   → {"needs_reauth": True}
    """
    # 1. Read L (Spotify saved set). A revoked token or missing scope aborts the pass.
    try:
        saved_albums = spotify_user.get_saved_albums()
    except SpotifyScopeError as e:
        logger.warning("Spotify library read missing scope: %s", e)
        _mark_pending_needs_attention(session_factory)
        return {"needs_reauth": True, "reason": "missing_scope"}
    except Exception as e:
        if _is_needs_reauth(e):
            logger.warning("Spotify library sync aborted — token re-auth required: %s", e)
            _mark_pending_needs_attention(session_factory)
            return {"needs_reauth": True}
        raise

    # L = saved spotify_ids (de-duped, first-seen order for stable PULL positions).
    saved_ids: List[str] = []
    seen: set = set()
    added_at_by_sid: Dict[str, str] = {}
    for alb in saved_albums:
        sid = alb.get("id")
        if sid and sid not in seen:
            seen.add(sid)
            saved_ids.append(sid)
            if alb.get("_added_at"):
                added_at_by_sid[sid] = alb["_added_at"]

    summary: Dict[str, Any] = {
        "added": 0,
        "removed": 0,
        "pulled": 0,
        "failed": 0,
        "skipped_unknown": 0,
        "writes_enabled": writes_enabled,
        "needs_reauth": False,
        "research_queued": 0,
    }
    unknown_ids: List[str] = []

    # ── Phase 1: READ ──────────────────────────────────────────────────────────
    # Materialize all catalog state in a short session, then CLOSE before any slow
    # Spotify call (check_saved_albums / save_albums / remove_albums). Neon's pooler
    # drops a connection left idle-in-transaction across an external call
    # (reference-db-session-across-long-external-loop).
    with session_factory() as session:
        bucket_id = _find_special_bucket_id(session)
        if bucket_id is None:
            # Get-or-create is the backend's job; nothing for the worker to do.
            logger.info("no kind='spotify_library' bucket — nothing to reconcile")
            return {"bucket": "absent"}

        # 2. Map L spotify_ids → albums.id; unknown → enqueue + skip this pass.
        sid_to_uuid: Dict[str, Any] = {}
        if saved_ids:
            rows = session.execute(
                text("SELECT id, spotify_id FROM albums WHERE spotify_id = ANY(:sids)"),
                {"sids": saved_ids},
            ).fetchall()
            sid_to_uuid = {r.spotify_id: r.id for r in rows}
        unknown_ids = [s for s in saved_ids if s not in sid_to_uuid]

        # B = catalog-known album uuids in the special bucket, with their sids.
        bucket_uuids = _bucket_album_ids(session, bucket_id)
        bucket_sid: Dict[Any, str] = {}
        if bucket_uuids:
            brows = session.execute(
                text("SELECT id, spotify_id FROM albums WHERE id = ANY(:ids)"),
                {"ids": list(bucket_uuids)},
            ).fetchall()
            bucket_sid = {r.id: r.spotify_id for r in brows if r.spotify_id}
        bucket_set = set(bucket_sid.keys())

        # Existing provenance (source is immutable once stamped).
        existing_source: Dict[Any, str] = {}
        for r in session.execute(
            text("SELECT album_id, source FROM spotify_library_albums")
        ).fetchall():
            existing_source[r.album_id] = r.source

    # Bucket albums lacking a side row need a first-touch source stamp
    # (contains-check → 'preexisting' if already saved else 'myblog_added').
    untouched = [aid for aid in bucket_set if aid not in existing_source]
    untouched_sids = [bucket_sid[aid] for aid in untouched]

    # ── Phase 2: EXTERNAL Spotify I/O (no DB session held open) ──────────────────
    # 3a. Contains-check for untouched bucket albums.
    contains: Dict[str, bool] = {}
    if untouched:
        try:
            contains = spotify_user.check_saved_albums(untouched_sids)
        except SpotifyScopeError as e:
            logger.warning("contains-check missing scope: %s", e)
            # Stamp the untouched rows needs_attention in a fresh short tx, then abort.
            with session_factory() as session, session.begin():
                for aid in untouched:
                    _upsert_side_row(
                        session, album_id=aid, spotify_id=bucket_sid[aid],
                        source="myblog_added", state="needs_attention",
                        in_bucket=True, last_error="missing user-library-* scope",
                        stamp_synced=True,
                    )
            summary["needs_reauth"] = True
            summary["failed"] += len(untouched)
            return summary

    # First-touch provenance (kept in-memory now; persisted in Phase 3). Updating
    # existing_source here is what the REMOVE/PULL filters below key off, exactly as
    # in the original single-transaction flow.
    first_touch: Dict[Any, tuple] = {}  # aid -> (source, is_saved)
    for aid in untouched:
        sid = bucket_sid[aid]
        is_saved = bool(contains.get(sid, False)) or sid in seen
        src = "preexisting" if is_saved else "myblog_added"
        first_touch[aid] = (src, is_saved)
        existing_source[aid] = src

    # Authoritative current Library membership for each bucket album, from L.
    in_spotify_now: Dict[Any, bool] = {aid: bucket_sid[aid] in seen for aid in bucket_set}

    # Compute the ADD / REMOVE / PULL sets (pure Python — no DB, no I/O).
    add_aids = [aid for aid in bucket_set if not in_spotify_now[aid]]
    add_sids = [bucket_sid[aid] for aid in add_aids]

    remove_pairs = [
        (sid_to_uuid[sid], sid)
        for sid in saved_ids
        if sid in sid_to_uuid
        and sid_to_uuid[sid] not in bucket_set
        and existing_source.get(sid_to_uuid[sid]) == "myblog_added"
    ]
    remove_sids = [sid for _, sid in remove_pairs]

    pull_pairs = [
        (sid_to_uuid[sid], sid)
        for sid in saved_ids
        if sid in sid_to_uuid
        and sid_to_uuid[sid] not in bucket_set
        and existing_source.get(sid_to_uuid[sid]) != "myblog_added"
    ]

    # 3b. Gated Spotify mutations (PUT/DELETE /me/albums). Plan-only just logs.
    add_err = add_scope = None
    if add_sids:
        _ok, add_err, add_scope = _maybe_write(
            writes_enabled, lambda: spotify_user.save_albums(add_sids),
            op="PUT /me/albums (ADD)", ids=add_sids,
        )
    remove_err = remove_scope = None
    if remove_sids:
        _ok, remove_err, remove_scope = _maybe_write(
            writes_enabled, lambda: spotify_user.remove_albums(remove_sids),
            op="DELETE /me/albums (REMOVE)", ids=remove_sids,
        )

    # ── Phase 3: WRITE ──────────────────────────────────────────────────────────
    # Fresh short transaction for every DB mutation (first-touch stamp + ADD/REMOVE
    # state + PULL inserts). Atomic as a unit, exactly like the original.
    with session_factory() as session:
        with session.begin():
            # 3. First-touch source stamp.
            for aid in untouched:
                src, is_saved = first_touch[aid]
                _upsert_side_row(
                    session, album_id=aid, spotify_id=bucket_sid[aid], source=src,
                    state="pending", in_bucket=True, in_spotify=is_saved,
                )

            # 4a. ADD = bucket albums NOT currently saved in Spotify.
            if add_sids:
                if add_scope:
                    for aid in add_aids:
                        _set_state(session, aid, "needs_attention", add_err)
                    summary["needs_reauth"] = True
                    summary["failed"] += len(add_aids)
                elif add_err:
                    for aid in add_aids:
                        _set_state(session, aid, "failed", add_err)
                    summary["failed"] += len(add_aids)
                else:
                    # write succeeded → in_spotify True; plan-only → keep observed state.
                    now_saved = True if writes_enabled else None  # None = leave unchanged
                    for aid in add_aids:
                        _upsert_side_row(
                            session, album_id=aid, spotify_id=bucket_sid[aid],
                            source=None, state="synced", in_bucket=True,
                            in_spotify=now_saved, last_error=None, stamp_synced=True,
                        )
                    summary["added"] += len(add_aids)

            # 4b. REMOVE = saved in Spotify, NOT in bucket, source='myblog_added'.
            #     NEVER a preexisting album (req 5).
            if remove_sids:
                if remove_scope:
                    for aid, _ in remove_pairs:
                        _set_state(session, aid, "needs_attention", remove_err)
                    summary["needs_reauth"] = True
                    summary["failed"] += len(remove_pairs)
                elif remove_err:
                    for aid, _ in remove_pairs:
                        _set_state(session, aid, "failed", remove_err)
                    summary["failed"] += len(remove_pairs)
                else:
                    now_in_spotify = False if writes_enabled else None
                    for aid, sid in remove_pairs:
                        _upsert_side_row(
                            session, album_id=aid, spotify_id=sid, source=None,
                            state="synced", in_bucket=False, in_spotify=now_in_spotify,
                            last_error=None, stamp_synced=True,
                        )
                    summary["removed"] += len(remove_pairs)

            # 4c. PULL = saved in Spotify, NOT in the bucket, and NOT a myblog_added
            #     row (those are REMOVE candidates). The other half of the never-delete
            #     rule: a Library album kept out of the bucket is pulled IN, not deleted.
            position = _next_bucket_position(session, bucket_id)
            for aid, sid in pull_pairs:
                # SAVEPOINT per item so a single bad PULL rolls back to the savepoint
                # and the pass continues — a plain try/except inside session.begin()
                # would NOT isolate (a statement error poisons the whole tx).
                try:
                    with session.begin_nested():
                        _insert_bucket_item(session, bucket_id, aid, position)
                        _upsert_side_row(
                            session, album_id=aid, spotify_id=sid, source="preexisting",
                            state="synced", in_bucket=True, in_spotify=True,
                            last_error=None, stamp_synced=True,
                        )
                    position += 1
                    summary["pulled"] += 1
                except Exception as e:  # savepoint rolled back; never abort the pass
                    logger.error("PULL of album %s failed: %s", aid, e, exc_info=True)
                    summary["failed"] += 1

            summary["skipped_unknown"] = len(unknown_ids)

            # 4d. Spotify saved time for the newest-saved order (V61). Every saved,
            #     catalog-known album, every pass; only rows whose value differs are
            #     written. Concurrent passes are excluded by the run claim.
            # 4e. Auto-research (owner, 2026-10-15): queue the bucket's note-less
            #     albums per its research_mode. PULLs are raw SQL, so add_item's
            #     enqueue never sees them; this is their only path in.
            # Both ride a SAVEPOINT: they are additions to the reconcile, and a
            # failure in either must not roll back the PULLs / ADD-REMOVE states
            # above (whose Spotify writes have already happened).
            try:
                with session.begin_nested():
                    _stamp_added_at(session, [
                        (sid_to_uuid[sid], added_at_by_sid[sid])
                        for sid in saved_ids
                        if sid in sid_to_uuid and sid in added_at_by_sid
                    ])
            except Exception as e:
                logger.error("saved-time stamp failed (reconcile kept): %s", e, exc_info=True)
            try:
                with session.begin_nested():
                    summary["research_queued"] = _enqueue_research(session, bucket_id)
            except Exception as e:
                logger.error("research enqueue failed (reconcile kept): %s", e, exc_info=True)

    # Enqueue unknown saved albums OUTSIDE the tx (best-effort; never blocks reconcile).
    if unknown_ids and enqueue_unknown is not None:
        try:
            enqueue_unknown(unknown_ids)
        except Exception as e:
            logger.warning("enqueue of unknown saved albums failed: %s", e)

    logger.info(
        "library sync: added=%d removed=%d pulled=%d failed=%d unknown=%d research=%d writes=%s",
        summary["added"], summary["removed"], summary["pulled"],
        summary["failed"], summary["skipped_unknown"], summary["research_queued"],
        writes_enabled,
    )
    return summary


def _stamp_added_at(session, pairs: List[tuple]) -> int:
    """Write spotify_added_at for (album_id, added_at ISO string) pairs. Passes
    are mutually exclusive (the run claim) and the backend never writes this table,
    so there is no concurrent writer to deadlock with; the sort only keeps the
    parameter arrays deterministic."""
    if not pairs:
        return 0
    pairs = sorted(pairs, key=lambda p: str(p[0]))
    result = session.execute(
        text(
            """
            UPDATE spotify_library_albums AS s
               SET spotify_added_at = v.added_at,
                   updated_at       = now()
              FROM unnest(CAST(:ids AS uuid[]), CAST(:ts AS timestamptz[]))
                   AS v(album_id, added_at)
             WHERE s.album_id = v.album_id
               AND s.spotify_added_at IS DISTINCT FROM v.added_at
            """
        ),
        {"ids": [p[0] for p in pairs], "ts": [p[1] for p in pairs]},
    )
    return int(getattr(result, "rowcount", 0) or 0)


def _enqueue_research(session, bucket_id) -> int:
    """Queue research for the library bucket's albums per research_mode ('all' =
    every album, 'selected' = checked ones, 'off' = none). An album with a research
    row of ANY prompt version is skipped, and the (album_id, prompt_version) unique
    backstops a race — so a re-run, or a re-PULL, never asks twice."""
    result = session.execute(
        text(
            """
            INSERT INTO album_research (album_id, prompt_version, status)
            SELECT DISTINCT i.album_id, :pv, 'queued'
              FROM review_bucket_items i
              JOIN review_buckets b ON b.id = i.bucket_id
             WHERE i.bucket_id = :bid
               AND i.item_type = 'album'
               AND i.album_id IS NOT NULL
               AND (b.research_mode = 'all'
                    OR (b.research_mode = 'selected' AND i.research_selected))
               AND NOT EXISTS (
                   SELECT 1 FROM album_research ar WHERE ar.album_id = i.album_id
               )
             ORDER BY i.album_id
            ON CONFLICT ON CONSTRAINT uq_album_research_album_prompt DO NOTHING
            """
        ),
        {"bid": bucket_id, "pv": RESEARCH_PROMPT_VERSION},
    )
    return int(getattr(result, "rowcount", 0) or 0)


def _claim_run(session, bucket_id) -> Optional[Any]:
    """Take the run row (V61) for this pass. Refused when a pass is already running
    (started after the last finish, and not stale) or when the latest request was
    already serviced within SYNC_DEBOUNCE_SECONDS — a duplicate delivery. Returns
    the claim time (started_at), or None when refused."""
    session.execute(
        text(
            "INSERT INTO spotify_library_sync_runs (bucket_id) VALUES (:bid) "
            "ON CONFLICT (bucket_id) DO NOTHING"
        ),
        {"bid": bucket_id},
    )
    row = session.execute(
        text(
            """
            UPDATE spotify_library_sync_runs
               SET started_at = now()
             WHERE bucket_id = :bid
               AND (started_at IS NULL
                    OR (finished_at IS NOT NULL AND finished_at >= started_at)
                    OR started_at < now() - make_interval(secs => :stale))
               AND NOT (finished_at IS NOT NULL
                        AND finished_at > now() - make_interval(secs => :debounce)
                        AND (requested_at IS NULL OR requested_at <= finished_at))
            RETURNING started_at
            """
        ),
        {"bid": bucket_id, "stale": STALE_RUN_SECONDS, "debounce": SYNC_DEBOUNCE_SECONDS},
    ).first()
    return row.started_at if row is not None else None


def _outcome(summary: Optional[Dict[str, Any]]) -> str:
    if summary is None:
        return "error"
    if summary.get("needs_reauth"):
        return "needs_reauth" + (f" ({summary['reason']})" if summary.get("reason") else "")
    return (
        f"added={summary.get('added', 0)} removed={summary.get('removed', 0)} "
        f"pulled={summary.get('pulled', 0)} failed={summary.get('failed', 0)} "
        f"unknown={summary.get('skipped_unknown', 0)} "
        f"research={summary.get('research_queued', 0)}"
    )[:200]


def _finish_run(session_factory, bucket_id, outcome: str, started_at) -> bool:
    """Stamp the finish on EVERY pass that claimed the run — a no-change pass, a
    re-auth abort and an exception alike — so the backend's gate and the board's poll
    see the run end.

    finished_at is the pass's CLAIM time, not now(): the pass read Spotify after
    that moment, so it services exactly the requests made before it. A request made
    while it ran stays newer than finished_at — still pending — and the return value
    tells the caller to go again (its own message was refused by the running pass).
    Best-effort: never masks the pass's own exception; False on failure."""
    try:
        with session_factory() as session, session.begin():
            row = session.execute(
                text(
                    "UPDATE spotify_library_sync_runs "
                    "SET finished_at = :started, last_outcome = :o WHERE bucket_id = :bid "
                    "RETURNING (requested_at IS NOT NULL AND requested_at > :started) AS pending"
                ),
                {"bid": bucket_id, "o": outcome[:200], "started": started_at},
            ).first()
            return bool(row.pending) if row is not None else False
    except Exception as e:  # pragma: no cover - best-effort
        logger.warning("failed to stamp library sync finish: %s", e)
        return False


def run_library_sync(
    session_factory,
    spotify_user,
    enqueue_unknown: Optional[Callable[[List[str]], None]] = None,
    writes_enabled: bool = False,
) -> Dict[str, Any]:
    """Entry point for the {"job": "spotify_library_sync"} message. Claims the V61
    run row in a short transaction BEFORE any Spotify call, reconciles, and always
    stamps the finish. A refused claim (already running / just serviced) is a no-op
    that never reads Spotify. See _reconcile for the algorithm."""
    result: Dict[str, Any] = {"skipped": "serviced_or_running"}
    for attempt in range(MAX_PASSES):
        with session_factory() as session, session.begin():
            bucket_id = _find_special_bucket_id(session)
            if bucket_id is None:
                logger.info("no kind='spotify_library' bucket — nothing to reconcile")
                return {"bucket": "absent"}
            started_at = _claim_run(session, bucket_id)
        if started_at is None:
            if attempt == 0:
                logger.info("library sync skipped — a run is in progress or the request was already serviced")
            return result

        summary: Optional[Dict[str, Any]] = None
        pending = False
        try:
            summary = _reconcile(
                session_factory, spotify_user,
                enqueue_unknown=enqueue_unknown, writes_enabled=writes_enabled,
            )
            result = summary
        finally:
            pending = _finish_run(session_factory, bucket_id, _outcome(summary), started_at)
        if not pending:
            return result
        logger.info("library sync: a request arrived mid-pass — running again")
    return result


def _maybe_write(writes_enabled: bool, fn: Callable[[], None], *, op: str, ids: List[str]):
    """Issue a gated Spotify mutation. PLAN-ONLY (writes_enabled=False): LOG the
    intended id set and return success without calling Spotify. When enabled: run
    `fn`, classifying the outcome.

    Returns (ok: bool, err: str|None, scope: bool). scope=True means a missing-scope
    403 (caller → needs_attention + needs_reauth); err set with scope=False means a
    transient/other failure (caller → 'failed')."""
    if not writes_enabled:
        logger.info("[plan-only] would %s ids=%s", op, ids)
        return True, None, False
    try:
        fn()
        return True, None, False
    except SpotifyScopeError as e:
        logger.warning("%s missing scope: %s", op, e)
        return False, str(e), True
    except Exception as e:
        logger.error("%s failed: %s", op, e, exc_info=True)
        return False, str(e), False
