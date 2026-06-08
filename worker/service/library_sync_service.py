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
    """Append an album to the special bucket. Idempotent: the
    uq_review_bucket_items_bucket_album constraint makes a re-PULL a no-op."""
    session.execute(
        text(
            """
            INSERT INTO review_bucket_items (bucket_id, album_id, position, status)
            VALUES (:bid, :album_id, :position, 'candidate')
            ON CONFLICT (bucket_id, album_id) DO NOTHING
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


def run_library_sync(
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
    for alb in saved_albums:
        sid = alb.get("id")
        if sid and sid not in seen:
            seen.add(sid)
            saved_ids.append(sid)

    summary: Dict[str, Any] = {
        "added": 0,
        "removed": 0,
        "pulled": 0,
        "failed": 0,
        "skipped_unknown": 0,
        "writes_enabled": writes_enabled,
        "needs_reauth": False,
    }
    unknown_ids: List[str] = []

    with session_factory() as session:
        with session.begin():
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

            # 3. First-touch source stamp for bucket albums lacking a side row:
            #    contains-check (chunked) → 'preexisting' if already saved else 'myblog_added'.
            untouched = [aid for aid in bucket_set if aid not in existing_source]
            if untouched:
                untouched_sids = [bucket_sid[aid] for aid in untouched]
                try:
                    contains = spotify_user.check_saved_albums(untouched_sids)
                except SpotifyScopeError as e:
                    logger.warning("contains-check missing scope: %s", e)
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
                for aid in untouched:
                    sid = bucket_sid[aid]
                    is_saved = bool(contains.get(sid, False)) or sid in seen
                    src = "preexisting" if is_saved else "myblog_added"
                    _upsert_side_row(
                        session, album_id=aid, spotify_id=sid, source=src,
                        state="pending", in_bucket=True, in_spotify=is_saved,
                    )
                    existing_source[aid] = src

            # Authoritative current Library membership for each bucket album, from L.
            in_spotify_now: Dict[Any, bool] = {aid: bucket_sid[aid] in seen for aid in bucket_set}

            # 4a. ADD = bucket albums NOT currently saved in Spotify.
            add_aids = [aid for aid in bucket_set if not in_spotify_now[aid]]
            add_sids = [bucket_sid[aid] for aid in add_aids]
            if add_sids:
                _ok, err, scope = _maybe_write(
                    writes_enabled, lambda: spotify_user.save_albums(add_sids),
                    op="PUT /me/albums (ADD)", ids=add_sids,
                )
                if scope:
                    for aid in add_aids:
                        _set_state(session, aid, "needs_attention", err)
                    summary["needs_reauth"] = True
                    summary["failed"] += len(add_aids)
                elif err:
                    for aid in add_aids:
                        _set_state(session, aid, "failed", err)
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
            remove_pairs = [
                (sid_to_uuid[sid], sid)
                for sid in saved_ids
                if sid in sid_to_uuid
                and sid_to_uuid[sid] not in bucket_set
                and existing_source.get(sid_to_uuid[sid]) == "myblog_added"
            ]
            remove_sids = [sid for _, sid in remove_pairs]
            if remove_sids:
                _ok, err, scope = _maybe_write(
                    writes_enabled, lambda: spotify_user.remove_albums(remove_sids),
                    op="DELETE /me/albums (REMOVE)", ids=remove_sids,
                )
                if scope:
                    for aid, _ in remove_pairs:
                        _set_state(session, aid, "needs_attention", err)
                    summary["needs_reauth"] = True
                    summary["failed"] += len(remove_pairs)
                elif err:
                    for aid, _ in remove_pairs:
                        _set_state(session, aid, "failed", err)
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
            pull_pairs = [
                (sid_to_uuid[sid], sid)
                for sid in saved_ids
                if sid in sid_to_uuid
                and sid_to_uuid[sid] not in bucket_set
                and existing_source.get(sid_to_uuid[sid]) != "myblog_added"
            ]
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

    # Enqueue unknown saved albums OUTSIDE the tx (best-effort; never blocks reconcile).
    if unknown_ids and enqueue_unknown is not None:
        try:
            enqueue_unknown(unknown_ids)
        except Exception as e:
            logger.warning("enqueue of unknown saved albums failed: %s", e)

    logger.info(
        "library sync: added=%d removed=%d pulled=%d failed=%d unknown=%d writes=%s",
        summary["added"], summary["removed"], summary["pulled"],
        summary["failed"], summary["skipped_unknown"], writes_enabled,
    )
    return summary


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
