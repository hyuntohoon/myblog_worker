"""Unit coverage for IsrcBackfillService's write-path branching.

Why this file exists: `tests/test_isrc_backfill.py` — despite the name — only exercises
`SpotifyClient.get_tracks`. The matched/miss branching that decides what lands in
`tracks.isrc` had ZERO unit coverage, and the single assertion that would have caught
the in-column sentinel defect lived in a DB-gated integration test. These tests need no
database: they capture the SQL + params the service hands to the session.

Invariants pinned here:
  1. a miss NEVER writes the `isrc` column and NEVER writes the `isrc` ext_refs key,
  2. a match writes the column and mirrors into `ext_refs.isrc`,
  3. a Track-Relinking response (`linked_from.id`) resolves instead of being marked as
     a permanent miss,
  4. write batches are sorted by track_id (bulk-write lock ordering rule),
  5. the wall-clock budget stops the loop instead of overrunning the Lambda timeout.
"""
from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import patch

import pytest

from worker.service.isrc_backfill_service import (
    STATUS_NO_ISRC,
    STATUS_NOT_FOUND,
    IsrcBackfillService,
)


class FakeSession:
    """Records every execute() as (sql_text, params); commit/rollback are counters."""

    def __init__(self) -> None:
        self.calls: List[tuple] = []
        self.commits = 0
        self.rollbacks = 0

    def execute(self, stmt, params=None):
        self.calls.append((str(stmt), params))
        raise AssertionError("selection must be stubbed in these tests")

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class WriteCapturingSession(FakeSession):
    def execute(self, stmt, params=None):
        self.calls.append((str(stmt), params))
        return None

    def writes(self) -> List[tuple]:
        return [(sql, p) for sql, p in self.calls if "UPDATE tracks" in sql]


def _svc(session, seeds: List[Dict[str, str]]) -> IsrcBackfillService:
    svc = IsrcBackfillService(session)
    svc._fetch_tracks_without_isrc = lambda limit: list(seeds)  # type: ignore[method-assign]
    return svc


def _run(seeds, spotify_payload, **kw) -> tuple:
    session = WriteCapturingSession()
    svc = _svc(session, seeds)
    with patch("worker.service.isrc_backfill_service.spotify") as mock_spotify:
        mock_spotify.get_tracks.return_value = spotify_payload
        metrics = svc.backfill_isrc(**kw)
    return session, metrics


def test_miss_never_touches_the_isrc_column_or_the_isrc_ext_refs_key():
    """A track Spotify has no ISRC for is marked in ext_refs.isrc_status only."""
    session, metrics = _run(
        [{"id": "t-1", "spotify_id": "sp-1"}],
        [{"id": "sp-1", "external_ids": {}}],
    )
    writes = session.writes()
    assert len(writes) == 1
    sql, params = writes[0]
    assert "isrc_status" in sql
    assert "SET isrc" not in sql, "miss must not write the isrc column"
    assert "'isrc'," not in sql, "miss must not write the real-ISRC ext_refs key"
    assert params == [{"track_id": "t-1", "status": STATUS_NO_ISRC}]
    assert metrics["sentinel_written"] == 1 and metrics["matched"] == 0


def test_unknown_id_is_marked_not_found_not_written_to_the_column():
    """Spotify returns a null placeholder for an unknown id → not_found marker."""
    session, metrics = _run(
        [{"id": "t-1", "spotify_id": "sp-gone"}],
        [None],
    )
    sql, params = session.writes()[0]
    assert "SET isrc" not in sql
    assert params == [{"track_id": "t-1", "status": STATUS_NOT_FOUND}]
    assert metrics["sentinel_written"] == 1
    assert metrics["fetched"] == 0, "a null placeholder is not a fetched track"


def test_match_writes_column_and_mirrors_into_ext_refs():
    session, metrics = _run(
        [{"id": "t-1", "spotify_id": "sp-1"}],
        [{"id": "sp-1", "external_ids": {"isrc": "USRC12345678"}}],
    )
    writes = session.writes()
    assert len(writes) == 1
    sql, params = writes[0]
    assert "SET isrc" in sql and "'isrc'" in sql
    assert "isrc_status" not in sql
    assert params == [{"track_id": "t-1", "isrc": "USRC12345678"}]
    assert metrics["matched"] == 1 and metrics["sentinel_written"] == 0


def test_relinked_track_resolves_instead_of_being_permanently_marked():
    """Track Relinking returns a DIFFERENT id; linked_from carries the one we asked for.

    Keyed only on the response `id`, this row would be written as a permanent
    `not_found` — and because the marker removes it from the selection pool, the error
    would never be revisited.
    """
    session, metrics = _run(
        [{"id": "t-1", "spotify_id": "sp-requested"}],
        [
            {
                "id": "sp-relinked",
                "linked_from": {"id": "sp-requested"},
                "external_ids": {"isrc": "GBAYE0601498"},
            }
        ],
        market="KR",
    )
    sql, params = session.writes()[0]
    assert "SET isrc" in sql
    assert params == [{"track_id": "t-1", "isrc": "GBAYE0601498"}]
    assert metrics["matched"] == 1 and metrics["sentinel_written"] == 0


def test_json_bound_params_are_explicitly_cast():
    """Every :param inside jsonb_build_object must carry an explicit CAST.

    Postgres cannot infer the type of an untyped placeholder there and raises
    `IndeterminateDatatype: could not determine data type of parameter $1` — at EXECUTE
    time, so the statement looks fine until the first real write. This shipped broken
    once: the miss path used `jsonb_build_object('isrc_status', :status)` and every
    DB-backed test that would have caught it was skipping (the Neon test branch has no
    `tracks.isrc`, so the V34 guard fired), while these unit tests passed because a
    FakeSession never binds to a real driver. This assertion is the DB-free backstop.
    """
    import re

    session = WriteCapturingSession()
    svc = _svc(session, [
        {"id": "t-1", "spotify_id": "sp-1"},
        {"id": "t-2", "spotify_id": "sp-2"},
    ])
    with patch("worker.service.isrc_backfill_service.spotify") as mock_spotify:
        mock_spotify.get_tracks.return_value = [
            {"id": "sp-1", "external_ids": {"isrc": "USRC12345678"}},   # matched path
            {"id": "sp-2", "external_ids": {}},                          # missed path
        ]
        svc.backfill_isrc()

    writes = session.writes()
    assert len(writes) == 2, "expected both the matched and the missed statement"
    for sql, _ in writes:
        for call in re.findall(r"jsonb_build_object\([^)]*\)", sql):
            bare = re.findall(r"(?<!AS )(?<!CAST\()\B:(\w+)", call)
            assert not bare, (
                f"bound param(s) {bare} inside {call!r} lack an explicit CAST — "
                "this raises IndeterminateDatatype against a real Postgres driver"
            )
            assert "CAST(" in call, f"no CAST in {call!r}"


def test_both_write_lists_are_sorted_by_track_id():
    """Bulk writes acquire row locks in a consistent order (deadlock-avoidance rule)."""
    seeds = [
        {"id": "t-9", "spotify_id": "sp-9"},
        {"id": "t-1", "spotify_id": "sp-1"},
        {"id": "t-5", "spotify_id": "sp-5"},
        {"id": "t-3", "spotify_id": "sp-3"},
    ]
    session, _ = _run(
        seeds,
        [
            {"id": "sp-9", "external_ids": {"isrc": "B"}},
            {"id": "sp-1", "external_ids": {"isrc": "A"}},
            {"id": "sp-5", "external_ids": {}},
            {"id": "sp-3", "external_ids": {}},
        ],
    )
    for sql, params in session.writes():
        ids = [p["track_id"] for p in params]
        assert ids == sorted(ids), f"unsorted write batch: {ids}"


def test_selection_excludes_prior_attempts_and_orders_deterministically():
    """The SQL must skip resolved rows, genre-CLI rows, and already-attempted rows."""
    captured: Dict[str, Any] = {}

    class SelectSession(FakeSession):
        def execute(self, stmt, params=None):
            captured["sql"] = str(stmt)
            captured["params"] = params

            class R:
                def fetchall(self_inner):
                    return []

            return R()

    IsrcBackfillService(SelectSession())._fetch_tracks_without_isrc(limit=7)
    sql = captured["sql"]
    assert "isrc IS NULL" in sql
    assert "ext_refs->>'isrc' IS NULL" in sql
    assert "ext_refs->>'isrc_status' IS NULL" in sql
    assert "ORDER BY spotify_id" in sql
    assert captured["params"] == {"limit": 7}


def test_batch_failure_rolls_back_and_later_batch_still_commits():
    """Failure isolation survives the rewrite (FIX-bug-audit-2026-07 WS-C H2)."""
    seeds = [{"id": f"t-{n:03d}", "spotify_id": f"sp-{n:03d}"} for n in range(60)]
    session = WriteCapturingSession()
    svc = _svc(session, seeds)
    calls = {"n": 0}

    def side_effect(ids, market=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("simulated Spotify outage on batch 1")
        return [{"id": sid, "external_ids": {"isrc": f"US{sid}"}} for sid in ids]

    with patch("worker.service.isrc_backfill_service.spotify") as mock_spotify:
        mock_spotify.get_tracks.side_effect = side_effect
        metrics = svc.backfill_isrc()

    assert calls["n"] == 2
    assert metrics["errors"] == 1
    assert metrics["matched"] == 10
    assert session.rollbacks == 1


def test_time_budget_stops_the_loop_and_reports_the_remainder():
    """A slow/throttled run stops cleanly rather than overrunning the 120s Lambda."""
    seeds = [{"id": f"t-{n:03d}", "spotify_id": f"sp-{n:03d}"} for n in range(150)]
    session = WriteCapturingSession()
    svc = _svc(session, seeds)

    ticks = iter([0.0] + [1000.0] * 40)  # deadline set at 0, every check is way past it

    with patch("worker.service.isrc_backfill_service.spotify") as mock_spotify, \
            patch("worker.service.isrc_backfill_service.time.monotonic",
                  side_effect=lambda: next(ticks)):
        mock_spotify.get_tracks.return_value = []
        metrics = svc.backfill_isrc()

    assert metrics["skipped_budget"] == 150
    assert mock_spotify.get_tracks.call_count == 0


def test_read_transaction_is_closed_before_the_first_spotify_call():
    """Neon rule: never hold a session open across an external-API loop."""
    order: List[str] = []

    class OrderSession(WriteCapturingSession):
        def commit(self):
            order.append("commit")
            super().commit()

    session = OrderSession()
    svc = IsrcBackfillService(session)

    def fetch(limit):
        order.append("select")
        return [{"id": "t-1", "spotify_id": "sp-1"}]

    svc._fetch_tracks_without_isrc = fetch  # type: ignore[method-assign]

    with patch("worker.service.isrc_backfill_service.spotify") as mock_spotify:
        def spotify_call(ids, market=None):
            order.append("spotify")
            return [{"id": "sp-1", "external_ids": {"isrc": "X"}}]

        mock_spotify.get_tracks.side_effect = spotify_call
        svc.backfill_isrc()

    assert order[:3] == ["select", "commit", "spotify"], order


def test_empty_pool_is_a_cheap_no_op():
    session = WriteCapturingSession()
    svc = _svc(session, [])
    with patch("worker.service.isrc_backfill_service.spotify") as mock_spotify:
        metrics = svc.backfill_isrc()
    assert mock_spotify.get_tracks.call_count == 0
    assert metrics == {
        "fetched": 0, "matched": 0, "sentinel_written": 0,
        "errors": 0, "skipped_budget": 0,
    }
