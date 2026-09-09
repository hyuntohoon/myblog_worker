"""Unit tests for the OQ5 backoff ladder (FEAT-lyrics-listening-experience Step 3).

DB-free. The ladder is the whole of OQ5's "retry interval" half, and it is deliberately
carried by two existing timestamp columns instead of an ``attempts`` counter — V57 has no
such column on ``lyrics_album_tracks``, because the owner's rule is that demand is never
discarded on a failure count. These tests pin both halves of that claim: the encoding
round-trips, and nothing in the ladder can terminate a row.

The orchestration around it is covered end-to-end against real Postgres in
``tests/integration/test_lyrics_demand_source_db.py`` — the state machine is entirely a
property of the store's SQL guards, which a mock session cannot observe
([[feedback-sa-session-lifecycle-mock-blind]]).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from worker.core.config import settings
from worker.service.lyrics_demand_source_service import _previous_gap, next_attempt

NOW = datetime(2026, 9, 9, 12, 0, 0, tzinfo=timezone.utc)

BASE = settings.LYRICS_DEMAND_SOURCE_RETRY_BASE_SEC   # 6h
CAP = settings.LYRICS_DEMAND_SOURCE_RETRY_CAP_SEC     # 30d


def _ladder(base_sec=BASE, cap_sec=CAP):
    """Walk the ladder the way the service does: each step re-reads the gap it just wrote."""
    gaps, previous = [], None
    for _ in range(12):
        nxt = next_attempt(NOW, previous, base_sec=base_sec, cap_sec=cap_sec)
        previous = nxt - NOW  # what _previous_gap recovers next time (updated_at == now)
        gaps.append(previous)
    return gaps


class TestLadder:
    def test_first_attempt_starts_at_base(self):
        assert next_attempt(NOW, None, base_sec=BASE, cap_sec=CAP) == NOW + timedelta(
            seconds=BASE
        )

    def test_ladder_doubles_then_settles_at_the_cap(self):
        gaps = [g.total_seconds() for g in _ladder()]
        assert gaps[:5] == [21_600, 43_200, 86_400, 172_800, 345_600]  # 6h 12h 24h 2d 4d
        assert gaps[-1] == CAP
        # Monotone non-decreasing: a re-check never gets *more* frequent by climbing.
        assert all(b >= a for a, b in zip(gaps, gaps[1:]))

    def test_cap_is_a_ceiling_not_a_terminator(self):
        """The OQ5 rule that matters most: reaching the cap must still yield a due date.

        A ladder that returned None / a sentinel at the top would be a failure-count
        termination wearing a different hat.
        """
        at_cap = timedelta(seconds=CAP)
        for _ in range(50):
            nxt = next_attempt(NOW, at_cap, base_sec=BASE, cap_sec=CAP)
            assert nxt == NOW + timedelta(seconds=CAP)
            at_cap = nxt - NOW

    @pytest.mark.parametrize(
        "corrupt",
        [timedelta(seconds=0), timedelta(seconds=-3600), timedelta(days=9999)],
        ids=["zero", "negative", "beyond-cap"],
    )
    def test_corrupt_gap_re_enters_the_ladder_within_bounds(self, corrupt):
        """A hand-edited row, a clock jump or a policy change must not escape the clamps.

        Without the ``max(base, ...)`` a zero/negative gap would make the row due
        immediately forever (a hot loop against LRCLIB); without ``min(cap, ...)`` a
        far-future gap would be doubled into effective abandonment.
        """
        gap = next_attempt(NOW, corrupt, base_sec=BASE, cap_sec=CAP) - NOW
        assert timedelta(seconds=BASE) <= gap <= timedelta(seconds=CAP)

    def test_catalog_ladder_is_shorter_than_the_source_ladder(self):
        """Different failures, different time constants — an un-ingested album is waiting
        on our own pipeline (hours), not on third-party lyric coverage (weeks)."""
        catalog = [
            g.total_seconds()
            for g in _ladder(
                base_sec=settings.LYRICS_DEMAND_CATALOG_RETRY_BASE_SEC,
                cap_sec=settings.LYRICS_DEMAND_CATALOG_RETRY_CAP_SEC,
            )
        ]
        assert catalog[0] == 900 and catalog[-1] == 86_400
        assert catalog[-1] < CAP


class TestGapRecovery:
    def test_round_trips_the_gap_the_previous_write_chose(self):
        """``next_attempt_at - updated_at`` IS the encoding. If this pair ever stops being
        written by the same statement, the ladder silently resets to base every run."""
        written = next_attempt(NOW, timedelta(hours=12), base_sec=BASE, cap_sec=CAP)
        # set_source_state writes updated_at=now() and next_attempt_at in one UPDATE.
        assert _previous_gap(
            {"next_attempt_at": written, "updated_at": NOW}
        ) == timedelta(hours=24)

    @pytest.mark.parametrize(
        "row",
        [
            {"next_attempt_at": None, "updated_at": NOW},
            {"next_attempt_at": NOW, "updated_at": None},
            {},
        ],
        ids=["never-scheduled", "no-updated_at", "empty"],
    )
    def test_missing_timestamps_restart_at_base(self, row):
        assert _previous_gap(row) is None
        assert next_attempt(NOW, _previous_gap(row), base_sec=BASE, cap_sec=CAP) == (
            NOW + timedelta(seconds=BASE)
        )
