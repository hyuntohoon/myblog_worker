# tests/test_lyrics_label_yield.py
"""FEAT-lyrics-annotations Thread 2 Step B — rule F (measured label yield).

These lock the invariants that a mock CAN prove: the thresholds match the RFC
measurement, the holdout is deterministic and the right size, and the sync marks
after it un-marks. The SQL itself was verified against prod on 2026-07-28 —
EXPLAIN on both rewritten selections, plus a BEGIN/ROLLBACK count showing
3,494 pool rows in dead labels, 3,297 marked, 197 held out, and the 2 already-
matched rows left untouched.
"""
from __future__ import annotations

import hashlib
from unittest.mock import MagicMock

from worker.service.lyrics_label_yield import (
    HOLDOUT_HEX_CUTOFF,
    MAX_MATCH_RATE,
    MIN_ATTEMPTS,
    RULE_VERSION,
    holdout_predicate,
    sync_exclusions,
)


def _in_holdout(value: str) -> bool:
    """Mirror of the SQL predicate, so the two can be compared."""
    return hashlib.md5(value.encode()).hexdigest()[:2] < HOLDOUT_HEX_CUTOFF


def test_thresholds_match_the_rfc_measurement():
    # RFC §4.3: ">= 50 corpus attempts and a < 2% match rate is a dead source".
    # Loosening either silently changes which labels are excluded in prod.
    assert MIN_ATTEMPTS == 50
    assert MAX_MATCH_RATE == 0.02


def test_holdout_is_deterministic_and_about_five_percent():
    ids = [f"track-{i}" for i in range(4000)]
    picked = [i for i in ids if _in_holdout(i)]
    pct = 100.0 * len(picked) / len(ids)
    # 13/256 = 5.08%. The band is wide enough for sampling noise but tight enough
    # that a changed cutoff fails here rather than silently resizing the holdout.
    assert 4.0 < pct < 6.5, f"holdout {pct:.2f}% — expected ~5%"
    # A row must not drift in and out between runs, or the signal is destroyed.
    assert [i for i in ids if _in_holdout(i)] == picked


def test_holdout_predicate_targets_the_column_it_is_given():
    assert holdout_predicate("tl.track_id").startswith("left(md5(tl.track_id::text), 2) <")
    assert holdout_predicate("t.id").startswith("left(md5(t.id::text), 2) <")


def test_sync_unmarks_before_marking_and_commits():
    """Order matters: a recovered label must release its rows in the same pass a
    newly-dead label claims its own, or a row could be marked by one statement and
    left stale by the other."""
    session = MagicMock()
    session.execute.return_value.rowcount = 7

    result = sync_exclusions(session)

    assert session.execute.call_count == 2
    first, second = (c.args[0].text for c in session.execute.call_args_list)
    assert "- 'excluded_by'" in first, "un-mark must run first"
    assert "'excluded_by',    :rule" in second, "mark must run second"
    session.commit.assert_called_once()
    assert result == {"marked": 7, "unmarked": 7, "rule": RULE_VERSION}


def test_mark_never_touches_a_resolved_row():
    """The 2 false positives are already `matched`. Marking them would park rows that
    demonstrably have lyrics — the one outcome the rule must never produce."""
    session = MagicMock()
    session.execute.return_value.rowcount = 0
    sync_exclusions(session)

    mark_sql = session.execute.call_args_list[1].args[0].text
    assert "match_status IN ('not_found', 'ambiguous', 'review_required')" in mark_sql
    assert "NOT (left(md5(tl.track_id::text), 2)" in mark_sql, "holdout arm must be excluded from marking"
