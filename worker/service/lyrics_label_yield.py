"""FEAT-lyrics-annotations Thread 2 Step B — pool hygiene by measured label yield.

Rule F (RFC §4.3), chosen by the owner 2026-07-28 over the genre+title-form rule E:
*a label with >= MIN_ATTEMPTS corpus attempts and a < MAX_MATCH_RATE match rate is a
dead source.* Re-measured against prod the same day: **15 labels, 3,493 pool rows
(24.9% of 14,029 not_found), 2 false positives.**

Why this shape rather than rule E — the genre + instrumental-title-form filter:

- It is **not a taste or genre judgement**. It measures our own source coverage from
  our own data, so it is identical for every user of the site (the owner's stated
  requirement: this is a multi-user product, not a personal whitelist).
- It is **self-correcting**. If a label starts matching, its statistic moves and the
  exclusion lifts with no human re-tuning. `sync_exclusions` un-marks on every run,
  which is why marks are recomputed rather than written once.
- Rule E needs a genre allowlist plus two regex vocabularies kept in step with each
  other forever, and RFC §4.2 records five traps that each cost a real album — a
  Roman-numeral pattern eating `I.F.L.Y.`, classical vocabulary appearing in pop
  titles, and Korean genre substring matching that would have deleted AC/DC, Queen,
  and 조수미. Rule F needs no vocabulary at all.

Rule E's one unique advantage is cold-start: a brand-new label has no statistic, so
it cannot be gated on its first release. The 5% holdout below is what measures
whether that gap actually matters before any code is written for it.

**Nothing is deleted.** Excluded rows are marked in `track_lyrics.evidence` with the
reason, rule version, and timestamp; reversal is one UPDATE. A deterministic ~5%
holdout stays in rotation: if the rule is right their promotion rate stays near zero,
and if it climbs, *that rate is the misclassification rate* — measured continuously
with no human effort (RFC §4.4). Baseline to compare against is the pool's own
1.54% (45 promotions / 2,925 re-checks over 20 days).
"""
from __future__ import annotations

import logging
from typing import Any, Dict

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

RULE_VERSION = "label-yield-v1"

# A label needs this many attempts before its rate means anything, and must fall
# below this match rate to count as dead. Both come from the RFC §4.3 measurement.
MIN_ATTEMPTS = 50
MAX_MATCH_RATE = 0.02

# Deterministic ~5.1% (13/256) holdout. md5 of the track id is stable across runs and
# across Postgres versions, so a row stays in or out of the holdout for its lifetime —
# a re-randomising predicate would let a row drift in and out and destroy the signal.
HOLDOUT_HEX_CUTOFF = "0d"

# Labels whose measured match rate marks them a dead source. Recomputed on every call
# so a recovering label lifts its own exclusion.
DEAD_LABELS_CTE = f"""
    label_yield AS (
        SELECT al.label,
               count(*)                                              AS attempts,
               count(*) FILTER (WHERE tl.match_status = 'matched')   AS hits
        FROM track_lyrics tl
        JOIN tracks t  ON t.id = tl.track_id
        JOIN albums al ON al.id = t.album_id
        WHERE al.label IS NOT NULL
        GROUP BY al.label
    ),
    dead_labels AS (
        SELECT label FROM label_yield
        WHERE attempts >= {MIN_ATTEMPTS}
          AND hits::float / attempts < {MAX_MATCH_RATE}
    )
"""


def holdout_predicate(id_expr: str) -> str:
    """SQL predicate that is true for the ~5% of rows held back from exclusion."""
    return f"left(md5({id_expr}::text), 2) < '{HOLDOUT_HEX_CUTOFF}'"


def sync_exclusions(session: Session) -> Dict[str, Any]:
    """Recompute dead labels and bring `evidence.excluded_by` marks in line.

    Marks and un-marks in one short transaction — no external calls, so this cannot
    hold a session open across an API loop (the Neon idle-in-transaction hazard that
    the lyrics pipeline is the reference fix for).

    Un-marking runs FIRST so a label that recovered releases its rows in the same run
    that a newly-dead label claims its own. Only unresolved rows are marked: a row
    that already matched is not in the pool and must keep its evidence intact.
    """
    unmarked = session.execute(
        text(
            f"""
            WITH {DEAD_LABELS_CTE}
            UPDATE track_lyrics tl
               SET evidence = tl.evidence - 'excluded_by' - 'excluded_at' - 'excluded_label',
                   updated_at = NOW()
              FROM tracks t
              JOIN albums al ON al.id = t.album_id
             WHERE t.id = tl.track_id
               AND tl.evidence ? 'excluded_by'
               AND (al.label IS NULL OR al.label NOT IN (SELECT label FROM dead_labels))
            """
        )
    ).rowcount

    marked = session.execute(
        text(
            f"""
            WITH {DEAD_LABELS_CTE}
            UPDATE track_lyrics tl
               SET evidence = COALESCE(tl.evidence, '{{}}'::jsonb) || jsonb_build_object(
                       'excluded_by',    :rule,
                       'excluded_at',    to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SSZ'),
                       'excluded_label', al.label
                   ),
                   updated_at = NOW()
              FROM tracks t
              JOIN albums al ON al.id = t.album_id
             WHERE t.id = tl.track_id
               AND NOT (tl.evidence ? 'excluded_by')
               AND tl.match_status IN ('not_found', 'ambiguous', 'review_required')
               AND al.label IN (SELECT label FROM dead_labels)
               AND NOT ({holdout_predicate('tl.track_id')})
            """
        ),
        {"rule": RULE_VERSION},
    ).rowcount

    session.commit()
    stats = {"marked": marked, "unmarked": unmarked, "rule": RULE_VERSION}
    if marked or unmarked:
        logger.info("label-yield exclusions synced: %s", stats)
    return stats


def holdout_audit(session: Session) -> Dict[str, Any]:
    """Promotion rate of the holdout vs the excluded set — the misclassification signal.

    A holdout promotion rate materially above the pool baseline (1.54%) means the rule
    is parking rows that would have resolved, and the exclusion should be reverted.
    """
    row = session.execute(
        text(
            f"""
            WITH {DEAD_LABELS_CTE},
            scoped AS (
                SELECT tl.match_status,
                       (tl.evidence ? 'excluded_by')             AS excluded,
                       {holdout_predicate('tl.track_id')}        AS in_holdout
                FROM track_lyrics tl
                JOIN tracks t  ON t.id = tl.track_id
                JOIN albums al ON al.id = t.album_id
                WHERE al.label IN (SELECT label FROM dead_labels)
            )
            SELECT
                count(*) FILTER (WHERE excluded)                        AS excluded_rows,
                count(*) FILTER (WHERE in_holdout)                      AS holdout_rows,
                count(*) FILTER (WHERE in_holdout
                                   AND match_status IN ('matched','no_lyrics')) AS holdout_promoted
            FROM scoped
            """
        )
    ).one()
    excluded_rows, holdout_rows, holdout_promoted = row
    pct = round(100.0 * holdout_promoted / holdout_rows, 2) if holdout_rows else None
    return {
        "excluded_rows": excluded_rows,
        "holdout_rows": holdout_rows,
        "holdout_promoted": holdout_promoted,
        "holdout_promotion_pct": pct,
        "pool_baseline_pct": 1.54,
    }
