# worker/service/lyrics_reassessment_service.py
"""FEAT-lyrics-corpus Step 4: periodic reassessment + replacement guard (worker).

LRCLIB coverage grows over time, so tracks that parked as ``not_found`` / ``ambiguous`` /
``review_required`` earlier may become matchable later. This periodic EventBridge job
re-checks the **unresolved pool** (stalest first, behind the best-of supersession backlog —
DATA-catalog-noise Step 3b) with the same canonical ``decide_match``, and:

  * **promotes** an unresolved track to ``matched`` / ``no_lyrics`` when the evidence now
    supports it,
  * **refreshes** a still-unresolved row (bumps ``updated_at`` so the queue rotates fairly
    across the whole unresolved pool instead of re-hitting the same stale rows),
  * **never silently overwrites a good match**: the replacement guard (``should_replace``)
    only lets a resolved ``matched`` / ``no_lyrics`` row be replaced by a new ``matched``
    outcome carrying **strictly stronger** evidence, never a downgrade or a lateral swap.

Step 4 *selects* only unresolved rows, so a good match is never even a candidate here; the
guard is the tested, defensive proof of the never-downgrade rule (and future-proofs the day
ISRC / MusicBrainz-recording evidence lets a re-check legitimately supersede a title+duration
match). Bounded to the 120s worker Lambda by the same batch-limit + wall-clock budget as
Step 3 (shared ``run_eval_batch``). RFC: ``docs/rfcs/FEAT-lyrics-corpus.md`` (Step 4).

FEAT-lyrics-match-correction Step 2 adds the **re-sweep**: resolved rows written by another
matcher version are re-judged under the running one (``_fetch_stale_version_tracks``, oldest
version first, a bounded share of the same daily batch), and the guard gains exactly one
lateral path — same basis, different body, strictly larger content cluster — so a matcher
fix finally reaches the rows an older version wrote (RC5). Every re-judged row is stamped
(``evidence["resweep"]``) whether replaced or kept, which is what makes the sweep terminate
and what re-opens it on the next version bump with no one-off script. A hand-fixed row
(``evidence["manual_fix"]``, later the Step 3a ``basis='manual'``) is locked: never replaced.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from worker.clients.lrclib_client import LrclibClient
from worker.core.config import settings
from worker.service.lyrics_eval_core import PRIMARY_ARTIST_NAMES_LATERAL, Verdict, run_eval_batch
from worker.service.lyrics_label_yield import sync_exclusions
from worker.service.lyrics_matcher import (
    CONTENT_AGREE_THRESHOLD,
    MATCHER_VERSION,
    STATUS_AMBIGUOUS,
    STATUS_MATCHED,
    STATUS_NO_LYRICS,
    STATUS_NOT_FOUND,
    STATUS_REVIEW_REQUIRED,
    Candidate,
    MatchOutcome,
    body_shingles,
    content_shingles,
    content_similarity,
    plausible_candidates,
)

logger = logging.getLogger(__name__)

_UNRESOLVED = (STATUS_NOT_FOUND, STATUS_AMBIGUOUS, STATUS_REVIEW_REQUIRED)
_RESOLVED = (STATUS_MATCHED, STATUS_NO_LYRICS)

# Evidence-strength ladder for the replacement guard. A resolved (matched / no_lyrics) row is
# replaced only when a NEW matched outcome carries a STRICTLY higher basis. The conservative
# matcher emits 'exact-title' for a real matched row; the best-of-* bases (FEAT-lyrics-best-of-
# promotion) sit just above fuzzy-title and below exact-title. Both best-of bases share the
# same rung so the strict-``>`` guard (a) lets an exact-title match supersede a best-of row,
# (b) refuses lateral best-of→best-of churn, (c) never lets a best-of displace an exact-title.
_BASIS_STRENGTH_MAP = {
    None: 0, "fuzzy-title": 1,
    "best-of-ambiguous": 2, "best-of-review": 2,
    "exact-title": 3, "mb-recording": 4, "isrc": 5,
}


def _basis_strength(basis: Optional[str]) -> int:
    """Strength of a basis on the replacement ladder (unknown basis ⇒ weakest)."""
    return _BASIS_STRENGTH_MAP.get(basis, 0)


def _base_version(version: Optional[str]) -> str:
    """``step2-v2+best-of-v1`` → ``step2-v2``: the decision core's version, promotion suffix off."""
    return (version or "").split("+", 1)[0]


def produced_by_other_matcher(row: Dict[str, Any]) -> bool:
    """True when the row's stored outcome came from a matcher other than the running one.

    Keyed on ``existing_matcher_version`` (the ``track_lyrics.matcher_version`` column).
    Rows selected without it — the demand-source job, the workspace backfill tool — are
    never eligible, so the lateral rule fails closed for them. The test is *different*,
    not *older*: the deployed matcher is the authority, and a sweep after a rollback should
    equally re-judge the rows the rolled-back version wrote.
    """
    if "existing_matcher_version" not in row:
        return False
    return _base_version(row.get("existing_matcher_version")) != MATCHER_VERSION


@dataclass(frozen=True)
class LateralAssessment:
    """How the running matcher's pick compares with the body a row already stores."""

    same_content: bool
    existing_cluster_size: int
    new_cluster_size: int

    @property
    def upgrade(self) -> bool:
        """The Step 2 lateral rule: a different body in a strictly larger content cluster."""
        return not self.same_content and self.new_cluster_size > self.existing_cluster_size


def assess_lateral(
    row: Dict[str, Any], outcome: MatchOutcome, candidates: Optional[List[Candidate]]
) -> Optional[LateralAssessment]:
    """Measure the stored body against today's candidates (FEAT-lyrics-match-correction Step 2).

    None when the rule cannot be applied: the outcome is not ``matched``, carries no
    consensus evidence, or there are no candidates to measure against. Otherwise:

    * ``new_cluster_size`` is the outcome's own ``content_cluster_size`` — the number of
      same-stance plausible candidates sharing the picked body, itself included.
    * ``existing_cluster_size`` is the same count for the STORED body: today's plausible
      candidates (``plausible_candidates``, the matcher's own gate) whose body agrees with it
      at ``CONTENT_AGREE_THRESHOLD``, plus the stored row's own vote when LRCLIB no longer
      returns that row (``/api/search`` caps at 20 and old ids do drop out — Bad Habit's
      8996695 had). Counted across both version stances, which can only inflate the
      existing side: the comparison is biased towards keeping.
    * ``same_content`` — the stored body and the new pick agree; a same-song transcription
      swap is never an upgrade.

    A ``no_lyrics`` sentinel stores no body: its cluster is 0, so a matched pick of any
    cluster size at the same basis counts as an upgrade.
    """
    if outcome.match_status != STATUS_MATCHED or not candidates:
        return None
    new_cluster = (outcome.evidence or {}).get("content_cluster_size")
    if not isinstance(new_cluster, int):
        return None
    stored = body_shingles(row.get("existing_lyric_synced"), row.get("existing_lyric_plain"))
    if stored is None:
        return LateralAssessment(False, 0, new_cluster)
    picked = body_shingles(outcome.lyric_synced, outcome.lyric_plain)
    plausible = plausible_candidates(
        row.get("title") or "", list(row.get("artist_names") or []),
        list(row.get("aliases") or []), row.get("duration_sec"), candidates,
    )
    existing_id = row.get("existing_lrclib_id")
    agreeing = 0
    returned = False
    for cand, *_ in plausible:
        if existing_id is not None and str(cand.id) == str(existing_id):
            returned = True
        if content_similarity(stored, content_shingles(cand)) >= CONTENT_AGREE_THRESHOLD:
            agreeing += 1
    existing_cluster = agreeing + (0 if returned else 1)
    same = content_similarity(stored, picked) >= CONTENT_AGREE_THRESHOLD
    return LateralAssessment(same, existing_cluster, new_cluster)


def should_replace(
    row: Dict[str, Any], outcome: MatchOutcome, candidates: Optional[List[Candidate]] = None
) -> bool:
    """Replacement guard consulted before persisting a re-evaluated outcome.

    ``row`` carries ``existing_status`` + ``existing_basis`` (the current row's state).
    Unresolved rows carry no good match to protect and are always rewritten (promote on
    success, refresh + rotate otherwise). A resolved good row is protected: only a NEW
    ``matched`` outcome with strictly stronger evidence may replace it — or, since
    FEAT-lyrics-match-correction Step 2, one at the SAME basis that passes the lateral rule
    (``assess_lateral``) on a row another matcher version wrote. A locked row never moves.
    """
    existing_status = row.get("existing_status")
    if existing_status in _UNRESOLVED:
        return True
    if row.get("existing_locked"):
        # A hand-fixed row (evidence.manual_fix; the Step 3a basis='manual') is the owner's
        # verdict. Nothing automatic replaces it, not even a stronger basis — the panel's
        # unlock is the only way back.
        return False
    if outcome.match_status != STATUS_MATCHED:
        return False  # never downgrade a resolved good row to an unresolved/no_lyrics state
    # A best-of-* matched row is now re-selected by the widened _fetch_unresolved_tracks
    # (FEAT-lyrics-best-of-promotion Step 2), so it reaches this guard. The strictly-``>``
    # ladder lets an exact-title / mb-recording / isrc match supersede it, refuses a
    # lateral best-of→best-of swap (both at rung 2), and never lets a best-of displace an
    # exact-title (3 > 3 is False) — the RFC's supersession-without-churn rule.
    new_strength = _basis_strength(outcome.match_basis)
    old_strength = _basis_strength(row.get("existing_basis"))
    if new_strength != old_strength:
        return new_strength > old_strength
    # Same rung. Before Step 2 every lateral swap was refused. Step 2 opens exactly one:
    # a row written by a DIFFERENT matcher version, whose stored body the running matcher
    # would not pick, and whose pick sits in a strictly larger content cluster than the
    # stored body does among today's candidates. Same-version rows, and rows selected
    # without version info, stay refused — that is still the no-churn rule.
    if not produced_by_other_matcher(row):
        return False
    lateral = assess_lateral(row, outcome, candidates)
    return lateral is not None and lateral.upgrade


def replacement_verdict(
    row: Dict[str, Any], outcome: MatchOutcome, candidates: Optional[List[Candidate]] = None
) -> Verdict:
    """The reassessment write gate: ``should_replace`` plus the Step 2 re-sweep record.

    For a RESOLVED row produced by another matcher version the verdict also carries an
    ``evidence["resweep"]`` block — stamped onto the row whether it is replaced or kept —
    and a metrics counter. The stamp is what makes the sweep terminate: the selection
    excludes rows already judged under the running version, so each row costs one LRCLIB
    call per matcher version, and a later bump re-opens all of them by itself. Unresolved
    rows are rewritten on every pass anyway and get no block.
    """
    write = should_replace(row, outcome, candidates)
    if row.get("existing_status") not in _RESOLVED or not produced_by_other_matcher(row):
        return Verdict(write)
    lateral = assess_lateral(row, outcome, candidates)
    ev = outcome.evidence or {}
    block: Dict[str, Any] = {
        "matcher_version": MATCHER_VERSION,
        "previous_matcher_version": row.get("existing_matcher_version"),
        "outcome_status": outcome.match_status,
        "outcome_basis": outcome.match_basis,
        "outcome_lrclib_id": ev.get("lrclib_id"),
    }
    if ev.get("reason"):
        block["outcome_reason"] = ev["reason"]
    if lateral is not None:
        block["same_content"] = lateral.same_content
        block["existing_cluster_size"] = lateral.existing_cluster_size
        block["new_cluster_size"] = lateral.new_cluster_size
    if row.get("existing_locked"):
        verdict = "locked"
    elif write and _basis_strength(outcome.match_basis) > _basis_strength(row.get("existing_basis")):
        verdict = "superseded"
    elif write:
        verdict = "replaced"
    elif outcome.match_status != STATUS_MATCHED:
        verdict = "parked"
    elif lateral is not None and lateral.same_content:
        verdict = "unchanged"
    else:
        verdict = "kept"
    block["verdict"] = verdict
    if write:
        block["replaced_lrclib_id"] = row.get("existing_lrclib_id")
    return Verdict(write, counter=f"resweep_{verdict}", evidence={"resweep": block})


# The columns every selection here projects from the existing ``track_lyrics`` row, so the
# replacement guard and the Step 2 re-sweep see one shape whichever arm selected the row.
# ``tl.track_id`` is in each GROUP BY, so these are functionally dependent on it. The
# lyric bodies are NOT here: they are attached afterwards for the chosen rows only
# (``_attach_stored_bodies``), so a pool-wide sort never carries 18k lyric texts.
_EXISTING_ROW_COLUMNS = """\
                       tl.match_status                     AS existing_status,
                       (tl.evidence ->> 'match_basis')     AS existing_basis,
                       tl.matcher_version                  AS existing_matcher_version,
                       ((tl.evidence ? 'manual_fix')
                        OR (tl.evidence ->> 'match_basis') = 'manual') AS existing_locked,
                       COALESCE(tl.evidence ->> 'lrclib_id',
                                tl.evidence #>> '{promotion,lrclib_id}') AS existing_lrclib_id"""


def _rows(rows) -> List[Dict[str, Any]]:
    return [
        {
            "id": r[0],
            "title": r[1],
            "duration_sec": r[2],
            "artist_names": list(r[3] or []),
            "aliases": list(r[4] or []),
            "existing_status": r[5],
            "existing_basis": r[6],
            "existing_matcher_version": r[7],
            "existing_locked": bool(r[8]),
            "existing_lrclib_id": r[9],
        }
        for r in rows
    ]


def _dedupe(tracks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for row in tracks:
        if row["id"] in seen:
            continue
        seen.add(row["id"])
        out.append(row)
    return out


class LyricsReassessmentService:
    """Re-evaluate unresolved corpus rows against current LRCLIB coverage."""

    def __init__(
        self,
        session: Session,
        client: Optional[LrclibClient] = None,
        *,
        concurrency: Optional[int] = None,
        time_budget_sec: Optional[float] = None,
    ) -> None:
        self.session = session
        self._client = client
        self.concurrency = concurrency or settings.LYRICS_REASSESS_CONCURRENCY
        self.time_budget_sec = (
            time_budget_sec if time_budget_sec is not None else settings.LYRICS_REASSESS_TIME_BUDGET_SEC
        )

    def reassess(
        self, limit: Optional[int] = None, sweep_limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """Re-check up to ``limit`` queue rows — best-of backlog first, then unresolved
        (stalest first) — plus up to ``sweep_limit`` resolved rows another matcher version
        wrote (FEAT-lyrics-match-correction Step 2), in one bounded LRCLIB batch.

        Reading the metrics: ``matched`` / ``no_lyrics`` are rows that left the unresolved
        pool (*promotions*); ``not_found`` / ``ambiguous`` / ``review_required`` are
        *refreshed* rows that stayed parked. Since Step 3b the batch also carries best-of
        rows, for which the story is different — ``bestof_superseded`` counts the ones
        upgraded to a stronger basis, and ``guard_kept`` counts the ones whose content the
        replacement guard protected (those are re-checked and rotated, never overwritten).
        The ``resweep_*`` counters are the Step 2 verdicts, one per re-judged resolved row;
        ``resweep_selected`` is how many the sweep arm selected this run.

        ``None`` means the setting; ``0`` means that arm runs empty, so
        ``reassess(limit=0, sweep_limit=N)`` is the sweep alone — the by-hand drain of the
        backlog a version bump re-opens, over the documented SQS fire path.
        """
        limit = limit if limit is not None else settings.LYRICS_REASSESS_BATCH_LIMIT
        sweep_limit = (
            sweep_limit if sweep_limit is not None else settings.LYRICS_RESWEEP_BATCH_LIMIT
        )
        # Recompute label-yield exclusions before selecting. This is what makes rule F
        # self-correcting: a label that starts matching releases its own rows here, in
        # the same pass that a newly-dead label claims its rows. It also repairs marks
        # that the matcher's `ON CONFLICT ... evidence = EXCLUDED.evidence` overwrote.
        exclusions = sync_exclusions(self.session)
        sweep = self._fetch_stale_version_tracks(sweep_limit) if sweep_limit > 0 else []
        queue = self._fetch_unresolved_tracks(limit) if limit > 0 else []
        # A best-of row can be due in both arms; the sweep's copy carries the same fields.
        tracks = self._attach_stored_bodies(_dedupe(sweep + queue))
        # The selections open a read transaction. Release it before `run_eval_batch`
        # starts the external LRCLIB loop.
        self.session.commit()
        metrics = run_eval_batch(
            self.session, tracks,
            concurrency=self.concurrency,
            time_budget_sec=self.time_budget_sec,
            client=self._client,
            should_write=replacement_verdict,
            touch_on_guard_kept=True,
            log_prefix="Lyrics reassessment",
        )
        metrics["exclusions"] = exclusions
        metrics["resweep_selected"] = len(sweep)
        if sweep:
            # WARNING, not INFO: prod Lambdas run LOG_LEVEL=WARNING, and this summary is
            # the only per-run signal the sweep leaves besides the stamps themselves.
            logger.warning(
                "Lyrics re-sweep under %s: selected=%d %s",
                MATCHER_VERSION, len(sweep),
                {k: v for k, v in metrics.items() if k.startswith("resweep_")},
            )
        return metrics

    def reassess_track(
        self, track_id: str, cooldown_sec: Optional[float] = None
    ) -> Dict[str, Any]:
        """Re-judge ONE row of any status, now (FEAT-lyrics-match-correction Step 2).

        The first-run check the RFC asks for before a bulk sweep: fire it at a known track,
        read the ``resweep`` verdict it leaves. Same matcher, same guard, same stamp as the
        scheduled pass; the label-yield exclusion and the sweep's lock filter are bypassed
        at selection (an explicit request is re-judged even when it would not be swept) —
        but not at the guard, so a locked row is judged, recorded as ``locked`` and never
        rewritten. ``cooldown_sec`` as in ``reassess_album``; pass ``0`` to force it.
        """
        cooldown_sec = (
            cooldown_sec if cooldown_sec is not None else settings.LYRICS_EXPEDITE_COOLDOWN_SEC
        )
        tracks = self._attach_stored_bodies(self._fetch_track(track_id, cooldown_sec))
        self.session.commit()
        metrics = run_eval_batch(
            self.session, tracks,
            concurrency=1,
            time_budget_sec=self.time_budget_sec,
            client=self._client,
            should_write=replacement_verdict,
            touch_on_guard_kept=True,
            log_prefix=f"Lyrics expedite (track {track_id})",
        )
        metrics["track_id"] = str(track_id)
        logger.warning("Lyrics track re-check %s: %s", track_id, metrics)  # prod LOG_LEVEL=WARNING
        return metrics

    def reassess_album(
        self,
        album_id: str,
        limit: Optional[int] = None,
        cooldown_sec: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Expedite one album: re-check its unresolved tracks NOW, out of turn.

        The periodic pass rotates stalest-first across a five-figure pool, so a just-released
        album whose lyrics DO exist on LRCLIB can sit months behind the queue head. This is the
        same evaluation under the same replacement guard, scoped to one album — nothing here
        can promote a row the daily job would not have promoted eventually.

        Two deliberate differences from ``reassess``:

        * **The label-yield exclusion is bypassed** (the selection below omits the
          ``excluded_by`` filter). An excluded row is one the pool decided not to spend a
          scheduled re-check on; an explicit request for THIS album overrides that, and a
          promotion here is also the strongest possible evidence that the exclusion was wrong.
        * **No ``sync_exclusions``.** That is pool-wide maintenance the daily pass owns; running
          it on a single-album request would add two writes and a lock for no benefit, since
          this path ignores the marks anyway.

        ``cooldown_sec`` is the idempotency bound: rows re-checked within the window are not
        selected. SQS delivers at least once and the writer's ``ON CONFLICT`` sets
        ``updated_at = NOW()``, so the cooldown is what makes a double-fire cheap instead of a
        second full LRCLIB sweep. Pass ``0`` to force an immediate re-run (``None`` ⇒ setting).

        Only tracks that already have a ``track_lyrics`` row are candidates; a brand-new album
        whose tracks have never been evaluated belongs to the incremental collector (Step 3),
        which the album-sync path chains automatically.
        """
        limit = limit if limit is not None else settings.LYRICS_REASSESS_BATCH_LIMIT
        cooldown_sec = (
            cooldown_sec if cooldown_sec is not None else settings.LYRICS_EXPEDITE_COOLDOWN_SEC
        )
        tracks = self._attach_stored_bodies(self._fetch_album_tracks(album_id, limit, cooldown_sec))
        # As above, do not leave the selection transaction open across LRCLIB.
        self.session.commit()
        metrics = run_eval_batch(
            self.session, tracks,
            concurrency=self.concurrency,
            time_budget_sec=self.time_budget_sec,
            client=self._client,
            should_write=replacement_verdict,
            touch_on_guard_kept=True,
            log_prefix=f"Lyrics expedite (album {album_id})",
        )
        metrics["album_id"] = str(album_id)
        return metrics

    def _fetch_album_tracks(
        self, album_id: str, limit: int, cooldown_sec: float
    ) -> List[Dict[str, Any]]:
        """Expedite targets: one album's unresolved (+ best-of) rows, exclusion bypassed.

        Deliberately a near-copy of ``_fetch_unresolved_tracks``: same status arms, same
        projection, same shared LATERAL — so the expedite evaluates exactly what the scheduled
        pass would, and the two cannot drift into disagreeing about what "unresolved" means.
        Two differences are the point of the method: the album filter, and the **absence**
        of the ``NOT (tl.evidence ? 'excluded_by')`` guard.

        Two further differences are scoping artefacts, not policy, and must stay that way:
        this path keeps unresolved rows FIRST (the scheduled pass inverts that to drain the
        best-of backlog — DATA-catalog-noise Step 3b), and it applies no best-of rest
        interval. Both are queue-fairness devices for a five-figure pool; an album holds a
        few dozen tracks and ``limit`` covers all of them, so ordering decides nothing here
        and a rest interval would only refuse work a human explicitly asked for.

        ``make_interval(secs => :cooldown_sec)`` rather than ``:cooldown_sec * INTERVAL '1 s'``:
        the function signature pins the bind parameter to ``double precision``, where the
        multiplication form leaves Postgres to infer a bare parameter's type and fail with
        "could not determine data type" (the workspace #84 → #85 failure, verbatim).
        """
        rows = self.session.execute(
            text(
                f"""
                SELECT t.id, t.title, t.duration_sec,
                       primary_artists.artist_names                     AS artist_names,
                       ARRAY_REMOVE(ARRAY_AGG(DISTINCT al.alias), NULL) AS aliases,
{_EXISTING_ROW_COLUMNS}
                FROM track_lyrics tl
                JOIN tracks t         ON t.id = tl.track_id
                JOIN track_artists ta ON ta.track_id = t.id
                JOIN artists a        ON a.id = ta.artist_id
                LEFT JOIN LATERAL jsonb_array_elements_text(a.aliases) AS al(alias) ON true
{PRIMARY_ARTIST_NAMES_LATERAL}
                WHERE t.album_id = :album_id
                  AND tl.updated_at < NOW() - make_interval(secs => :cooldown_sec)
                  AND (tl.match_status IN ('not_found', 'ambiguous', 'review_required')
                       OR (tl.match_status = 'matched'
                           AND tl.evidence ->> 'match_basis' LIKE 'best-of-%'))
                GROUP BY tl.track_id, t.id, t.title, t.duration_sec,
                         primary_artists.artist_names
                ORDER BY
                    CASE WHEN tl.match_status IN ('not_found', 'ambiguous', 'review_required')
                         THEN 0 ELSE 1 END,
                    tl.updated_at ASC
                LIMIT :limit
                """
            ),
            {"album_id": album_id, "cooldown_sec": cooldown_sec, "limit": limit},
        ).fetchall()
        return _rows(rows)

    def _fetch_unresolved_tracks(self, limit: int) -> List[Dict[str, Any]]:
        """Reassessment targets: best-of-* matched rows first, then the unresolved pool.

        FEAT-lyrics-best-of-promotion Step 2 widens the selection: a ``best-of-*``
        ``matched`` row must stay a reassessment target so a later ``exact-title`` match
        can supersede it via the replacement guard — without this arm a promoted row
        would leave the pool forever and the promised supersession could never occur.

        **That arm had never executed.** It sat behind the unresolved pool, and a
        stalest-first ``ORDER BY`` with ``LIMIT 150`` reaches it only when fewer than 150
        unresolved rows exist. Measured against prod 2026-08-03: 11,033 unresolved rows
        ahead of 2,765 best-of rows, with ~57 new unresolved rows arriving per day. The
        pool never empties, so the promise was structurally unreachable rather than merely
        rare.

        DATA-catalog-noise Step 3b therefore **inverts the priority**. The justification is
        measured yield per re-check, live against LRCLIB on 2026-08-03: **83.6% of best-of
        rows (112/134 sampled) now resolve to a strictly stronger ``exact-title`` match**,
        against **~5.5%** for the unresolved pool. A best-of slot is worth ~15 unresolved
        slots, so the backlog is drained first. (The RFC's own proposal for this method — a
        recency-tiered re-order of the unresolved pool — was measured and dropped: yield is
        flat across release-recency tiers, 10.95 / 10.77 / 8.70%. See the RFC's Step 3b.)

        **Why this terminates.** A superseded row leaves the ``best-of-%`` arm by changing
        its own basis, so the backlog is self-consuming: ~2,765 rows at 150/run ≈ 19 runs.
        The ~16% that cannot be superseded are the hazard — the guard refuses a lateral
        best-of→best-of swap, so they are re-checked but never rewritten. Two things stop
        them pinning the queue head forever: ``TrackLyricsWriter.touch`` advances their
        rotation cursor even when the guard keeps the row, and the interval below then
        rests them for 30 days. Once the backlog clears, this arm goes quiet on its own and
        the budget returns to unresolved recovery, no human action and no follow-up PR.

        Within each arm ``ORDER BY tl.updated_at ASC`` re-checks the longest-parked rows
        first; because a rewrite bumps ``updated_at`` (the writer's
        ``ON CONFLICT ... updated_at = NOW()``), reassessment rotates fairly across the
        whole pool over successive runs.

        ``make_interval(days => :n)`` rather than ``:n * INTERVAL '1 day'``: the function
        signature pins the bind parameter's type, where the multiplication form leaves
        Postgres to infer a bare parameter and fail with "could not determine data type"
        (the worker #84 → #85 failure, verbatim).
        """
        rows = self.session.execute(
            text(
                f"""
                SELECT t.id, t.title, t.duration_sec,
                       primary_artists.artist_names                     AS artist_names,
                       ARRAY_REMOVE(ARRAY_AGG(DISTINCT al.alias), NULL) AS aliases,
{_EXISTING_ROW_COLUMNS}
                FROM track_lyrics tl
                JOIN tracks t         ON t.id = tl.track_id
                JOIN track_artists ta ON ta.track_id = t.id
                JOIN artists a        ON a.id = ta.artist_id
                LEFT JOIN LATERAL jsonb_array_elements_text(a.aliases) AS al(alias) ON true
{PRIMARY_ARTIST_NAMES_LATERAL}
                WHERE NOT (tl.evidence ? 'excluded_by')
                  AND (tl.match_status IN ('not_found', 'ambiguous', 'review_required')
                       OR (tl.match_status = 'matched'
                           AND tl.evidence ->> 'match_basis' LIKE 'best-of-%'
                           AND tl.updated_at
                               < NOW() - make_interval(days => :bestof_rest_days)))
                GROUP BY tl.track_id, t.id, t.title, t.duration_sec,
                         primary_artists.artist_names
                ORDER BY
                    CASE WHEN tl.match_status = 'matched' THEN 0 ELSE 1 END,
                    tl.updated_at ASC
                LIMIT :limit
                """
            ),
            {"limit": limit, "bestof_rest_days": settings.LYRICS_BESTOF_RECHECK_INTERVAL_DAYS},
        ).fetchall()
        return _rows(rows)


    def _fetch_stale_version_tracks(self, limit: int) -> List[Dict[str, Any]]:
        """Re-sweep targets (FEAT-lyrics-match-correction Step 2): RESOLVED rows whose decision
        core version is not the running one, oldest version first.

        Same joins and projection as the scheduled selection, so a swept row is judged exactly
        like a queued one. The filters are the point:

        * ``matched`` / ``no_lyrics`` only — unresolved rows are the queue's job and are
          rewritten (and re-versioned) on every pass anyway;
        * the decision core's version differs from ``MATCHER_VERSION`` (the ``+best-of-v1``
          promotion suffix is dropped first, so a ``step2-v2+best-of-v1`` row is swept too);
        * not already stamped under the running version (``evidence.resweep.matcher_version``)
          — the stamp is what makes this arm terminate, and a version bump invalidates every
          stamp at once, which is how the sweep re-opens itself;
        * not hand-fixed (``manual_fix`` / ``basis='manual'``): the guard would keep such a
          row anyway, so the LRCLIB call is not spent. ``reassess_track`` bypasses this one.

        Ordering parses ``stepN-vM`` out of the version string (``substring(... from
        pattern)`` returns the first capture group; unparseable ⇒ 0, i.e. first) so
        ``step10-v1`` sorts after ``step9-v1`` — lexical order would not. Within a version,
        stalest first.
        """
        rows = self.session.execute(
            text(
                f"""
                SELECT t.id, t.title, t.duration_sec,
                       primary_artists.artist_names                     AS artist_names,
                       ARRAY_REMOVE(ARRAY_AGG(DISTINCT al.alias), NULL) AS aliases,
{_EXISTING_ROW_COLUMNS}
                FROM track_lyrics tl
                JOIN tracks t         ON t.id = tl.track_id
                JOIN track_artists ta ON ta.track_id = t.id
                JOIN artists a        ON a.id = ta.artist_id
                LEFT JOIN LATERAL jsonb_array_elements_text(a.aliases) AS al(alias) ON true
{PRIMARY_ARTIST_NAMES_LATERAL}
                WHERE NOT (tl.evidence ? 'excluded_by')
                  AND tl.match_status IN ('matched', 'no_lyrics')
                  AND split_part(COALESCE(tl.matcher_version, ''), '+', 1) <> :current_version
                  AND COALESCE(tl.evidence #>> '{{resweep,matcher_version}}', '')
                      <> :current_version
                  AND NOT (tl.evidence ? 'manual_fix')
                  AND COALESCE(tl.evidence ->> 'match_basis', '') <> 'manual'
                GROUP BY tl.track_id, t.id, t.title, t.duration_sec,
                         primary_artists.artist_names
                ORDER BY
                    COALESCE(substring(tl.matcher_version FROM '^step(\\d+)')::int, 0),
                    COALESCE(substring(tl.matcher_version FROM '^step\\d+-v(\\d+)')::int, 0),
                    tl.updated_at ASC
                LIMIT :limit
                """
            ),
            {"current_version": MATCHER_VERSION, "limit": limit},
        ).fetchall()
        return _rows(rows)

    def _fetch_track(self, track_id: str, cooldown_sec: float) -> List[Dict[str, Any]]:
        """One explicitly requested row, any status; exclusion and lock filters bypassed."""
        rows = self.session.execute(
            text(
                f"""
                SELECT t.id, t.title, t.duration_sec,
                       primary_artists.artist_names                     AS artist_names,
                       ARRAY_REMOVE(ARRAY_AGG(DISTINCT al.alias), NULL) AS aliases,
{_EXISTING_ROW_COLUMNS}
                FROM track_lyrics tl
                JOIN tracks t         ON t.id = tl.track_id
                JOIN track_artists ta ON ta.track_id = t.id
                JOIN artists a        ON a.id = ta.artist_id
                LEFT JOIN LATERAL jsonb_array_elements_text(a.aliases) AS al(alias) ON true
{PRIMARY_ARTIST_NAMES_LATERAL}
                WHERE t.id = :track_id
                  AND tl.updated_at < NOW() - make_interval(secs => :cooldown_sec)
                GROUP BY tl.track_id, t.id, t.title, t.duration_sec,
                         primary_artists.artist_names
                """
            ),
            {"track_id": track_id, "cooldown_sec": cooldown_sec},
        ).fetchall()
        return _rows(rows)

    def _attach_stored_bodies(self, tracks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Add ``existing_lyric_plain`` / ``existing_lyric_synced`` to the chosen rows.

        Only ``matched`` rows store a body and only the Step 2 lateral rule reads it, so the
        pool-wide selections stay body-free and this one short query fetches the texts for
        the rows that were actually chosen. Runs inside the selection transaction, before the
        caller commits it ahead of the LRCLIB loop.
        """
        ids = [row["id"] for row in tracks if row.get("existing_status") == STATUS_MATCHED]
        for row in tracks:
            row.setdefault("existing_lyric_plain", None)
            row.setdefault("existing_lyric_synced", None)
        if not ids:
            return tracks
        # Keyed by str: callers hand in UUID objects (selections) or strings (a track id
        # from an SQS body), and the driver returns UUIDs.
        bodies = {
            str(r[0]): (r[1], r[2])
            for r in self.session.execute(
                text(
                    "SELECT track_id, lyric_plain, lyric_synced FROM track_lyrics "
                    "WHERE track_id = ANY(:ids)"
                ),
                {"ids": ids},
            ).fetchall()
        }
        for row in tracks:
            plain, synced = bodies.get(str(row["id"]), (None, None))
            row["existing_lyric_plain"] = plain
            row["existing_lyric_synced"] = synced
        return tracks
