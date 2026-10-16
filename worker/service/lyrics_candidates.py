# worker/service/lyrics_candidates.py
"""FEAT-lyrics-match-correction Step 3a: the owner's candidate panel, worker side.

Three pieces, all keyed by track:

* **Rejections** (``track_lyrics_rejections``). ``load_rejections`` reads the ids the owner
  rejected and ``drop_rejected`` removes them from a candidate list BEFORE ``decide_match``
  sees it, so a rejected candidate can never win, cluster, or be promoted again.
  ``run_eval_batch`` applies it to every producer that runs through it.
* **Judging** (``judge_candidates``). Pure. Says, per LRCLIB candidate, why it would win or
  lose under the running matcher — the same gates ``decide_match`` applies, in the same
  order — so the panel can show the reason next to each one.
* **The stored set** (``replace_candidate_set``). Written only by an explicit single-track
  re-check (the panel's refresh), never by the scheduled jobs (owner decision 2026-10-16):
  the set is replaced wholesale, metadata + a short preview only.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, Iterable, List, Optional, Set

from sqlalchemy import text
from sqlalchemy.orm import Session

from worker.service.lyrics_matcher import (
    CONTENT_AGREE_THRESHOLD,
    MATCHER_VERSION,
    STATUS_MATCHED,
    STATUS_NO_LYRICS,
    TITLE_FUZZY_THRESHOLD,
    _LANGUAGE_TOKENS,
    Candidate,
    MatchOutcome,
    TitleNormalizer,
    _artist_identity_ok,
    _identity_norms,
    canonical_base_title,
    content_shingles,
    content_similarity,
    duration_matches,
    exact_base_equal,
    extract_version_tokens,
    plain_base_title,
)

logger = logging.getLogger(__name__)

# The V63 CHECK caps the stored preview at 300 characters; stay well inside it.
PREVIEW_MAX_CHARS = 200
PREVIEW_MAX_LINES = 4

# Verdicts, in the order a candidate meets them. The panel maps each to Korean copy.
VERDICT_PICKED = "picked"                  # the running matcher's pick
VERDICT_SAME_BODY = "same_body"            # agrees with the pick's body
VERDICT_OUTVOTED = "outvoted"              # plausible, but a larger cluster won
VERDICT_VERSION_MISMATCH = "version_mismatch"  # a different rendition (live, Japanese ver., …)
VERDICT_FUZZY_TITLE = "fuzzy_title"        # title only fuzzily equal — never auto-attached
VERDICT_NO_BODY = "no_body"                # instrumental or empty upload
VERDICT_UNDECIDED = "undecided"            # plausible, but the matcher picked nothing
VERDICT_ARTIST_MISMATCH = "artist_mismatch"
VERDICT_TITLE_MISMATCH = "title_mismatch"
VERDICT_DURATION_MISMATCH = "duration_mismatch"
VERDICT_REJECTED = "rejected"              # the owner rejected it for this track

_LRC_TIMESTAMP_RE = re.compile(r"\[\d{1,3}:\d{2}(?:[.:]\d{1,3})?\]")


def _int_id(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ── rejections ────────────────────────────────────────────────────────────────────────


def load_rejections(session: Session, track_ids: Iterable[Any]) -> Dict[str, Set[int]]:
    """``{str(track_id): {lrclib_id, …}}`` for the tracks that have any rejection.

    One short read, committed here: it runs right before the caller's external LRCLIB
    loop, which must not start inside an open transaction.
    """
    ids = [str(t) for t in track_ids]
    if not ids:
        return {}
    out: Dict[str, Set[int]] = {}
    rows = session.execute(
        text(
            "SELECT track_id, lrclib_id FROM track_lyrics_rejections "
            "WHERE track_id = ANY(CAST(:ids AS uuid[]))"
        ),
        {"ids": ids},
    ).fetchall()
    session.commit()
    for track_id, lrclib_id in rows:
        out.setdefault(str(track_id), set()).add(int(lrclib_id))
    return out


def drop_rejected(candidates: List[Candidate], rejected: Optional[Set[int]]) -> List[Candidate]:
    """``candidates`` without the ones the owner rejected for this track."""
    if not rejected:
        return candidates
    return [c for c in candidates if _int_id(c.id) not in rejected]


# ── judging ───────────────────────────────────────────────────────────────────────────


def preview_of(cand: Candidate) -> Optional[str]:
    """The first few lyric lines, LRC timestamps removed, capped at ``PREVIEW_MAX_CHARS``.

    Enough to tell two bodies apart on the panel; never the body. Plain text first, synced
    when an upload carries only that.
    """
    body = cand.plain_lyrics or cand.synced_lyrics or ""
    lines: List[str] = []
    for raw in body.splitlines():
        line = _LRC_TIMESTAMP_RE.sub("", raw).strip()
        if line:
            lines.append(line)
        if len(lines) >= PREVIEW_MAX_LINES:
            break
    if not lines:
        return None
    preview = " / ".join(lines).replace("\x00", "")
    if len(preview) > PREVIEW_MAX_CHARS:
        preview = preview[: PREVIEW_MAX_CHARS - 1].rstrip() + "…"
    return preview


def _picked_id(outcome: Optional[MatchOutcome]) -> Optional[int]:
    if outcome is None or outcome.match_status not in (STATUS_MATCHED, STATUS_NO_LYRICS):
        return None
    ev = outcome.evidence or {}
    picked = ev.get("lrclib_id")
    if picked is None and isinstance(ev.get("promotion"), dict):
        picked = ev["promotion"].get("lrclib_id")
    return _int_id(picked)


def _rendition_free(raw_title: str) -> str:
    """The base title with language names dropped as well — ``canonical_base_title``
    keeps them (``go go japanese``), which is right for matching and wrong for telling the
    owner WHY a candidate lost: that one is another rendition, not another song."""
    return " ".join(w for w in canonical_base_title(raw_title).split() if w not in _LANGUAGE_TOKENS)


def _gate(
    cand: Candidate,
    *,
    title: str,
    identity_norms: List[str],
    duration_sec: Optional[float],
) -> tuple:
    """``(failed_verdict | None, kind)`` — ``plausible_candidates``' gates, in its order."""
    if not _artist_identity_ok(TitleNormalizer.normalize(cand.artist), identity_norms):
        return VERDICT_ARTIST_MISMATCH, None
    if not TitleNormalizer.normalize(cand.title):
        return VERDICT_TITLE_MISMATCH, None
    stripped_track = canonical_base_title(title)
    plain_track = plain_base_title(title)
    plain_cand = plain_base_title(cand.title)
    if stripped_track and exact_base_equal(title, cand.title):
        kind = "exact"
    elif plain_track and plain_cand and (
        TitleNormalizer.similarity(plain_track, plain_cand) >= TITLE_FUZZY_THRESHOLD
    ):
        kind = "fuzzy"
    elif (extract_version_tokens(cand.title) ^ extract_version_tokens(title)) and (
        _rendition_free(cand.title) and _rendition_free(cand.title) == _rendition_free(title)
    ):
        # Same song, other rendition: the matcher's title gate drops it, but "another
        # version" is the reason the owner needs to see (``Go Go (Japanese Ver.)``).
        return VERDICT_VERSION_MISMATCH, None
    else:
        return VERDICT_TITLE_MISMATCH, None
    if not duration_matches(duration_sec, cand.duration_sec):
        return VERDICT_DURATION_MISMATCH, kind
    return None, kind


def judge_candidates(
    *,
    title: str,
    artist_names: List[str],
    aliases: Optional[List[str]],
    duration_sec: Optional[float],
    candidates: List[Candidate],
    outcome: Optional[MatchOutcome],
    rejected: Optional[Set[int]] = None,
) -> List[Dict[str, Any]]:
    """One panel row per distinct LRCLIB id: metadata, preview, cluster size, verdict.

    ``candidates`` is what LRCLIB returned, rejected ones included (they are shown as
    ``rejected``; the decision itself never saw them). ``outcome`` is the running matcher's
    decision over the non-rejected rest. Pure.
    """
    title = title or ""
    rejected = rejected or set()
    identity_norms = _identity_norms(artist_names, aliases)
    track_tokens = extract_version_tokens(title)
    picked_id = _picked_id(outcome)

    seen: Set[int] = set()
    rows: List[Dict[str, Any]] = []
    for cand in candidates:
        cid = _int_id(cand.id)
        if cid is None or cid in seen:
            continue
        seen.add(cid)
        failed, kind = _gate(
            cand, title=title, identity_norms=identity_norms, duration_sec=duration_sec
        )
        rows.append({
            "cand": cand,
            "lrclib_id": cid,
            "failed": failed,
            "kind": kind,
            "agrees": not (extract_version_tokens(cand.title) ^ track_tokens),
            "shingles": content_shingles(cand),
        })

    plausible = [r for r in rows if r["failed"] is None and r["lrclib_id"] not in rejected]
    picked = next((r for r in plausible if r["lrclib_id"] == picked_id), None)

    out: List[Dict[str, Any]] = []
    for r in rows:
        cand = r["cand"]
        cluster: Optional[int] = None
        if r["failed"] is None and r["shingles"] is not None:
            cluster = sum(
                1 for o in plausible
                if o["shingles"] is not None and o["agrees"] == r["agrees"]
                and (o is r or content_similarity(r["shingles"], o["shingles"])
                     >= CONTENT_AGREE_THRESHOLD)
            )
            if r["lrclib_id"] in rejected:
                cluster = None  # it took no part in the vote
        if r["lrclib_id"] in rejected:
            verdict = VERDICT_REJECTED
        elif r["failed"] is not None:
            verdict = r["failed"]
        elif r["lrclib_id"] == picked_id:
            verdict = VERDICT_PICKED
        elif not r["agrees"]:
            verdict = VERDICT_VERSION_MISMATCH
        elif r["kind"] == "fuzzy":
            verdict = VERDICT_FUZZY_TITLE
        elif r["shingles"] is None:
            verdict = VERDICT_NO_BODY
        elif picked is not None and picked["shingles"] is not None:
            same = content_similarity(r["shingles"], picked["shingles"]) >= CONTENT_AGREE_THRESHOLD
            verdict = VERDICT_SAME_BODY if same else VERDICT_OUTVOTED
        else:
            verdict = VERDICT_UNDECIDED
        out.append({
            "lrclib_id": r["lrclib_id"],
            "title": (cand.title or "")[:500],
            "artist": (cand.artist or "")[:500],
            "album": (cand.album or None) and cand.album[:500],
            "duration_sec": cand.duration_sec,
            "has_synced": bool(cand.synced_lyrics),
            "has_plain": bool(cand.plain_lyrics),
            "instrumental": bool(cand.instrumental),
            "preview": preview_of(cand),
            "cluster_size": cluster,
            "verdict": verdict,
        })
    return out


# ── the stored set ────────────────────────────────────────────────────────────────────


def replace_candidate_set(session: Session, track_id: Any, rows: List[Dict[str, Any]]) -> None:
    """Replace a track's stored candidate set with ``rows`` in one transaction; commits.

    Delete-then-insert rather than a merge: the set is what ONE re-check saw, and a
    candidate LRCLIB stopped returning must not linger as if it had. Inserts go in
    ``lrclib_id`` order (the conflict key), the repo's rule for bulk writes.
    """
    session.execute(
        text("DELETE FROM track_lyrics_candidates WHERE track_id = :tid"), {"tid": track_id}
    )
    for row in sorted(rows, key=lambda r: r["lrclib_id"]):
        session.execute(
            text(
                """
                INSERT INTO track_lyrics_candidates
                    (track_id, lrclib_id, title, artist, album, duration_sec,
                     has_synced, has_plain, instrumental, preview, cluster_size,
                     verdict, matcher_version)
                VALUES
                    (:track_id, :lrclib_id, :title, :artist, :album, :duration_sec,
                     :has_synced, :has_plain, :instrumental, :preview, :cluster_size,
                     :verdict, :matcher_version)
                """
            ),
            {
                **{k: (v.replace("\x00", "") if isinstance(v, str) else v) for k, v in row.items()},
                "track_id": track_id,
                "matcher_version": MATCHER_VERSION,
            },
        )
    session.commit()
