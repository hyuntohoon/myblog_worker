"""
FEAT-lyrics-corpus Step 2: canonical conservative lyrics matcher.

A single decision core (``decide_match``) over normalized candidate dicts, plus
an LRCLIB SQLite-dump candidate provider and a ``track_lyrics`` writer. Both the
Phase 1 API runner (``tools/lyrics_dump_matcher.py``) and the Phase 2 dump batch
(``tools/lyrics_batch_dump.py``) import the decision core from here, so there is
exactly one matcher logic.

Built against the REAL LRCLIB schema (verified 2026-07-01 from lrclib.net/docs
+ repo ``server/migrations/01-initial/up.sql``)::

    tracks(id, name, name_lower, artist_name, artist_name_lower, album_name,
           album_name_lower, duration FLOAT seconds, last_lyrics_id, ...)
    lyrics(id, plain_lyrics, synced_lyrics, track_id, has_plain_lyrics,
           has_synced_lyrics, instrumental, source, lyricsfile)
    -- joined via tracks.last_lyrics_id -> lyrics.id

LRCLIB exposes NO isrc anywhere (API nor dump), so ISRC is NOT a match signal
here. Artist identity is resolved through the catalog's own ``artists.aliases``.
Correctness-first: ambiguity / weak evidence / version-token conflicts park in
``ambiguous`` / ``review_required`` and are never auto-attached. RFC:
``docs/rfcs/FEAT-lyrics-corpus.md``.
"""

import json
import logging
import re
import sqlite3
import unicodedata
import uuid
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Optional, Set

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

MATCHER_VERSION = "step3-v1"

# Duration tolerance. LRCLIB's own /api/get docs gate on +/-2s; we mirror it.
DURATION_TOLERANCE_SEC = 2.0
# Above this fuzzy ratio a normalized title is a "fuzzy" candidate (else dropped).
TITLE_FUZZY_THRESHOLD = 0.80

# Rendition tokens. If a candidate carries one the track title does not (or vice
# versa) they are different versions -> park, never auto-merge. Feature markers
# (feat / featuring) are intentionally NOT here.
_VERSION_TOKENS_EN = {
    "remix", "remixed", "live", "demo", "edit", "edited", "cover", "covered",
    "remaster", "remastered", "remasterd", "rerecording", "recording",
    "rerecorded", "instrumental", "acoustic", "radio", "club", "extended",
    "version", "unplugged", "session", "karaoke", "orchestral", "symphonic",
    "plur", "bonustrack",
}
_VERSION_TOKENS_KO = {
    "리믹스", "라이브", "데모", "커버", "어쿠스틱", "리마스터", "재녹음", "편곡",
    "반주", "오케스트라",
}
_VERSION_TOKENS = _VERSION_TOKENS_EN | _VERSION_TOKENS_KO

# Match-status vocabulary (mirrors the RFC + V33 CHECK constraint).
STATUS_MATCHED = "matched"
STATUS_NO_LYRICS = "no_lyrics"
STATUS_NOT_FOUND = "not_found"
STATUS_AMBIGUOUS = "ambiguous"
STATUS_REVIEW_REQUIRED = "review_required"


class TitleNormalizer:
    """Normalize titles/artists for comparison."""

    @staticmethod
    def normalize(value: str) -> str:
        """Lowercase, strip diacritics, drop punctuation (keep alnum + space + apostrophe), collapse whitespace.

        NFD-strips Latin combining marks then NFC-recomposes — critical so Hangul
        syllables stay composed (NFD alone decomposes 좋은날 into jamo). Hyphens
        become spaces (so ``re-recording`` splits into tokens). Korean alnum chars
        survive (``str.isalnum()`` is True for Hangul); only punctuation is removed.
        """
        if not value:
            return ""
        value = value.lower()
        decomposed = unicodedata.normalize("NFD", value)
        stripped = "".join(c for c in decomposed if unicodedata.category(c) != "Mn")
        value = unicodedata.normalize("NFC", stripped)  # re-compose Hangul syllables
        value = "".join(c if (c.isalnum() or c in " '") else " " for c in value)
        return " ".join(value.split())

    @staticmethod
    def similarity(a: str, b: str) -> float:
        return SequenceMatcher(None, a, b).ratio()


def extract_version_tokens(value: str) -> Set[str]:
    """Return the rendition tokens present in ``value`` (lowercase, normalized)."""
    norm = TitleNormalizer.normalize(value)
    words = set(re.split(r"[\s'\-]+", norm))
    return words & _VERSION_TOKENS


def _strip_version_tokens(norm_title: str) -> str:
    """Remove rendition tokens from an already-normalized title.

    Lets a base title match across versions (``Crazy`` vs ``Crazy (Remix)`` both
    reduce to ``crazy``); the version check then decides match vs park.
    """
    return " ".join(
        w for w in re.split(r"[\s'\-]+", norm_title) if w and w not in _VERSION_TOKENS
    )


# Crowd-data title noise (FIX-lyrics-matcher-noise). A leading track-number
# prefix requires digits + separator + whitespace so genuine numeric titles
# ("1999", "7 rings", "1-800-273-8255") are never touched.
_TRACK_NO_PREFIX_RE = re.compile(r"^\s*\d{1,2}\s*[-–—.]\s+")
_PAREN_SEGMENT_RE = re.compile(r"\(([^()]*)\)|\[([^\[\]]*)\]")


def _strip_noise(raw_title: str) -> str:
    """Strip crowd-data title noise BEFORE normalization.

    (1) a leading track-number prefix (``01 - Title``) when text remains after
    it; (2) parenthetical/bracket segments carrying NO version token
    (``(feat. X)``, ``(From "Movie")``) — rendition parentheticals (``(Live)``)
    are kept so version-token logic still sees and gates them. Falls back to
    the original title if stripping would leave nothing.
    """
    if not raw_title:
        return raw_title
    out = _TRACK_NO_PREFIX_RE.sub("", raw_title, count=1)

    def _drop_non_rendition(m: "re.Match[str]") -> str:
        inner = m.group(1) if m.group(1) is not None else m.group(2)
        return m.group(0) if extract_version_tokens(inner or "") else " "

    out = _PAREN_SEGMENT_RE.sub(_drop_non_rendition, out)
    out = " ".join(out.split())
    return out if out else raw_title


def canonical_base_title(raw_title: str) -> str:
    """Noise-stripped, normalized, version-token-free base title.

    The single comparison/grouping key for both ``decide_match`` and the
    best-of tier-1 gate — applied symmetrically to catalog and candidate
    titles (FIX-lyrics-matcher-noise OQ1/OQ2).
    """
    return _strip_version_tokens(TitleNormalizer.normalize(_strip_noise(raw_title or "")))


def duration_matches(catalog_sec: Optional[float], candidate_sec: Optional[float],
                     tolerance: float = DURATION_TOLERANCE_SEC) -> bool:
    """True if durations are within ``tolerance`` seconds (both in seconds)."""
    if catalog_sec is None or candidate_sec is None:
        return False
    try:
        return abs(float(catalog_sec) - float(candidate_sec)) <= tolerance
    except (TypeError, ValueError):
        return False


_ARTICLE_NOISE = {"the", "a", "an"}


def _artist_identity_ok(cand_artist_norm: str, identity_norms: Iterable[str]) -> bool:
    """Artist-identity gate.

    Accept exact normalized equality, or a difference that is ONLY articles
    ("the beatles" vs "beatles"). Deliberately tight: arbitrary substring is NOT
    accepted (``drake`` does not match ``drake miller band``) — aliases carry the
    burden of credited/featured identity. Loose artist retrieval (LIKE / API
    search) feeds candidates; this gate is the precision filter.
    """
    if not cand_artist_norm:
        return False
    cand_words = set(cand_artist_norm.split())
    for ident in identity_norms:
        if not ident:
            continue
        if cand_artist_norm == ident:
            return True
        ident_words = set(ident.split())
        if cand_words and ident_words and (cand_words ^ ident_words) <= _ARTICLE_NOISE:
            return True
    return False


@dataclass
class Candidate:
    """A normalized LRCLIB candidate (source-agnostic)."""

    id: Any
    title: str
    artist: str
    album: Optional[str]
    duration_sec: Optional[float]
    instrumental: bool
    plain_lyrics: Optional[str]
    synced_lyrics: Optional[str]

    @classmethod
    def from_api(cls, rec: Dict) -> "Candidate":
        """Adapt an LRCLIB /api/search or /api/get record (camelCase, seconds)."""
        return cls(
            id=rec.get("id"),
            title=rec.get("trackName") or "",
            artist=rec.get("artistName") or "",
            album=rec.get("albumName"),
            duration_sec=rec.get("duration"),
            instrumental=bool(rec.get("instrumental")),
            plain_lyrics=rec.get("plainLyrics"),
            synced_lyrics=rec.get("syncedLyrics"),
        )

    @classmethod
    def from_dump_row(cls, row: sqlite3.Row) -> "Candidate":
        """Adapt a real-schema dump row (snake_case, tracks+lyrics joined)."""
        d = dict(row)
        return cls(
            id=d.get("id"),
            title=d.get("name") or "",
            artist=d.get("artist_name") or "",
            album=d.get("album_name"),
            duration_sec=d.get("duration"),
            instrumental=bool(d.get("instrumental")),
            plain_lyrics=d.get("plain_lyrics"),
            synced_lyrics=d.get("synced_lyrics"),
        )


@dataclass
class MatchOutcome:
    """A single track's match outcome."""

    track_id: uuid.UUID
    match_status: str
    evidence: Dict
    lyric_plain: Optional[str] = None
    lyric_synced: Optional[str] = None
    matcher_version: str = MATCHER_VERSION
    match_basis: Optional[str] = None  # exact-title | fuzzy-title | None
    version_tokens_track: List[str] = field(default_factory=list)
    version_tokens_candidate: List[str] = field(default_factory=list)
    version_agrees: Optional[bool] = None


def _build(
    track_id: uuid.UUID,
    status: str,
    evidence: Dict,
    *,
    lyric_plain: Optional[str] = None,
    lyric_synced: Optional[str] = None,
    match_basis: Optional[str] = None,
    track_tokens: Optional[Set[str]] = None,
    cand_tokens: Optional[Set[str]] = None,
    version_agrees: Optional[bool] = None,
) -> MatchOutcome:
    track_tokens = sorted(track_tokens or set())
    cand_tokens = sorted(cand_tokens or set())
    # version_agrees is meaningful only when a candidate was considered.
    if version_agrees is None and cand_tokens is not None:
        version_agrees = (set(track_tokens) == set(cand_tokens)) if track_tokens or cand_tokens else None
    return MatchOutcome(
        track_id=track_id,
        match_status=status,
        evidence=evidence,
        lyric_plain=lyric_plain,
        lyric_synced=lyric_synced,
        match_basis=match_basis,
        version_tokens_track=track_tokens,
        version_tokens_candidate=cand_tokens,
        version_agrees=version_agrees,
    )


def decide_match(
    track_id: uuid.UUID,
    title: str,
    artist_names: List[str],
    aliases: Optional[List[str]],
    duration_sec: Optional[float],
    candidates: List[Candidate],
) -> MatchOutcome:
    """Pure, no-I/O conservative match decision.

    Evidence priority: (1) artist identity via ``artist_names`` + ``aliases``,
    (2) normalized title exact / fuzzy, (3) duration within tolerance,
    (4) version-token agreement. Outcomes: matched / no_lyrics / not_found /
    ambiguous / review_required.
    """
    track_tokens = extract_version_tokens(title or "")
    stripped_track = canonical_base_title(title or "")

    identity_norms = [TitleNormalizer.normalize(n) for n in (artist_names or []) if n]
    for a in (aliases or []):
        if isinstance(a, str) and a:
            identity_norms.append(TitleNormalizer.normalize(a))

    if not artist_names:
        return _build(
            track_id, STATUS_NOT_FOUND,
            {"reason": "no_artist_data"}, track_tokens=track_tokens,
        )

    plausible: List[tuple] = []  # (candidate, kind, similarity, stripped_cand_title)
    for cand in candidates:
        if not _artist_identity_ok(TitleNormalizer.normalize(cand.artist), identity_norms):
            continue
        if not TitleNormalizer.normalize(cand.title):
            continue
        stripped_cand = canonical_base_title(cand.title)
        if stripped_track and stripped_cand == stripped_track:
            kind, sim = "exact", 1.0
        elif stripped_track and stripped_cand:
            sim = TitleNormalizer.similarity(stripped_track, stripped_cand)
            if sim < TITLE_FUZZY_THRESHOLD:
                continue
            kind = "fuzzy"
        else:
            continue
        if not duration_matches(duration_sec, cand.duration_sec):
            continue
        plausible.append((cand, kind, sim, stripped_cand))

    if not plausible:
        return _build(
            track_id, STATUS_NOT_FOUND,
            {"reason": "no_plausible_candidate", "identity": identity_norms[:3]},
            track_tokens=track_tokens,
        )

    # Collapse near-duplicate rows for the SAME song (LRCLIB carries many
    # same-track uploads at slightly different durations) — group by artist +
    # noise-stripped base title. Genuine ambiguity (two distinct base titles /
    # artists) survives as multiple groups. The representative prefers a
    # version-AGREEING candidate, then an exact-kind title, then the richest
    # lyric (FIX-lyrics-matcher-noise: lyric richness is the tiebreak, not the
    # criterion — a richer Live upload must not shadow the plain sibling).
    groups: Dict[tuple, List[tuple]] = {}
    for cand, kind, sim, stripped_cand in plausible:
        key = (TitleNormalizer.normalize(cand.artist), stripped_cand)
        groups.setdefault(key, []).append((cand, kind, sim))

    def _representative(entries: List[tuple]) -> tuple:
        def _key(e: tuple):
            cand = e[0]
            agrees = not (extract_version_tokens(cand.title) ^ track_tokens)
            return (agrees, e[1] == "exact", bool(cand.synced_lyrics), bool(cand.plain_lyrics))
        return sorted(entries, key=_key, reverse=True)[0]

    reps = [_representative(g) for g in groups.values()]

    if len(reps) > 1:
        previews = [
            {"title": c.title, "artist": c.artist, "duration_sec": c.duration_sec}
            for c, _, _ in reps[:3]
        ]
        return _build(
            track_id, STATUS_AMBIGUOUS,
            {"reason": "multiple_plausible", "count": len(reps), "candidates": previews},
            track_tokens=track_tokens,
        )

    cand, kind, sim = reps[0]
    cand_tokens = extract_version_tokens(cand.title)
    symmetric = set(track_tokens) ^ set(cand_tokens)  # tokens in one but not both
    version_agrees = len(symmetric) == 0

    has_lyrics = bool(cand.plain_lyrics) or bool(cand.synced_lyrics)
    if cand.instrumental or not has_lyrics:
        # no_lyrics still needs lyric_plain set (non-NULL) per the V33 CHECK.
        return _build(
            track_id, STATUS_NO_LYRICS,
            {
                "reason": "instrumental_or_empty",
                "lrclib_id": cand.id,
                "lrclib_title": cand.title,
                "lrclib_artist": cand.artist,
            },
            lyric_plain="",
            track_tokens=track_tokens,
            cand_tokens=cand_tokens,
            version_agrees=version_agrees,
        )

    if not version_agrees:
        return _build(
            track_id, STATUS_REVIEW_REQUIRED,
            {
                "reason": "version_token_mismatch",
                "asymmetric_tokens": sorted(symmetric),
                "lrclib_id": cand.id,
                "lrclib_title": cand.title,
                "lrclib_artist": cand.artist,
            },
            track_tokens=track_tokens,
            cand_tokens=cand_tokens,
            version_agrees=False,
        )

    if kind == "fuzzy":
        return _build(
            track_id, STATUS_REVIEW_REQUIRED,
            {
                "reason": "title_fuzzy_only",
                "similarity": round(sim, 3),
                "lrclib_id": cand.id,
                "lrclib_title": cand.title,
                "lrclib_artist": cand.artist,
            },
            track_tokens=track_tokens,
            cand_tokens=cand_tokens,
            version_agrees=True,
        )

    matched_ev = {
        "lrclib_id": cand.id,
        "lrclib_artist": cand.artist,
        "lrclib_title": cand.title,
        "lrclib_album": cand.album,
        "lrclib_duration_sec": cand.duration_sec,
    }
    noise_stripped = [
        side for side, raw in (("track", title or ""), ("candidate", cand.title))
        if _strip_noise(raw) != raw
    ]
    if noise_stripped:
        matched_ev["title_noise_stripped"] = noise_stripped
    return _build(
        track_id, STATUS_MATCHED,
        matched_ev,
        lyric_plain=cand.plain_lyrics,
        lyric_synced=cand.synced_lyrics,
        match_basis="exact-title",
        track_tokens=track_tokens,
        cand_tokens=cand_tokens,
        version_agrees=True,
    )


class LRCLIBDumpMatcher:
    """Candidate provider + decider over the real LRCLIB SQLite dump."""

    # Columns we require on the dump's `tracks` table (guarded at construction).
    _EXPECTED_TRACK_COLS = {"id", "name", "artist_name", "duration", "last_lyrics_id"}

    def __init__(self, dump_path: str):
        self.conn = sqlite3.connect(dump_path)
        self.conn.row_factory = sqlite3.Row
        self._inspect()

    def _inspect(self) -> None:
        """Log schema + fail loudly on drift (LRCLIB dump schema changes — issue #104)."""
        cur = self.conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cur.fetchall()}
        logger.info("LRCLIB dump tables: %s", sorted(tables))
        cur.execute("PRAGMA table_info(tracks)")
        cols = {row[1] for row in cur.fetchall()}
        missing = self._EXPECTED_TRACK_COLS - cols
        if missing:
            raise RuntimeError(
                f"LRCLIB dump `tracks` missing expected columns {sorted(missing)}; "
                f"got {sorted(cols)}. Dump schema drifted (lrclib issue #104) — adapt "
                "candidate_from_dump_row / the query before running."
            )

    def search_candidates(
        self,
        title: str,
        artist_names: List[str],
        duration_sec: Optional[float],
    ) -> List[Candidate]:
        """Retrieve candidate rows (loose), then the pure ``decide_match`` gates them."""
        if not artist_names:
            return []
        primary = TitleNormalizer.normalize(artist_names[0])
        title_norm = TitleNormalizer.normalize(title or "")
        if not primary:
            return []
        order = float(duration_sec) if duration_sec is not None else 0.0
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT t.id, t.name, t.artist_name, t.album_name, t.duration,
                   l.plain_lyrics, l.synced_lyrics, l.instrumental
            FROM tracks t
            LEFT JOIN lyrics l ON t.last_lyrics_id = l.id
            WHERE LOWER(t.artist_name) LIKE ?
               OR (
                ? <> '' AND (
                    LOWER(t.name) LIKE ? OR LOWER(t.artist_name) LIKE ?
                )
              )
            ORDER BY ABS(t.duration - ?) ASC
            LIMIT 200
            """,
            (f"%{primary}%", title_norm, f"%{title_norm}%", f"%{title_norm}%", order),
        )
        return [Candidate.from_dump_row(row) for row in cur.fetchall()]

    def match_track(
        self,
        track_id: uuid.UUID,
        title: str,
        artist_names: List[str],
        duration_sec: Optional[float],
        aliases: Optional[List[str]] = None,
    ) -> MatchOutcome:
        candidates = self.search_candidates(title, artist_names, duration_sec)
        return decide_match(track_id, title, artist_names, aliases, duration_sec, candidates)

    def close(self) -> None:
        self.conn.close()


class TrackLyricsWriter:
    """Persist match outcomes to ``track_lyrics`` (per-row commit, failure isolated).

    RFC: commit per row + sentinel so the row leaves the pool. ``no_lyrics`` is
    written with ``lyric_plain=''`` to satisfy the V33 CHECK (non-NULL for
    resolved statuses matched/no_lyrics); unresolved statuses write NULL text.
    """

    def __init__(self, session: Session):
        self.session = session

    def write_outcomes(self, outcomes: List[MatchOutcome], batch_size: int = 100) -> int:
        written = 0
        failed = 0
        for i, outcome in enumerate(outcomes):
            try:
                self.session.execute(
                    text(
                        """
                        INSERT INTO track_lyrics
                            (track_id, match_status, evidence,
                             lyric_plain, lyric_synced, matcher_version)
                        VALUES
                            (:track_id, :match_status, CAST(:evidence AS jsonb),
                             :lyric_plain, :lyric_synced, :matcher_version)
                        ON CONFLICT (track_id) DO UPDATE SET
                            match_status   = EXCLUDED.match_status,
                            evidence       = EXCLUDED.evidence,
                            lyric_plain    = EXCLUDED.lyric_plain,
                            lyric_synced   = EXCLUDED.lyric_synced,
                            matcher_version= EXCLUDED.matcher_version,
                            updated_at     = NOW()
                        """
                    ),
                    {
                        "track_id": outcome.track_id,
                        "match_status": outcome.match_status,
                        # JSONB bind: repo pattern is json.dumps(...) + CAST(:x AS jsonb)
                        # (psycopg does not adapt a raw dict). ensure_ascii=False keeps
                        # Hangul readable in the stored evidence.
                        "evidence": json.dumps(_evidence_json(outcome), ensure_ascii=False),
                        "lyric_plain": _strip_nul(_lyric_plain_for(outcome)),
                        "lyric_synced": _strip_nul(outcome.lyric_synced),
                        "matcher_version": outcome.matcher_version,
                    },
                )
                self.session.commit()
                written += 1
                if (i + 1) % batch_size == 0:
                    logger.info("Wrote %d/%d outcomes", written, len(outcomes))
            except Exception as exc:  # noqa: BLE001 - isolation: one row must not abort the batch
                failed += 1
                logger.error("Failed to write track %s: %s", outcome.track_id, exc)
                self.session.rollback()
                continue
        logger.info("Write complete: %d success, %d failed", written, failed)
        return written


def _strip_nul(value):
    """Recursively remove NUL (``\\x00``) from strings.

    PostgreSQL ``text`` and ``jsonb`` cannot store a NUL code point (it rejects
    the ``\\u0000`` JSON escape too), and LRCLIB crowd-sourced metadata / lyric
    text occasionally carries an embedded NUL (e.g. ``"Kanye West\\x00\\ufeffKanye
    West"``). Without this, every such row fails the insert and is silently
    dropped by the per-row isolation. Only NUL is illegal — other control chars
    and the BOM are valid Postgres text, so they are left untouched.
    """
    if isinstance(value, str):
        return value.replace("\x00", "")
    if isinstance(value, dict):
        return {k: _strip_nul(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_strip_nul(v) for v in value]
    return value


def _lyric_plain_for(outcome: MatchOutcome) -> Optional[str]:
    """Coerce lyric_plain to satisfy the V33 ``ck_track_lyrics_lyric_on_resolved`` CHECK.

    Resolved states (``matched`` / ``no_lyrics``) require a **non-NULL** lyric_plain;
    unresolved states (``ambiguous`` / ``review_required`` / ``not_found``) require
    NULL. ``decide_match`` already writes ``''`` for ``no_lyrics``, but a ``matched``
    candidate that carried **only synced** lyrics has ``lyric_plain=None`` — store
    ``''`` (the text lives in ``lyric_synced``) so the row is written instead of being
    silently rejected by the CHECK.
    """
    if outcome.match_status in (STATUS_MATCHED, STATUS_NO_LYRICS):
        return outcome.lyric_plain if outcome.lyric_plain is not None else ""
    return outcome.lyric_plain


def _evidence_json(outcome: MatchOutcome) -> Dict:
    """Evidence payload (JSONB-safe: sets -> sorted lists)."""
    ev = dict(outcome.evidence or {})
    ev.setdefault("matcher_version", outcome.matcher_version)
    if outcome.match_basis:
        ev.setdefault("match_basis", outcome.match_basis)
    ev["version_tokens_track"] = outcome.version_tokens_track
    ev["version_tokens_candidate"] = outcome.version_tokens_candidate
    if outcome.version_agrees is not None:
        ev["version_agrees"] = outcome.version_agrees
    # Strip NUL from any source-derived string (candidate titles/artists in the
    # evidence previews) so json.dumps never emits a NUL escape that jsonb rejects.
    return _strip_nul(ev)
