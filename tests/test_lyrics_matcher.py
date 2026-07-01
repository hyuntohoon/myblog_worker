"""Unit tests for the canonical lyrics matcher (FEAT-lyrics-corpus Step 2).

The decision core (``decide_match``) is pure and tested with in-memory
``Candidate`` lists — no DB. The dump provider is tested against a mock that
mirrors the REAL LRCLIB schema (separate ``tracks`` + ``lyrics`` tables joined
by ``last_lyrics_id``, ``duration`` in float seconds, snake_case). RFC:
``docs/rfcs/FEAT-lyrics-corpus.md``.
"""

import json
import sqlite3
import uuid

import pytest

from worker.service.lyrics_matcher import (
    Candidate,
    LRCLIBDumpMatcher,
    MatchOutcome,
    STATUS_AMBIGUOUS,
    STATUS_MATCHED,
    STATUS_NO_LYRICS,
    STATUS_NOT_FOUND,
    STATUS_REVIEW_REQUIRED,
    TITLE_FUZZY_THRESHOLD,
    TitleNormalizer,
    _evidence_json,
    _lyric_plain_for,
    decide_match,
    extract_version_tokens,
)


# --------------------------------------------------------------------------
# Normalization + token helpers
# --------------------------------------------------------------------------
class TestTitleNormalizer:
    def test_normalize_lowercase(self):
        assert TitleNormalizer.normalize("HELLO World") == "hello world"

    def test_normalize_removes_accents(self):
        assert TitleNormalizer.normalize("Café") == "cafe"

    def test_normalize_collapses_whitespace(self):
        assert TitleNormalizer.normalize("Hello  world  ") == "hello world"

    def test_normalize_keeps_basic_punctuation(self):
        assert TitleNormalizer.normalize("Don't-Stop") == "don't stop"

    def test_normalize_keeps_hangul(self):
        # Korean alnum chars survive; only punctuation is stripped.
        assert TitleNormalizer.normalize("좋은날 (Feat. 아이유)") == "좋은날 feat 아이유"

    def test_similarity_exact(self):
        assert TitleNormalizer.similarity("hello", "hello") == 1.0

    def test_similarity_partial(self):
        sim = TitleNormalizer.similarity("hello world", "hello there")
        assert 0.5 < sim < 1.0


class TestVersionTokens:
    def test_remix_detected(self):
        assert "remix" in extract_version_tokens("Song (Remix)")

    def test_no_tokens_plain_title(self):
        assert extract_version_tokens("Let It Be") == set()

    def test_korean_live_token(self):
        assert "라이브" in extract_version_tokens("노래 (라이브)")

    def test_feat_is_not_a_version_token(self):
        assert extract_version_tokens("Song (Feat. Other)") == set()

    def test_remastered_variant(self):
        assert "remastered" in extract_version_tokens("Song - Remastered")


# --------------------------------------------------------------------------
# decide_match — pure core (in-memory candidates)
# --------------------------------------------------------------------------
def _cand(title, artist, duration_sec, *, plain="lyrics...", synced=None,
          instrumental=False, cid=1):
    return Candidate(
        id=cid, title=title, artist=artist, album=None, duration_sec=duration_sec,
        instrumental=instrumental, plain_lyrics=plain, synced_lyrics=synced,
    )


class TestDecideMatch:
    def test_exact_match(self):
        tid = uuid.uuid4()
        out = decide_match(
            tid, "Let It Be", ["The Beatles"], None, 243.0,
            [_cand("Let It Be", "The Beatles", 243.0)],
        )
        assert out.match_status == "matched"
        assert out.match_basis == "exact-title"
        assert out.lyric_plain == "lyrics..."
        assert out.version_agrees is True

    def test_no_candidates_is_not_found(self):
        out = decide_match(uuid.uuid4(), "Unknown", ["X"], None, 200.0, [])
        assert out.match_status == "not_found"
        assert out.evidence["reason"] == "no_plausible_candidate"

    def test_no_artist_names_is_not_found(self):
        out = decide_match(uuid.uuid4(), "Song", [], None, 200.0,
                           [_cand("Song", "X", 200.0)])
        assert out.match_status == "not_found"
        assert out.evidence["reason"] == "no_artist_data"

    def test_duration_out_of_tolerance_drops_candidate(self):
        out = decide_match(
            uuid.uuid4(), "Let It Be", ["The Beatles"], None, 250.0,
            [_cand("Let It Be", "The Beatles", 243.0)],
        )
        # 7s off -> candidate not plausible -> not_found
        assert out.match_status == "not_found"

    def test_duration_within_tolerance_matches(self):
        out = decide_match(
            uuid.uuid4(), "Let It Be", ["The Beatles"], None, 244.0,
            [_cand("Let It Be", "The Beatles", 243.0)],
        )
        assert out.match_status == "matched"

    def test_korean_title_exact(self):
        out = decide_match(
            uuid.uuid4(), "좋은날", ["IU"], None, 267.0,
            [_cand("좋은날", "IU", 267.0)],
        )
        assert out.match_status == "matched"
        assert out.match_basis == "exact-title"

    def test_alias_resolves_identity(self):
        # catalog artist "IU" with alias "아이유"; LRCLIB candidate is "아이유"
        out = decide_match(
            uuid.uuid4(), "좋은날", ["IU"], ["아이유", "Lee Ji-eun"], 267.0,
            [_cand("좋은날", "아이유", 267.0)],
        )
        assert out.match_status == "matched"

    def test_wrong_artist_not_matched(self):
        out = decide_match(
            uuid.uuid4(), "Let It Be", ["The Beatles"], None, 243.0,
            [_cand("Let It Be", "Rolling Stones", 243.0)],
        )
        assert out.match_status == "not_found"

    def test_article_only_artist_difference_matches(self):
        # "the beatles" vs identity "beatles" -> whole-word subset, shared len>=3
        out = decide_match(
            uuid.uuid4(), "Let It Be", ["Beatles"], None, 243.0,
            [_cand("Let It Be", "The Beatles", 243.0)],
        )
        assert out.match_status == "matched"

    def test_substring_short_name_not_accepted(self):
        # "drake" must not match "drake miller band" (no alias) -> not_found
        out = decide_match(
            uuid.uuid4(), "Nice For What", ["Drake"], None, 240.0,
            [_cand("Nice For What", "Drake Miller Band", 240.0)],
        )
        assert out.match_status == "not_found"

    def test_multiple_plausible_is_ambiguous(self):
        # Two DISTINCT base titles by the same artist (same-song duplicates get
        # collapsed; genuine ambiguity = different base title/artist -> park).
        out = decide_match(
            uuid.uuid4(), "Crazy Love", ["X Artist"], None, 200.0,
            [
                _cand("Crazy Love", "X Artist", 200.0, cid=1),
                _cand("Crazy Love II", "X Artist", 200.0, cid=2),  # fuzzy >= 0.80
            ],
        )
        assert out.match_status == "ambiguous"
        assert out.evidence["reason"] == "multiple_plausible"
        assert out.lyric_plain is None  # never attached out of ambiguity

    def test_same_song_duplicate_rows_collapse_to_matched(self):
        # LRCLIB carries near-duplicate uploads at slightly different durations;
        # they are the SAME song -> collapse to one, match (do not park).
        out = decide_match(
            uuid.uuid4(), "Crazy", ["Gnarls Barkley"], None, 180.0,
            [
                _cand("Crazy", "Gnarls Barkley", 180.0, cid=1, plain="v1"),
                _cand("Crazy", "Gnarls Barkley", 179.5, cid=2, plain="v2", synced="[00:00]v2"),
            ],
        )
        assert out.match_status == "matched"
        # representative = richest (synced preferred)
        assert out.lyric_synced == "[00:00]v2"

    def test_version_token_mismatch_parks_to_review(self):
        # track is the album version; candidate is a Remix -> never auto-merge
        out = decide_match(
            uuid.uuid4(), "Crazy", ["Gnarls Barkley"], None, 180.0,
            [_cand("Crazy (Remix)", "Gnarls Barkley", 180.0)],
        )
        assert out.match_status == "review_required"
        assert out.evidence["reason"] == "version_token_mismatch"
        assert "remix" in out.evidence["asymmetric_tokens"]
        assert out.version_agrees is False
        assert out.lyric_plain is None

    def test_matching_version_tokens_agree(self):
        # both sides carry "live" -> agrees -> matched
        out = decide_match(
            uuid.uuid4(), "Crazy (Live)", ["Gnarls Barkley"], None, 180.0,
            [_cand("Crazy (Live)", "Gnarls Barkley", 180.0)],
        )
        assert out.match_status == "matched"
        assert out.version_agrees is True
        assert out.version_tokens_track == ["live"]

    def test_fuzzy_title_only_parks_to_review(self):
        # title not exact but similarity >= threshold, no version conflict
        out = decide_match(
            uuid.uuid4(), "Hello World", ["X Artist"], None, 200.0,
            [_cand("Hello Word", "X Artist", 200.0)],  # 1-char diff
        )
        assert out.match_status == "review_required"
        assert out.evidence["reason"] == "title_fuzzy_only"

    def test_below_fuzzy_threshold_dropped(self):
        out = decide_match(
            uuid.uuid4(), "Hello World", ["X Artist"], None, 200.0,
            [_cand("Totally Different Title", "X Artist", 200.0)],
        )
        assert out.match_status == "not_found"

    def test_no_lyrics_sets_empty_string(self):
        # instrumental candidate, single plausible -> no_lyrics, lyric_plain=""
        out = decide_match(
            uuid.uuid4(), "Interlude", ["X Artist"], None, 60.0,
            [_cand("Interlude", "X Artist", 60.0, plain=None, synced=None, instrumental=True)],
        )
        assert out.match_status == "no_lyrics"
        assert out.lyric_plain == ""  # satisfies V33 CHECK (non-NULL)


# --------------------------------------------------------------------------
# Candidate adapters
# --------------------------------------------------------------------------
class TestCandidateAdapters:
    def test_from_api_camelcase(self):
        c = Candidate.from_api({
            "id": 7, "trackName": "T", "artistName": "A", "albumName": "Al",
            "duration": 200, "instrumental": False,
            "plainLyrics": "p", "syncedLyrics": "s",
        })
        assert c.title == "T" and c.artist == "A"
        assert c.duration_sec == 200 and c.plain_lyrics == "p"

    def test_from_dump_row_snakecase(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT 1 AS id, 'T' AS name, 'A' AS artist_name, 'Al' AS album_name, "
            "200.0 AS duration, 'p' AS plain_lyrics, 's' AS synced_lyrics, 0 AS instrumental"
        ).fetchone()
        c = Candidate.from_dump_row(row)
        assert c.title == "T" and c.artist == "A"
        assert c.duration_sec == 200.0 and c.plain_lyrics == "p"


class TestMatchOutcomeDefaults:
    def test_matcher_version(self):
        out = MatchOutcome(track_id=uuid.uuid4(), match_status="matched", evidence={})
        assert out.matcher_version == "step2-v2"


# --------------------------------------------------------------------------
# LRCLIBDumpMatcher — real-schema mock
# --------------------------------------------------------------------------
@pytest.fixture
def real_schema_db(tmp_path):
    """Mock mirroring the REAL LRCLIB schema (tracks + lyrics, last_lyrics_id join)."""
    path = tmp_path / "lrclib.db"
    conn = sqlite3.connect(str(path))
    conn.execute(
        """
        CREATE TABLE tracks (
          id INTEGER PRIMARY KEY, name TEXT, name_lower TEXT, artist_name TEXT,
          artist_name_lower TEXT, album_name TEXT, album_name_lower TEXT,
          duration FLOAT, last_lyrics_id INTEGER,
          created_at TEXT, updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE lyrics (
          id INTEGER PRIMARY KEY, plain_lyrics TEXT, synced_lyrics TEXT,
          track_id INTEGER, has_plain_lyrics BOOLEAN, has_synced_lyrics BOOLEAN,
          instrumental BOOLEAN, source TEXT, created_at TEXT, updated_at TEXT
        )
        """
    )

    def add_lyrics(plain, synced, instrumental):
        cur = conn.execute(
            "INSERT INTO lyrics (plain_lyrics, synced_lyrics, instrumental) VALUES (?,?,?)",
            (plain, synced, instrumental),
        )
        return cur.lastrowid

    beatles_lyrics = add_lyrics("Let it be...", "[00:00.00]Let it be...", 0)
    instrumental_id = add_lyrics(None, None, 1)
    korean_lyrics = add_lyrics("좋은 날씨네...", None, 0)

    conn.executemany(
        "INSERT INTO tracks (id, name, name_lower, artist_name, artist_name_lower, "
        "album_name, duration, last_lyrics_id) VALUES (?,?,?,?,?,?,?,?)",
        [
            (1, "Let It Be", "let it be", "The Beatles", "the beatles", "Album", 243.0, beatles_lyrics),
            (2, "Interlude", "interlude", "X Artist", "x artist", "Album", 60.0, instrumental_id),
            (3, "좋은날", "좋은날", "IU", "iu", "Album", 267.0, korean_lyrics),
        ],
    )
    conn.commit()
    conn.close()
    return str(path)


class TestLRCLIBDumpMatcher:
    def test_exact_match_via_dump(self, real_schema_db):
        m = LRCLIBDumpMatcher(real_schema_db)
        out = m.match_track(uuid.uuid4(), "Let It Be", ["The Beatles"], 243.0)
        assert out.match_status == "matched"
        assert out.lyric_plain == "Let it be..."
        m.close()

    def test_korean_via_dump(self, real_schema_db):
        m = LRCLIBDumpMatcher(real_schema_db)
        out = m.match_track(uuid.uuid4(), "좋은날", ["IU"], 267.0)
        assert out.match_status == "matched"
        m.close()

    def test_instrumental_via_dump(self, real_schema_db):
        m = LRCLIBDumpMatcher(real_schema_db)
        out = m.match_track(uuid.uuid4(), "Interlude", ["X Artist"], 60.0)
        assert out.match_status == "no_lyrics"
        assert out.lyric_plain == ""
        m.close()

    def test_schema_drift_guard(self, tmp_path):
        """A dump missing expected columns fails loudly instead of silent all-not_found."""
        path = tmp_path / "bad.db"
        conn = sqlite3.connect(str(path))
        conn.execute("CREATE TABLE tracks (id INTEGER PRIMARY KEY, title TEXT)")  # wrong cols
        conn.commit()
        conn.close()
        with pytest.raises(RuntimeError, match="missing expected columns"):
            LRCLIBDumpMatcher(str(path))


# --------------------------------------------------------------------------
# TrackLyricsWriter persistence helpers (V33 CHECK + JSONB bind safety)
# --------------------------------------------------------------------------
class TestWriterHelpers:
    """Guards the two Phase-2 writer fixes without needing a live Postgres.

    (1) ``_lyric_plain_for`` keeps the V33 ``ck_track_lyrics_lyric_on_resolved``
        CHECK satisfiable (resolved states non-NULL, unresolved states NULL).
    (2) ``_evidence_json`` output is JSON-serializable (it is bound via
        ``json.dumps(...) + CAST(:evidence AS jsonb)`` — a raw dict is not
        adaptable by psycopg).
    """

    def _outcome(self, status, lyric_plain=None, lyric_synced=None):
        return MatchOutcome(
            track_id=uuid.uuid4(), match_status=status, evidence={"reason": "x"},
            lyric_plain=lyric_plain, lyric_synced=lyric_synced,
        )

    def test_matched_none_plain_coerced_to_empty(self):
        # matched candidate that carried only synced lyrics -> plain must not be NULL
        o = self._outcome(STATUS_MATCHED, lyric_plain=None, lyric_synced="[00:01.00] hi")
        assert _lyric_plain_for(o) == ""

    def test_matched_keeps_real_plain(self):
        o = self._outcome(STATUS_MATCHED, lyric_plain="real words")
        assert _lyric_plain_for(o) == "real words"

    def test_no_lyrics_stays_empty_string(self):
        o = self._outcome(STATUS_NO_LYRICS, lyric_plain="")
        assert _lyric_plain_for(o) == ""

    @pytest.mark.parametrize("status", [STATUS_NOT_FOUND, STATUS_AMBIGUOUS, STATUS_REVIEW_REQUIRED])
    def test_unresolved_states_stay_null(self, status):
        o = self._outcome(status, lyric_plain=None)
        assert _lyric_plain_for(o) is None

    def test_evidence_json_strips_nul(self):
        # LRCLIB crowd metadata can carry an embedded NUL; jsonb rejects .
        o = MatchOutcome(
            track_id=uuid.uuid4(), match_status=STATUS_AMBIGUOUS,
            evidence={"reason": "multiple_plausible",
                      "candidates": [{"artist": "Kanye West\x00﻿Kanye West"}]},
        )
        s = json.dumps(_evidence_json(o), ensure_ascii=False)
        assert "\x00" not in s
        assert "\\u0000" not in s
        assert "﻿" in s  # BOM is valid Postgres text — left intact

    def test_evidence_json_is_serializable(self):
        # tokens are stored as lists; ensure the whole payload round-trips through json
        o = MatchOutcome(
            track_id=uuid.uuid4(), match_status=STATUS_REVIEW_REQUIRED,
            evidence={"reason": "version_token_mismatch", "asymmetric_tokens": ["live"]},
            version_tokens_track=["live"], version_tokens_candidate=[], version_agrees=False,
            match_basis="fuzzy-title",
        )
        payload = _evidence_json(o)
        s = json.dumps(payload, ensure_ascii=False)
        assert json.loads(s)["version_agrees"] is False
        assert json.loads(s)["reason"] == "version_token_mismatch"
