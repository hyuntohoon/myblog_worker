# tests/test_genius_fetch.py
"""Genius annotation fetch (FEAT-lyrics-annotations Thread 1).

What these pin, in order of how badly the codebase has been burned by each:

  * the DB session is CLOSED before the first HTTP call, and reopened per track —
    holding it across the API loop is what Neon kills with ProtocolViolation
  * the songs row is written BEFORE its annotations, in one transaction — the read
    path gates on the parent row, so the reverse order makes them invisible
  * annotation upserts are ordered by the conflict key — row-lock deadlocks
  * a weak match is recorded as `ambiguous` and its annotations are NOT written
  * a transient failure leaves the row UNWRITTEN so it retries; only a real miss
    parks it as `not_found`
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from worker.clients.genius_client import (
    GeniusAnnotation,
    GeniusAuthError,
    GeniusSong,
    GeniusTransientError,
    match_confidence,
    match_scores,
)
from worker.service.genius_fetch_service import GeniusFetchService


TRACK = {"track_id": "11111111-1111-1111-1111-111111111111",
         "title": "Berghain", "album": "LUX", "artists": ["ROSALÍA"]}


class _Session:
    """Records the order of statements and whether it was closed."""

    def __init__(self, log, rows=None):
        self._log = log
        self._rows = rows or []
        self.closed = False
        self.committed = False

    def execute(self, stmt, params=None):
        sql = str(stmt)
        if "SELECT" in sql and "track_genius_songs g" in sql:
            self._log.append(("select", None))
            r = MagicMock()
            r.fetchall.return_value = self._rows
            return r
        if "INSERT INTO track_genius_songs" in sql:
            self._log.append(("song", params["track_id"]))
        elif "INSERT INTO track_genius_annotations" in sql:
            self._log.append(("anno", params["genius_annotation_id"]))
        return MagicMock()

    def commit(self):
        self.committed = True
        self._log.append(("commit", None))

    def rollback(self):
        self._log.append(("rollback", None))

    def close(self):
        self.closed = True
        self._log.append(("close", None))


def _factory(log, rows):
    made = []

    def make():
        s = _Session(log, rows if not made else [])
        made.append(s)
        return s
    make.sessions = made
    return make


def _client(*, song=None, annotations=None, enabled=True, find_raises=None):
    c = MagicMock()
    c.enabled = enabled
    if find_raises:
        c.find_song.side_effect = find_raises
    else:
        c.find_song.return_value = song
    c.load_song.side_effect = lambda s: s
    c.load_annotations.return_value = annotations or []
    return c


def _song(conf=0.95):
    return GeniusSong(song_id=7, title="Berghain", artist="ROSALÍA",
                      url="https://genius.com/x", confidence=conf,
                      title_score=conf, artist_score=conf,
                      description="d", annotation_count=3, language="es")


def _anno(aid, ordinal=1):
    return GeniusAnnotation(annotation_id=aid, referent_ordinal=ordinal,
                            fragment="f", body="b", votes_total=5,
                            is_verified=False, state="accepted")


# ── match scoring ───────────────────────────────────────────────────────────

def test_match_confidence_weights_the_artist():
    """Same title, wrong artist must not outscore right artist, different title.

    This is the 로꼬 "2025" failure: "2025 by Molly Yam (Ft. Loco)" has an exact
    title and the wrong primary artist.
    """
    wrong_artist = match_confidence("2025", "로꼬", "2025", "Molly Yam")
    right_artist = match_confidence("2025", "로꼬", "2025 (Remix)", "로꼬")
    assert right_artist > wrong_artist


def test_match_confidence_ignores_accents_and_case():
    assert match_confidence("De Madrugá", "ROSALÍA", "de madruga", "rosalia") > 0.95


def test_right_artist_wrong_song_clears_the_blend_and_must_be_caught_elsewhere():
    """The live failure: shared artist carries a disjoint title over the threshold.

    "GUIZ CORLEONE" (Guizmo, Freeze Corleone) → Genius "Braquage" by Freeze
    Corleone. This asserts the blend genuinely does NOT catch it, which is why the
    service floors the title separately — if this ever starts failing, the blend
    changed and the separate floor deserves a re-think rather than silent removal.
    """
    t, a, blend = match_scores("GUIZ CORLEONE", "Freeze Corleone", "Braquage", "Freeze Corleone")
    assert t < 0.5 and a > 0.99
    assert blend > 0.62, "the blend alone would admit this wrong song"


def test_wrong_song_is_rejected_by_the_title_floor():
    log = []
    factory = _factory(log, [(TRACK["track_id"], "a", "al", ["ar"])])
    bad = GeniusSong(song_id=7, title="Braquage", artist="Freeze Corleone",
                     url="u", confidence=0.676, title_score=0.19, artist_score=1.0)
    client = _client(song=bad, annotations=[_anno(1)])
    metrics = GeniusFetchService(factory, client).run(limit=1)
    assert metrics["ambiguous"] == 1
    assert [k for k, _ in log if k == "anno"] == []
    client.load_annotations.assert_not_called()


# ── session lifecycle ───────────────────────────────────────────────────────

def test_read_session_is_closed_before_any_http_call():
    log = []
    factory = _factory(log, [(TRACK["track_id"], "Berghain", "LUX", ["ROSALÍA"])])
    client = _client(song=_song(), annotations=[_anno(1)])

    calls = []
    client.find_song.side_effect = lambda t, artists: (calls.append(list(log)), _song())[1]

    GeniusFetchService(factory, client).run(limit=1)

    # At the moment of the first HTTP call, the read session had already closed.
    assert ("close", None) in calls[0], "session still open across the Genius call"


def test_each_track_gets_its_own_short_write_session():
    log = []
    rows = [(TRACK["track_id"], "a", "al", ["ar"]), ("22222222-2222-2222-2222-222222222222", "b", "al", ["ar"])]
    factory = _factory(log, rows)
    GeniusFetchService(factory, _client(song=_song(), annotations=[_anno(1)])).run(limit=2)
    # 1 read + 2 writes, all closed.
    assert len(factory.sessions) == 3
    assert all(s.closed for s in factory.sessions)


# ── the ordering invariant ──────────────────────────────────────────────────

def test_songs_row_is_written_before_its_annotations():
    log = []
    factory = _factory(log, [(TRACK["track_id"], "a", "al", ["ar"])])
    GeniusFetchService(factory, _client(song=_song(), annotations=[_anno(9), _anno(3)])).run(limit=1)
    kinds = [k for k, _ in log if k in ("song", "anno")]
    assert kinds[0] == "song", "annotations without a parent row are invisible to the read path"
    assert kinds[1:] == ["anno", "anno"]


def test_song_and_annotations_share_one_transaction():
    log = []
    factory = _factory(log, [(TRACK["track_id"], "a", "al", ["ar"])])
    GeniusFetchService(factory, _client(song=_song(), annotations=[_anno(1)])).run(limit=1)
    seq = [k for k, _ in log]
    first_commit = seq.index("commit")
    assert seq.index("song") < first_commit
    assert seq.index("anno") < first_commit, "a commit between the two exposes the broken order"


def test_annotation_upserts_are_sorted_by_the_conflict_key():
    log = []
    factory = _factory(log, [(TRACK["track_id"], "a", "al", ["ar"])])
    GeniusFetchService(factory, _client(song=_song(), annotations=[_anno(30), _anno(7), _anno(19)])).run(limit=1)
    ids = [v for k, v in log if k == "anno"]
    assert ids == sorted(ids), "unsorted bulk upserts deadlock under concurrency"


# ── match policy ────────────────────────────────────────────────────────────

def test_weak_match_is_recorded_but_its_annotations_are_not_written():
    log = []
    factory = _factory(log, [(TRACK["track_id"], "a", "al", ["ar"])])
    client = _client(song=_song(conf=0.10), annotations=[_anno(1)])
    metrics = GeniusFetchService(factory, client).run(limit=1)
    assert metrics["ambiguous"] == 1
    assert [k for k, _ in log if k == "anno"] == [], "wrong credits must not enter silently"
    assert ("song", TRACK["track_id"]) in log, "but the bad match IS recorded, with its confidence"
    client.load_annotations.assert_not_called()


def test_no_search_hit_parks_the_track_as_not_found():
    log = []
    factory = _factory(log, [(TRACK["track_id"], "a", "al", ["ar"])])
    metrics = GeniusFetchService(factory, _client(song=None)).run(limit=1)
    assert metrics["not_found"] == 1
    assert ("song", TRACK["track_id"]) in log


# ── failure handling ────────────────────────────────────────────────────────

def test_transient_failure_leaves_the_row_unwritten_so_it_retries():
    log = []
    factory = _factory(log, [(TRACK["track_id"], "a", "al", ["ar"])])
    client = _client(find_raises=GeniusTransientError("boom"))
    metrics = GeniusFetchService(factory, client).run(limit=1)
    assert metrics["errors"] == 1
    assert [k for k, _ in log if k == "song"] == [], \
        "a not_found row would remove the track from the pool for good"


def test_auth_failure_stops_the_run_instead_of_hammering():
    log = []
    rows = [(f"{i}" * 8, "a", "al", ["ar"]) for i in range(3)]
    factory = _factory(log, rows)
    client = _client(find_raises=GeniusAuthError("401"))
    metrics = GeniusFetchService(factory, client).run(limit=3)
    assert metrics["errors"] == 1, "stopped after the first rejection, not once per track"


def test_missing_token_no_ops_rather_than_raising():
    log = []
    factory = _factory(log, [(TRACK["track_id"], "a", "al", ["ar"])])
    metrics = GeniusFetchService(factory, _client(enabled=False)).run(limit=1)
    assert metrics == {"considered": 0, "matched": 0, "ambiguous": 0,
                       "not_found": 0, "annotations": 0, "errors": 0}
    assert log == [], "no DB work at all when the integration is unconfigured"


def test_limit_zero_is_a_no_op_not_a_full_batch():
    """`limit or settings.X` would read 0 as "unset" and run the default batch."""
    log = []
    factory = _factory(log, [(TRACK["track_id"], "a", "al", ["ar"])])
    metrics = GeniusFetchService(factory, _client(song=_song())).run(limit=0)
    assert metrics["considered"] == 0
    assert log == []


def test_empty_pool_does_no_write_work():
    log = []
    factory = _factory(log, [])
    metrics = GeniusFetchService(factory, _client(song=_song())).run(limit=5)
    assert metrics["considered"] == 0
    assert [k for k, _ in log if k in ("song", "anno")] == []


def test_artist_score_takes_the_best_credit_not_the_most_popular():
    """Genius names a collaboration by its own primary credit, not ours.

    Live: our "Entertain" credits WILLOW first while Genius says THE ANXIETY (the
    project name), and "YO MA" credits 식케이 first while Genius says Leellamarz.
    Scoring against only the top credit rejected both, with the right name sitting
    second and third in our own list.
    """
    top_only = match_scores("Entertain", "WILLOW", "Entertain", "THE ANXIETY")[2]
    best_of = max(
        match_scores("Entertain", n, "Entertain", "THE ANXIETY")[2]
        for n in ["WILLOW", "THE ANXIETY", "Tyler Cole"]
    )
    assert top_only < 0.62, "the top credit alone reads as a wrong artist"
    assert best_of > 0.99, "the right credit is in our list and must win"
