"""Primary-artist ordering regression — real engine on disposable Postgres 16.

The LRCLIB search artist is ``artist_names[0]``. The selection queries used to build
that array with ``ARRAY_AGG(DISTINCT a.name)``, which sorts ALPHABETICALLY, so on a
multi-credit track a featured guest silently became the search artist
(*"Ariana Grande — I Don't Do Drugs (feat. Ariana Grande)"*). 62% of the parked
``not_found`` pool has >=2 credits.

This has to be a real-engine test ([[feedback-sa-session-lifecycle-mock-blind]]): the
ordering is entirely a property of the SQL — a mocked session asserts nothing, and
Postgres additionally REJECTS the obvious ``ARRAY_AGG(DISTINCT x ORDER BY y)`` form, so
only an executed query proves the rewrite is even valid.

The fixture is built so the correct answer differs from BOTH the old behaviour and the
runner-up rule:

    album artist : "Zulu Primary"  popularity 10   (alphabetically LAST, least popular)
    guest        : "Alpha Guest"   popularity 99   (alphabetically FIRST, most popular)

  alphabetical (old)  -> "Alpha Guest"   x
  popularity only     -> "Alpha Guest"   x
  album-artist first  -> "Zulu Primary"  ok

so a regression to either rule fails loudly rather than passing by luck.

**Two tiers, deliberately.** The ordering rule lives in one shared SQL constant,
``PRIMARY_ARTIST_NAMES_LATERAL``, and the first tier exercises that constant directly —
it needs only ``tracks`` / ``artists`` / ``track_artists`` / ``album_artists``, so it
runs against the pinned canonical schema and is the focused regression gate.

The second tier drives the two selection queries end-to-end, which additionally requires
``track_lyrics``. CI's canonical schema includes it, so both tiers run in the deploy gate;
the probe remains as a clear diagnostic for local runs pointed at an older database.

Guarded by TEST_DB_URL; skipped when unset.
"""
from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from worker.service.lyrics_eval_core import PRIMARY_ARTIST_NAMES_LATERAL
from worker.service.lyrics_incremental_service import LyricsIncrementalService
from worker.service.lyrics_reassessment_service import LyricsReassessmentService

_TEST_DB_URL = os.environ.get("TEST_DB_URL")

pytestmark = pytest.mark.skipif(
    not _TEST_DB_URL,
    reason="integration test requires TEST_DB_URL env var (Postgres test database)",
)

_PREFIX = "lyr_primary_"

# The fragment under test, run standalone against `tracks t`.
_LATERAL_PROBE_SQL = f"""
    SELECT primary_artists.artist_names
    FROM tracks t
{PRIMARY_ARTIST_NAMES_LATERAL}
    WHERE t.id = CAST(:tid AS UUID)
"""


@pytest.fixture(scope="module")
def factory():
    eng = create_engine(_TEST_DB_URL, pool_pre_ping=True, future=True)
    return sessionmaker(bind=eng)


@pytest.fixture(scope="module")
def has_track_lyrics(factory):
    """Diagnose a non-canonical local schema before the end-to-end tier runs."""
    with factory() as s:
        return s.execute(
            text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = 'track_lyrics'"
            )
        ).first() is not None


def _seed(session, *, tag: str, album_artist_name: str | None, credits):
    """Insert album + artists + track. ``credits`` = [(name, popularity), ...].

    ``album_artist_name`` (when given) is also linked via ``album_artists`` — that link
    is the primary-artist signal under test. Returns the track id.
    """
    sfx = f"{_PREFIX}{tag}_{uuid.uuid4().hex[:8]}"
    album_id, track_id = str(uuid.uuid4()), str(uuid.uuid4())

    session.execute(
        text("INSERT INTO albums (id, spotify_id, title) VALUES (:id, :sid, 'Primary Test')"),
        {"id": album_id, "sid": f"{sfx}_alb"},
    )
    session.execute(
        text(
            "INSERT INTO tracks (id, album_id, spotify_id, title, duration_sec) "
            "VALUES (:id, :alb, :sid, 'Primary Test Track', 200)"
        ),
        {"id": track_id, "alb": album_id, "sid": f"{sfx}_trk"},
    )
    for idx, (name, pop) in enumerate(credits):
        artist_id = str(uuid.uuid4())
        session.execute(
            text(
                "INSERT INTO artists (id, spotify_id, name, popularity) "
                "VALUES (:id, :sid, :name, :pop)"
            ),
            {"id": artist_id, "sid": f"{sfx}_art{idx}", "name": name, "pop": pop},
        )
        session.execute(
            text("INSERT INTO track_artists (track_id, artist_id) VALUES (:t, :a)"),
            {"t": track_id, "a": artist_id},
        )
        if album_artist_name is not None and name == album_artist_name:
            session.execute(
                text("INSERT INTO album_artists (album_id, artist_id) VALUES (:al, :a)"),
                {"al": album_id, "a": artist_id},
            )
    return track_id


def _cleanup(Session, has_track_lyrics=False):
    with Session() as s, s.begin():
        if has_track_lyrics:
            s.execute(
                text(
                    "DELETE FROM track_lyrics WHERE track_id IN "
                    "(SELECT id FROM tracks WHERE spotify_id LIKE :p)"
                ),
                {"p": f"{_PREFIX}%"},
            )
        s.execute(text("DELETE FROM tracks WHERE spotify_id LIKE :p"), {"p": f"{_PREFIX}%"})
        s.execute(text("DELETE FROM albums WHERE spotify_id LIKE :p"), {"p": f"{_PREFIX}%"})
        s.execute(text("DELETE FROM artists WHERE spotify_id LIKE :p"), {"p": f"{_PREFIX}%"})


def _probe(session, track_id):
    return list(
        session.execute(text(_LATERAL_PROBE_SQL), {"tid": track_id}).scalar_one() or []
    )


def _row_for(rows, track_id):
    match = [r for r in rows if str(r["id"]) == track_id]
    assert match, f"seeded track {track_id} not returned by the selection"
    return match[0]


# ── Tier 1: the shared ordering constant, standalone (no track_lyrics needed) ──────


def test_lateral_puts_the_album_artist_first(factory):
    """Beats BOTH the alphabetical old behaviour and a popularity-only rule."""
    Session = factory
    try:
        with Session() as s:
            with s.begin():
                track_id = _seed(
                    s,
                    tag="lat",
                    album_artist_name="Zulu Primary",
                    credits=[("Alpha Guest", 99), ("Zulu Primary", 10)],
                )
            names = _probe(s, track_id)

        assert names[0] == "Zulu Primary", (
            f"LRCLIB would be searched with {names[0]!r}; the album artist must win over "
            "both the alphabetically-first and the more-popular credit"
        )
        assert sorted(names) == ["Alpha Guest", "Zulu Primary"], "no credit may be dropped"
    finally:
        _cleanup(Session)


def test_lateral_falls_back_to_popularity_without_an_album_artist(factory):
    """Singles/compilations with no album_artists row fall back to popularity."""
    Session = factory
    try:
        with Session() as s:
            with s.begin():
                track_id = _seed(
                    s,
                    tag="latpop",
                    album_artist_name=None,
                    credits=[("Alpha Quiet", 5), ("Zulu Loud", 90)],
                )
            names = _probe(s, track_id)

        assert names[0] == "Zulu Loud", f"expected the popular credit first, got {names}"
    finally:
        _cleanup(Session)


def test_lateral_single_credit_is_unaffected(factory):
    """The single-credit majority of the pool must behave exactly as before."""
    Session = factory
    try:
        with Session() as s:
            with s.begin():
                track_id = _seed(
                    s, tag="latsolo", album_artist_name="Solo Act", credits=[("Solo Act", 50)]
                )
            names = _probe(s, track_id)

        assert names == ["Solo Act"]
    finally:
        _cleanup(Session)


def test_lateral_dedupes_by_name(factory):
    """Preserves the ARRAY_AGG(DISTINCT a.name) contract the rewrite replaced."""
    Session = factory
    try:
        with Session() as s:
            with s.begin():
                track_id = _seed(
                    s,
                    tag="latdup",
                    album_artist_name=None,
                    credits=[("Same Name", 10), ("Same Name", 80)],
                )
            names = _probe(s, track_id)

        assert names == ["Same Name"], f"duplicate credit names must collapse, got {names}"
    finally:
        _cleanup(Session)


# ── Tier 2: the two selection queries end-to-end (needs track_lyrics) ──────────────


def test_incremental_selection_puts_the_album_artist_first(factory, has_track_lyrics):
    if not has_track_lyrics:
        pytest.skip("track_lyrics missing from test database (FEAT-lyrics-corpus migration)")
    Session = factory
    try:
        with Session() as s:
            with s.begin():
                track_id = _seed(
                    s,
                    tag="inc",
                    album_artist_name="Zulu Primary",
                    credits=[("Alpha Guest", 99), ("Zulu Primary", 10)],
                )
            rows = LyricsIncrementalService(s)._fetch_uncorpused_tracks(limit=5000)

        assert _row_for(rows, track_id)["artist_names"][0] == "Zulu Primary"
    finally:
        _cleanup(Session, has_track_lyrics)


def test_reassessment_selection_uses_the_same_ordering(factory, has_track_lyrics):
    """The twin query must not drift from the incremental one."""
    if not has_track_lyrics:
        pytest.skip("track_lyrics missing from test database (FEAT-lyrics-corpus migration)")
    Session = factory
    try:
        with Session() as s:
            with s.begin():
                track_id = _seed(
                    s,
                    tag="rea",
                    album_artist_name="Zulu Primary",
                    credits=[("Alpha Guest", 99), ("Zulu Primary", 10)],
                )
                s.execute(
                    text(
                        "INSERT INTO track_lyrics (track_id, match_status, evidence) "
                        "VALUES (CAST(:t AS UUID), 'not_found', '{}'::jsonb) "
                        "ON CONFLICT (track_id) DO NOTHING"
                    ),
                    {"t": track_id},
                )
            rows = LyricsReassessmentService(s)._fetch_unresolved_tracks(limit=50000)

        assert _row_for(rows, track_id)["artist_names"][0] == "Zulu Primary"
    finally:
        _cleanup(Session, has_track_lyrics)
