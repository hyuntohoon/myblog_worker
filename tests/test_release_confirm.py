# Unit tests for the release-day confirm service (FEAT-release-calendar Step 5).
#
# Pure-logic: fake session records executed SQL (blind to real Postgres
# semantics — the flip/insert SQL got a prod BEGIN…ROLLBACK dry-run before
# merge, per feedback-sa-session-lifecycle-mock-blind). The title normalizer is
# the WORKER-SIDE TWIN of myblog_music release_calendar_service.normalize_title
# (Step 6 display soft-grouping) — these tests pin the twin's behavior so drift
# between the copies fails loudly.
from __future__ import annotations

from datetime import date

import pytest

from worker.service.release_confirm_service import (
    confirm_release_events,
    match_events,
    normalize_title,
)


class _Row:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class _Result:
    def __init__(self, rows=None, rowcount=1):
        self._rows = rows or []
        self.rowcount = rowcount

    def fetchall(self):
        return self._rows


class _FakeSession:
    def __init__(self, events=None):
        self.events = events or []
        self.executed = []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.executed.append((sql, params or {}))
        if "FROM artist_release_events" in sql:
            return _Result(rows=self.events)
        return _Result()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def begin(self):
        return _FakeSession._Ctx()

    class _Ctx:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def sql_of(self, pred):
        return [e for e in self.executed if pred(e[0])]


def _cand(**kw):
    return {
        "artist_id": "artist-1",
        "spotify_album_id": "sp-1",
        "title": "Blue Album",
        "release_type": "album",
        "release_date": "2026-07-13",
        **kw,
    }


def _ev(**kw):
    base = {
        "id": "ev-1",
        "artist_id": "artist-1",
        "title": "Blue Album",
        "release_date": date(2026, 7, 13),
        "status": "announced",
        "spotify_album_id": None,
    }
    base.update(kw)
    return base


class TestNormalizeTitleTwin:
    # Mirrors myblog_music/app/services/release_calendar_service.py exactly.
    def test_casefold_and_whitespace_collapse(self):
        assert normalize_title("  Blue   ALBUM ") == "blue album"

    def test_strips_itunes_storefront_suffixes(self):
        assert normalize_title("Blue Album - Single") == "blue album"
        assert normalize_title("Blue Album - EP") == "blue album"

    def test_strips_only_one_trailing_suffix(self):
        assert normalize_title("X - EP - Single") == "x - ep"

    def test_no_suffix_mid_title(self):
        assert normalize_title("The - Single Life") == "the - single life"

    def test_empty(self):
        assert normalize_title("") == ""
        assert normalize_title(None) == ""


class TestMatchEvents:
    def test_matches_all_sources_on_normalized_title(self):
        events = [
            _ev(id="mb"),
            _ev(id="it", title="Blue Album - Single"),
            _ev(id="no", title="Red Album"),
        ]
        matched = match_events(_cand(), events, 7)
        assert sorted(m["id"] for m in matched) == ["it", "mb"]

    def test_artist_scoped(self):
        events = [_ev(artist_id="artist-2")]
        assert match_events(_cand(), events, 7) == []

    def test_date_proximity_boundary_inclusive(self):
        assert match_events(_cand(), [_ev(release_date=date(2026, 7, 6))], 7)
        assert match_events(_cand(), [_ev(release_date=date(2026, 7, 20))], 7)
        assert not match_events(_cand(), [_ev(release_date=date(2026, 7, 5))], 7)
        assert not match_events(_cand(), [_ev(release_date=date(2026, 7, 21))], 7)


class TestConfirmReleaseEvents:
    def _run(self, session, candidates):
        counters = {"confirm_candidates": 0, "confirm_flipped": 0, "confirm_inserted": 0}
        confirm_release_events(lambda: session, candidates, counters)
        return counters

    def test_no_candidates_touches_nothing(self):
        session = _FakeSession()
        counters = self._run(session, [])
        assert counters == {
            "confirm_candidates": 0, "confirm_flipped": 0, "confirm_inserted": 0,
        }
        assert session.executed == []

    def test_flip_updates_matching_rows_sorted_ids(self):
        session = _FakeSession(events=[
            _Row(id="ev-b", artist_id="artist-1", title="Blue Album",
                 release_date=date(2026, 7, 12), status="announced", spotify_album_id=None),
            _Row(id="ev-a", artist_id="artist-1", title="Blue Album - EP",
                 release_date=date(2026, 7, 13), status="announced", spotify_album_id=None),
        ])
        counters = self._run(session, [_cand()])
        assert counters["confirm_flipped"] == 1 and counters["confirm_inserted"] == 0
        (sql, params), = session.sql_of(lambda s: "UPDATE artist_release_events" in s)
        assert params["ids"] == ["ev-a", "ev-b"]  # sorted (deadlock rule)
        assert params["spotify_album_id"] == "sp-1"
        assert "status = 'released'" in sql

    def test_never_announced_inserts_spotify_source_released_row(self):
        session = _FakeSession(events=[])
        counters = self._run(session, [_cand()])
        assert counters["confirm_inserted"] == 1 and counters["confirm_flipped"] == 0
        (sql, params), = session.sql_of(
            lambda s: "INSERT INTO artist_release_events" in s
        )
        assert params == {
            "artist_id": "artist-1",
            "source_key": "sp-1",
            "title": "Blue Album",
            "release_type": "album",
            "release_date": "2026-07-13",
            "spotify_album_id": "sp-1",
        }
        # the confirm upsert is the ONLY path allowed to set status on conflict,
        # and its guard keeps re-encounters rowcount-silent
        assert "'released'" in sql and "IS DISTINCT FROM" in sql

    def test_inserts_sorted_by_conflict_key(self):
        session = _FakeSession(events=[])
        counters = self._run(session, [
            _cand(spotify_album_id="sp-z", title="Zed"),
            _cand(spotify_album_id="sp-a", title="Aye"),
        ])
        assert counters["confirm_inserted"] == 2
        keys = [p["source_key"] for s, p in session.executed
                if "INSERT INTO artist_release_events" in s]
        assert keys == ["sp-a", "sp-z"]

    def test_collab_candidate_deduped_per_artist(self):
        # same album seen under the same artist twice in one tick → one candidate
        session = _FakeSession(events=[])
        counters = self._run(session, [_cand(), _cand()])
        assert counters["confirm_candidates"] == 1
        # …but two artists each confirm their own rows
        session2 = _FakeSession(events=[])
        counters2 = self._run(
            session2, [_cand(), _cand(artist_id="artist-2")]
        )
        assert counters2["confirm_candidates"] == 2

    def test_mixed_flip_and_insert(self):
        session = _FakeSession(events=[
            _Row(id="ev-1", artist_id="artist-1", title="Blue Album",
                 release_date=date(2026, 7, 13), status="announced", spotify_album_id=None),
        ])
        counters = self._run(session, [
            _cand(),
            _cand(spotify_album_id="sp-2", title="Surprise Drop"),
        ])
        assert counters["confirm_flipped"] == 1
        assert counters["confirm_inserted"] == 1


@pytest.mark.unit
def test_step4_observation_upsert_still_never_touches_status():
    """THE INVARIANT: status transitions happen only through the confirm path —
    the Step-4 observation upsert must never regain status/spotify_album_id."""
    from worker.service.release_upcoming_service import _UPSERT_EVENT

    update_clause = str(_UPSERT_EVENT).split("DO UPDATE SET", 1)[1]
    assert "status" not in update_clause
    assert "spotify_album_id" not in update_clause
