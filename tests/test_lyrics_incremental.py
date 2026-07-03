"""Unit tests for incremental lyrics collection (FEAT-lyrics-corpus Step 3).

Two units, both DB-free:

  * ``LrclibClient`` — the ``/api/search`` adapter, with ``httpx`` mocked, asserting the
    no-match vs transient-error split (404/empty -> ``[]``; 5xx/429/transport ->
    ``LrclibTransientError`` after retries).
  * ``LyricsIncrementalService.collect`` — the alias-fill orchestration over a fake client
    + stubbed selection + mock session, asserting per-row commit, sentinel counting,
    transient-skip isolation, the wall-clock budget guard, and the matched-consistency
    guard. The pure matcher itself is covered by ``test_lyrics_matcher.py``.
"""
from __future__ import annotations

import json
import uuid
from unittest.mock import MagicMock, patch

import httpx
import pytest

from worker.clients.lrclib_client import LrclibClient, LrclibTransientError
from worker.service.lyrics_incremental_service import LyricsIncrementalService
from worker.service.lyrics_matcher import (
    STATUS_MATCHED,
    Candidate,
    MatchOutcome,
    decide_match,
)
from worker.service.lyrics_promote import (
    BASIS_BEST_OF_AMBIGUOUS,
    BASIS_BEST_OF_REVIEW,
)


# --------------------------------------------------------------------------
# LrclibClient — /api/search adapter
# --------------------------------------------------------------------------
def _resp(status_code, json_body=None):
    r = MagicMock()
    r.status_code = status_code
    r.json.return_value = json_body if json_body is not None else []
    r.raise_for_status.return_value = None
    return r


class TestLrclibClient:
    def test_404_is_legitimate_no_match(self):
        client = LrclibClient()
        client._client.get = MagicMock(return_value=_resp(404))
        assert client.search_candidates("Hello", "Adele") == []

    def test_empty_list_is_no_match(self):
        client = LrclibClient()
        client._client.get = MagicMock(return_value=_resp(200, []))
        assert client.search_candidates("Hello", "Adele") == []

    def test_good_response_adapts_candidates(self):
        client = LrclibClient()
        client._client.get = MagicMock(return_value=_resp(200, [
            {"id": 1, "trackName": "Hello", "artistName": "Adele",
             "albumName": "25", "duration": 295.0, "instrumental": False,
             "plainLyrics": "Hello, it's me", "syncedLyrics": None},
        ]))
        cands = client.search_candidates("Hello", "Adele")
        assert len(cands) == 1
        assert isinstance(cands[0], Candidate)
        assert cands[0].title == "Hello" and cands[0].duration_sec == 295.0

    @patch("worker.clients.lrclib_client.time.sleep", return_value=None)
    def test_retryable_status_raises_transient_after_retries(self, _sleep):
        client = LrclibClient()
        client._client.get = MagicMock(return_value=_resp(503))
        with pytest.raises(LrclibTransientError):
            client.search_candidates("Hello", "Adele", max_retries=3)
        assert client._client.get.call_count == 3  # exhausted retries, never a not_found

    @patch("worker.clients.lrclib_client.time.sleep", return_value=None)
    def test_transport_error_raises_transient(self, _sleep):
        client = LrclibClient()
        client._client.get = MagicMock(side_effect=httpx.ConnectError("boom"))
        with pytest.raises(LrclibTransientError):
            client.search_candidates("Hello", "Adele", max_retries=2)


# --------------------------------------------------------------------------
# LyricsIncrementalService.collect — alias-fill orchestration
# --------------------------------------------------------------------------
def _matching_candidate():
    return Candidate(
        id=1, title="Hello", artist="Adele", album="25", duration_sec=295.0,
        instrumental=False, plain_lyrics="Hello, it's me", synced_lyrics=None,
    )


def _track_row(title="Hello", artist="Adele", duration=295):
    return {
        "id": uuid.uuid4(),
        "title": title,
        "duration_sec": duration,
        "artist_names": [artist],
        "aliases": [],
    }


class _FakeClient:
    """Stands in for LrclibClient — returns canned candidates or raises transient."""

    def __init__(self, result=None, raise_exc=None):
        self.result = result if result is not None else []
        self.raise_exc = raise_exc
        self.closed = False

    def search_candidates(self, title, artist, **kw):
        if self.raise_exc is not None:
            raise self.raise_exc
        return list(self.result)

    def close(self):
        self.closed = True


def _service(client, tracks, **kw):
    session = MagicMock()
    svc = LyricsIncrementalService(session, client=client, **kw)
    svc._fetch_uncorpused_tracks = MagicMock(return_value=tracks)
    return svc, session


def test_empty_pool_returns_zero_metrics_no_client_call():
    client = _FakeClient()
    svc, session = _service(client, [])
    metrics = svc.collect()
    assert metrics["evaluated"] == 0
    session.execute.assert_not_called()


def test_matched_track_is_written_per_row():
    client = _FakeClient(result=[_matching_candidate()])
    svc, session = _service(client, [_track_row()])
    metrics = svc.collect()
    assert metrics["evaluated"] == 1
    assert metrics[STATUS_MATCHED] == 1
    # per-row commit: writer INSERTed once and committed
    assert session.execute.call_count == 1
    assert session.commit.call_count == 1


def test_no_candidate_writes_not_found_sentinel():
    client = _FakeClient(result=[])  # legitimate no-match
    svc, session = _service(client, [_track_row()])
    metrics = svc.collect()
    assert metrics["evaluated"] == 1
    assert metrics["not_found"] == 1
    assert session.execute.call_count == 1  # sentinel row written -> leaves the pool


def test_transient_error_is_skipped_not_written():
    client = _FakeClient(raise_exc=LrclibTransientError("outage"))
    svc, session = _service(client, [_track_row()])
    metrics = svc.collect()
    assert metrics["evaluated"] == 0
    assert metrics["skipped_transient"] == 1
    session.execute.assert_not_called()  # never poisoned as not_found


def test_over_budget_tracks_are_left_for_next_run():
    client = _FakeClient(result=[_matching_candidate()])
    # a negative budget makes the deadline already past -> every result is skipped
    svc, session = _service(client, [_track_row(), _track_row()], time_budget_sec=-1.0)
    metrics = svc.collect()
    assert metrics["evaluated"] == 0
    assert metrics["skipped_budget"] == 2
    session.execute.assert_not_called()


def test_consistency_violation_is_not_written():
    client = _FakeClient(result=[_matching_candidate()])
    svc, session = _service(client, [_track_row()])
    bad = MatchOutcome(
        track_id=uuid.uuid4(), match_status=STATUS_MATCHED, evidence={},
        version_agrees=False,
    )
    with patch("worker.service.lyrics_eval_core.decide_match", return_value=bad):
        metrics = svc.collect()
    assert metrics["consistency_errors"] == 1
    assert metrics["evaluated"] == 0
    session.execute.assert_not_called()


def test_owned_client_is_closed():
    client = _FakeClient(result=[_matching_candidate()])
    svc, _ = _service(client, [_track_row()])
    # client was injected (not owned) -> service must NOT close it
    svc.collect()
    assert client.closed is False


# --------------------------------------------------------------------------
# Best-of promotion inside the shared eval loop
# (FEAT-lyrics-best-of-promotion Step 2 — promote_best runs after decide_match)
# --------------------------------------------------------------------------
def _dup_noise_candidates():
    """The RFC's canonical duplicate-noise shape (Come Back to Earth): three
    same-recording uploads whose base titles differ only by a track-number prefix /
    a non-rendition parenthetical -> decide_match parks ambiguous; tier 1 promotes
    the clean title."""
    def c(title, cid, duration=162.0, plain="la la", synced=None):
        return Candidate(
            id=cid, title=title, artist="Mac Miller", album=None,
            duration_sec=duration, instrumental=False,
            plain_lyrics=plain, synced_lyrics=synced,
        )
    return [
        c("Come Back to Earth", cid=10, synced="[00:01.00] la la"),
        c("Come Back to Earth (Paused)", cid=11, duration=161.84),
        c("01 - Come Back to Earth", cid=12),
    ]


def _mac_miller_row():
    return {
        "id": uuid.uuid4(),
        "title": "Come Back to Earth",
        "duration_sec": 161.0,
        "artist_names": ["Mac Miller"],
        "aliases": [],
    }


def test_ambiguous_candidates_promote_to_best_of_matched():
    client = _FakeClient(result=_dup_noise_candidates())
    svc, session = _service(client, [_mac_miller_row()])
    metrics = svc.collect()
    # promoted row is written as matched (one row, per-row commit)
    assert metrics["evaluated"] == 1
    assert metrics[STATUS_MATCHED] == 1
    assert metrics["ambiguous"] == 0            # no longer parked
    assert metrics["promoted_ambiguous"] == 1
    assert metrics["promoted_review"] == 0
    assert metrics["promotion_criteria"]["exact-base-title"] == 1
    assert metrics["consistency_errors"] == 0
    assert session.execute.call_count == 1
    # the written row carries the best-of basis + the promotion block + a lyric body
    params = session.execute.call_args[0][1]
    assert params["match_status"] == STATUS_MATCHED
    evidence = json.loads(params["evidence"])
    assert evidence["match_basis"] == BASIS_BEST_OF_AMBIGUOUS
    assert evidence["promotion"]["criterion"] == "exact-base-title"
    assert evidence["promotion"]["from_status"] == "ambiguous"
    assert evidence["reason"] == "multiple_plausible"  # original evidence preserved
    assert params["lyric_synced"] == "[00:01.00] la la"  # richest (synced) tier-1 pick


def test_review_sibling_promotes_to_best_of_review():
    # decide_match picks the richest (synced Live) representative -> version mismatch
    # parks review_required; the clean sibling agrees on version and is the tier-1 pick.
    cands = [
        Candidate(id=20, title="Song", artist="Adele", album=None, duration_sec=200.0,
                  instrumental=False, plain_lyrics="hello", synced_lyrics=None),
        Candidate(id=21, title="Song (Live)", artist="Adele", album=None, duration_sec=200.5,
                  instrumental=False, plain_lyrics="hello",
                  synced_lyrics="[00:01.00] hello"),
    ]
    client = _FakeClient(result=cands)
    row = {"id": uuid.uuid4(), "title": "Song", "duration_sec": 200.0,
           "artist_names": ["Adele"], "aliases": []}
    svc, session = _service(client, [row])
    metrics = svc.collect()
    assert metrics[STATUS_MATCHED] == 1
    assert metrics["promoted_review"] == 1
    assert metrics["review_required"] == 0
    params = session.execute.call_args[0][1]
    evidence = json.loads(params["evidence"])
    assert evidence["match_basis"] == BASIS_BEST_OF_REVIEW
    assert evidence["promotion"]["chosen"]["lrclib_id"] == 20


def test_unpromotable_ambiguous_stays_parked_and_counted():
    # Two genuinely distinct fuzzy base titles, neither equal to the track's ->
    # no tier-1 candidate; the row stays parked with the attempt recorded.
    cands = [
        Candidate(id=30, title="Hello Worlds", artist="Adele", album=None,
                  duration_sec=200.0, instrumental=False,
                  plain_lyrics="x", synced_lyrics=None),
        Candidate(id=31, title="Hello Worldz", artist="Adele", album=None,
                  duration_sec=200.0, instrumental=False,
                  plain_lyrics="y", synced_lyrics=None),
    ]
    client = _FakeClient(result=cands)
    row = {"id": uuid.uuid4(), "title": "Hello World", "duration_sec": 200.0,
           "artist_names": ["Adele"], "aliases": []}
    svc, session = _service(client, [row])
    metrics = svc.collect()
    assert metrics["ambiguous"] == 1             # still parked (row written as sentinel)
    assert metrics["promoted_ambiguous"] == 0
    assert metrics["promotion_parked"] == 1
    assert session.execute.call_count == 1
    params = session.execute.call_args[0][1]
    evidence = json.loads(params["evidence"])
    assert evidence["promotion"]["reason"] == "no_tier1_candidate"
    assert params["lyric_plain"] is None         # unresolved stays NULL (V33 CHECK)


def test_best_of_matched_row_passes_basis_aware_invariant():
    # A best-of matched row must NOT be refused as a consistency error (the invariant
    # is basis-aware: best-of requires the promotion block, which promote_best sets).
    client = _FakeClient(result=_dup_noise_candidates())
    svc, session = _service(client, [_mac_miller_row()])
    metrics = svc.collect()
    assert metrics["consistency_errors"] == 0
    assert session.execute.call_count == 1


def test_stray_best_of_without_promotion_block_is_refused():
    # Defensive: a best-of matched row that lost its promotion provenance is refused.
    client = _FakeClient(result=[_matching_candidate()])
    svc, session = _service(client, [_track_row()])
    stray = MatchOutcome(
        track_id=uuid.uuid4(), match_status=STATUS_MATCHED, evidence={},
        match_basis=BASIS_BEST_OF_AMBIGUOUS, version_agrees=True,
    )
    with patch("worker.service.lyrics_eval_core.decide_match", return_value=stray):
        metrics = svc.collect()
    assert metrics["consistency_errors"] == 1
    assert metrics["evaluated"] == 0
    session.execute.assert_not_called()
