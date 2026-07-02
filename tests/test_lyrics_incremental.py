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
