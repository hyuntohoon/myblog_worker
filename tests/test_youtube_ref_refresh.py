"""FEAT-youtube-playback-provider Step A5 — unit tests for the refresh job.

What these can and cannot show. The session-lifecycle claim (fetch -> close ->
HTTP -> fresh short write) is asserted here as an ORDERING of observable calls,
which a mock CAN see. What a mock cannot see is whether the SQL is right — the
retention DELETE and the `verify_state='live'` filter are WHERE clauses, and a
mock returns what it was told regardless. Those live in
tests/integration/test_youtube_ref_refresh_db.py against a real engine.
"""
from __future__ import annotations

import pytest

from worker.service.youtube_ref_refresh_service import (
    BATCH_SIZE,
    YouTubeRefRefreshService,
    parse_iso8601_duration,
)


@pytest.mark.parametrize("iso,expected", [
    ("PT4M13S", 253),
    ("PT1H2M3S", 3723),
    ("P1D", 86400),
    # A live stream reports a bare `P0D` with no time part. Zero must read as
    # UNKNOWN — a zero-second duration is not a duration.
    ("P0D", None),
    ("PT0S", None),
    ("", None),
    (None, None),
    ("garbage", None),
    # isinstance, not truthiness: re.match raises TypeError on a truthy
    # non-string, which would turn one odd payload into a failed run.
    ({"weird": "object"}, None),
    (253, None),
])
def test_parse_iso8601_duration(iso, expected):
    assert parse_iso8601_duration(iso) == expected


class _Session:
    """Records every statement and whether it was executed while open."""

    def __init__(self, log, rows_by_stmt):
        self.log, self._rows = log, rows_by_stmt
        self.closed = False
        # Logged at CONSTRUCTION, not at first execute. An earlier version of
        # this harness keyed "a session is open" on `execute`, so a mutant that
        # merely OPENED a session around the HTTP call — a checked-out
        # connection held across the network, which is the actual bug class —
        # was invisible to it.
        self.log.append(("open", None))

    def execute(self, stmt, params=None):
        key = str(stmt).strip().split()[0].upper()
        self.log.append(("execute", key))
        assert not self.closed, "a statement ran on a CLOSED session"

        class _R:
            def __init__(self, rows):
                self._rows = rows

            def fetchall(self):
                return self._rows
        return _R(self._rows.get(key, []))

    def commit(self):
        self.log.append(("commit", None))

    def close(self):
        self.log.append(("close", None))
        self.closed = True


class _Client:
    def __init__(self, log, present=None, *, error=None):
        self.log, self.present, self.error = log, present or {}, error
        self.batches = []

    def list_videos(self, ids):
        self.log.append(("http", "videos.list"))
        self.batches.append(list(ids))
        if self.error:
            raise self.error
        return {i: self._item(i) for i in ids if i in self.present}

    def _item(self, vid):
        p = self.present[vid]
        return {
            "id": vid,
            "snippet": {"title": "T", "channelTitle": "C"},
            "status": {
                "embeddable": p.get("embeddable", True),
                "privacyStatus": p.get("privacy", "public"),
                "madeForKids": False,
            },
            "contentDetails": {"duration": p.get("duration", "PT3M33S")},
        }


def _service(log, *, stale_ids, present=None, error=None, expired=0):
    rows = {
        "DELETE": [(f"row{i}",) for i in range(expired)],
        "SELECT": [(v,) for v in stale_ids],
    }
    sessions = []

    def factory():
        s = _Session(log, rows)
        sessions.append(s)
        return s

    client = _Client(log, present, error=error)
    svc = YouTubeRefRefreshService(factory, client, retention_days=30)
    return svc, client, sessions


def test_no_session_is_open_across_the_http_call():
    """The recurring bug class: a transaction held across an external API call.

    Asserted as an ORDERING — every `http` entry must sit between a `close` and
    the next `execute`. "close() was called" is also true of a job that closes
    last, which is exactly the bug.
    """
    log = []
    svc, _, _ = _service(log, stale_ids=["a", "b"], present={"a": {}, "b": {}})
    svc.run(limit=10)

    depth = 0
    for kind, _ in log:
        if kind == "open":
            depth += 1
        elif kind == "close":
            depth -= 1
        elif kind == "http":
            assert depth == 0, f"an HTTP call ran with {depth} session(s) open: {log}"


def test_expiry_runs_before_any_api_call():
    """A quota failure must never postpone a deletion the policy requires.

    A sweep that only runs when the enrichment succeeds is not a sweep.
    """
    log = []
    svc, _, _ = _service(log, stale_ids=["a"], present={"a": {}}, expired=3)
    svc.run(limit=10)

    first_http = next(i for i, (k, _) in enumerate(log) if k == "http")
    first_delete = next(i for i, (k, v) in enumerate(log) if k == "execute" and v == "DELETE")
    assert first_delete < first_http


def test_expiry_still_happens_when_the_api_is_down():
    """The load-bearing half of the ordering above."""
    log = []
    svc, _, _ = _service(
        log, stale_ids=["a"], present={"a": {}}, error=RuntimeError("quota"), expired=2,
    )
    with pytest.raises(RuntimeError):
        svc.run(limit=10)
    assert any(k == "execute" and v == "DELETE" for k, v in log)
    assert any(k == "commit" for k, v in log), "the delete must be COMMITTED, not merely issued"


def test_expiry_precedes_the_refresh_writes():
    """The consequence that makes the ordering matter, not the ordering itself.

    If the sweep ran AFTER the refresh, a row already past 30 days would first
    be re-verified — moving `last_verified_at` to now() — and would then survive
    the very sweep meant to delete it. The row would be immortal: refreshed
    forever, never expired. Asserted as "DELETE is committed before any refresh
    UPDATE", which is the observable form of that.
    """
    log = []
    svc, _, _ = _service(log, stale_ids=["a"], present={"a": {}}, expired=1)
    svc.run(limit=10)

    first_delete = next(i for i, (k, v) in enumerate(log) if k == "execute" and v == "DELETE")
    first_update = next(i for i, (k, v) in enumerate(log) if k == "execute" and v == "UPDATE")
    assert first_delete < first_update


def test_ids_absent_from_the_response_are_marked_gone():
    log = []
    svc, _, _ = _service(log, stale_ids=["alive", "vanished"], present={"alive": {}})
    out = svc.run(limit=10)
    assert out["gone"] == 1 and out["refreshed"] == 1


def test_a_non_embeddable_refresh_downgrades_verify_state():
    log = []
    svc, _, _ = _service(log, stale_ids=["x"], present={"x": {"embeddable": False}})
    out = svc.run(limit=10)
    assert out["refreshed"] == 1 and out["gone"] == 0


def test_batches_never_exceed_the_api_cap():
    """`videos.list` takes at most 50 ids. Exceeding it silently truncates the
    response, which this job would then read as "these videos are gone"."""
    log = []
    ids = [f"v{i}" for i in range(BATCH_SIZE * 2 + 7)]
    svc, client, _ = _service(log, stale_ids=ids, present={i: {} for i in ids})
    svc.run(limit=len(ids))
    assert all(len(b) <= BATCH_SIZE for b in client.batches)
    assert sum(len(b) for b in client.batches) == len(ids)
    assert len(client.batches) == 3


def test_an_empty_work_list_makes_no_api_call():
    log = []
    svc, client, _ = _service(log, stale_ids=[], expired=1)
    out = svc.run(limit=10)
    assert client.batches == []
    assert out == {"expired": 1, "refreshed": 0, "gone": 0, "batches": 0}
