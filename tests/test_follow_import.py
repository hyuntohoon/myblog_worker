# FEAT-for-you-releases Step 2 — followed-artists import tests.
# Pure-mock (library-sync style): Spotify HTTP, boto3 SQS, and the DB session are
# all faked — TEST_DB_URL-bound coverage can't gate merges (no worker PR CI), so
# orchestration decisions are what these tests pin down.
import json
import time
import uuid
from unittest.mock import MagicMock, patch

import httpx
import pytest

from worker.clients.spotify_user_client import SpotifyScopeError, SpotifyUserClient
from worker.service.follow_import_service import run_follow_import, run_follow_ingest

USER_ID = "0468fd3c-0000-4000-8000-000000000001"


# ---------- client: GET /me/following cursor paging ----------

def _resp(status: int, payload=None) -> httpx.Response:
    return httpx.Response(
        status, json=payload or {}, request=httpx.Request("GET", "https://api.test/me/following")
    )


def _client() -> SpotifyUserClient:
    c = SpotifyUserClient(creds={"client_id": "i", "client_secret": "s", "refresh_token": "r"})
    c._token = "tok"
    c._exp = time.time() + 3600
    return c


def _follow_page(ids, after):
    return {
        "artists": {
            "items": [{"id": i, "name": f"artist-{i}"} for i in ids],
            "cursors": {"after": after},
        }
    }


@pytest.mark.unit
def test_get_followed_artists_pages_by_cursor():
    pages = [
        _resp(200, _follow_page(["a1", "a2"], after="a2")),
        _resp(200, _follow_page(["a3"], after=None)),
    ]
    calls = []

    def fake_request(method, url, **kwargs):
        calls.append(kwargs.get("params"))
        return pages[len(calls) - 1]

    with patch("worker.clients.spotify_user_client._request_with_retry", side_effect=fake_request):
        artists = _client().get_followed_artists()

    assert [a["id"] for a in artists] == ["a1", "a2", "a3"]
    assert "after" not in calls[0]
    assert calls[1]["after"] == "a2"


@pytest.mark.unit
def test_get_followed_artists_403_raises_scope_error():
    with patch(
        "worker.clients.spotify_user_client._request_with_retry",
        return_value=_resp(403),
    ):
        with pytest.raises(SpotifyScopeError):
            _client().get_followed_artists()


@pytest.mark.unit
def test_get_followed_artists_drops_idless_items():
    page = _follow_page(["a1"], after=None)
    page["artists"]["items"].append({"name": "no-id"})
    with patch(
        "worker.clients.spotify_user_client._request_with_retry",
        return_value=_resp(200, page),
    ):
        artists = _client().get_followed_artists()
    assert [a["id"] for a in artists] == ["a1"]


# ---------- service: run_follow_import orchestration ----------

class _FakeSession:
    """Answers the service's three statements by SQL shape. `matched` maps
    spotify_id → artist uuid; `user_exists` gates the users check."""

    def __init__(self, matched, user_exists=True, already_imported=frozenset()):
        self.matched = matched
        self.user_exists = user_exists
        self.already = set(already_imported)
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement, params=None):
        sql = str(statement)
        result = MagicMock()
        if "FROM users" in sql:
            result.first.return_value = (1,) if self.user_exists else None
        elif sql.strip().startswith("SELECT spotify_id"):
            hits = [s for s in params["sids"] if s in self.matched]
            result.scalars.return_value = iter(hits)
        elif "INSERT INTO user_artist_tracks" in sql:
            inserted = [
                self.matched[s]
                for s in params["sids"]
                if s in self.matched and self.matched[s] not in self.already
            ]
            result.scalars.return_value = iter(inserted)
        else:  # pragma: no cover - unexpected statement means the test is stale
            raise AssertionError(f"unexpected SQL: {sql}")
        return result

    def commit(self):
        self.committed = True


def _user_client_with(sids):
    client = MagicMock()
    client.get_followed_artists.return_value = [{"id": s, "name": s} for s in sids]
    return client


@pytest.mark.unit
def test_import_matched_only_no_fanout():
    aid = uuid.uuid4()
    session = _FakeSession(matched={"sp1": aid})
    ingest, rerun = MagicMock(return_value=0), MagicMock(return_value=True)

    metrics = run_follow_import(
        lambda: session, _user_client_with(["sp1"]),
        enqueue_ingest=ingest, enqueue_rerun=rerun, user_id=USER_ID,
    )

    assert metrics["followed"] == 1
    assert metrics["matched"] == 1
    assert metrics["imported"] == 1
    assert metrics["unmatched"] == 0
    assert session.committed
    ingest.assert_not_called()
    rerun.assert_not_called()


@pytest.mark.unit
def test_import_unmatched_fans_out_ingest_and_rerun():
    aid = uuid.uuid4()
    session = _FakeSession(matched={"sp1": aid})
    ingest, rerun = MagicMock(return_value=2), MagicMock(return_value=True)

    metrics = run_follow_import(
        lambda: session, _user_client_with(["sp1", "sp2", "sp3"]),
        enqueue_ingest=ingest, enqueue_rerun=rerun, user_id=USER_ID,
    )

    assert metrics["unmatched"] == 2
    assert metrics["ingest_enqueued"] == 2
    assert metrics["rerun_chained"] is True
    ingest.assert_called_once_with(["sp2", "sp3"])
    rerun.assert_called_once_with(USER_ID)


@pytest.mark.unit
def test_rerun_never_fans_out_again():
    session = _FakeSession(matched={})
    ingest, rerun = MagicMock(), MagicMock()

    metrics = run_follow_import(
        lambda: session, _user_client_with(["sp2"]),
        enqueue_ingest=ingest, enqueue_rerun=rerun, user_id=USER_ID, rerun=True,
    )

    assert metrics["unmatched"] == 1
    ingest.assert_not_called()
    rerun.assert_not_called()


@pytest.mark.unit
def test_already_tracked_rows_count_as_matched_not_imported():
    aid = uuid.uuid4()
    session = _FakeSession(matched={"sp1": aid}, already_imported={aid})
    metrics = run_follow_import(
        lambda: session, _user_client_with(["sp1"]),
        enqueue_ingest=MagicMock(), enqueue_rerun=MagicMock(), user_id=USER_ID,
    )
    assert metrics["matched"] == 1
    assert metrics["imported"] == 0


@pytest.mark.unit
def test_invalid_user_id_drops_before_spotify():
    client = MagicMock()
    metrics = run_follow_import(
        MagicMock(), client,
        enqueue_ingest=MagicMock(), enqueue_rerun=MagicMock(), user_id="not-a-uuid",
    )
    assert metrics["followed"] == 0
    client.get_followed_artists.assert_not_called()


@pytest.mark.unit
def test_unknown_user_drops_without_fanout():
    session = _FakeSession(matched={}, user_exists=False)
    ingest = MagicMock()
    metrics = run_follow_import(
        lambda: session, _user_client_with(["sp1"]),
        enqueue_ingest=ingest, enqueue_rerun=MagicMock(), user_id=USER_ID,
    )
    assert metrics["imported"] == 0
    assert not session.committed
    ingest.assert_not_called()


@pytest.mark.unit
def test_scope_error_is_dropped_not_raised():
    client = MagicMock()
    client.get_followed_artists.side_effect = SpotifyScopeError("missing scope")
    factory = MagicMock()
    metrics = run_follow_import(
        factory, client,
        enqueue_ingest=MagicMock(), enqueue_rerun=MagicMock(), user_id=USER_ID,
    )
    assert metrics["scope_error"] is True
    factory.assert_not_called()


@pytest.mark.unit
def test_empty_follow_list_never_opens_a_session():
    factory = MagicMock()
    metrics = run_follow_import(
        factory, _user_client_with([]),
        enqueue_ingest=MagicMock(), enqueue_rerun=MagicMock(), user_id=USER_ID,
    )
    assert metrics["followed"] == 0
    factory.assert_not_called()


# ---------- service: run_follow_ingest ----------

@pytest.mark.unit
def test_ingest_dedupes_sorts_and_enqueues():
    catalog = MagicMock()
    catalog.get_artist_albums.side_effect = [
        [{"id": "albB"}, {"id": "albA"}],
        [{"id": "albA"}, {"id": None}, None],
    ]
    enqueue = MagicMock()

    metrics = run_follow_ingest(catalog, enqueue, ["sp1", "sp2"])

    enqueue.assert_called_once_with(["albA", "albB"])
    assert metrics == {"artists": 2, "failed_artists": 0, "albums_enqueued": 2}
    assert catalog.get_artist_albums.call_args_list[0].kwargs["include_groups"] == "album,single"


@pytest.mark.unit
def test_ingest_isolates_per_artist_failure():
    catalog = MagicMock()
    catalog.get_artist_albums.side_effect = [RuntimeError("boom"), [{"id": "alb1"}]]
    enqueue = MagicMock()

    metrics = run_follow_ingest(catalog, enqueue, ["sp1", "sp2"])

    assert metrics["failed_artists"] == 1
    enqueue.assert_called_once_with(["alb1"])


@pytest.mark.unit
def test_ingest_no_albums_no_enqueue():
    catalog = MagicMock()
    catalog.get_artist_albums.return_value = []
    enqueue = MagicMock()
    metrics = run_follow_ingest(catalog, enqueue, ["sp1"])
    enqueue.assert_not_called()
    assert metrics["albums_enqueued"] == 0


# ---------- producer: chunking + delayed rerun ----------

@pytest.mark.unit
def test_enqueue_follow_ingest_chunks_and_sorts(monkeypatch):
    from worker.clients import sqs_producer

    monkeypatch.setattr(sqs_producer.settings, "SQS_QUEUE_URL", "https://q")
    fake_sqs = MagicMock()
    monkeypatch.setattr("boto3.client", lambda *a, **k: fake_sqs)

    sids = [f"sp{i:02d}" for i in range(25)]
    count = sqs_producer.enqueue_follow_ingest(list(reversed(sids)) + ["sp00", None])

    assert count == 25
    bodies = [
        json.loads(c.kwargs["MessageBody"]) for c in fake_sqs.send_message.call_args_list
    ]
    assert [len(b["artist_sids"]) for b in bodies] == [10, 10, 5]
    assert [b["job"] for b in bodies] == ["spotify_follow_ingest"] * 3
    assert [s for b in bodies for s in b["artist_sids"]] == sids


@pytest.mark.unit
def test_enqueue_follow_import_rerun_sets_delay(monkeypatch):
    from worker.clients import sqs_producer

    monkeypatch.setattr(sqs_producer.settings, "SQS_QUEUE_URL", "https://q")
    fake_sqs = MagicMock()
    monkeypatch.setattr("boto3.client", lambda *a, **k: fake_sqs)

    assert sqs_producer.enqueue_follow_import_rerun(USER_ID) is True
    call = fake_sqs.send_message.call_args
    assert call.kwargs["DelaySeconds"] == sqs_producer.FOLLOW_RERUN_DELAY_SECONDS
    body = json.loads(call.kwargs["MessageBody"])
    assert body == {"job": "spotify_follow_import", "user_id": USER_ID, "rerun": True}


@pytest.mark.unit
def test_producers_noop_without_queue_url(monkeypatch):
    from worker.clients import sqs_producer

    monkeypatch.setattr(sqs_producer.settings, "SQS_QUEUE_URL", "")
    assert sqs_producer.enqueue_follow_ingest(["sp1"]) == 0
    assert sqs_producer.enqueue_follow_import_rerun(USER_ID) is False


# ---------- handler routing ----------

@pytest.mark.unit
@patch("worker.handler._run_follow_import")
def test_handler_routes_follow_import(mock_run):
    from worker.handler import lambda_handler

    event = {
        "Records": [
            {"body": json.dumps({"job": "spotify_follow_import", "user_id": USER_ID, "rerun": True})}
        ]
    }
    assert lambda_handler(event, None) == {"batchItemFailures": []}
    mock_run.assert_called_once_with(USER_ID, rerun=True)


@pytest.mark.unit
@patch("worker.handler._run_follow_ingest")
def test_handler_routes_follow_ingest(mock_run):
    from worker.handler import lambda_handler

    event = {
        "Records": [
            {"body": json.dumps({"job": "spotify_follow_ingest", "artist_sids": ["sp1", "sp2"]})}
        ]
    }
    assert lambda_handler(event, None) == {"batchItemFailures": []}
    mock_run.assert_called_once_with(["sp1", "sp2"])
