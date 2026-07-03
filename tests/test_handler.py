# tests/test_handler.py
"""lambda_handler 테스트.
sync_service와 spotify를 mock해서 handler 로직만 검증.
"""
import json
import pytest
from unittest.mock import patch, MagicMock

from worker.handler import lambda_handler


@pytest.mark.unit
@patch("worker.handler.spotify")
@patch("worker.handler.AlbumSyncService")
@patch("worker.handler.SessionLocal")
def test_handler_batch_format(mock_session_local, mock_svc_class, mock_spotify):
    """album_ids 배치 포맷 메시지를 처리하는지 확인."""
    mock_session = MagicMock()
    mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_session_local.return_value.__exit__ = MagicMock(return_value=False)

    event = {
        "Records": [{
            "body": json.dumps({
                "album_ids": ["abc123", "def456"],
                "market": "KR",
            })
        }]
    }

    results = lambda_handler(event, None)

    assert results == {"batchItemFailures": []}


@pytest.mark.unit
def test_handler_empty_records():
    """빈 Records를 넘겨도 에러 없이 동작하는지 확인."""
    event = {"Records": []}
    results = lambda_handler(event, None)
    assert results == {"batchItemFailures": []}


@pytest.mark.unit
@patch("worker.handler.AlbumSyncService")
@patch("worker.handler.SessionLocal")
def test_handler_unknown_format(mock_session_local, mock_svc_class):
    """알 수 없는 메시지 포맷은 스킵(성공 처리)하는지 확인."""
    event = {
        "Records": [{
            "body": json.dumps({"unknown_field": "value"})
        }]
    }

    results = lambda_handler(event, None)
    assert results == {"batchItemFailures": []}


@pytest.mark.unit
@patch("worker.handler.AlbumSyncService")
@patch("worker.handler.SessionLocal")
def test_handler_error_returns_true(mock_session_local, mock_svc_class):
    """처리 중 에러가 나면 해당 record를 batchItemFailures에 추가하는지 확인."""
    mock_session_local.side_effect = Exception("DB connection failed")

    event = {
        "Records": [{
            "messageId": "msg-001",
            "body": json.dumps({
                "album_ids": ["abc123"],
                "market": "KR",
            })
        }]
    }

    results = lambda_handler(event, None)
    assert results == {"batchItemFailures": [{"itemIdentifier": "msg-001"}]}


@pytest.mark.unit
@patch("worker.handler.generate_and_save_aliases")
def test_handler_eventbridge_trigger_runs_alias_generation(mock_alias):
    """EventBridge scheduled event triggers alias generation, not SQS processing."""
    event = {
        "source": "aws.events",
        "detail-type": "Scheduled Event",
        "detail": {},
    }

    result = lambda_handler(event, None)

    mock_alias.assert_called_once()
    assert result == {}


@pytest.mark.unit
@patch("worker.handler.generate_and_save_aliases")
@patch("worker.handler.AlbumSyncService")
@patch("worker.handler.SessionLocal")
def test_handler_sqs_does_not_call_alias_generation(mock_session_local, mock_svc_class, mock_alias):
    """SQS sync path must NOT trigger alias generation (decoupled)."""
    mock_session = MagicMock()
    mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_session_local.return_value.__exit__ = MagicMock(return_value=False)

    event = {
        "Records": [{
            "body": json.dumps({"album_ids": ["abc123"], "market": "KR"})
        }]
    }

    lambda_handler(event, None)

    mock_alias.assert_not_called()


# ── FEAT-member-dashboard Step 3: Spotify listening sync routing ────────────────

@pytest.mark.unit
@patch("worker.handler._run_listening_sync")
@patch("worker.handler.generate_and_save_aliases")
def test_handler_eventbridge_listening_job(mock_alias, mock_listening):
    """EventBridge 1h rule sends {"job": "spotify_listening"} → listening sync, and
    must NOT hit the alias path even though both are EventBridge crons."""
    result = lambda_handler({"job": "spotify_listening"}, None)
    mock_listening.assert_called_once()
    mock_alias.assert_not_called()
    assert result == {}


@pytest.mark.unit
@patch("worker.handler._run_listening_sync")
def test_handler_sqs_manual_refresh_job(mock_listening):
    """Manual '지금 새로고침' SQS message → async listening sync (rule #9)."""
    event = {"Records": [{"body": json.dumps({"job": "spotify_refresh"})}]}
    result = lambda_handler(event, None)
    mock_listening.assert_called_once()
    assert result == {"batchItemFailures": []}


# ── FEAT-spotify-library-sync Step 2: Spotify Library reconcile routing ──────────

@pytest.mark.unit
@patch("worker.handler._run_library_sync")
def test_handler_sqs_library_sync_job(mock_library):
    """{"job": "spotify_library_sync"} SQS message → library reconcile (rule #9: the
    backend endpoint only enqueues)."""
    event = {"Records": [{"body": json.dumps({"job": "spotify_library_sync"})}]}
    result = lambda_handler(event, None)
    mock_library.assert_called_once()
    assert result == {"batchItemFailures": []}


@pytest.mark.unit
@patch("worker.handler._run_library_sync")
@patch("worker.handler._run_listening_sync")
def test_handler_library_sync_does_not_call_listening(mock_listening, mock_library):
    """The library job must route ONLY to the library reconcile, not the listening
    sync (both are 'job'-tagged SQS messages)."""
    event = {"Records": [{"body": json.dumps({"job": "spotify_library_sync"})}]}
    lambda_handler(event, None)
    mock_library.assert_called_once()
    mock_listening.assert_not_called()


# ── FEAT-lyrics-corpus Step 3: incremental lyrics collection routing ─────────────

@pytest.mark.unit
@patch("worker.handler._run_lyrics_incremental")
@patch("worker.handler.generate_and_save_aliases")
def test_handler_eventbridge_lyrics_incremental_job(mock_alias, mock_lyrics):
    """EventBridge rule sends {"job": "lyrics_incremental"} → incremental collection,
    and must NOT hit the alias path (both are EventBridge crons; job is routed first)."""
    result = lambda_handler({"job": "lyrics_incremental"}, None)
    mock_lyrics.assert_called_once_with(limit=None)
    mock_alias.assert_not_called()
    assert result == {}


@pytest.mark.unit
@patch("worker.handler._run_lyrics_incremental")
def test_handler_lyrics_incremental_honors_limit_override(mock_lyrics):
    """An explicit "limit" (manual SQS/EventBridge trigger) is passed through."""
    result = lambda_handler({"job": "lyrics_incremental", "limit": 25}, None)
    mock_lyrics.assert_called_once_with(limit=25)
    assert result == {}


# ── FEAT-lyrics-corpus Step 4: periodic reassessment routing ─────────────────────

@pytest.mark.unit
@patch("worker.handler._run_lyrics_reassessment")
@patch("worker.handler._run_lyrics_incremental")
@patch("worker.handler.generate_and_save_aliases")
def test_handler_eventbridge_lyrics_reassessment_job(mock_alias, mock_incr, mock_reassess):
    """{"job": "lyrics_reassessment"} → reassessment only; must NOT hit incremental or alias."""
    result = lambda_handler({"job": "lyrics_reassessment"}, None)
    mock_reassess.assert_called_once_with(limit=None)
    mock_incr.assert_not_called()
    mock_alias.assert_not_called()
    assert result == {}


@pytest.mark.unit
@patch("worker.handler._run_lyrics_reassessment")
def test_handler_lyrics_reassessment_honors_limit_override(mock_reassess):
    """An explicit "limit" is passed through to the reassessment job."""
    result = lambda_handler({"job": "lyrics_reassessment", "limit": 40}, None)
    mock_reassess.assert_called_once_with(limit=40)
    assert result == {}


# --------------------------------------------------------------------------
# Near-real-time lyrics chaining (album sync → SQS {"job":"lyrics_incremental"})
# --------------------------------------------------------------------------
@pytest.mark.unit
@patch("worker.handler.settings.DRY_RUN", False)  # CI test env sets DRY_RUN=true (deploy.yml)
@patch("worker.clients.sqs_producer.enqueue_lyrics_incremental")
@patch("worker.handler.spotify")
@patch("worker.handler.AlbumSyncService")
@patch("worker.handler.SessionLocal")
def test_album_sync_chains_one_lyrics_pass(mock_session_local, mock_svc_class, mock_spotify, mock_chain):
    """Album-sync records (any count, both formats) chain exactly ONE lyrics message."""
    event = {"Records": [
        {"body": json.dumps({"album_ids": ["a1", "a2"], "market": "KR"})},
        {"body": json.dumps({"spotify_album_id": "a3", "market": "KR"})},
    ]}
    results = lambda_handler(event, None)
    assert results == {"batchItemFailures": []}
    mock_chain.assert_called_once()


@pytest.mark.unit
@patch("worker.clients.sqs_producer.enqueue_lyrics_incremental")
@patch("worker.handler._run_listening_sync")
def test_non_album_records_do_not_chain(mock_listening, mock_chain):
    """A non-album SQS job must not trigger the lyrics chain."""
    event = {"Records": [{"body": json.dumps({"job": "spotify_refresh"})}]}
    lambda_handler(event, None)
    mock_chain.assert_not_called()


@pytest.mark.unit
@patch("worker.handler.settings.DRY_RUN", False)  # CI test env sets DRY_RUN=true (deploy.yml)
@patch("worker.clients.sqs_producer.enqueue_lyrics_incremental", side_effect=RuntimeError("sqs down"))
@patch("worker.handler.spotify")
@patch("worker.handler.AlbumSyncService")
@patch("worker.handler.SessionLocal")
def test_chain_failure_does_not_fail_album_records(mock_session_local, mock_svc_class, mock_spotify, mock_chain):
    """Best-effort send: a failed chain enqueue must not fail the album batch (cron covers)."""
    event = {"Records": [{"body": json.dumps({"album_ids": ["a1"], "market": "KR"})}]}
    results = lambda_handler(event, None)
    assert results == {"batchItemFailures": []}
    mock_chain.assert_called_once()


@pytest.mark.unit
@patch("worker.handler.settings.DRY_RUN", True)
@patch("worker.clients.sqs_producer.enqueue_lyrics_incremental")
@patch("worker.handler.spotify")
@patch("worker.handler.AlbumSyncService")
@patch("worker.handler.SessionLocal")
def test_dry_run_does_not_chain(mock_session_local, mock_svc_class, mock_spotify, mock_chain):
    """DRY_RUN wrote no tracks, so there is nothing to chain."""
    event = {"Records": [{"body": json.dumps({"album_ids": ["a1"], "market": "KR"})}]}
    results = lambda_handler(event, None)
    assert results == {"batchItemFailures": []}
    mock_chain.assert_not_called()


@pytest.mark.unit
@patch("worker.clients.sqs_producer.enqueue_lyrics_incremental")
@patch("worker.handler._run_lyrics_incremental")
def test_sqs_lyrics_incremental_routed_in_record_loop(mock_run, mock_chain):
    """SQS-delivered {"job":"lyrics_incremental"} routes in the record loop (the
    eventbridge.tf manual-message contract) and must NOT re-chain (no feedback loop)."""
    event = {"Records": [{"body": json.dumps({"job": "lyrics_incremental", "limit": 7})}]}
    results = lambda_handler(event, None)
    assert results == {"batchItemFailures": []}
    mock_run.assert_called_once_with(limit=7)
    mock_chain.assert_not_called()


@pytest.mark.unit
@patch("worker.handler._run_lyrics_reassessment")
def test_sqs_lyrics_reassessment_routed_in_record_loop(mock_run):
    """SQS-delivered {"job":"lyrics_reassessment"} routes in the record loop too."""
    event = {"Records": [{"body": json.dumps({"job": "lyrics_reassessment"})}]}
    results = lambda_handler(event, None)
    assert results == {"batchItemFailures": []}
    mock_run.assert_called_once_with(limit=None)
