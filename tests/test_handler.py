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
