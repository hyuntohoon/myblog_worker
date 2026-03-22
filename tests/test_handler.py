# tests/test_handler.py
"""lambda_handler 테스트.
sync_service와 spotify를 mock해서 handler 로직만 검증.
"""
import json
import pytest
from unittest.mock import patch, MagicMock

from worker.handler import lambda_handler


@pytest.mark.unit
@patch("worker.handler.AlbumSyncService")
@patch("worker.handler.SessionLocal")
def test_handler_batch_format(mock_session_local, mock_svc_class):
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

    assert results == [True]


@pytest.mark.unit
def test_handler_empty_records():
    """빈 Records를 넘겨도 에러 없이 동작하는지 확인."""
    event = {"Records": []}
    results = lambda_handler(event, None)
    assert results == []


@pytest.mark.unit
@patch("worker.handler.AlbumSyncService")
@patch("worker.handler.SessionLocal")
def test_handler_unknown_format(mock_session_local, mock_svc_class):
    """알 수 없는 메시지 포맷은 스킵하는지 확인."""
    event = {
        "Records": [{
            "body": json.dumps({"unknown_field": "value"})
        }]
    }

    results = lambda_handler(event, None)
    assert results == [True]  # 스킵하되 True 반환


@pytest.mark.unit
@patch("worker.handler.AlbumSyncService")
@patch("worker.handler.SessionLocal")
def test_handler_error_returns_true(mock_session_local, mock_svc_class):
    """처리 중 에러가 나도 True를 반환하는지 확인 (재시도 비활성 정책)."""
    mock_session_local.side_effect = Exception("DB connection failed")

    event = {
        "Records": [{
            "body": json.dumps({
                "album_ids": ["abc123"],
                "market": "KR",
            })
        }]
    }

    results = lambda_handler(event, None)
    assert results == [True]
