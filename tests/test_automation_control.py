"""Owner pause for scheduled worker runs (FEAT-automation-admin 3b)."""
from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from worker.handler import lambda_handler
from myblog_shared_db.automation import WORKER_CRON_KEYS

from worker.service.automation_control import all_control_keys, control_key, skip_if_paused

# The shared list (myblog_shared_db.automation, V60) that the backend catalog also
# pins. A key changed on one side only must fail here, or that job would silently
# never pause.
CATALOG_CRON_KEYS = WORKER_CRON_KEYS


@pytest.mark.unit
def test_every_catalog_cron_key_is_reachable_and_nothing_else():
    assert all_control_keys() == CATALOG_CRON_KEYS


@pytest.mark.unit
@pytest.mark.parametrize("event, key", [
    # The constant inputs infra/eventbridge.tf sends.
    ({"version": "0", "source": "aws.events", "detail-type": "Scheduled Event"}, "musicbrainz_alias_fill"),
    ({"job": "spotify_listening"}, "spotify_listening"),
    ({"job": "release_upcoming_poll", "mode": "musicbrainz"}, "release_upcoming_poll_musicbrainz"),
    ({"job": "release_upcoming_poll", "mode": "itunes"}, "release_upcoming_poll_itunes"),
    ({"job": "release_upcoming_poll"}, "release_upcoming_poll_musicbrainz"),  # handler default
    ({"job": "spotify_saved_tracks_sync", "mode": "incremental"}, "spotify_saved_tracks_incremental"),
    ({"job": "spotify_saved_tracks_sync", "mode": "full"}, "spotify_saved_tracks_full"),
    ({"job": "spotify_saved_tracks_sync"}, "spotify_saved_tracks_incremental"),  # handler default
    ({"job": "isrc_backfill", "limit": 50}, "isrc_backfill"),  # a direct invoke with overrides
    # No control: manual-only, unknown, malformed.
    ({"job": "disc_no_backfill"}, None),
    ({"job": "release_upcoming_poll", "mode": "spotify"}, None),
    ({"job": ["spotify_listening"]}, None),
    ({"source": "aws.s3"}, None),
    ({}, None),
])
def test_control_key(event, key):
    assert control_key(event) == key


def _factory(paused):
    db = MagicMock()
    db.execute.return_value.first.return_value = ("k",) if paused else None
    factory = MagicMock()
    factory.return_value.__enter__.return_value = db
    return factory, db


@pytest.mark.unit
@pytest.mark.pause_check
def test_paused_cron_is_skipped_and_its_job_never_runs():
    factory, db = _factory(paused=True)
    with patch("worker.handler.SessionLocal", factory), \
            patch("worker.handler._run_listening_sync") as run:
        assert lambda_handler({"job": "spotify_listening"}, None) == {"skipped": "paused"}
    run.assert_not_called()
    db.commit.assert_called_once()
    sql = str(db.execute.call_args_list[-1].args[0])
    assert "honored_at = clock_timestamp()" in sql and "AND paused" in sql
    assert db.execute.call_args_list[-1].args[1] == {"key": "spotify_listening"}


@pytest.mark.unit
@pytest.mark.pause_check
def test_unpaused_cron_runs():
    factory, _db = _factory(paused=False)
    with patch("worker.handler.SessionLocal", factory), \
            patch("worker.handler._run_listening_sync") as run:
        assert lambda_handler({"job": "spotify_listening"}, None) == {}
    run.assert_called_once()


@pytest.mark.unit
@pytest.mark.pause_check
def test_sqs_records_are_never_paused():
    factory, db = _factory(paused=True)
    event = {"Records": [{"body": '{"job": "spotify_refresh"}'}]}
    with patch("worker.handler.SessionLocal", factory), \
            patch("worker.handler._run_listening_sync") as run:
        assert lambda_handler(event, None) == {"batchItemFailures": []}
    run.assert_called_once_with(is_manual_refresh=True)
    db.execute.assert_not_called()


@pytest.mark.unit
@pytest.mark.pause_check
def test_event_without_control_never_touches_the_db():
    factory = MagicMock()
    assert skip_if_paused({"job": "disc_no_backfill"}, factory) is False
    factory.assert_not_called()


@pytest.mark.unit
@pytest.mark.pause_check
def test_read_failure_fails_open_and_logs_only_the_type(caplog):
    factory = MagicMock(side_effect=RuntimeError("postgresql://user:secret@host/db"))
    with caplog.at_level(logging.ERROR):
        assert skip_if_paused({"job": "album_ingest"}, factory) is False
    assert "RuntimeError" in caplog.text and "running anyway" in caplog.text
    assert "secret" not in caplog.text


@pytest.mark.unit
@pytest.mark.pause_check
def test_session_is_forced_read_write_before_the_update():
    factory, db = _factory(paused=False)
    skip_if_paused({"job": "album_ingest"}, factory)
    first = str(db.execute.call_args_list[0].args[0])
    assert first == "SET TRANSACTION READ WRITE"


@pytest.mark.unit
@pytest.mark.pause_check
def test_self_chained_sqs_lyrics_incremental_honours_the_pause():
    # The worker enqueues this itself after every album sync; exempting it would let
    # a paused job keep running while the page reads "confirmed".
    factory, db = _factory(paused=True)
    event = {"Records": [{"body": '{"job": "lyrics_incremental"}'}]}
    with patch("worker.handler.SessionLocal", factory), \
            patch("worker.handler._run_lyrics_incremental") as run:
        assert lambda_handler(event, None) == {"batchItemFailures": []}
    run.assert_not_called()
    assert db.execute.call_args_list[-1].args[1] == {"key": "lyrics_incremental"}


@pytest.mark.unit
@pytest.mark.pause_check
def test_unpaused_sqs_lyrics_incremental_runs():
    factory, _db = _factory(paused=False)
    event = {"Records": [{"body": '{"job": "lyrics_incremental", "limit": 5}'}]}
    with patch("worker.handler.SessionLocal", factory), \
            patch("worker.handler._run_lyrics_incremental") as run:
        lambda_handler(event, None)
    run.assert_called_once_with(limit=5)
