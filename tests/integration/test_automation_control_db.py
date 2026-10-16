"""FEAT-automation-admin 3b — the owner-pause check against real Postgres.

The unit suite stubs the session, so it cannot see whether the check-and-stamp
statement is valid SQL against V60, whether `AND paused` really gates the stamp,
or whether a missing row reads as "run". This file can. Gated on TEST_DB_URL;
each test resets the control row it touched.
"""
from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from worker.service.automation_control import skip_if_paused

_TEST_DB_URL = os.environ.get("TEST_DB_URL")

pytestmark = pytest.mark.skipif(
    not _TEST_DB_URL,
    reason="integration test requires TEST_DB_URL env var (Postgres test database)",
)

KEY = "artist_photo_backfill"
EVENT = {"job": KEY}


@pytest.fixture(scope="module")
def engine():
    eng = create_engine(_TEST_DB_URL, pool_pre_ping=True, future=True)
    yield eng
    eng.dispose()


@pytest.fixture
def control(engine):
    def set_row(paused):
        with engine.begin() as c:
            c.execute(text("INSERT INTO automation_controls (job_key, paused) VALUES (:k, :p) "
                           "ON CONFLICT (job_key) DO UPDATE SET paused = EXCLUDED.paused, honored_at = NULL"),
                      {"k": KEY, "p": paused})

    def honored():
        with engine.connect() as c:
            return c.execute(text("SELECT honored_at FROM automation_controls WHERE job_key = :k"),
                             {"k": KEY}).scalar_one_or_none()

    with engine.begin() as c:
        c.execute(text("DELETE FROM automation_controls WHERE job_key = :k"), {"k": KEY})
    yield set_row, honored
    with engine.begin() as c:
        c.execute(text("DELETE FROM automation_controls WHERE job_key = :k"), {"k": KEY})


def test_no_row_means_run(engine, control):
    assert skip_if_paused(EVENT, sessionmaker(bind=engine)) is False


def test_paused_skips_and_stamps(engine, control):
    set_row, honored = control
    set_row(True)
    assert skip_if_paused(EVENT, sessionmaker(bind=engine)) is True
    assert honored() is not None


def test_resumed_runs_and_does_not_stamp(engine, control):
    set_row, honored = control
    set_row(False)
    assert skip_if_paused(EVENT, sessionmaker(bind=engine)) is False
    assert honored() is None


def test_another_jobs_pause_does_not_leak(engine, control):
    set_row, _ = control
    set_row(True)
    assert skip_if_paused({"job": "isrc_backfill"}, sessionmaker(bind=engine)) is False


# ── run-now stamps (FEAT-automation-admin 3c, V62) ──────────────────────────────

import uuid

from worker.service.automation_control import stamp_run_end, stamp_run_start


@pytest.fixture
def run_rows(engine):
    """A run row as the backend leaves it (invoked) and a pause row beside it."""
    run_id, pause_id = uuid.uuid4(), uuid.uuid4()
    insert = text("INSERT INTO automation_control_events (request_id, job_key, action, outcome, actor_sub) "
                  "VALUES (:rid, :k, :a, :o, 'it-worker')")
    with engine.begin() as c:
        c.execute(insert, {"rid": run_id, "k": KEY, "a": "run", "o": "invoked"})
        c.execute(insert, {"rid": pause_id, "k": KEY, "a": "pause", "o": "applied"})

    def read(rid):
        with engine.connect() as c:
            return c.execute(text("SELECT started_at, finished_at, run_outcome FROM automation_control_events "
                                  "WHERE request_id = :rid"), {"rid": rid}).one()

    yield run_id, pause_id, read
    with engine.begin() as c:
        c.execute(text("DELETE FROM automation_control_events WHERE request_id IN (:a, :b)"),
                  {"a": run_id, "b": pause_id})


def test_run_stamps_start_then_end(engine, run_rows):
    run_id, _, read = run_rows
    factory = sessionmaker(bind=engine)
    stamp_run_start(run_id, factory)
    started, finished, outcome = read(run_id)
    assert started is not None and finished is None and outcome is None
    stamp_run_end(run_id, "ok", factory)
    started2, finished, outcome = read(run_id)
    assert started2 == started and finished >= started and outcome == "ok"


def test_a_retry_restamps_start_and_clears_the_end(engine, run_rows):
    run_id, _, read = run_rows
    factory = sessionmaker(bind=engine)
    stamp_run_start(run_id, factory)
    stamp_run_end(run_id, "error", factory)
    first_start = read(run_id)[0]
    stamp_run_start(run_id, factory)
    started, finished, outcome = read(run_id)
    assert started > first_start and finished is None and outcome is None


def test_a_pause_rows_id_is_never_stamped(engine, run_rows, caplog):
    _, pause_id, read = run_rows
    factory = sessionmaker(bind=engine)
    stamp_run_start(pause_id, factory)
    stamp_run_end(pause_id, "ok", factory)
    assert read(pause_id) == (None, None, None)
    # Matched out by `action = 'run'`, not bounced off V62's CHECK as an error.
    assert "run stamp failed" not in caplog.text


def test_an_unknown_id_is_a_no_op(engine, run_rows):
    stamp_run_end(uuid.uuid4(), "ok", sessionmaker(bind=engine))  # no row, no raise
