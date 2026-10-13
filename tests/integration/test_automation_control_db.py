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
