"""Every SQS send in the worker carries an explicit timeout (CLAUDE.md "every outbound HTTP
request has an explicit timeout"; swept with backend #189's SQS client fix)."""
from __future__ import annotations

import inspect
from unittest.mock import patch

from worker.clients import sqs_producer


def test_the_one_sqs_client_has_explicit_timeouts():
    with patch("boto3.client") as boto:
        sqs_producer._sqs()
    cfg = boto.call_args.kwargs["config"]
    assert (cfg.connect_timeout, cfg.read_timeout) == (3, 5)


def test_no_send_builds_its_own_client():
    assert inspect.getsource(sqs_producer).count("boto3.client(") == 1
