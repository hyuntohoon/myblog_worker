# worker/clients/lrclib_client.py
"""LRCLIB ``/api/search`` client for FEAT-lyrics-corpus Step 3 (incremental collection).

The historical backfill (Step 2) ran the canonical matcher over the LRCLIB dump / a
one-off API batch tool. Incremental collection needs **current** data for a handful of
newly-ingested tracks per run, so it uses the live API instead of a stale local dump.

This client only *retrieves* candidate rows and adapts them into
``worker.service.lyrics_matcher.Candidate``; the pure ``decide_match`` core does all the
matching. It deliberately distinguishes a legitimate **no-match** (HTTP 404 / empty list
-> the caller parks the track as ``not_found``) from a **transient failure** (network /
5xx / 429 -> retried with backoff, then ``LrclibTransientError`` so the caller *skips* the
track and leaves it unwritten for the next run — a source outage never poisons a row as
``not_found`` and never blocks album sync). Mirrors ``tools/lyrics_batch_api.py``.
"""
from __future__ import annotations

import logging
import time
from typing import List

import httpx

from worker.core.config import settings
from worker.service.lyrics_matcher import Candidate

logger = logging.getLogger(__name__)

_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_USER_AGENT = "myblog-lyrics-incremental/1.0 (private research; FEAT-lyrics-corpus)"


class LrclibTransientError(Exception):
    """LRCLIB was unreachable / erroring after retries — skip + resume, never ``not_found``."""


class LrclibClient:
    """Thin, thread-safe (shared ``httpx.Client``) LRCLIB search client."""

    def __init__(self, *, timeout: float = 20.0, max_connections: int = 24) -> None:
        limits = httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_connections,
        )
        self._client = httpx.Client(
            timeout=timeout,
            limits=limits,
            headers={"User-Agent": _USER_AGENT},
        )

    def search_candidates(self, title: str, artist: str, *, max_retries: int = 3) -> List[Candidate]:
        """GET ``/api/search`` -> ``list[Candidate]``.

        404 / empty body -> ``[]`` (legitimate no-match; caller parks as ``not_found``).
        network / 5xx / 429 -> retried with linear backoff, then ``LrclibTransientError``.
        """
        params = {"track_name": title, "artist_name": artist}
        last = None
        for attempt in range(max_retries):
            try:
                resp = self._client.get(settings.LYRICS_LRCLIB_SEARCH_URL, params=params)
                if resp.status_code == 404:
                    return []
                if resp.status_code in _RETRYABLE_STATUS:
                    last = f"HTTP {resp.status_code}"
                    time.sleep(1.0 * (attempt + 1))
                    continue
                resp.raise_for_status()
                data = resp.json()
                recs = data if isinstance(data, list) else []
                return [Candidate.from_api(r) for r in recs]
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                last = repr(exc)
                time.sleep(1.0 * (attempt + 1))
                continue
            except httpx.HTTPError as exc:
                # A non-retryable HTTP error (4xx other than 404) — treat as a transient
                # skip rather than fabricate a not_found; the row is retried next run.
                last = repr(exc)
                break
        raise LrclibTransientError(last or "unknown")

    def close(self) -> None:
        self._client.close()
