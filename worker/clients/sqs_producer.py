# Minimal SQS producer for the worker.
#
# The worker is normally an SQS *consumer*; this is the one place it produces — to
# re-enqueue recently-played albums that aren't in our catalog yet, reusing the
# existing candidates→SQS album-sync pipeline (worker/handler.py SQS branch consumes
# {"album_ids": [...], "market": ...}). Best-effort: a missing queue URL or IAM
# permission logs and no-ops rather than failing the cache sync.
from __future__ import annotations

import json
import logging
from typing import List

from worker.core.config import settings

logger = logging.getLogger(__name__)

# Spotify /albums?ids= caps at 20 per call; the consumer chunks anyway, but keep
# producer messages within the same bound.
_MAX_PER_MESSAGE = 20


def enqueue_album_sync(album_ids: List[str]) -> None:
    """Send album-sync messages for the given Spotify album IDs. No-op if no queue
    URL is configured."""
    album_ids = [a for a in (album_ids or []) if a]
    if not album_ids:
        return
    if not settings.SQS_QUEUE_URL:
        logger.info("SQS_QUEUE_URL unset; skipping enqueue of %d unknown albums", len(album_ids))
        return

    import boto3

    sqs = boto3.client(
        "sqs",
        region_name=settings.AWS_DEFAULT_REGION,
        endpoint_url=(settings.LOCALSTACK_ENDPOINT or None),
    )
    for i in range(0, len(album_ids), _MAX_PER_MESSAGE):
        chunk = album_ids[i : i + _MAX_PER_MESSAGE]
        body = json.dumps({"album_ids": chunk, "market": settings.SPOTIFY_DEFAULT_MARKET})
        sqs.send_message(QueueUrl=settings.SQS_QUEUE_URL, MessageBody=body)
    logger.info("enqueued %d unknown recently-played albums for catalog sync", len(album_ids))
