# myblog_worker

AWS Lambda function that consumes SQS album-sync messages and writes album, track, and artist data to PostgreSQL via the Spotify API.

## Stack

- **Runtime**: Python 3.12, Lambda (no FastAPI — pure handler)
- **DB**: PostgreSQL via SQLAlchemy 2 + psycopg3; table objects from `myblog-shared-db` package (`myblog_shared_db.tables`)
- **External**: Spotify Web API (via `worker/clients/spotify_client.py`)
- **Trigger**: AWS SQS (`album-sync-queue`)
- **Deploy**: `build.sh` → zip

## Structure

```
worker/
├── handler.py           ← Lambda entry point (lambda_handler)
├── core/config.py       ← Settings (pydantic BaseSettings)
├── infra/db.py          ← SessionLocal factory
├── clients/
│   └── spotify_client.py ← Spotify API wrapper
└── service/
    └── sync_service.py  ← AlbumSyncService (bulk upsert), generate_and_save_aliases

Note: `worker/infra/tables.py` was removed (ARCH-6). Table definitions now come from
`myblog_shared_db.tables`; `sync_service.py` uses raw `sqlalchemy.text()` directly.
```

## Message Formats

See `docs/contracts/sqs-album-sync.md` for the full contract.

**Format A (batch)** — preferred:
```json
{"album_ids": ["<spotify_id>", ...], "market": "KR"}
```

**Format B (single)** — legacy:
```json
{"spotify_album_id": "<spotify_id>", "market": "KR"}
```

## Lambda Return Format

The handler **must** return `batchItemFailures` — never raise an exception at the top level:

```python
return {"batchItemFailures": [{"itemIdentifier": mid} for mid in failed]}
```

Raising instead of returning causes SQS to retry the **entire batch**. Per-record exceptions are caught, the `messageId` collected, and only failed records are retried.

## Processing Flow

1. Parse `record["body"]` as JSON
2. Dispatch to `_process_batch` (Format A) or `_process_single` (Format B)
3. `AlbumSyncService.sync_albums_batch` — bulk upsert albums, tracks, artists in one transaction
4. After commit, call `generate_and_save_aliases` (separate transaction) — Gemini API alias generation for artists without aliases

## Hard Rules

- **Never raise an exception inside the `for record in records` loop** — catch it, append to `failed`, and continue.
- **Never add a synchronous Spotify API call to any user-facing path** — this service is async-only.
- **Never call `print()`** — use `logging.getLogger(__name__)`.
- **Never work directly on `main`** — branch from `main`, PR back.

## Config

```
DATABASE_URL=postgresql+psycopg://...
SPOTIFY_CLIENT_ID=...
SPOTIFY_CLIENT_SECRET=...
AWS_DEFAULT_REGION=ap-northeast-2
SQS_QUEUE_URL=...
SPOTIFY_DEFAULT_MARKET=KR
DRY_RUN=false        # set true to fetch from Spotify without writing to DB
GEMINI_API_KEY=...   # optional; skip alias generation if unset
LOCALSTACK_ENDPOINT= # local only
```

## Running Locally

```bash
pip install -r requirements.txt
python worker/run_local.py   # polls SQS in a loop
```

For end-to-end local testing, start LocalStack and set `SQS_QUEUE_URL` and `LOCALSTACK_ENDPOINT`.

## Tests

```bash
pytest
```

Test config in `pytest.ini` (if present). Tests in `tests/`.

## Verification

```bash
python -c "from worker.handler import lambda_handler; print('import ok')"
pytest --tb=short
```
