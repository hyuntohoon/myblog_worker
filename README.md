# worker_lambda (minimal)

SQS → Lambda worker that fetches Spotify album data.

## Files
- `worker/handler.py` : Lambda entrypoint
- `worker/clients/spotify_client.py` : httpx-based Spotify client
- `worker/core/config.py` : pydantic-settings loader

## Configure
Edit `.env` with your Spotify credentials.

## Lambda Handler
Set the handler to:
```
worker.handler.lambda_handler
```

## Local quick test
```bash
python -c 'from worker.handler import lambda_handler; import json; lambda_handler({"Records":[{"body": json.dumps({"spotify_album_id":"4a6NzYL1YHRUgx9e3YZI6I","market":"KR"})}]}, None)'
```
