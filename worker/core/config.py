from __future__ import annotations

import json
import logging
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    # App / Env
    APP_NAME: str = "music-backend"
    ENV: str = "local"

    DATABASE_URL: str = ""

    # Spotify
    SPOTIFY_CLIENT_ID: str = ""
    SPOTIFY_CLIENT_SECRET: str = ""
    SPOTIFY_TOKEN_URL: str = "https://accounts.spotify.com/api/token"
    SPOTIFY_API_BASE: str = "https://api.spotify.com/v1"
    SPOTIFY_DEFAULT_MARKET: str = "KR"

    # Last.fm (FEAT-multi-user Phase 3a) — public-profile reads need only an api_key
    # + username (no OAuth). The key lives in the SSM /myblog/worker blob (no new IAM).
    # OPTIONAL: unset ⇒ the poll no-ops (never a boot failure).
    LASTFM_API_KEY: str = ""
    LASTFM_API_BASE: str = "https://ws.audioscrobbler.com/2.0/"
    # Per-tick user bound so the 120s Lambda always finishes.
    LASTFM_MAX_USERS_PER_TICK: int = 50

    # Spotify user-scoped player reads (FEAT-member-dashboard Step 3).
    # Refresh token + client creds live in Secrets Manager myblog/spotify (Q17);
    # SPOTIFY_REFRESH_TOKEN is an env fallback for local dev / tests only.
    SPOTIFY_SECRETS_ARN: str = ""
    # SSM SecureString name (e.g. /myblog/spotify) — takes priority over the ARN
    # for both the creds read and the token write-back (CHORE-secrets-ssm-migration).
    SPOTIFY_SECRETS_PARAM: str = ""
    SPOTIFY_REFRESH_TOKEN: str = ""

    # Per-user Spotify listening poll (FEAT-multi-user Phase 3b-d).
    # USER_TOKENS_KMS_KEY_ID = the customer-managed CMK (alias/myblog-user-tokens,
    # 3b-a) — Lambda env var, same as the backend connect path; needed only for the
    # rotation re-encrypt (Decrypt reads the key id from the envelope). Unset ⇒
    # decrypt+poll still work; a rotation keeps the old payload (logged).
    USER_TOKENS_KMS_KEY_ID: str = ""
    # Per-tick member bound so the 120s Lambda always finishes (the ≤5-user tier
    # means one tick covers everyone; stalest-synced-first rotation above that).
    SPOTIFY_MEMBER_MAX_USERS_PER_TICK: int = 10

    # AWS / SQS (for local testing convenience)
    AWS_DEFAULT_REGION: str = "ap-northeast-2"
    LOCALSTACK_ENDPOINT: str | None = None
    SQS_QUEUE_URL: str | None = None

    # Control flags
    DRY_RUN: bool = False

    # Spotify Library two-way sync (FEAT-spotify-library-sync Step 2).
    # PLAN-ONLY by default: the reconcile reads /me/albums, computes diffs, PULLs
    # pre-existing saved albums into the special bucket, stamps source, and updates
    # our DB state + logs the intended PUT/DELETE sets — but issues NO real
    # PUT/DELETE /me/albums. Flip True to execute real Spotify writes. (DB writes
    # always happen; only the Spotify mutations are gated.) The worker reads THIS
    # flag, never the SQS message, so a stray/replayed message can't force a write.
    SPOTIFY_LIBRARY_WRITES_ENABLED: bool = False

    # Scheduled album-catalog ingest (FEAT-album-catalog-ingest Step 2).
    # New-releases-only sweep of catalog artists: gates + bounds are curation
    # policy (owner-accepted 2026-06-10), NOT storage limits. INGEST_SINCE is the
    # mode switch — albums released before it are never batch-ingested (the
    # reactive candidates path covers back-catalog on demand); relax it to
    # enable backfill.
    ARTIST_POP_MIN: int = 60
    ALBUM_POP_MIN: int = 20
    SWEEP_ARTISTS_PER_TICK: int = 30
    MAX_ENQUEUE_PER_TICK: int = 60
    MAX_CATALOG_ALBUMS: int = 5000
    INGEST_SINCE: str = "2026-06-10"

    # Multi-source upcoming-release poller (FEAT-release-calendar Step 4).
    # Watchlist floor + horizon are owner-decided 2026-07-12 (RFC OQ1/OQ2).
    # Per-tick artist bounds keep each EventBridge tick inside the 120s Lambda:
    # MB ~1 req/s (musicbrainzngs limiter) → 70 artists ≈ 70 s; iTunes 3.5 s
    # throttle → 22 artists ≈ 77 s (resolution pre-pass misses cost a 2nd
    # request, which the wall-clock budget absorbs). Coverage cadence at the
    # eventbridge.tf rates (MB hourly / iTunes 30 min): ≥50 tier ≈ 1,530
    # artists → full MB cycle ≈ 22 h, iTunes ≈ 21 h — "fresh within a day".
    RELEASE_POLL_POP_MIN: int = 50
    RELEASE_POLL_HORIZON_DAYS: int = 180
    RELEASE_POLL_MB_ARTISTS_PER_TICK: int = 70
    RELEASE_POLL_ITUNES_ARTISTS_PER_TICK: int = 22
    RELEASE_POLL_TIME_BUDGET_SEC: float = 90.0
    # Failed iTunes artistId resolutions are sentinel-cached (artist_source_ids
    # 'not_found') and re-attempted after this many days — new UPC-bearing
    # catalog albums can make a previously-unresolvable artist resolvable.
    RELEASE_POLL_RESOLVE_RETRY_DAYS: int = 30
    ITUNES_LOOKUP_URL: str = "https://itunes.apple.com/lookup"
    ITUNES_THROTTLE_S: float = 3.5

    # Secrets Manager (legacy) + SSM Parameter Store (CHORE-secrets-ssm-migration).
    # SECRETS_PARAM (SSM SecureString name, e.g. /myblog/worker) takes priority;
    # SECRETS_ARN is the fallback. Setting SECRETS_PARAM is the cutover switch.
    SECRETS_ARN: str = ""
    SECRETS_PARAM: str = ""

    # Incremental lyrics collection (FEAT-lyrics-corpus Step 3, worker EventBridge job).
    # LRCLIB /api/search freshness path for newly-ingested tracks lacking a corpus row.
    # Bounded per invocation so the job always finishes inside the 120s Lambda timeout;
    # per-row commits make an over-budget batch resumable (leftovers picked up next run).
    # Concurrency mirrors the Phase 2 finding (~2.5 req/s effective LRCLIB cap around 20-30
    # workers; higher only adds throttle-skips). URL is the same endpoint the batch used.
    LYRICS_LRCLIB_SEARCH_URL: str = "https://lrclib.net/api/search"
    LYRICS_INCR_BATCH_LIMIT: int = 150
    LYRICS_INCR_CONCURRENCY: int = 20
    LYRICS_INCR_TIME_BUDGET_SEC: float = 90.0

    # Periodic reassessment (FEAT-lyrics-corpus Step 4, worker EventBridge job). Re-checks the
    # unresolved pool (not_found / ambiguous / review_required, stalest first) since LRCLIB
    # coverage grows over time — promotes on new evidence, refreshes otherwise, never overwrites
    # a good match. Same bounding as Step 3; a lower cadence (the rule) since coverage changes
    # slowly. Same shared eval loop, so the limit/concurrency/budget knobs mirror the Step 3 set.
    LYRICS_REASSESS_BATCH_LIMIT: int = 150
    LYRICS_REASSESS_CONCURRENCY: int = 20
    LYRICS_REASSESS_TIME_BUDGET_SEC: float = 90.0

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    SQS_MAX_MESSAGES: int = 1
    SQS_WAIT_TIME_SECONDS: int = 10
    SQS_RETRY_DELAY_SECONDS: int = 5


def _load_secrets(param: str, arn: str) -> dict:
    """Prefer SSM Parameter Store (``param``), fall back to Secrets Manager
    (``arn``) on unset-or-error (CHORE-secrets-ssm-migration)."""
    if param:
        try:
            import boto3
            ssm = boto3.client("ssm", region_name="ap-northeast-2")
            return json.loads(ssm.get_parameter(Name=param, WithDecryption=True)["Parameter"]["Value"])
        except Exception as e:
            logger.error("SSM load failed for %s, falling back to Secrets Manager: %s", param, e)
    if arn:
        try:
            import boto3
            sm = boto3.client("secretsmanager", region_name="ap-northeast-2")
            return json.loads(sm.get_secret_value(SecretId=arn)["SecretString"])
        except Exception as e:
            logger.error("Failed to load secrets from %s: %s", arn, e)
    return {}


@lru_cache
def get_settings() -> Settings:
    s = Settings()
    if s.SECRETS_ARN or s.SECRETS_PARAM:
        secrets = _load_secrets(s.SECRETS_PARAM, s.SECRETS_ARN)
        if secrets.get("DATABASE_URL"):
            s.DATABASE_URL = secrets["DATABASE_URL"]
        if secrets.get("SPOTIFY_CLIENT_ID"):
            s.SPOTIFY_CLIENT_ID = secrets["SPOTIFY_CLIENT_ID"]
        if secrets.get("SPOTIFY_CLIENT_SECRET"):
            s.SPOTIFY_CLIENT_SECRET = secrets["SPOTIFY_CLIENT_SECRET"]
        # Last.fm key is OPTIONAL — absent ⇒ the poll no-ops; do NOT add to `missing`.
        if secrets.get("LASTFM_API_KEY"):
            s.LASTFM_API_KEY = secrets["LASTFM_API_KEY"]
        missing = [k for k, v in {
            "DATABASE_URL": s.DATABASE_URL,
            "SPOTIFY_CLIENT_ID": s.SPOTIFY_CLIENT_ID,
            "SPOTIFY_CLIENT_SECRET": s.SPOTIFY_CLIENT_SECRET,
        }.items() if not v]
        if missing:
            raise ValueError(f"Required secrets missing after Secrets Manager load: {missing}. Check SECRETS_ARN and IAM policy.")
    return s


settings = get_settings()
