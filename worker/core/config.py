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

    DATABASE_URL: str = "postgresql+psycopg://blog:blog@127.0.0.1:5433/blog"

    # Spotify
    SPOTIFY_CLIENT_ID: str = ""
    SPOTIFY_CLIENT_SECRET: str = ""
    SPOTIFY_TOKEN_URL: str = "https://accounts.spotify.com/api/token"
    SPOTIFY_API_BASE: str = "https://api.spotify.com/v1"
    SPOTIFY_DEFAULT_MARKET: str = "KR"

    # AWS / SQS (for local testing convenience)
    AWS_DEFAULT_REGION: str = "ap-northeast-2"
    LOCALSTACK_ENDPOINT: str | None = None
    SQS_QUEUE_URL: str | None = None

    # Control flags
    DRY_RUN: bool = False

    # Secrets Manager
    SECRETS_ARN: str = ""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    SQS_MAX_MESSAGES: int = 1
    SQS_WAIT_TIME_SECONDS: int = 10
    SQS_RETRY_DELAY_SECONDS: int = 5


def _load_secrets(arn: str) -> dict:
    try:
        import boto3
        sm = boto3.client("secretsmanager", region_name="ap-northeast-2")
        val = sm.get_secret_value(SecretId=arn)
        return json.loads(val["SecretString"])
    except Exception as e:
        logger.error("Failed to load secrets from %s: %s", arn, e)
        return {}


@lru_cache
def get_settings() -> Settings:
    s = Settings()
    if s.SECRETS_ARN:
        secrets = _load_secrets(s.SECRETS_ARN)
        if secrets.get("DATABASE_URL"):
            s.DATABASE_URL = secrets["DATABASE_URL"]
        if secrets.get("SPOTIFY_CLIENT_ID"):
            s.SPOTIFY_CLIENT_ID = secrets["SPOTIFY_CLIENT_ID"]
        if secrets.get("SPOTIFY_CLIENT_SECRET"):
            s.SPOTIFY_CLIENT_SECRET = secrets["SPOTIFY_CLIENT_SECRET"]
        missing = [k for k, v in {
            "SPOTIFY_CLIENT_ID": s.SPOTIFY_CLIENT_ID,
            "SPOTIFY_CLIENT_SECRET": s.SPOTIFY_CLIENT_SECRET,
        }.items() if not v]
        if missing:
            raise ValueError(f"Required secrets missing after Secrets Manager load: {missing}. Check SECRETS_ARN and IAM policy.")
    return s


settings = get_settings()
