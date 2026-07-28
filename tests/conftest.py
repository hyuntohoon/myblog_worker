# tests/conftest.py
from __future__ import annotations
import os
import sys

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Neon test branch URL is fetched from env. CI (GHA) injects via
# `secrets.TEST_DB_URL`; local dev exports from AWS Secrets Manager:
#   export TEST_DB_URL=$(aws secretsmanager get-secret-value \
#     --secret-id myblog/test-db --query SecretString --output text \
#     | python3 -c "import sys,json; print(json.load(sys.stdin)['TEST_DB_URL'])")
# When unset, DB-bound fixtures skip rather than fall back to a hardcoded URL
# (BUG-16 Step 1 — removed plaintext credential from source).
TEST_DB_URL = os.environ.get("TEST_DB_URL")


@pytest.fixture(scope="session")
def db_engine():
    """전체 테스트에서 DB 엔진 하나만 생성."""
    if not TEST_DB_URL:
        pytest.skip("TEST_DB_URL not set — see conftest.py for Secrets Manager fetch command")
    engine = create_engine(TEST_DB_URL, pool_pre_ping=True, future=True)
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def db_bound_connection(db_engine):
    """테스트 1건이 공유하는 커넥션 + 바깥 트랜잭션 (끝나면 통째로 롤백)."""
    connection = db_engine.connect()
    transaction = connection.begin()

    yield connection

    if transaction.is_active:
        transaction.rollback()
    connection.close()


@pytest.fixture(scope="function")
def db_session(db_bound_connection):
    """매 테스트마다 새 세션 + 끝나면 롤백."""
    Session = sessionmaker(
        bind=db_bound_connection, autoflush=False, autocommit=False, future=True,
        # The service under test commits its own short transactions now
        # (FIX-worker-txn-across-http). Joining via SAVEPOINT keeps those commits
        # nested inside the fixture's outer transaction, so teardown still rolls
        # the whole test back instead of leaving rows on the Neon test branch.
        join_transaction_mode="create_savepoint",
    )
    session = Session()

    yield session

    session.close()


@pytest.fixture(scope="function")
def db_connection(db_session):
    """읽기 검증용 connection 객체 (테스트가 직접 SELECT 할 때)."""
    return db_session.connection()


@pytest.fixture(scope="function")
def db_session_factory(db_bound_connection):
    """`AlbumSyncService` 가 받는 session factory.

    Binds to the SAME connection as `db_session`/`db_connection`, so a test can
    run the service and then assert on the rows through `db_connection`, while
    every write stays inside the outer transaction that teardown rolls back.
    """
    return sessionmaker(
        bind=db_bound_connection, autoflush=False, autocommit=False, future=True,
        join_transaction_mode="create_savepoint",
    )


@pytest.fixture
def sample_spotify_album():
    """Spotify API 앨범 응답 mock 데이터."""
    return {
        "id": "test_album_001",
        "name": "OK Computer",
        "artists": [
            {"id": "test_artist_001", "name": "Radiohead"}
        ],
        "images": [{"url": "https://example.com/cover.jpg"}],
        "release_date": "1997-06-16",
        "album_type": "album",
        "total_tracks": 12,
        "label": "Parlophone",
        "popularity": 85,
        "external_urls": {"spotify": "https://open.spotify.com/album/test_album_001"},
        "external_ids": {"upc": "0724385522925"},
        "tracks": {
            "items": [
                {
                    "id": "test_track_001",
                    "name": "Airbag",
                    "track_number": 1,
                    "duration_ms": 284000,
                    "artists": [{"id": "test_artist_001", "name": "Radiohead"}],
                },
                {
                    "id": "test_track_002",
                    "name": "Paranoid Android",
                    "track_number": 2,
                    "duration_ms": 386000,
                    "artists": [{"id": "test_artist_001", "name": "Radiohead"}],
                },
            ]
        },
    }


@pytest.fixture
def sample_spotify_albums(sample_spotify_album):
    """여러 앨범 mock 데이터."""
    album2 = {
        "id": "test_album_002",
        "name": "Kid A",
        "artists": [
            {"id": "test_artist_001", "name": "Radiohead"}
        ],
        "images": [{"url": "https://example.com/cover2.jpg"}],
        "release_date": "2000-10-02",
        "album_type": "album",
        "total_tracks": 10,
        "label": "Parlophone",
        "popularity": 80,
        "external_urls": {"spotify": "https://open.spotify.com/album/test_album_002"},
        "tracks": {
            "items": [
                {
                    "id": "test_track_003",
                    "name": "Everything In Its Right Place",
                    "track_number": 1,
                    "duration_ms": 251000,
                    "artists": [{"id": "test_artist_001", "name": "Radiohead"}],
                },
            ]
        },
    }
    return [sample_spotify_album, album2]
