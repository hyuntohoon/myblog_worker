# tests/conftest.py
from __future__ import annotations
import os
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# 테스트용 DB URL (Neon test 브랜치)
# 환경 변수로 주입하거나, 여기서 직접 설정
TEST_DB_URL = os.environ.get(
    "TEST_DB_URL",
    "postgresql+psycopg://neondb_owner:npg_IOLjirGU52Bm@ep-fancy-butterfly-a1xolnxf-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require",
)


@pytest.fixture(scope="session")
def db_engine():
    """전체 테스트에서 DB 엔진 하나만 생성."""
    engine = create_engine(TEST_DB_URL, pool_pre_ping=True, future=True)
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def db_session(db_engine):
    """매 테스트마다 새 세션 + 끝나면 롤백."""
    connection = db_engine.connect()
    transaction = connection.begin()
    Session = sessionmaker(bind=connection, autoflush=False, autocommit=False, future=True)
    session = Session()

    yield session

    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture(scope="function")
def db_connection(db_session):
    """sync_service에서 쓰는 connection 객체."""
    return db_session.connection()


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
