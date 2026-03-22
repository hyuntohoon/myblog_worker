# tests/test_sync_service.py
"""AlbumSyncService 통합 테스트.
Spotify는 mock, DB는 Neon test 브랜치 실제 연동.
"""
import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy import text

from worker.service.sync_service import AlbumSyncService


@pytest.mark.integration
@patch("worker.service.sync_service.spotify")
def test_sync_albums_batch_inserts_artist(mock_spotify, db_connection, sample_spotify_album):
    """앨범 동기화 시 아티스트가 DB에 저장되는지 확인."""
    mock_spotify.get_albums.return_value = [sample_spotify_album]

    svc = AlbumSyncService(db_connection)
    svc.sync_albums_batch(["test_album_001"], "KR")

    result = db_connection.execute(
        text("SELECT name FROM artists WHERE spotify_id = :sid"),
        dict(sid="test_artist_001"),
    ).fetchone()

    assert result is not None
    assert result[0] == "Radiohead"


@pytest.mark.integration
@patch("worker.service.sync_service.spotify")
def test_sync_albums_batch_inserts_album(mock_spotify, db_connection, sample_spotify_album):
    """앨범 데이터가 DB에 저장되는지 확인."""
    mock_spotify.get_albums.return_value = [sample_spotify_album]

    svc = AlbumSyncService(db_connection)
    svc.sync_albums_batch(["test_album_001"], "KR")

    result = db_connection.execute(
        text("SELECT title, album_type, total_tracks FROM albums WHERE spotify_id = :sid"),
        dict(sid="test_album_001"),
    ).fetchone()

    assert result is not None
    assert result[0] == "OK Computer"
    assert result[1] == "album"
    assert result[2] == 12


@pytest.mark.integration
@patch("worker.service.sync_service.spotify")
def test_sync_albums_batch_inserts_tracks(mock_spotify, db_connection, sample_spotify_album):
    """트랙이 DB에 저장되는지 확인."""
    mock_spotify.get_albums.return_value = [sample_spotify_album]

    svc = AlbumSyncService(db_connection)
    svc.sync_albums_batch(["test_album_001"], "KR")

    result = db_connection.execute(
        text("SELECT COUNT(*) FROM tracks WHERE spotify_id IN ('test_track_001', 'test_track_002')"),
    ).scalar()

    assert result == 2


@pytest.mark.integration
@patch("worker.service.sync_service.spotify")
def test_sync_albums_batch_links_album_artists(mock_spotify, db_connection, sample_spotify_album):
    """album_artists 관계가 생성되는지 확인."""
    mock_spotify.get_albums.return_value = [sample_spotify_album]

    svc = AlbumSyncService(db_connection)
    svc.sync_albums_batch(["test_album_001"], "KR")

    result = db_connection.execute(
        text("""
            SELECT COUNT(*) FROM album_artists aa
            JOIN albums a ON aa.album_id = a.id
            JOIN artists ar ON aa.artist_id = ar.id
            WHERE a.spotify_id = 'test_album_001'
              AND ar.spotify_id = 'test_artist_001'
        """),
    ).scalar()

    assert result == 1


@pytest.mark.integration
@patch("worker.service.sync_service.spotify")
def test_sync_albums_batch_links_track_artists(mock_spotify, db_connection, sample_spotify_album):
    """track_artists 관계가 생성되는지 확인."""
    mock_spotify.get_albums.return_value = [sample_spotify_album]

    svc = AlbumSyncService(db_connection)
    svc.sync_albums_batch(["test_album_001"], "KR")

    result = db_connection.execute(
        text("""
            SELECT COUNT(*) FROM track_artists ta
            JOIN tracks t ON ta.track_id = t.id
            WHERE t.spotify_id = 'test_track_001'
        """),
    ).scalar()

    assert result == 1


@pytest.mark.integration
@patch("worker.service.sync_service.spotify")
def test_sync_albums_batch_upsert_idempotent(mock_spotify, db_connection, sample_spotify_album):
    """같은 데이터를 두 번 동기화해도 중복 없이 정상 동작하는지 확인 (멱등성)."""
    mock_spotify.get_albums.return_value = [sample_spotify_album]

    svc = AlbumSyncService(db_connection)
    svc.sync_albums_batch(["test_album_001"], "KR")
    svc.sync_albums_batch(["test_album_001"], "KR")  # 두 번째 실행

    artist_count = db_connection.execute(
        text("SELECT COUNT(*) FROM artists WHERE spotify_id = 'test_artist_001'"),
    ).scalar()

    album_count = db_connection.execute(
        text("SELECT COUNT(*) FROM albums WHERE spotify_id = 'test_album_001'"),
    ).scalar()

    assert artist_count == 1
    assert album_count == 1


@pytest.mark.integration
@patch("worker.service.sync_service.spotify")
def test_sync_albums_batch_multiple_albums(mock_spotify, db_connection, sample_spotify_albums):
    """여러 앨범을 한번에 동기화할 수 있는지 확인."""
    mock_spotify.get_albums.return_value = sample_spotify_albums

    svc = AlbumSyncService(db_connection)
    svc.sync_albums_batch(["test_album_001", "test_album_002"], "KR")

    album_count = db_connection.execute(
        text("SELECT COUNT(*) FROM albums WHERE spotify_id IN ('test_album_001', 'test_album_002')"),
    ).scalar()

    track_count = db_connection.execute(
        text("SELECT COUNT(*) FROM tracks WHERE spotify_id IN ('test_track_001', 'test_track_002', 'test_track_003')"),
    ).scalar()

    assert album_count == 2
    assert track_count == 3


@pytest.mark.integration
@patch("worker.service.sync_service.spotify")
def test_sync_albums_batch_empty_list(mock_spotify, db_connection):
    """빈 리스트를 넘겨도 에러 없이 동작하는지 확인."""
    svc = AlbumSyncService(db_connection)
    svc.sync_albums_batch([], "KR")

    # Spotify 호출 자체가 안 일어나야 함
    mock_spotify.get_albums.assert_not_called()


@pytest.mark.integration
@patch("worker.service.sync_service.spotify")
def test_sync_albums_batch_track_duration(mock_spotify, db_connection, sample_spotify_album):
    """트랙 duration_ms → duration_sec 변환이 정확한지 확인."""
    mock_spotify.get_albums.return_value = [sample_spotify_album]

    svc = AlbumSyncService(db_connection)
    svc.sync_albums_batch(["test_album_001"], "KR")

    result = db_connection.execute(
        text("SELECT duration_sec FROM tracks WHERE spotify_id = 'test_track_001'"),
    ).scalar()

    # 284000ms → 284초
    assert result == 284
