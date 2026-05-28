# tests/test_sync_service.py
"""AlbumSyncService 통합 테스트.
Spotify는 mock, DB는 Neon test 브랜치 실제 연동.
"""
import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from worker.service.sync_service import AlbumSyncService, generate_and_save_aliases


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


# --- generate_and_save_aliases ---------------------------------------------
# BUG-17: per-row tx so one UNIQUE collision doesn't roll back the rest.

def _build_session_mock(rows, collision_mbid):
    """Build a session double for generate_and_save_aliases.

    SELECT returns `rows`. UPDATE with mbid == collision_mbid raises
    IntegrityError; other UPDATEs succeed. Returns (factory, persisted_sids,
    rollback_count_holder, commit_count_holder).
    """
    persisted_sids: list[str] = []
    rollback_count = [0]
    commit_count = [0]

    def execute_side_effect(stmt, params=None):
        sql = str(stmt).upper()
        if "SELECT" in sql and "FROM ARTISTS" in sql:
            result = MagicMock()
            result.fetchall.return_value = rows
            return result
        # UPDATE
        if params and params.get("mbid") == collision_mbid:
            raise IntegrityError("UPDATE artists ...", params, Exception("unique violation"))
        if params:
            persisted_sids.append(params["sid"])
        return MagicMock()

    session = MagicMock()
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=False)
    session.execute = MagicMock(side_effect=execute_side_effect)
    session.commit = MagicMock(side_effect=lambda: commit_count.__setitem__(0, commit_count[0] + 1))
    session.rollback = MagicMock(
        side_effect=lambda: rollback_count.__setitem__(0, rollback_count[0] + 1)
    )

    factory = MagicMock(return_value=session)
    return factory, persisted_sids, rollback_count, commit_count


@pytest.mark.unit
@patch("worker.service.sync_service.fetch_artist_mbid_and_aliases")
def test_generate_and_save_aliases_isolates_unique_collision(mock_fetch):
    """One row's IntegrityError must not block the other 9 rows from persisting."""
    rows = [(f"sid{i}", f"name{i}", []) for i in range(10)]
    collision_mbid = "collide-mbid"

    def fetch_side_effect(name, spotify_genres=None):
        if name == "name5":
            return (collision_mbid, [])
        idx = name[4:]
        return (f"mbid-{idx}", [f"alias-{idx}"])

    mock_fetch.side_effect = fetch_side_effect

    factory, persisted_sids, rollback_count, commit_count = _build_session_mock(
        rows, collision_mbid
    )

    generate_and_save_aliases(factory)

    # 9 rows persisted, the collision row skipped.
    assert sorted(persisted_sids) == sorted([f"sid{i}" for i in range(10) if i != 5])
    # 1 rollback for the collision row.
    assert rollback_count[0] == 1
    # 1 commit for the SELECT release + 9 commits for the per-row UPDATEs.
    assert commit_count[0] == 10


@pytest.mark.unit
@patch("worker.service.sync_service.fetch_artist_mbid_and_aliases")
def test_generate_and_save_aliases_all_succeed(mock_fetch):
    """No collisions: all 10 rows persist with no rollbacks."""
    rows = [(f"sid{i}", f"name{i}", ["케이팝"]) for i in range(10)]
    mock_fetch.side_effect = lambda name, spotify_genres=None: (
        f"mbid-{name[4:]}",
        [f"alias-{name[4:]}"],
    )

    factory, persisted_sids, rollback_count, commit_count = _build_session_mock(
        rows, collision_mbid="never-fires"
    )

    generate_and_save_aliases(factory)

    assert sorted(persisted_sids) == sorted([f"sid{i}" for i in range(10)])
    assert rollback_count[0] == 0
    assert commit_count[0] == 11  # 1 SELECT release + 10 UPDATEs


@pytest.mark.unit
@patch("worker.service.sync_service.fetch_artist_mbid_and_aliases")
def test_generate_and_save_aliases_empty_select(mock_fetch):
    """No NULL-MBID rows: MB lookup never called, no commits beyond SELECT release."""
    factory, persisted_sids, rollback_count, commit_count = _build_session_mock(
        rows=[], collision_mbid="never-fires"
    )

    generate_and_save_aliases(factory)

    mock_fetch.assert_not_called()
    assert persisted_sids == []
    assert rollback_count[0] == 0
    assert commit_count[0] == 1  # SELECT release only
