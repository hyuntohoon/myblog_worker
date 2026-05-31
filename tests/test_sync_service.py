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


@patch("worker.service.sync_service.spotify")
def test_sync_albums_batch_skips_null_album(mock_spotify, db_connection, sample_spotify_album):
    """Spotify가 배치 응답에 null 원소(알 수 없는 id)를 섞어 보내도 한 건이
    전체 레코드를 죽이지 않고, 유효한 앨범은 정상 동기화된다 (B1)."""
    # GET /v1/albums?ids= 는 알 수 없는 id 위치에 null 을 넣어 반환한다.
    mock_spotify.get_albums.return_value = [None, sample_spotify_album]

    svc = AlbumSyncService(db_connection)
    # null 원소에서 TypeError 없이 완주해야 한다.
    svc.sync_albums_batch(["bogus_id", "test_album_001"], "KR")

    album_count = db_connection.execute(
        text("SELECT COUNT(*) FROM albums WHERE spotify_id = 'test_album_001'"),
    ).scalar()
    assert album_count == 1


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

def _build_session_mock(rows, collision_mbid, taken_mbids=()):
    """Build a session double for generate_and_save_aliases.

    SELECT artists IS NULL → returns `rows` (head batch).
    SELECT artists WHERE musicbrainz_id = :mbid → returns 1-row iff in taken_mbids
    (BUG-18 pre-check path).
    UPDATE with mbid == collision_mbid → IntegrityError; otherwise succeed.

    Returns (factory, persisted_sids, rollback_count_holder, commit_count_holder).
    """
    persisted_sids: list[str] = []
    rollback_count = [0]
    commit_count = [0]
    taken_set = set(taken_mbids)

    def execute_side_effect(stmt, params=None):
        sql = str(stmt).upper()
        if "SELECT" in sql and "FROM ARTISTS" in sql and "MUSICBRAINZ_ID = :MBID" in sql:
            # BUG-18 pre-check: closure SELECT
            result = MagicMock()
            mbid = (params or {}).get("mbid")
            result.first.return_value = (1,) if mbid in taken_set else None
            return result
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

    def fetch_side_effect(name, spotify_genres=None, is_mbid_taken=None):
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
    mock_fetch.side_effect = lambda name, spotify_genres=None, is_mbid_taken=None: (
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


@pytest.mark.unit
def test_generate_and_save_aliases_reraises_catastrophic_failure():
    """A catastrophic failure (e.g. DB/session_factory down) must propagate so the
    EventBridge invocation is marked failed and the Lambda Errors alarm fires —
    not be swallowed into a silent success (B2)."""
    factory = MagicMock(side_effect=RuntimeError("DB unreachable"))

    with pytest.raises(RuntimeError, match="DB unreachable"):
        generate_and_save_aliases(factory)


# --- BUG-18 pre-check accounting ---------------------------------------------
# RFC §Steps Step 1 의 test_sync_service 시나리오: 1 정상 + 1 pre-check 거절 후
# 2nd 후보 정상 + 1 모든 후보 pre-check 거절 → sentinel. 결과 succeeded=2,
# skipped_precheck=1, skipped_collision=0. is_mbid_taken 호출은 client 내부에서
# 일어나므로 mocked-out — closure SQL 의 실엔진 호출은 통합 테스트가 잡음.

@pytest.mark.unit
@patch("worker.service.sync_service.fetch_artist_mbid_and_aliases")
def test_generate_and_save_aliases_pre_check_outcome_accounting(mock_fetch):
    rows = [
        ("sid-ok",       "Normal Artist",   ["pop"]),
        ("sid-retry",    "Pre-check Retry", ["k-pop"]),
        ("sid-evict",    "Pre-check Evict", ["k-pop"]),
    ]

    def fetch_side_effect(name, spotify_genres=None, is_mbid_taken=None):
        # is_mbid_taken is provided by the service in this PR — assert that
        # the plumbing actually wires it through (not silently dropped).
        assert callable(is_mbid_taken)
        if name == "Normal Artist":
            return ("mbid-ok", ["alias-ok"])
        if name == "Pre-check Retry":
            # Client decided that one candidate was taken and adopted the next.
            return ("mbid-retry-2nd", ["alias-retry"])
        # All candidates taken → client returns sentinel.
        return ("not_found", [])

    mock_fetch.side_effect = fetch_side_effect

    factory, persisted_sids, rollback_count, commit_count = _build_session_mock(
        rows, collision_mbid="never-fires"
    )

    generate_and_save_aliases(factory)

    # All 3 rows wrote (sentinel row also UPDATEs to 'not_found' so head pool
    # shrinks next cycle — BUG-18 §Goal eviction).
    assert sorted(persisted_sids) == ["sid-evict", "sid-ok", "sid-retry"]
    assert rollback_count[0] == 0
    assert commit_count[0] == 4  # 1 SELECT release + 3 UPDATEs


@pytest.mark.unit
@patch("worker.service.sync_service.fetch_artist_mbid_and_aliases")
def test_generate_and_save_aliases_forwards_is_mbid_taken(mock_fetch):
    """The service must pass is_mbid_taken to every fetch call (plumbing guard)."""
    rows = [("sid0", "X", []), ("sid1", "Y", [])]
    mock_fetch.return_value = ("mbid-x", ["a"])

    factory, *_ = _build_session_mock(rows, collision_mbid="never-fires")

    generate_and_save_aliases(factory)

    for call in mock_fetch.call_args_list:
        kwargs = call.kwargs
        assert "is_mbid_taken" in kwargs and callable(kwargs["is_mbid_taken"]), (
            "service must forward is_mbid_taken=<closure> to the client "
            "(BUG-18 plumbing — silent drop = dead pre-check)"
        )
