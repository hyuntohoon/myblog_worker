# worker/infra/schema_catalog.py
from __future__ import annotations
from sqlalchemy import MetaData, Table, Column, Text, Date, Integer, ForeignKey, UniqueConstraint, Index, text
from sqlalchemy.dialects.postgresql import UUID, JSONB

md = MetaData()

artists = Table(
    "artists", md,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column("name", Text, nullable=False),                # UNIQUE 제거
    Column("spotify_id", Text, nullable=False, unique=True),
    Column("photo_url", Text),
    Column("ext_refs", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("genres", JSONB, nullable=False, server_default=text("'[]'::jsonb")),
    Column("followers", Integer),
    Column("popularity", Integer),
    # created_at은 DB default로 처리(필요시 컬럼 추가 가능)
)

albums = Table(
    "albums", md,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column("title", Text, nullable=False),
    Column("release_date", Date),
    Column("cover_url", Text),
    Column("album_type", Text),
    Column("spotify_id", Text, nullable=False, unique=True),
    Column("ext_refs", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("views", Integer, nullable=False, server_default=text("0")),
    Column("total_tracks", Integer),
    Column("label", Text),
    Column("popularity", Integer),

    # CHECK 제약은 필요 시 마이그레이션 스크립트로 추가
)

album_artists = Table(
    "album_artists", md,
    Column("album_id", UUID(as_uuid=True), ForeignKey("albums.id", ondelete="CASCADE"), nullable=False),
    Column("artist_id", UUID(as_uuid=True), ForeignKey("artists.id", ondelete="CASCADE"), nullable=False),
    Column("role", Text),
    UniqueConstraint("album_id", "artist_id", name="pk_album_artists"),  # 복합 PK 성격
)
tracks = Table(
    "tracks", md,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column("album_id", UUID(as_uuid=True), ForeignKey("albums.id", ondelete="CASCADE"), nullable=False),
    Column("title", Text, nullable=False),
    Column("track_no", Integer),
    Column("duration_sec", Integer),
    Column("spotify_id", Text, nullable=False, unique=True),
    Column("ext_refs", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("views", Integer, nullable=False, server_default=text("0")),
)
track_artists = Table(
    "track_artists", md,
    Column("track_id", UUID(as_uuid=True), ForeignKey("tracks.id", ondelete="CASCADE"), nullable=False),
    Column("artist_id", UUID(as_uuid=True), ForeignKey("artists.id", ondelete="CASCADE"), nullable=False),
    Column("role", Text),
    UniqueConstraint("track_id", "artist_id", name="pk_track_artists"),
)