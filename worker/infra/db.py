from __future__ import annotations
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

_factory = None


def _get_factory():
    global _factory
    if _factory is None:
        from worker.core.config import get_settings
        engine = create_engine(get_settings().DATABASE_URL, pool_pre_ping=True, future=True)
        _factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return _factory


def SessionLocal() -> Session:
    return _get_factory()()
