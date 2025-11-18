from __future__ import annotations
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from worker.core.config import settings

#DB_URL = os.environ["DB_URL"]  # e.g. postgresql+psycopg://user:pass@host:5432/db

engine = create_engine(settings.DB_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)