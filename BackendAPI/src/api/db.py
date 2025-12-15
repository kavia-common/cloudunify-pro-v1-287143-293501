import os
import logging
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import declarative_base
from sqlalchemy import text

# Configure module-level logger
logger = logging.getLogger("cloudunify.db")

# Declarative base shared by all models
Base = declarative_base()

# PUBLIC_INTERFACE
def get_database_url() -> str:
    """Resolve the database URL from environment variables.

    Prefers DATABASE_URL. If not set, falls back to a local SQLite file using aiosqlite
    so tests and local development can run without a Postgres instance.

    IMPORTANT: In production, set DATABASE_URL to a PostgreSQL URL like:
      postgresql+asyncpg://USER:PASS@HOST:PORT/DBNAME
    """
    return os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./app.db")


_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def _ensure_engine() -> None:
    """Create engine and session factory if not already created."""
    global _engine, _session_factory
    if _engine is not None:
        return

    db_url = get_database_url()
    _engine = create_async_engine(
        db_url,
        echo=os.getenv("SQL_ECHO", "0") == "1",
        future=True,
        pool_pre_ping=True,
    )
    _session_factory = async_sessionmaker(_engine, expire_on_commit=False, class_=AsyncSession)
    logger.info("Initialized async DB engine for %s", db_url)


# PUBLIC_INTERFACE
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Yield an AsyncSession for use inside FastAPI dependencies."""
    if _engine is None:
        _ensure_engine()
    assert _session_factory is not None
    session = _session_factory()
    try:
        yield session
    finally:
        await session.close()


# PUBLIC_INTERFACE
async def init_db() -> None:
    """Create tables if they do not exist.

    Runs at application startup. In production, prefer proper migrations.
    """
    if _engine is None:
        _ensure_engine()

    # Local import to avoid circulars
    from src.api import models  # noqa: F401  # ensure models are registered

    assert _engine is not None
    async with _engine.begin() as conn:
        # For PostgreSQL we ensure timezone is UTC for session
        try:
            await conn.execute(text("SET TIME ZONE 'UTC'"))
        except Exception:
            # SQLite or other dialects may not support this; ignore
            pass

        # Create all tables
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables ensured.")
