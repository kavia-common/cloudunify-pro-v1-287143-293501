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


def _run_alembic_migrations() -> None:
    """Run Alembic migrations programmatically to the latest head.

    This reads alembic.ini from the project root and honors DATABASE_URL.
    On failure, the caller may choose to fallback to create_all for dev.
    """
    try:
        from alembic import command
        from alembic.config import Config
    except Exception as exc:  # pragma: no cover - only when Alembic missing
        logger.warning("Alembic not available, skipping migrations: %s", exc)
        return

    # Resolve paths relative to BackendAPI root
    here = os.path.abspath(os.path.dirname(__file__))  # .../BackendAPI/src/api
    project_root = os.path.abspath(os.path.join(here, "..", ".."))  # .../BackendAPI
    alembic_ini = os.path.join(project_root, "alembic.ini")
    alembic_dir = os.path.join(project_root, "alembic")

    if not os.path.exists(alembic_ini) or not os.path.isdir(alembic_dir):
        logger.warning("Alembic configuration not found at %s; skipping migrations", alembic_ini)
        return

    cfg = Config(alembic_ini)
    cfg.set_main_option("script_location", alembic_dir)
    db_url = get_database_url()
    if db_url:
        cfg.set_main_option("sqlalchemy.url", db_url)

    logger.info("Applying Alembic migrations to head...")
    command.upgrade(cfg, "head")
    logger.info("Alembic migrations applied.")


# PUBLIC_INTERFACE
async def init_db() -> None:
    """Initialize the database schema.

    Behavior:
    - If DATABASE_URL is SQLite, create tables via SQLAlchemy (dev/test).
    - If DATABASE_URL is PostgreSQL (or other non-sqlite) and USE_ALEMBIC=1 (default),
      apply Alembic migrations to head. On failure, fall back to create_all for dev.
    - Ensures DB timezone UTC for PostgreSQL sessions when supported.
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

        db_url = get_database_url()
        is_sqlite = db_url.startswith("sqlite")
        prefer_alembic = os.getenv("USE_ALEMBIC", "1") == "1"

        if is_sqlite:
            # Dev/test path: create tables directly
            await conn.run_sync(Base.metadata.create_all)
            logger.info("Database tables ensured via SQLAlchemy (SQLite/dev).")
            return

    # Non-sqlite path: run Alembic migrations if preferred
    if prefer_alembic:
        try:
            _run_alembic_migrations()
            return
        except Exception as exc:
            logger.warning("Alembic migration failed (%s). Falling back to create_all for dev.", exc)

    # Fallback: try to create tables directly (dev convenience)
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables ensured via SQLAlchemy (fallback).")
