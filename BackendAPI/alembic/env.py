import asyncio
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy import engine_from_config
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

# Ensure application modules are importable
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# Import metadata from app
from src.api.db import Base  # noqa: E402
from src.api import models  # noqa: F401  # ensure models are imported & mapped

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name:
    fileConfig(config.config_file_name)

# Provide target metadata for 'autogenerate'
target_metadata = Base.metadata


def get_database_url() -> str:
    # Use DATABASE_URL env var; default to SQLite for dev
    return os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./app.db")


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL and not an Engine.
    """
    url = get_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection):
    """Run migrations given an open connection."""
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online():
    """Run migrations in 'online' mode, supporting async DB URLs."""
    url = get_database_url()

    # Detect async drivers
    is_async = "+asyncpg" in url or "+aiosqlite" in url

    if is_async:
        connectable: AsyncEngine = create_async_engine(url, poolclass=pool.NullPool, future=True)
        async with connectable.connect() as connection:
            await connection.run_sync(do_run_migrations)
        await connectable.dispose()
    else:
        connectable = engine_from_config(
            config.get_section(config.config_ini_section, {}),
            prefix="sqlalchemy.",
            poolclass=pool.NullPool,
            url=url,
            future=True,
        )
        with connectable.connect() as connection:
            do_run_migrations(connection)


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
