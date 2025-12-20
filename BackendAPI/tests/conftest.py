import asyncio
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

# IMPORTANT:
# Pytest loads conftest.py BEFORE importing test modules.
# Many existing test modules set DATABASE_URL at import time, but that is too late for shared init
# if conftest already imported the app/DB. We therefore set DATABASE_URL here, early.
_BACKEND_ROOT = Path(__file__).resolve().parents[1]  # .../BackendAPI
_TEST_DB_PATH = _BACKEND_ROOT / "test_app.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{_TEST_DB_PATH}"
os.environ.setdefault("USE_ALEMBIC", "0")  # ensure create_all path for sqlite tests


def _run(coro):
    """Run an async coroutine from sync pytest code, creating an event loop if needed."""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


@pytest.fixture(scope="session", autouse=True)
def _ensure_test_db_schema_and_startup():
    """
    Ensure the test SQLite DB has tables before any test runs.

    Why this is needed:
    - Many tests call SQLAlchemy directly BEFORE making any HTTP request.
    - FastAPI startup (which calls init_db()) may not have run yet in that situation.

    This fixture:
    1) Deletes the sqlite DB file to ensure a clean slate (best-effort).
    2) Resets the global DB engine (in case something imported it early).
    3) Calls init_db() to create tables.
    4) Enters a TestClient(app) context once to force FastAPI startup tasks (including dev seeding).
    """
    try:
        if _TEST_DB_PATH.exists():
            _TEST_DB_PATH.unlink()
    except Exception:
        # If file is locked/can't be removed, ignore; init_db will still attempt create_all.
        pass

    # Reset DB globals to ensure engine uses the DATABASE_URL we set above.
    from src.api import db as db_mod  # imported after DATABASE_URL is set

    db_mod._engine = None
    db_mod._session_factory = None

    from src.api.db import init_db
    from src.api.main import app

    _run(init_db())

    # Force FastAPI startup to run once (startup seeds demo/dev users in sqlite/dev envs).
    with TestClient(app) as c:
        c.get("/health")

    yield
