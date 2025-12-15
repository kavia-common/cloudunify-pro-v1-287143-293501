import os
import asyncio
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

# Use SQLite for tests
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///./test_app.db"

from src.api.main import app  # noqa: E402
from src.api.models import User  # noqa: E402
from src.api.security import get_password_hash  # noqa: E402

client = TestClient(app)

TEST_USER_EMAIL = "auth-user@example.com"
TEST_USER_PASS = "Pa$$w0rd!"
TEST_ADMIN_EMAIL = "auth-admin@example.com"
TEST_ADMIN_PASS = "Adm1nPa$$!"


async def _create_user(email: str, password: str, role: str = "user", is_active: bool = True):
    engine = create_async_engine(os.environ["DATABASE_URL"], future=True)
    session_maker = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with session_maker() as session:
        res = await session.execute(select(User).where(User.email == email))
        existing = res.scalar_one_or_none()
        if existing:
            # update role/active if needed
            existing.role = role
            existing.is_active = is_active
            await session.commit()
            return
        user = User(email=email, hashed_password=get_password_hash(password), role=role, is_active=is_active)
        session.add(user)
        await session.commit()


def test_login_success_and_failure():
    # Ensure user exists
    asyncio.get_event_loop().run_until_complete(_create_user(TEST_USER_EMAIL, TEST_USER_PASS, role="user"))

    # Success
    r_ok = client.post("/auth/login", json={"email": TEST_USER_EMAIL, "password": TEST_USER_PASS})
    assert r_ok.status_code == 200, r_ok.text
    body = r_ok.json()
    assert "access_token" in body and "refresh_token" in body and body["token_type"] == "bearer"

    # Failure (bad password)
    r_bad = client.post("/auth/login", json={"email": TEST_USER_EMAIL, "password": "wrong"})
    assert r_bad.status_code == 401


def test_rbac_admin_only_access():
    # Create regular user and admin
    asyncio.get_event_loop().run_until_complete(_create_user(TEST_USER_EMAIL, TEST_USER_PASS, role="user"))
    asyncio.get_event_loop().run_until_complete(_create_user(TEST_ADMIN_EMAIL, TEST_ADMIN_PASS, role="admin"))

    # Login user (non-admin) and attempt admin-only endpoint
    r_user_login = client.post("/auth/login", json={"email": TEST_USER_EMAIL, "password": TEST_USER_PASS})
    assert r_user_login.status_code == 200
    user_token = r_user_login.json()["access_token"]
    r_forbidden = client.get("/auth/admin-ping", headers={"Authorization": f"Bearer {user_token}"})
    assert r_forbidden.status_code == 403

    # Login admin and access admin-only endpoint
    r_admin_login = client.post("/auth/login", json={"email": TEST_ADMIN_EMAIL, "password": TEST_ADMIN_PASS})
    assert r_admin_login.status_code == 200
    admin_token = r_admin_login.json()["access_token"]
    r_ok = client.get("/auth/admin-ping", headers={"Authorization": f"Bearer {admin_token}"})
    assert r_ok.status_code == 200
    assert r_ok.json().get("ok") is True
