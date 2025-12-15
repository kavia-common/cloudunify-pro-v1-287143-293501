import os
import asyncio
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

# Ensure a lightweight test DB (SQLite) is used before importing the app
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///./test_app.db"

from src.api.main import app  # noqa: E402
from src.api.security import get_password_hash  # noqa: E402
from src.api.models import User  # noqa: E402

client = TestClient(app)

TEST_EMAIL = "ingest-tester@example.com"
TEST_PASSWORD = "Secret123!"


async def _create_user_if_missing(email: str, password: str, role: str = "user"):
    engine = create_async_engine(os.environ["DATABASE_URL"], future=True)
    session_maker = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with session_maker() as session:
        res = await session.execute(select(User).where(User.email == email))
        existing = res.scalar_one_or_none()
        if existing:
            return
        user = User(email=email, hashed_password=get_password_hash(password), role=role, is_active=True)
        session.add(user)
        await session.commit()


def _get_access_token(email: str, password: str) -> str:
    r = client.post("/auth/login", json={"email": email, "password": password})
    assert r.status_code == 200, f"Login failed: {r.text}"
    return r.json()["access_token"]


def _auth_headers() -> dict:
    asyncio.get_event_loop().run_until_complete(_create_user_if_missing(TEST_EMAIL, TEST_PASSWORD))
    token = _get_access_token(TEST_EMAIL, TEST_PASSWORD)
    return {"Authorization": f"Bearer {token}"}


def test_resources_bulk_validation_bad_provider():
    payload = {
        "items": [
            {
                "organization_id": "11111111-1111-1111-1111-111111111111",
                "cloud_account_id": "22222222-2222-2222-2222-222222222222",
                "provider": "digitalocean",  # invalid
                "resource_id": "i-abc",
                "resource_type": "ec2.instance",
                "region": "us-east-1",
                "state": "running",
                "tags": {"env": "dev"},
            }
        ]
    }
    r = client.post("/resources/bulk", json=payload, headers=_auth_headers())
    assert r.status_code == 400
    body = r.json().get("detail")
    assert body is not None
    assert body["inserted"] == 0 and body["updated"] == 0
    assert len(body["errors"]) == 1


def test_costs_bulk_validation_bad_date():
    payload = {
        "items": [
            {
                "organization_id": "11111111-1111-1111-1111-111111111111",
                "cloud_account_id": "22222222-2222-2222-2222-222222222222",
                "provider": "aws",
                "service_name": "AmazonEC2",
                "region": "us-east-1",
                "cost_date": "2025-13-40",  # invalid date
                "usage_quantity": 10,
                "usage_unit": "Hours",
                "cost_amount": 5.5,
                "currency": "USD",
            }
        ]
    }
    r = client.post("/costs/bulk", json=payload, headers=_auth_headers())
    assert r.status_code == 400
    body = r.json().get("detail")
    assert body is not None
    assert body["inserted"] == 0 and body["updated"] == 0
    assert len(body["errors"]) == 1


def test_resources_upsert_insert_then_update():
    headers = _auth_headers()
    first = {
        "items": [
            {
                "organization_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "cloud_account_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "provider": "aws",
                "resource_id": "i-123456",
                "resource_type": "ec2.instance",
                "region": "us-east-1",
                "state": "stopped",
                "tags": {"team": "alpha"},
                "cost_daily": 1.2,
                "cost_monthly": 36.5,
            }
        ]
    }
    r1 = client.post("/resources/bulk", json=first, headers=headers)
    assert r1.status_code == 200
    body1 = r1.json()
    assert body1["inserted"] == 1
    assert body1["updated"] == 0
    assert body1["errors"] == []

    # Update same natural key with new state
    second = {
        "items": [
            {
                "organization_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "cloud_account_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "provider": "aws",
                "resource_id": "i-123456",
                "resource_type": "ec2.instance",
                "region": "us-east-1",
                "state": "running",  # changed
                "tags": {"team": "alpha"},
                "cost_daily": 1.3,
                "cost_monthly": 40.0,
            }
        ]
    }
    r2 = client.post("/resources/bulk", json=second, headers=headers)
    assert r2.status_code == 200
    body2 = r2.json()
    assert body2["inserted"] == 0
    assert body2["updated"] == 1
    assert body2["errors"] == []
