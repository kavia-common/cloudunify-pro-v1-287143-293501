import os
import asyncio
from datetime import date

from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Use SQLite for tests
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///./test_app.db"

from src.api.main import app  # noqa: E402
from src.api.models import Recommendation, User  # noqa: E402
from src.api.security import get_password_hash  # noqa: E402

client = TestClient(app)

TEST_EMAIL = "public-read-user@example.com"
TEST_PASSWORD = "PublicRead123!"


async def _ensure_user(email: str, password: str, role: str = "user", is_active: bool = True) -> None:
    engine = create_async_engine(os.environ["DATABASE_URL"], future=True)
    session_maker = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with session_maker() as session:
        res = await session.execute(select(User).where(User.email == email))
        u = res.scalar_one_or_none()
        if u:
            u.role = role
            u.is_active = is_active
            await session.commit()
            return
        u = User(email=email, hashed_password=get_password_hash(password), role=role, is_active=is_active)
        session.add(u)
        await session.commit()


def _login(email: str, password: str) -> str:
    r = client.post("/auth/login", json={"email": email, "password": password})
    assert r.status_code == 200, f"Login failed: {r.text}"
    return r.json()["access_token"]


def _auth_header(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


async def _insert_recommendation() -> None:
    engine = create_async_engine(os.environ["DATABASE_URL"], future=True)
    session_maker = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with session_maker() as session:
        rec = Recommendation(
            organization_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            resource_id=None,
            recommendation_type="cleanup",
            priority="medium",
            potential_savings_monthly=3.21,
            description="Delete unattached volumes",
            action_items=["review", "delete"],
        )
        session.add(rec)
        await session.commit()


def test_public_reads_root_and_v1_paths():
    # Ensure we can ingest data using auth-protected endpoints
    asyncio.get_event_loop().run_until_complete(_ensure_user(TEST_EMAIL, TEST_PASSWORD, role="user"))
    token = _login(TEST_EMAIL, TEST_PASSWORD)

    # Ingest one resource + one cost (auth required)
    r_res_ing = client.post(
        "/resources/bulk",
        json={
            "items": [
                {
                    "organization_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    "cloud_account_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                    "provider": "aws",
                    "resource_id": "i-public-1",
                    "resource_type": "ec2.instance",
                    "region": "us-east-1",
                    "state": "running",
                    "tags": {"env": "test"},
                    "cost_daily": 0.5,
                    "cost_monthly": 15.0,
                }
            ]
        },
        headers=_auth_header(token),
    )
    assert r_res_ing.status_code == 200, r_res_ing.text

    r_cost_ing = client.post(
        "/costs/bulk",
        json={
            "items": [
                {
                    "organization_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    "cloud_account_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                    "provider": "aws",
                    "service_name": "AmazonEC2",
                    "region": "us-east-1",
                    "cost_date": str(date(2025, 1, 1)),
                    "usage_quantity": 1,
                    "usage_unit": "Hours",
                    "cost_amount": 1.0,
                    "currency": "usd",
                }
            ]
        },
        headers=_auth_header(token),
    )
    assert r_cost_ing.status_code == 200, r_cost_ing.text

    asyncio.get_event_loop().run_until_complete(_insert_recommendation())

    # Public reads (root)
    r_resources = client.get("/resources")
    assert r_resources.status_code == 200, r_resources.text
    body = r_resources.json()
    assert "items" in body and "total" in body

    r_costs = client.get("/costs/summary?period=monthly")
    assert r_costs.status_code == 200, r_costs.text
    body = r_costs.json()
    assert "total_cost" in body and "by_provider" in body and "by_region" in body

    r_recs = client.get("/recommendations")
    assert r_recs.status_code == 200, r_recs.text
    assert isinstance(r_recs.json(), list)

    # Public reads (versioned)
    r_resources_v1 = client.get("/api/v1/resources")
    assert r_resources_v1.status_code == 200, r_resources_v1.text
    body = r_resources_v1.json()
    assert "items" in body and "total" in body

    r_costs_v1 = client.get("/api/v1/costs/summary?period=monthly")
    assert r_costs_v1.status_code == 200, r_costs_v1.text
    body = r_costs_v1.json()
    assert "total_cost" in body and "by_provider" in body and "by_region" in body

    r_recs_v1 = client.get("/api/v1/recommendations")
    assert r_recs_v1.status_code == 200, r_recs_v1.text
    assert isinstance(r_recs_v1.json(), list)
