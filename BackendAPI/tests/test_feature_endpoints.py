import os
import asyncio
from datetime import date

from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

# Use SQLite for tests
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///./test_app.db"

from src.api.main import app  # noqa: E402
from src.api.models import User, Recommendation  # noqa: E402
from src.api.security import get_password_hash  # noqa: E402

client = TestClient(app)

TEST_USER_EMAIL = "features-user@example.com"
TEST_USER_PASS = "UserPa55!"
TEST_ADMIN_EMAIL = "features-admin@example.com"
TEST_ADMIN_PASS = "AdminPa55!"


async def _ensure_user(email: str, password: str, role: str = "user", is_active: bool = True):
    engine = create_async_engine(os.environ["DATABASE_URL"], future=True)
    session_maker = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with session_maker() as session:
        res = await session.execute(select(User).where(User.email == email))
        user = res.scalar_one_or_none()
        if user:
            user.role = role
            user.is_active = is_active
            await session.commit()
            return user
        user = User(email=email, hashed_password=get_password_hash(password), role=role, is_active=is_active)
        session.add(user)
        await session.commit()
        return user


def _login(email: str, password: str) -> str:
    r = client.post("/auth/login", json={"email": email, "password": password})
    assert r.status_code == 200, f"Login failed: {r.text}"
    return r.json()["access_token"]


def _auth_header(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def test_resources_list_and_authz():
    # ensure users
    asyncio.get_event_loop().run_until_complete(_ensure_user(TEST_USER_EMAIL, TEST_USER_PASS, role="user"))
    token = _login(TEST_USER_EMAIL, TEST_USER_PASS)

    # unauthorized should 401
    r_unauth = client.get("/resources")
    assert r_unauth.status_code == 401

    # Ingest some resources via existing endpoint
    payload = {
        "items": [
            {
                "organization_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "cloud_account_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "provider": "aws",
                "resource_id": "i-100",
                "resource_type": "ec2.instance",
                "region": "us-east-1",
                "state": "running",
                "tags": {"env": "prod"},
                "cost_daily": 1.1,
                "cost_monthly": 33.0,
            },
            {
                "organization_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "cloud_account_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "provider": "aws",
                "resource_id": "i-200",
                "resource_type": "ec2.instance",
                "region": "us-west-2",
                "state": "stopped",
                "tags": {"env": "dev"},
                "cost_daily": 0.2,
                "cost_monthly": 6.0,
            },
        ]
    }
    r_ing = client.post("/resources/bulk", json=payload, headers=_auth_header(token))
    assert r_ing.status_code == 200, r_ing.text

    # list all
    r_all = client.get("/resources", headers=_auth_header(token))
    assert r_all.status_code == 200, r_all.text
    body = r_all.json()
    assert "items" in body and "total" in body and body["total"] >= 2

    # filter by region
    r_region = client.get("/resources?region=us-east-1", headers=_auth_header(token))
    assert r_region.status_code == 200
    items = r_region.json()["items"]
    assert all(i["region"] == "us-east-1" for i in items)


def test_costs_summary_and_authz():
    # ensure user
    asyncio.get_event_loop().run_until_complete(_ensure_user(TEST_USER_EMAIL, TEST_USER_PASS, role="user"))
    token = _login(TEST_USER_EMAIL, TEST_USER_PASS)

    # unauthorized
    r_unauth = client.get("/costs/summary")
    assert r_unauth.status_code == 401

    # Ingest costs
    payload = {
        "items": [
            {
                "organization_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "cloud_account_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "provider": "aws",
                "service_name": "AmazonEC2",
                "region": "us-east-1",
                "cost_date": str(date(2025, 1, 1)),
                "usage_quantity": 10,
                "usage_unit": "Hours",
                "cost_amount": 5.5,
                "currency": "usd",
            },
            {
                "organization_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "cloud_account_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "provider": "aws",
                "service_name": "AmazonEC2",
                "region": "us-west-2",
                "cost_date": str(date(2025, 1, 2)),
                "usage_quantity": 5,
                "usage_unit": "Hours",
                "cost_amount": 2.5,
                "currency": "usd",
            },
        ]
    }
    r_ing = client.post("/costs/bulk", json=payload, headers=_auth_header(token))
    assert r_ing.status_code == 200, r_ing.text

    r_sum = client.get("/costs/summary", headers=_auth_header(token))
    assert r_sum.status_code == 200
    body = r_sum.json()
    assert body["total_cost"] >= 8.0
    assert "aws" in body["by_provider"]
    assert "us-east-1" in body["by_region"]


def test_recommendations_list_filters_and_authz():
    # user
    asyncio.get_event_loop().run_until_complete(_ensure_user(TEST_USER_EMAIL, TEST_USER_PASS, role="user"))
    token = _login(TEST_USER_EMAIL, TEST_USER_PASS)

    # unauthorized
    r_unauth = client.get("/recommendations")
    assert r_unauth.status_code == 401

    # Insert a recommendation directly
    async def _insert_rec():
        engine = create_async_engine(os.environ["DATABASE_URL"], future=True)
        session_maker = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
        async with session_maker() as session:
            rec = Recommendation(
                organization_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                resource_id=None,
                recommendation_type="rightsizing",
                priority="high",
                potential_savings_monthly=12.34,
                description="Stop idle instance",
                action_items=["stop", "schedule"],
            )
            session.add(rec)
            await session.commit()
    asyncio.get_event_loop().run_until_complete(_insert_rec())

    r_list = client.get("/recommendations?priority=high", headers=_auth_header(token))
    assert r_list.status_code == 200
    items = r_list.json()
    assert isinstance(items, list)
    assert any(i["priority"] == "high" for i in items)


def test_automation_rules_get_post_rbac():
    # create non-admin and admin
    asyncio.get_event_loop().run_until_complete(_ensure_user(TEST_USER_EMAIL, TEST_USER_PASS, role="user"))
    asyncio.get_event_loop().run_until_complete(_ensure_user(TEST_ADMIN_EMAIL, TEST_ADMIN_PASS, role="admin"))
    user_token = _login(TEST_USER_EMAIL, TEST_USER_PASS)
    admin_token = _login(TEST_ADMIN_EMAIL, TEST_ADMIN_PASS)

    # GET unauthorized
    r_unauth = client.get("/automation-rules")
    assert r_unauth.status_code == 401

    # GET as user
    r_get_user = client.get("/automation-rules", headers=_auth_header(user_token))
    assert r_get_user.status_code == 200

    # POST as user should be forbidden
    payload = {
        "organization_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "name": "shut-down-idle",
        "rule_type": "scheduling",
        "is_enabled": True,
        "match_criteria": {"tag:env": "dev"},
        "action_type": "stop",
        "cron_schedule": "0 2 * * *",
    }
    r_post_user = client.post("/automation-rules", json=payload, headers=_auth_header(user_token))
    assert r_post_user.status_code == 403

    # POST as admin
    r_post_admin = client.post("/automation-rules", json=payload, headers=_auth_header(admin_token))
    assert r_post_admin.status_code == 201, r_post_admin.text
    body = r_post_admin.json()
    assert body["name"] == payload["name"]

    # GET and ensure rule exists
    r_get_admin = client.get("/automation-rules", headers=_auth_header(admin_token))
    assert r_get_admin.status_code == 200
    items = r_get_admin.json()
    assert any(i["name"] == payload["name"] for i in items)
