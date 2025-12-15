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

TEST_EMAIL = "ws-user@example.com"
TEST_PASSWORD = "Sup3rSecret!"


async def _ensure_user(email: str, password: str, role: str = "user"):
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


def _login(email: str, password: str) -> str:
    r = client.post("/auth/login", json={"email": email, "password": password})
    assert r.status_code == 200, f"Login failed: {r.text}"
    return r.json()["access_token"]


def test_websocket_auth_required():
    # Attempt to connect without token should fail (close 1008)
    org_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    try:
        with client.websocket_connect(f"/ws/activity-stream/{org_id}") as ws:
            # Should not succeed; server should close immediately
            data = ws.receive_json()
            assert False, f"Unexpected data: {data}"
    except Exception:
        # WebSocketDisconnect or handshake failure expected
        pass


def test_websocket_broadcast_on_resource_ingest():
    # Ensure user and login
    asyncio.get_event_loop().run_until_complete(_ensure_user(TEST_EMAIL, TEST_PASSWORD))
    token = _login(TEST_EMAIL, TEST_PASSWORD)
    org_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

    # Connect to WebSocket with token query parameter
    with client.websocket_connect(f"/ws/activity-stream/{org_id}?token={token}") as ws:
        # The server may send an initial 'connected' event; consume it
        initial = ws.receive_json()
        assert isinstance(initial, dict)

        # Trigger ingestion for the same org
        payload = {
            "items": [
                {
                    "organization_id": org_id,
                    "cloud_account_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                    "provider": "aws",
                    "resource_id": "i-abc123",
                    "resource_type": "ec2.instance",
                    "region": "us-east-1",
                    "state": "running",
                    "tags": {"env": "test"},
                    "cost_daily": 0.1,
                    "cost_monthly": 3.0,
                }
            ]
        }
        r_ing = client.post("/resources/bulk", json=payload, headers={"Authorization": f"Bearer {token}"})
        assert r_ing.status_code == 200, r_ing.text

        # Expect an activity event broadcast
        evt = ws.receive_json()
        # If a ping slipped in, keep reading until we get our event
        attempts = 0
        while evt.get("type") in ("ping", "connected") and attempts < 3:
            evt = ws.receive_json()
            attempts += 1

        assert evt["type"] == "resources.bulk"
        assert evt["organization_id"] == org_id
        assert "payload" in evt
        assert evt["payload"]["processed_count"] >= 1
