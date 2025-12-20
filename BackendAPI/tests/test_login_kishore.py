import os
from fastapi.testclient import TestClient

# Configure environment BEFORE importing the app so startup seeding runs with these values
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///./test_app.db"
os.environ["DEV_SEED_USERS"] = "1"  # enable seeding explicitly for test
os.environ["DEV_SEED_EMAIL"] = "kishore@kavia.ai"
os.environ["DEV_SEED_PASSWORD"] = "Kishore@15404"
os.environ["DEV_SEED_ROLE"] = "user"
os.environ["DEV_SEED_ACTIVE"] = "1"

from src.api.main import app  # noqa: E402

client = TestClient(app)


def test_login_v1_route_with_custom_seed_user():
    # Attempt login against /api/v1 prefix to ensure the route mapping is correct
    resp = client.post(
        "/api/v1/auth/login",
        json={"email": "kishore@kavia.ai", "password": "Kishore@15404"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "access_token" in data and "refresh_token" in data
    assert data.get("token_type") == "bearer"
