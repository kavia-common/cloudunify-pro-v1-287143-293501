import os
from fastapi.testclient import TestClient

# Ensure a lightweight test DB (SQLite) is used before importing the app
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///./test_app.db"

from src.api.main import app  # noqa: E402

client = TestClient(app)


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
    r = client.post("/resources/bulk", json=payload)
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
    r = client.post("/costs/bulk", json=payload)
    assert r.status_code == 400
    body = r.json().get("detail")
    assert body is not None
    assert body["inserted"] == 0 and body["updated"] == 0
    assert len(body["errors"]) == 1


def test_resources_upsert_insert_then_update():
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
    r1 = client.post("/resources/bulk", json=first)
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
    r2 = client.post("/resources/bulk", json=second)
    assert r2.status_code == 200
    body2 = r2.json()
    assert body2["inserted"] == 0
    assert body2["updated"] == 1
    assert body2["errors"] == []
