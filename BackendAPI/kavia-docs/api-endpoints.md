# Bulk Ingestion Endpoints

Two new ingestion endpoints are available for API-based data loading. Both endpoints accept JSON arrays and perform validated upserts into the database.

Security: endpoints require bearerAuth (JWT) and validate tokens.

## POST /resources/bulk

Request:
{
  "items": [
    {
      "organization_id": "uuid",
      "cloud_account_id": "uuid",
      "provider": "aws|azure|gcp",
      "resource_id": "string",
      "resource_type": "string",
      "region": "string",
      "state": "string",
      "tags": {"k":"v"},
      "cost_daily": 0.0,
      "cost_monthly": 0.0,
      "created_at": "2024-01-01T00:00:00Z" // optional
    }
  ]
}

Behavior:
- Upsert keyed by (organization_id, provider, resource_id)
- created_at preserved on update
- tags replaced with incoming
- updated_at refreshed on update

Response:
{
  "inserted": 1,
  "updated": 0,
  "errors": [{ "index": 0, "message": "..." }]
}

Returns 400 if all items fail validation.

## POST /costs/bulk

Request:
{
  "items": [
    {
      "organization_id": "uuid",
      "cloud_account_id": "uuid",
      "provider": "aws|azure|gcp",
      "service_name": "string",
      "region": "string",
      "cost_date": "YYYY-MM-DD",
      "usage_quantity": 0.0,
      "usage_unit": "string",
      "cost_amount": 0.0,
      "currency": "USD"
    }
  ]
}

Behavior:
- Upsert keyed by (organization_id, cloud_account_id, provider, service_name, region, cost_date)
- REPLACE (overwrite) semantics on conflict
- updated_at refreshed on update

Response:
{
  "inserted": 1,
  "updated": 0,
  "errors": []
}

# WebSocket Activity Stream

Endpoint:
- ws: `/ws/activity-stream/{organization_id}`
- docs (GET): `/ws/activity-stream/{organization_id}`

Authentication:
- Provide access JWT via either:
  - Header: `Authorization: Bearer <token>`
  - Query: `?token=<token>`

Notes:
- Server sends an initial `{"type":"connected"}` event on connect.
- Keepalive `{"type":"ping"}` may be sent periodically.
- Events are concise; e.g., `resources.bulk` and `costs.bulk`.

Minimal client (browser JS):
```js
const orgId = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const token = "<access_token>";
const ws = new WebSocket(`ws://localhost:8000/ws/activity-stream/${orgId}?token=${token}`);

ws.onopen = () => console.log("connected");
ws.onmessage = (ev) => {
  try {
    const msg = JSON.parse(ev.data);
    if (msg.type === "ping") { /* optionally respond */ ws.send(JSON.stringify({type:"pong"})); return; }
    console.log("activity event:", msg);
  } catch (e) {
    console.log("message:", ev.data);
  }
};
ws.onclose = (ev) => console.log("closed", ev.code);
```

Minimal client (Python):
```python
import asyncio
import websockets
import json

async def run():
    org_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    token = "<access_token>"
    uri = f"ws://localhost:8000/ws/activity-stream/{org_id}?token={token}"
    async with websockets.connect(uri) as ws:
        async for raw in ws:
            msg = json.loads(raw)
            if msg.get("type") == "ping":
                await ws.send(json.dumps({"type": "pong"}))
                continue
            print("activity:", msg)

asyncio.run(run())
```
