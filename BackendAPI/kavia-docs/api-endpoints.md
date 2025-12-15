# CloudUnify Pro Backend API - Endpoint Notes

Security: All API endpoints (except `/`) require JWT bearer auth unless noted. Provide `Authorization: Bearer <access_token>`.

The OpenAPI definition is generated from the FastAPI app and stored at `interfaces/openapi.json`.

## Health

- GET `/` — Basic health check

## Auth

- POST `/auth/login` — Authenticate and receive `{ access_token, refresh_token, token_type }`
- GET `/auth/me` — Current user profile
- GET `/auth/admin-ping` — Admin-only smoke test (requires admin role)

## Resources

- GET `/resources` — List resources with optional filters:
  - provider, region, state, page (default 1), size (default 20)
- POST `/resources/bulk` — Bulk upsert resources
  - Request: `{ "items": ResourceIngestRow[] }`
  - Upsert key: `(organization_id, provider, resource_id)`
  - On conflict: update fields (created_at preserved), tags replaced, updated_at refreshed
  - Response: `{ inserted, updated, errors[] }`
  - Returns 400 if all items fail validation

## Costs / Analytics

- GET `/costs/summary` — Aggregated cost summary (by provider/region)
  - Query: `period` (daily|monthly; default monthly)
  - Response: `CostSummary`
- POST `/costs/bulk` — Bulk upsert costs
  - Request: `{ "items": CostIngestRow[] }`
  - Upsert key: `(organization_id, cloud_account_id, provider, service_name, region, cost_date)`
  - On conflict: REPLACE semantics (overwrite), updated_at refreshed
  - Response: `{ inserted, updated, errors[] }`
  - Returns 400 if all items fail validation

## Recommendations

- GET `/recommendations` — List recommendations with optional filters:
  - priority (low|medium|high|critical)
  - resource_id (UUID)

## Automation

- GET `/automation-rules` — List rules
- POST `/automation-rules` — Create rule (requires admin)

## WebSocket Activity Stream

- GET `/ws/activity-stream/{organization_id}` — Returns usage information for WebSocket clients
- WS `/ws/activity-stream/{organization_id}` — Real-time activity stream
  - Auth via `Authorization: Bearer <token>` or `?token=<token>`
  - Messages:
    - Server may send `{ "type": "connected" }` and periodic `{ "type": "ping" }`
    - Activity events such as `resources.bulk` and `costs.bulk`

Minimal browser client:
```js
const orgId = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const token = "<access_token>";
const ws = new WebSocket(`ws://localhost:8000/ws/activity-stream/${orgId}?token=${token}`);

ws.onmessage = (ev) => {
  const msg = JSON.parse(ev.data);
  if (msg.type === "ping") { ws.send(JSON.stringify({ type: "pong" })); return; }
  console.log("activity:", msg);
};
```

## Samples

CSV samples are provided under `BackendAPI/samples/`:
- `resources.csv`
- `costs.csv`

Load via the WebFrontend bulk uploaders (Resources/Costs pages) or convert to JSON for the REST endpoints above.
