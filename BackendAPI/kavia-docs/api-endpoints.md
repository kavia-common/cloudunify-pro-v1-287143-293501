# Bulk Ingestion Endpoints

Two new ingestion endpoints are available for API-based data loading. Both endpoints accept JSON arrays and perform validated upserts into the database.

Security: endpoints are scaffolded with bearerAuth (JWT) via a placeholder dependency. Validation currently allows all requests; enable real auth later.

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
