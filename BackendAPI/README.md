# CloudUnify Pro - BackendAPI

FastAPI backend for CloudUnify Pro-v1 providing authentication, resource inventory, cost analytics, recommendations, automation, and WebSocket activity streams.

## Quickstart (Development)

1) Python 3.11+ recommended. Create a virtual environment and install deps:
   python -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt

2) Configure environment:
   cp .env.example .env
   # For local dev, SQLite is fine (default). To use Postgres, set DATABASE_URL accordingly.

3) Run the API:
   uvicorn src.api.main:app --reload --port 8000

4) OpenAPI docs:
   - Swagger UI: http://localhost:8000/docs
   - JSON: http://localhost:8000/openapi.json
   - Export to interfaces/openapi.json:
     python -m src.api.generate_openapi

## Environment Variables

See .env.example for full list. Key variables:

- DATABASE_URL: postgresql+asyncpg://USER:PASS@HOST:5432/DBNAME (prod) or sqlite+aiosqlite:///./app.db (dev/tests)
- SECRET_KEY: Change for non-dev; used to sign JWTs
- ALGORITHM: JWT signing algorithm (HS256)
- ACCESS_TOKEN_EXPIRE_MINUTES: Access token lifetime in minutes
- REFRESH_TOKEN_EXPIRE_DAYS: Refresh token lifetime in days
- CORS_ORIGINS: Comma-separated allowed origins (e.g., http://localhost:5173)
- USE_ALEMBIC: If "1", apply Alembic migrations at startup for non-sqlite DBs
- SQL_ECHO: "1" to echo SQL for debugging

Note on JWT secrets:
- SECRET_KEY must be long and random in production.
- Never commit real secrets to source control.

## Database Migrations (Alembic)

- Local dev/tests using SQLite auto-create tables on startup.
- For PostgreSQL, prefer migrations:

   # Upgrade to latest
   alembic upgrade head

   # Create a new revision (when models change)
   alembic revision -m "describe changes" --autogenerate
   alembic upgrade head

See kavia-docs/db-migrations.md for details.

## WebSocket Usage

- WebSocket endpoint: /ws/activity-stream/{organization_id}
- Auth: Provide a JWT access token via either:
  - Authorization header: "Bearer <token>"
  - Query string: ?token=<token>
- The GET route on the same path returns usage info for docs and tooling.

Minimal browser client:
  const orgId = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
  const token = "<access_token>";
  const ws = new WebSocket(`ws://localhost:8000/ws/activity-stream/${orgId}?token=${token}`);

## Running Tests

- Ensure virtualenv is active and deps installed:
   pytest -q

Tests use SQLite via DATABASE_URL=sqlite+aiosqlite:///./test_app.db and do not require PostgreSQL.

## Sample Datasets (CSV)

Sample CSVs for bulk ingestion are provided under samples/:

- samples/resources.csv
- samples/costs.csv

Use the WebFrontend bulk uploader to load these files (Resources and Costs pages). The frontend maps CSV headers, performs client-side validation, and calls:
- POST /resources/bulk with { items: ResourceIngestRow[] }
- POST /costs/bulk with { items: CostIngestRow[] }

If ingesting directly via API, convert CSV rows to the appropriate JSON payload format above.

## API Reference

- OpenAPI JSON in interfaces/openapi.json
- Additional endpoint notes in kavia-docs/api-endpoints.md

## Notes

- For production deploys, always:
  - Set REAL secrets and DATABASE_URL
  - Apply alembic upgrade head
  - Configure CORS_ORIGINS appropriately
  - Serve behind HTTPS and ensure secure JWT handling
