# CloudUnify Pro - BackendAPI

FastAPI backend for CloudUnify Pro-v1 providing authentication, resource inventory, cost analytics, recommendations, automation, and WebSocket activity streams.

## Quickstart (Development)

1) Python 3.11+ recommended. Create a virtual environment and install deps:
   python -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt

2) Configure environment:
   cp .env.example .env
   # For local dev, SQLite is fine (default). To use Postgres, set DATABASE_URL accordingly.

3) Run the API (binds to 0.0.0.0:3001 by default):

   # Option A: module entrypoint (recommended)
   python -m src.api

   # Option B: specify host/port explicitly with uvicorn CLI
   uvicorn src.api.main:app --host 0.0.0.0 --port 3001

   # Override via env:
   #   HOST=0.0.0.0 REACT_APP_PORT=3001 RELOAD=1 python -m src.api

4) OpenAPI docs:
   - Swagger UI: http://localhost:3001/docs
   - JSON: http://localhost:3001/openapi.json
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
- HOST: Host interface to bind the server (default 0.0.0.0)
- REACT_APP_PORT / PORT: HTTP port to bind (default 3001)
- REACT_APP_HEALTHCHECK_PATH: Optional custom health path alias (in addition to "/", "/health", "/api/v1/health")
- REACT_APP_LOG_LEVEL: Uvicorn log level (default "info")
- RELOAD: "1" to enable uvicorn auto-reload (development only)

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

## Development Login

For development convenience, the API can seed default users at startup so you can sign in immediately.

- Enabled when:
  - DEV_SEED_USERS (or alias DEV_SEED_ENABLED) is truthy (1/true/yes/on), or
  - Neither flag is set and:
    - DATABASE_URL points to SQLite (default dev/test), or
    - ENV, NODE_ENV or REACT_APP_NODE_ENV is 'development'.

Behavior:
- Emails are normalized to lowercase on storage and lookup (login is case-insensitive).
- If a seeded user already exists but the configured password doesn’t match, the hash is refreshed so login works in dev.

Default credentials (override via environment variables):
- Admin: admin@cloudunify.pro / Admin123!
- User:  user@cloudunify.pro / User123!

Environment variables:
- DEV_SEED_USERS=1  (alias: DEV_SEED_ENABLED=1)
- DEV_ADMIN_EMAIL=admin@cloudunify.pro
- DEV_ADMIN_PASSWORD=Admin123!
- DEV_USER_EMAIL=user@cloudunify.pro
- DEV_USER_PASSWORD=User123!

Custom demo/user seeding:
- You can also seed an additional custom user (useful for demos):
  - DEV_SEED_EMAIL=<email>
  - DEV_SEED_PASSWORD=<password>
  - DEV_SEED_ROLE=user|admin (default: user)
  - DEV_SEED_ACTIVE=1|0 (default: 1)

Example: seed the reported demo credentials and login via /api/v1 path
  export DEV_SEED_USERS=1
  export DEV_SEED_EMAIL=kishore@kavia.ai
  export DEV_SEED_PASSWORD=Kishore@15404
  # start the API, then:
  curl -s -X POST http://localhost:3001/api/v1/auth/login \
    -H "Content-Type: application/json" \
    -d '{"email":"kishore@kavia.ai","password":"Kishore@15404"}'

Minimal diagnostics for failed auth:
- Set AUTH_DEV_LOGS=1 (preferred) to log sanitized reasons for login failures without leaking passwords.
- Legacy: AUTH_LOG_FAILURES=1 is still supported for backward compatibility.

CORS:
- The app reads allowed origins from CORS_ORIGINS, ALLOWED_ORIGINS, or CORS_ALLOW_ORIGINS (comma-separated).
- In dev, '*' is allowed by default if none are set. Set CORS_ORIGINS to your frontend origin (e.g., http://localhost:5173).

Dev seeding flags:
- DEV_SEED_USERS or DEV_SEED_ENABLED enable seeding explicitly. DEV_SEED is also accepted as an alias.
- The startup logs will print normalized DEV_SEED_EMAIL (if provided) and whether seeding is enabled.

Dev diagnostics endpoint (dev-only):
- GET /api/v1/__dev/seed-status
  Returns JSON including:
    {
      "dev_seed_enabled": true|false,
      "users": {
        "kishore_email_exists": true|false,
        "custom_email_exists": true|false,
        "emails": ["kishore@kavia.ai", "user@cloudunify.pro", "admin@cloudunify.pro", "..."]
      }
    }
  This endpoint is available in dev-like environments (SQLite or NODE_ENV=development), when any seeding flag is set, or when DEV_TOOLS=1.

## Health Endpoints

- GET /
- GET /health
- GET /api/v1/health
- Optionally: GET $REACT_APP_HEALTHCHECK_PATH (if set)

All paths return HTTP 200 quickly with a small JSON payload.

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
