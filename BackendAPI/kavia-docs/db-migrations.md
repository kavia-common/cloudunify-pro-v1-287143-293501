# Database Migrations (Alembic)

This backend uses Alembic to manage PostgreSQL schema migrations. For local testing (SQLite), the app falls back to SQLAlchemy `create_all()` automatically.

## Prerequisites
- Install dependencies:
  pip install -r BackendAPI/requirements.txt
- Copy env template and set required variables:
  cd BackendAPI && cp .env.example .env
  # Set:
  #   DATABASE_URL (e.g., postgresql+asyncpg://USER:PASS@HOST:5432/DBNAME)
  #   SECRET_KEY (non-empty), ALGORITHM (HS256), ACCESS_TOKEN_EXPIRE_MINUTES, REFRESH_TOKEN_EXPIRE_DAYS
  #   CORS_ORIGINS (comma-separated origins)
  # Optional:
  #   USE_ALEMBIC=1 to auto-apply on startup; SQL_ECHO=1 for debugging

## Running Migrations
From the BackendAPI directory:

1) Generate the OpenAPI (optional):
   python -m src.api.generate_openapi

2) Upgrade database to the latest head:
   alembic upgrade head

3) To downgrade (caution):
   alembic downgrade -1

If you run the FastAPI app with a PostgreSQL DATABASE_URL and USE_ALEMBIC=1 (default), migrations will also be applied automatically at startup.

## Creating New Revisions
When you change SQLAlchemy models (in src/api/models.py):

1) Ensure all models are imported in alembic/env.py via:
   from src.api import models  # already present

2) Create a new revision with autogenerate:
   alembic revision -m "describe changes" --autogenerate

3) Review the generated migration in alembic/versions/ and adjust if needed.

4) Apply it:
   alembic upgrade head

## Notes
- Alembic is configured for async URLs (asyncpg/aiosqlite) and will handle both offline and online modes.
- Production deployments should always apply migrations (alembic upgrade head) as part of the release process.
- Dev/CI tests based on SQLite will not use Alembic and will auto-create tables for fast startup.
