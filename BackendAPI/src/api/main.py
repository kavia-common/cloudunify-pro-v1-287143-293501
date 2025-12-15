import os
import logging
from typing import Optional

from fastapi import FastAPI, APIRouter, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.db import init_db, get_session
from src.api.routes.ingest import router as ingest_router
from src.api.routes.auth import router as auth_router
from src.api.routes.resources import router as resources_router
from src.api.routes.costs import router as costs_router
from src.api.routes.recommendations import router as recommendations_router
from src.api.routes.automation import router as automation_router
from src.api.routes.ws import router as ws_router
from src.api.services.dev_seed import maybe_seed_dev_users
from src.api.models import User

openapi_tags = [
    {"name": "Auth", "description": "Authentication and user session endpoints"},
    {"name": "Ingestion", "description": "Bulk ingestion endpoints for resources and costs"},
    {"name": "Resources", "description": "Resource inventory endpoints"},
    {"name": "Analytics", "description": "Cost analytics and summaries"},
    {"name": "Recommendations", "description": "Optimization recommendations"},
    {"name": "Automation", "description": "Automation rules and operations"},
    {"name": "Realtime", "description": "WebSocket activity streams and real-time updates"},
    {"name": "Health", "description": "Health and diagnostics"},
]

app = FastAPI(
    title="CloudUnify Pro Backend API",
    version="1.0.0",
    description="Multi-cloud resource management API with authentication and bulk ingestion endpoints.",
    openapi_tags=openapi_tags,
)

# CORS from environment (comma-separated origins). Defaults to '*' for dev.
# Prefer CORS_ORIGINS; also accept ALLOWED_ORIGINS and legacy CORS_ALLOW_ORIGINS.
cors_env = os.getenv("CORS_ORIGINS") or os.getenv("ALLOWED_ORIGINS") or os.getenv("CORS_ALLOW_ORIGINS", "*")
allow_origins = ["*"] if cors_env.strip() == "*" else [o.strip() for o in cors_env.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def on_startup():
    """Initialize database and perform startup tasks."""
    _log = logging.getLogger("cloudunify.startup")
    try:
        shown_origins = "*" if cors_env.strip() == "*" else allow_origins
        _log.info("Startup CORS allow_origins=%s", shown_origins)
    except Exception:
        pass

    # Log dev seed flags and normalized email that would be seeded (without password)
    _seed_users = os.getenv("DEV_SEED_USERS")
    _seed_enabled = os.getenv("DEV_SEED_ENABLED")
    _seed_alias = os.getenv("DEV_SEED")
    _custom_email = (os.getenv("DEV_SEED_EMAIL") or "").strip().lower()
    _log.info(
        "Startup dev-seed flags DEV_SEED_USERS=%r DEV_SEED_ENABLED=%r DEV_SEED=%r custom_email=%s",
        _seed_users, _seed_enabled, _seed_alias, _custom_email or "(none)",
    )

    # Determine if seeding is enabled (mirrors the seeding logic defaults)
    def _seed_default() -> bool:
        db_url = os.getenv("DATABASE_URL", "")
        node_env = (os.getenv("NODE_ENV") or os.getenv("REACT_APP_NODE_ENV") or os.getenv("ENV") or "").strip().lower()
        return db_url.startswith("sqlite") or node_env == "development"

    _raw_seed_flag = _seed_users or _seed_enabled or _seed_alias
    _will_seed = _truthy(_raw_seed_flag) or _seed_default()
    if _will_seed:
        _log.info("Startup: dev seeding is ENABLED (demo users will be ensured).")
    else:
        _log.info("Startup: dev seeding is DISABLED (set DEV_SEED_ENABLED=1 to enable).")

    await init_db()
    # Seed default development users when enabled (dev environments by default)
    await maybe_seed_dev_users()


# PUBLIC_INTERFACE
def health_response():
    """Basic health check endpoint for readiness probes. Returns HTTP 200 with a small JSON payload quickly."""
    return {"status": "ok"}


# Register health endpoints for compatibility across environments:
# - Root path "/"
# - Conventional "/health"
# - Versioned "/api/v1/health"
# - Optional custom path via REACT_APP_HEALTHCHECK_PATH
app.add_api_route(
    "/",
    endpoint=health_response,
    methods=["GET"],
    tags=["Health"],
    summary="Health Check",
    description="Basic health check endpoint.",
)
app.add_api_route(
    "/health",
    endpoint=health_response,
    methods=["GET"],
    tags=["Health"],
    summary="Health Check",
    description="Basic health check endpoint.",
)
app.add_api_route(
    "/api/v1/health",
    endpoint=health_response,
    methods=["GET"],
    tags=["Health"],
    summary="Health Check",
    description="Basic health check endpoint.",
)
_health_env_path = (os.getenv("REACT_APP_HEALTHCHECK_PATH") or "").strip()
if _health_env_path and _health_env_path not in {"/", "/health", "/api/v1/health"}:
    app.add_api_route(
        _health_env_path,
        endpoint=health_response,
        methods=["GET"],
        tags=["Health"],
        summary="Health Check",
        description=f"Health check alias for configured path {_health_env_path}",
    )

# Register routers at root
app.include_router(auth_router)
app.include_router(ingest_router)
app.include_router(resources_router)
app.include_router(costs_router)
app.include_router(recommendations_router)
app.include_router(automation_router)
app.include_router(ws_router)

# Also expose the same routes under /api/v1 to avoid base-path mismatches in some environments
api_v1 = APIRouter(prefix="/api/v1")
api_v1.include_router(auth_router)
api_v1.include_router(ingest_router)
api_v1.include_router(resources_router)
api_v1.include_router(costs_router)
api_v1.include_router(recommendations_router)
api_v1.include_router(automation_router)
api_v1.include_router(ws_router)
app.include_router(api_v1)


def _truthy(value: Optional[str]) -> bool:
    return bool(value) and value.strip().lower() in {"1", "true", "yes", "on"}


def _dev_routes_enabled() -> bool:
    # Enable if explicitly requested
    if _truthy(os.getenv("DEV_TOOLS")):
        return True
    # Otherwise follow the same defaults as seeding logic
    db_url = os.getenv("DATABASE_URL", "")
    node_env = (os.getenv("NODE_ENV") or os.getenv("REACT_APP_NODE_ENV") or os.getenv("ENV") or "").strip().lower()
    if db_url.startswith("sqlite") or node_env == "development":
        return True
    # Also enable if any seeding flag is truthy
    if _truthy(os.getenv("DEV_SEED_USERS")) or _truthy(os.getenv("DEV_SEED_ENABLED")) or _truthy(os.getenv("DEV_SEED")):
        return True
    return False


if _dev_routes_enabled():

    class DevUsersStatus(BaseModel):
        """Status of dev users that should exist when seeding is enabled."""
        kishore_email_exists: bool = Field(..., description="Whether the demo user 'kishore@kavia.ai' exists")
        custom_email_exists: bool = Field(..., description="Whether the custom DEV_SEED_EMAIL user exists (if provided)")
        emails: list[str] = Field(default_factory=list, description="List of dev-seeded emails found")

    class DevSeedStatus(BaseModel):
        """Dev seed status response wrapper."""
        dev_seed_enabled: bool = Field(..., description="Whether dev seeding would run (inferred from env/dev defaults)")
        users: DevUsersStatus = Field(..., description="User existence summary for expected dev users")

    # PUBLIC_INTERFACE
    @app.get(
        "/api/v1/__dev/seed-status",
        summary="Dev seed status",
        description="Return whether dev seeding is enabled and if key dev users exist (dev-only).",
        response_model=DevSeedStatus,
        tags=["Health"],
        operation_id="dev_seed_status",
    )
    async def dev_seed_status(session: AsyncSession = Depends(get_session)) -> DevSeedStatus:
        """Return the status of dev seeding and presence of demo/custom users.

        This endpoint is intended for development verification only and is enabled automatically
        in dev-like environments or when DEV_TOOLS=1 is set.
        """
        # Infer if seeding is enabled using the same flags/defaults
        def _seed_default() -> bool:
            db_url = os.getenv("DATABASE_URL", "")
            node_env = (os.getenv("NODE_ENV") or os.getenv("REACT_APP_NODE_ENV") or os.getenv("ENV") or "").strip().lower()
            return db_url.startswith("sqlite") or node_env == "development"

        raw_flag = os.getenv("DEV_SEED_USERS") or os.getenv("DEV_SEED_ENABLED") or os.getenv("DEV_SEED")
        dev_seed_enabled = _truthy(raw_flag) or _seed_default()

        # Expected dev/demo users (normalized to lowercase)
        extra_email_raw = (os.getenv("DEV_SEED_EMAIL") or "").strip().lower()
        kishore_email = "kishore@kavia.ai"
        admin_email = (os.getenv("DEV_ADMIN_EMAIL") or "admin@cloudunify.pro").strip().lower()
        user_email = (os.getenv("DEV_USER_EMAIL") or "user@cloudunify.pro").strip().lower()

        target_emails = {kishore_email, admin_email, user_email}
        if extra_email_raw:
            target_emails.add(extra_email_raw)

        res = await session.execute(
            select(func.lower(User.email)).where(func.lower(User.email).in_(list(target_emails)))
        )
        existing_emails = sorted({row[0] for row in res.all()})

        kishore_exists = kishore_email in existing_emails
        custom_exists = extra_email_raw in existing_emails if extra_email_raw else False

        return DevSeedStatus(
            dev_seed_enabled=bool(dev_seed_enabled),
            users=DevUsersStatus(
                kishore_email_exists=kishore_exists,
                custom_email_exists=custom_exists,
                emails=existing_emails,
            ),
        )


if __name__ == "__main__":
    # Allow direct execution: python src/api/main.py
    import uvicorn

    host = os.getenv("HOST", "0.0.0.0")
    port_str = os.getenv("REACT_APP_PORT") or os.getenv("PORT") or "3001"
    try:
        port = int(port_str)
    except ValueError:
        port = 3001
    reload_flag = os.getenv("RELOAD", "0") == "1"
    log_level = os.getenv("REACT_APP_LOG_LEVEL", "info")

    uvicorn.run(app, host=host, port=port, reload=reload_flag, log_level=log_level)
