import os
from fastapi import FastAPI, APIRouter
from fastapi.middleware.cors import CORSMiddleware

from src.api.db import init_db
from src.api.routes.ingest import router as ingest_router
from src.api.routes.auth import router as auth_router
from src.api.routes.resources import router as resources_router
from src.api.routes.costs import router as costs_router
from src.api.routes.recommendations import router as recommendations_router
from src.api.routes.automation import router as automation_router
from src.api.routes.ws import router as ws_router
from src.api.services.dev_seed import maybe_seed_dev_users

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
