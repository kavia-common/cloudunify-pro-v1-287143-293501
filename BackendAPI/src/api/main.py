import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.db import init_db
from src.api.routes.ingest import router as ingest_router
from src.api.routes.auth import router as auth_router
from src.api.routes.resources import router as resources_router
from src.api.routes.costs import router as costs_router
from src.api.routes.recommendations import router as recommendations_router
from src.api.routes.automation import router as automation_router
from src.api.routes.ws import router as ws_router

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
# Prefer CORS_ORIGINS; fallback to legacy CORS_ALLOW_ORIGINS for backward compatibility.
cors_env = os.getenv("CORS_ORIGINS") or os.getenv("CORS_ALLOW_ORIGINS", "*")
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


@app.get("/", tags=["Health"])
def health_check():
    """Basic health check endpoint."""
    return {"message": "Healthy"}


# Register routers
app.include_router(auth_router)
app.include_router(ingest_router)
app.include_router(resources_router)
app.include_router(costs_router)
app.include_router(recommendations_router)
app.include_router(automation_router)
app.include_router(ws_router)
