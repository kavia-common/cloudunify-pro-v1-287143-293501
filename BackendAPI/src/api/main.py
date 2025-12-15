from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.db import init_db
from src.api.routes.ingest import router as ingest_router

openapi_tags = [
    {"name": "Ingestion", "description": "Bulk ingestion endpoints for resources and costs"},
    {"name": "Health", "description": "Health and diagnostics"},
]

app = FastAPI(
    title="CloudUnify Pro Backend API",
    version="1.0.0",
    description="Multi-cloud resource management API with bulk ingestion endpoints.",
    openapi_tags=openapi_tags,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # TODO tighten for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def on_startup():
    await init_db()


@app.get("/", tags=["Health"])
def health_check():
    """Basic health check endpoint."""
    return {"message": "Healthy"}


# Register routers
app.include_router(ingest_router)
