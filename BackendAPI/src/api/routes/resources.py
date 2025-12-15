from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.db import get_session
from src.api.schemas import ResourceListResponse, ResourceOut
from src.api.security import get_current_user
from src.api.services.resources import list_resources as svc_list_resources

router = APIRouter(tags=["Resources"])


# PUBLIC_INTERFACE
@router.get(
    "/resources",
    summary="List resources",
    response_model=ResourceListResponse,
    responses={
        200: {"description": "Resource list"},
        401: {"description": "Unauthorized"},
    },
)
async def list_resources_endpoint(
    provider: Optional[str] = Query(None, description="Filter by provider (aws|azure|gcp)"),
    region: Optional[str] = Query(None, description="Filter by region (e.g., us-east-1)"),
    state: Optional[str] = Query(None, description="Filter by state"),
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    size: int = Query(20, ge=1, le=100, description="Page size"),
    _user=Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> ResourceListResponse:
    """List resources with optional filters and pagination.

    Parameters:
        provider: Optional cloud provider filter.
        region: Optional region filter.
        state: Optional state filter.
        page: 1-based page number (default 1).
        size: Page size (default 20).

    Returns:
        Paginated list of resources.
    """
    items, total = await svc_list_resources(
        session,
        provider=provider,
        region=region,
        state=state,
        page=page,
        size=size,
    )
    out_items = [
        ResourceOut(
            id=r.id,
            organization_id=r.organization_id,
            cloud_account_id=r.cloud_account_id,
            provider=r.provider,
            resource_id=r.resource_id,
            resource_type=r.resource_type,
            region=r.region,
            state=r.state,
            tags=r.tags or {},
            cost_daily=float(r.cost_daily) if r.cost_daily is not None else None,
            cost_monthly=float(r.cost_monthly) if r.cost_monthly is not None else None,
            created_at=r.created_at,
        )
        for r in items
    ]
    return ResourceListResponse(items=out_items, total=total, page=page, size=size)
