from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.db import get_session
from src.api.schemas import RecommendationOut
from src.api.security import get_current_user
from src.api.services.recommendations import list_recommendations as svc_list_recs

router = APIRouter(tags=["Recommendations"])


# PUBLIC_INTERFACE
@router.get(
    "/recommendations",
    summary="List recommendations",
    response_model=List[RecommendationOut],
    responses={
        200: {"description": "Recommendations list"},
        401: {"description": "Unauthorized"},
    },
)
async def list_recommendations_endpoint(
    priority: Optional[str] = Query(None, description="Filter by priority (low|medium|high|critical)"),
    resource_id: Optional[str] = Query(None, description="Filter by resource UUID"),
    _user=Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> list[RecommendationOut]:
    """List recommendations with optional filters for priority and resource association."""
    recs = await svc_list_recs(session, priority=priority, resource_id=resource_id, limit=500)
    return [
        RecommendationOut(
            id=r.id,
            organization_id=r.organization_id,
            resource_id=r.resource_id,
            recommendation_type=r.recommendation_type,
            priority=r.priority,
            potential_savings_monthly=float(r.potential_savings_monthly) if r.potential_savings_monthly else None,
            description=r.description,
            action_items=list(r.action_items) if isinstance(r.action_items, list) else r.action_items,
            created_at=r.created_at,
        )
        for r in recs
    ]
