from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.db import get_session
from src.api.schemas import CostSummary
from src.api.security import get_current_user
from src.api.services.costs import get_cost_summary

router = APIRouter(tags=["Analytics"])


# PUBLIC_INTERFACE
@router.get(
    "/costs/summary",
    summary="Get cost summary",
    response_model=CostSummary,
    responses={
        200: {"description": "Cost summary"},
        401: {"description": "Unauthorized"},
    },
)
async def costs_summary_endpoint(
    period: str = Query("monthly", description="Aggregation period (daily|monthly)"),
    _user=Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> CostSummary:
    """Return total costs and breakdown by provider and region.

    Parameters:
        period: Aggregation period, defaults to 'monthly'.

    Returns:
        CostSummary containing totals and breakdowns.
    """
    summary = await get_cost_summary(session, period=period or "monthly")
    return CostSummary(**summary)
