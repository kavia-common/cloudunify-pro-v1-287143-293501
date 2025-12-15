from __future__ import annotations

from typing import Optional, List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.models import Recommendation


# PUBLIC_INTERFACE
async def list_recommendations(
    session: AsyncSession,
    *,
    priority: Optional[str] = None,
    resource_id: Optional[str] = None,
    limit: int = 100,
) -> List[Recommendation]:
    """List recommendations with optional filtering by priority and resource_id."""
    limit = max(1, min(1000, limit))
    conditions = []
    if priority:
        conditions.append(Recommendation.priority == priority)
    if resource_id:
        conditions.append(Recommendation.resource_id == resource_id)
    stmt = select(Recommendation).where(*conditions).order_by(Recommendation.created_at.desc()).limit(limit)
    res = await session.execute(stmt)
    return list(res.scalars().all())
