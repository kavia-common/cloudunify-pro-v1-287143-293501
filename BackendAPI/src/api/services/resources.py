from __future__ import annotations

from typing import Optional, Tuple, List

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.models import Resource


# PUBLIC_INTERFACE
async def list_resources(
    session: AsyncSession,
    *,
    provider: Optional[str] = None,
    region: Optional[str] = None,
    state: Optional[str] = None,
    page: int = 1,
    size: int = 20,
) -> Tuple[List[Resource], int]:
    """List resources with optional filters and pagination.

    Parameters:
        session: Async SQLAlchemy session.
        provider: Optional provider filter ('aws'|'azure'|'gcp').
        region: Optional region filter (e.g., 'us-east-1').
        state: Optional state filter (e.g., 'running', 'stopped').
        page: 1-based page number.
        size: Page size (max 100).

    Returns:
        (items, total) where items is a list of Resource ORM objects and total is the total count.
    """
    page = 1 if page < 1 else page
    size = 1 if size < 1 else min(size, 100)
    offset = (page - 1) * size

    conditions = []
    if provider:
        conditions.append(Resource.provider == provider)
    if region:
        conditions.append(Resource.region == region)
    if state:
        conditions.append(Resource.state == state)

    base_stmt = select(Resource).where(*conditions).order_by(Resource.created_at.desc())
    count_stmt = select(func.count()).select_from(select(Resource.id).where(*conditions).subquery())

    total_res = await session.execute(count_stmt)
    total = int(total_res.scalar_one() or 0)

    res = await session.execute(base_stmt.limit(size).offset(offset))
    items = list(res.scalars().all())
    return items, total
