from __future__ import annotations

from decimal import Decimal
from typing import Dict

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.models import Cost


def _to_float(v) -> float:
    if v is None:
        return 0.0
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except Exception:
        return 0.0


# PUBLIC_INTERFACE
async def get_cost_summary(session: AsyncSession, *, period: str = "monthly") -> dict:
    """Compute cost summary and breakdowns.

    Currently aggregates across all available Cost rows. The 'period' parameter is returned
    as-is for client context. Future iterations can limit by date range based on the period.

    Returns:
        dict with keys: total_cost, by_provider, by_region, period
    """
    # Total
    res_total = await session.execute(select(func.sum(Cost.cost_amount)))
    total_cost = _to_float(res_total.scalar_one())

    # By provider
    res_provider = await session.execute(
        select(Cost.provider, func.sum(Cost.cost_amount)).group_by(Cost.provider)
    )
    by_provider: Dict[str, float] = {row[0]: _to_float(row[1]) for row in res_provider.all()}

    # By region
    res_region = await session.execute(
        select(Cost.region, func.sum(Cost.cost_amount)).group_by(Cost.region)
    )
    by_region: Dict[str, float] = {row[0]: _to_float(row[1]) for row in res_region.all()}

    return {
        "total_cost": _to_float(total_cost),
        "by_provider": by_provider,
        "by_region": by_region,
        "period": period or "monthly",
    }
