from __future__ import annotations

from typing import List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.models import AutomationRule


# PUBLIC_INTERFACE
async def list_automation_rules(session: AsyncSession) -> List[AutomationRule]:
    """Return all automation rules (ordered by creation time descending)."""
    stmt = select(AutomationRule).order_by(AutomationRule.created_at.desc())
    res = await session.execute(stmt)
    return list(res.scalars().all())


# PUBLIC_INTERFACE
async def create_automation_rule(
    session: AsyncSession,
    *,
    organization_id: str,
    name: str,
    rule_type: str,
    is_enabled: bool = True,
    match_criteria: dict | None = None,
    action_type: str | None = None,
    cron_schedule: str | None = None,
) -> AutomationRule:
    """Create a new automation rule and persist it."""
    rule = AutomationRule(
        organization_id=organization_id,
        name=name,
        rule_type=rule_type,
        is_enabled=is_enabled,
        match_criteria=match_criteria,
        action_type=action_type,
        cron_schedule=cron_schedule,
    )
    session.add(rule)
    await session.flush()  # obtain primary key
    return rule
