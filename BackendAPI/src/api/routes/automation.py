from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.db import get_session
from src.api.schemas import AutomationRuleIn, AutomationRuleOut
from src.api.security import get_current_user, require_roles, Role
from src.api.services.automation import list_automation_rules, create_automation_rule

router = APIRouter(tags=["Automation"])


# PUBLIC_INTERFACE
@router.get(
    "/automation-rules",
    summary="List automation rules",
    response_model=List[AutomationRuleOut],
    responses={200: {"description": "List of automation rules"}, 401: {"description": "Unauthorized"}},
)
async def list_rules_endpoint(
    _user=Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> list[AutomationRuleOut]:
    """Return all automation rules."""
    rules = await list_automation_rules(session)
    return [
        AutomationRuleOut(
            id=r.id,
            organization_id=r.organization_id,
            name=r.name,
            rule_type=r.rule_type,
            is_enabled=r.is_enabled,
            match_criteria=r.match_criteria,
            action_type=r.action_type,
            cron_schedule=r.cron_schedule,
            created_at=r.created_at,
        )
        for r in rules
    ]


# PUBLIC_INTERFACE
@router.post(
    "/automation-rules",
    summary="Create automation rule",
    status_code=status.HTTP_201_CREATED,
    response_model=AutomationRuleOut,
    responses={
        201: {"description": "Rule created"},
        400: {"description": "Validation error"},
        401: {"description": "Unauthorized"},
        403: {"description": "Forbidden"},
    },
)
async def create_rule_endpoint(
    payload: AutomationRuleIn,
    _user=Depends(require_roles(Role.admin)),  # Admin-only creation
    session: AsyncSession = Depends(get_session),
) -> AutomationRuleOut:
    """Create a new automation rule. Admin role required."""
    async with session.begin():
        rule = await create_automation_rule(
            session,
            organization_id=str(payload.organization_id),
            name=payload.name,
            rule_type=payload.rule_type,
            is_enabled=payload.is_enabled if payload.is_enabled is not None else True,
            match_criteria=payload.match_criteria,
            action_type=payload.action_type,
            cron_schedule=payload.cron_schedule,
        )
    return AutomationRuleOut(
        id=rule.id,
        organization_id=rule.organization_id,
        name=rule.name,
        rule_type=rule.rule_type,
        is_enabled=rule.is_enabled,
        match_criteria=rule.match_criteria,
        action_type=rule.action_type,
        cron_schedule=rule.cron_schedule,
        created_at=rule.created_at,
    )
