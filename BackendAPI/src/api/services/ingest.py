from __future__ import annotations

import logging
from datetime import datetime, date
from typing import Iterable, List, Sequence, Tuple

from sqlalchemy import select, func, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.models import Resource, Cost
from src.api.schemas import ResourceIngestRow, CostIngestRow

logger = logging.getLogger("cloudunify.services.ingest")


def _dialect_insert(session: AsyncSession, model):
    """Return the appropriate dialect-specific insert() to support ON CONFLICT."""
    bind = session.get_bind()
    dialect = getattr(bind, "dialect", None)
    name = getattr(dialect, "name", "")
    if name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        return pg_insert(model)
    if name == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        return sqlite_insert(model)
    # Fallback (may not support on_conflict_do_update)
    from sqlalchemy import insert as generic_insert

    return generic_insert(model)


def _chunked(seq: Sequence, size: int) -> Iterable[Sequence]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _resource_key_tuple(r: ResourceIngestRow) -> Tuple[str, str, str]:
    return (str(r.organization_id), r.provider.value, r.resource_id)


def _cost_key_tuple(c: CostIngestRow) -> Tuple[str, str, str, str, str, date]:
    return (
        str(c.organization_id),
        str(c.cloud_account_id),
        c.provider.value,
        c.service_name,
        c.region,
        c.cost_date,
    )


async def _existing_resource_keys(session: AsyncSession, keys: List[Tuple[str, str, str]]) -> set[Tuple[str, str, str]]:
    if not keys:
        return set()
    existing: set[Tuple[str, str, str]] = set()
    for batch in _chunked(keys, 1000):
        stmt = (
            select(Resource.organization_id, Resource.provider, Resource.resource_id)
            .where(tuple_(Resource.organization_id, Resource.provider, Resource.resource_id).in_(batch))
        )
        res = await session.execute(stmt)
        existing.update(set(res.all()))
    return existing


async def _existing_cost_keys(
    session: AsyncSession, keys: List[Tuple[str, str, str, str, str, date]]
) -> set[Tuple[str, str, str, str, str, date]]:
    if not keys:
        return set()
    existing: set[Tuple[str, str, str, str, str, date]] = set()
    for batch in _chunked(keys, 1000):
        stmt = (
            select(
                Cost.organization_id,
                Cost.cloud_account_id,
                Cost.provider,
                Cost.service_name,
                Cost.region,
                Cost.cost_date,
            )
            .where(
                tuple_(
                    Cost.organization_id,
                    Cost.cloud_account_id,
                    Cost.provider,
                    Cost.service_name,
                    Cost.region,
                    Cost.cost_date,
                ).in_(batch)
            )
        )
        res = await session.execute(stmt)
        existing.update(set(res.all()))
    return existing


# PUBLIC_INTERFACE
async def bulk_upsert_resources(session: AsyncSession, rows: List[ResourceIngestRow]) -> tuple[int, int]:
    """Bulk upsert Resource rows.

    Returns:
        (inserted_count, updated_count)
    """
    if not rows:
        return (0, 0)

    keys = [_resource_key_tuple(r) for r in rows]
    existing = await _existing_resource_keys(session, keys)

    # Build payload dictionaries
    values: List[dict] = []
    for r in rows:
        values.append(
            {
                "organization_id": str(r.organization_id),
                "cloud_account_id": str(r.cloud_account_id),
                "provider": r.provider.value,
                "resource_id": r.resource_id,
                "resource_type": r.resource_type,
                "region": r.region,
                "state": r.state,
                "tags": r.tags or {},
                "cost_daily": r.cost_daily,
                "cost_monthly": r.cost_monthly,
                # Preserve provided created_at for new inserts, do not update on conflict
                "created_at": r.created_at or datetime.utcnow(),
                # updated_at will be set on conflict to now()
            }
        )

    stmt = _dialect_insert(session, Resource).values(values)
    # Update everything except the immutable created_at and natural key columns
    stmt = stmt.on_conflict_do_update(
        index_elements=[Resource.organization_id, Resource.provider, Resource.resource_id],
        set_={
            "cloud_account_id": stmt.excluded.cloud_account_id,
            "resource_type": stmt.excluded.resource_type,
            "region": stmt.excluded.region,
            "state": stmt.excluded.state,
            "tags": stmt.excluded.tags,
            "cost_daily": stmt.excluded.cost_daily,
            "cost_monthly": stmt.excluded.cost_monthly,
            "updated_at": func.current_timestamp(),
        },
    )

    await session.execute(stmt)

    inserted = len(rows) - len(existing)
    updated = len(existing)
    logger.info("Resources upsert complete: inserted=%s updated=%s", inserted, updated)
    return inserted, updated


# PUBLIC_INTERFACE
async def bulk_upsert_costs(session: AsyncSession, rows: List[CostIngestRow]) -> tuple[int, int]:
    """Bulk upsert Cost rows (REPLACE semantics on conflict).

    Returns:
        (inserted_count, updated_count)
    """
    if not rows:
        return (0, 0)

    keys = [_cost_key_tuple(c) for c in rows]
    existing = await _existing_cost_keys(session, keys)

    values: List[dict] = []
    for c in rows:
        values.append(
            {
                "organization_id": str(c.organization_id),
                "cloud_account_id": str(c.cloud_account_id),
                "provider": c.provider.value,
                "service_name": c.service_name,
                "region": c.region,
                "cost_date": c.cost_date,
                "usage_quantity": c.usage_quantity,
                "usage_unit": c.usage_unit,
                "cost_amount": c.cost_amount,
                "currency": c.currency,
                # Keep created_at from existing row, otherwise default now() on insert
                "created_at": datetime.utcnow(),
            }
        )

    stmt = _dialect_insert(session, Cost).values(values)
    # Replace (not accumulate) on conflict, maintain created_at
    stmt = stmt.on_conflict_do_update(
        index_elements=[
            Cost.organization_id,
            Cost.cloud_account_id,
            Cost.provider,
            Cost.service_name,
            Cost.region,
            Cost.cost_date,
        ],
        set_={
            "usage_quantity": stmt.excluded.usage_quantity,
            "usage_unit": stmt.excluded.usage_unit,
            "cost_amount": stmt.excluded.cost_amount,
            "currency": stmt.excluded.currency,
            "updated_at": func.current_timestamp(),
        },
    )

    await session.execute(stmt)

    inserted = len(rows) - len(existing)
    updated = len(existing)
    logger.info("Costs upsert complete: inserted=%s updated=%s", inserted, updated)
    return inserted, updated
