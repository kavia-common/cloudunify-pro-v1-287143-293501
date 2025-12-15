from __future__ import annotations

import logging
import time

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import ValidationError

from src.api.db import get_session
from src.api.security import get_current_user
from src.api.schemas import (
    BulkIngestError,
    BulkIngestResponse,
    BulkItemsPayloadRaw,
    ResourceIngestRow,
    CostIngestRow,
)
from src.api.services.ingest import bulk_upsert_resources, bulk_upsert_costs
from src.api.services.activity_stream import activity_manager

logger = logging.getLogger("cloudunify.routes.ingest")

router = APIRouter(tags=["Ingestion"])


def _validate_items(payload: BulkItemsPayloadRaw, item_model) -> tuple[list, list[BulkIngestError]]:
    """Validate items one-by-one to support partial success."""
    valid_items: list = []
    errors: list[BulkIngestError] = []
    for idx, item in enumerate(payload.items or []):
        try:
            valid_items.append(item_model.model_validate(item))
        except ValidationError as ve:
            errors.append(BulkIngestError(index=idx, message=ve.errors()[0]["msg"]))
    return valid_items, errors


@router.post(
    "/resources/bulk",
    summary="Bulk upsert resources",
    response_model=BulkIngestResponse,
    responses={
        200: {"description": "Bulk upsert completed"},
        400: {"description": "All items failed validation"},
    },
)
async def resources_bulk_ingest(
    payload: BulkItemsPayloadRaw,
    session: AsyncSession = Depends(get_session),
    _user=Depends(get_current_user),
) -> BulkIngestResponse:
    """Ingest an array of resource rows, performing validated upserts.

    Request shape:
      { "items": ResourceIngestRow[] }

    Uniqueness key: (organization_id, provider, resource_id).
    - Inserts new rows; for conflicts updates fields except created_at (preserved).
    - Tags are replaced by incoming value.
    - updated_at is refreshed on update.

    Returns { inserted, updated, errors[] } and HTTP 400 if all items fail validation.
    """
    t0 = time.time()
    valid_rows, errors = _validate_items(payload, ResourceIngestRow)

    inserted = updated = 0
    if valid_rows:
        async with session.begin():
            ins, upd = await bulk_upsert_resources(session, valid_rows)
            inserted += ins
            updated += upd

    elapsed_ms = int((time.time() - t0) * 1000)
    logger.info("resources/bulk processed=%s valid=%s errors=%s time_ms=%s", len(payload.items), len(valid_rows), len(errors), elapsed_ms)

    if len(valid_rows) == 0:
        # All failed
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=BulkIngestResponse(inserted=0, updated=0, errors=errors).model_dump(),
        )

    # Broadcast concise events per-organization for connected WS clients
    try:
        org_counts: dict[str, int] = {}
        for r in valid_rows:
            oid = str(r.organization_id)
            org_counts[oid] = org_counts.get(oid, 0) + 1

        for org_id, processed_count in org_counts.items():
            event = activity_manager.make_event(
                event_type="resources.bulk",
                organization_id=org_id,
                payload={
                    "source": "resources/bulk",
                    "processed_count": processed_count,
                    "inserted_total": inserted,
                    "updated_total": updated,
                },
            )
            await activity_manager.broadcast_event(event)
    except Exception as ex:
        logger.warning("WS broadcast failed for resources/bulk: %s", ex)

    return BulkIngestResponse(inserted=inserted, updated=updated, errors=errors)


@router.post(
    "/costs/bulk",
    summary="Bulk upsert costs",
    response_model=BulkIngestResponse,
    responses={
        200: {"description": "Bulk upsert completed"},
        400: {"description": "All items failed validation"},
    },
)
async def costs_bulk_ingest(
    payload: BulkItemsPayloadRaw,
    session: AsyncSession = Depends(get_session),
    _user=Depends(get_current_user),
) -> BulkIngestResponse:
    """Ingest an array of cost rows, performing validated upserts.

    Request shape:
      { "items": CostIngestRow[] }

    Uniqueness key:
      (organization_id, cloud_account_id, provider, service_name, region, cost_date).

    On conflict uses REPLACE semantics (fields overwritten). updated_at refreshed.

    Returns { inserted, updated, errors[] } and HTTP 400 if all items fail validation.
    """
    t0 = time.time()
    valid_rows, errors = _validate_items(payload, CostIngestRow)

    inserted = updated = 0
    if valid_rows:
        async with session.begin():
            ins, upd = await bulk_upsert_costs(session, valid_rows)
            inserted += ins
            updated += upd

    elapsed_ms = int((time.time() - t0) * 1000)
    logger.info("costs/bulk processed=%s valid=%s errors=%s time_ms=%s", len(payload.items), len(valid_rows), len(errors), elapsed_ms)

    if len(valid_rows) == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=BulkIngestResponse(inserted=0, updated=0, errors=errors).model_dump(),
        )

    # Broadcast concise events per-organization for connected WS clients
    try:
        org_counts: dict[str, int] = {}
        for r in valid_rows:
            oid = str(r.organization_id)
            org_counts[oid] = org_counts.get(oid, 0) + 1

        for org_id, processed_count in org_counts.items():
            event = activity_manager.make_event(
                event_type="costs.bulk",
                organization_id=org_id,
                payload={
                    "source": "costs/bulk",
                    "processed_count": processed_count,
                    "inserted_total": inserted,
                    "updated_total": updated,
                },
            )
            await activity_manager.broadcast_event(event)
    except Exception as ex:
        logger.warning("WS broadcast failed for costs/bulk: %s", ex)

    return BulkIngestResponse(inserted=inserted, updated=updated, errors=errors)
