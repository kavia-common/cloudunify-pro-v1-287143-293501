from __future__ import annotations

from datetime import datetime, date
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, UUID4, field_validator


class ProviderEnum(str, Enum):
    """Supported cloud providers."""

    aws = "aws"
    azure = "azure"
    gcp = "gcp"


class ResourceIngestRow(BaseModel):
    """Row for bulk resource ingestion."""

    organization_id: UUID4 = Field(..., description="Owning organization UUID")
    cloud_account_id: UUID4 = Field(..., description="Cloud account UUID")
    provider: ProviderEnum = Field(..., description="Cloud provider")
    resource_id: str = Field(..., min_length=1, description="Provider resource identifier (e.g., EC2 ID)")
    resource_type: str = Field(..., min_length=1, description="Type of resource (e.g., ec2.instance)")
    region: str = Field(..., min_length=1, description="Cloud region")
    state: str = Field(..., min_length=1, description="Resource state")
    tags: Dict[str, str] = Field(default_factory=dict, description="Key-value tags")
    cost_daily: Optional[float] = Field(None, ge=0, description="Approximate daily cost")
    cost_monthly: Optional[float] = Field(None, ge=0, description="Approximate monthly cost")
    created_at: Optional[datetime] = Field(
        None, description="Resource creation timestamp; server defaults to now() if omitted"
    )


class CostIngestRow(BaseModel):
    """Row for bulk costs ingestion."""

    organization_id: UUID4 = Field(..., description="Owning organization UUID")
    cloud_account_id: UUID4 = Field(..., description="Cloud account UUID")
    provider: ProviderEnum = Field(..., description="Cloud provider")
    service_name: str = Field(..., min_length=1, description="Cloud service name (e.g., AmazonEC2)")
    region: str = Field(..., min_length=1, description="Cloud region")
    cost_date: date = Field(..., description="Cost date (YYYY-MM-DD)")

    usage_quantity: float = Field(..., ge=0, description="Usage quantity")
    usage_unit: str = Field(..., min_length=1, description="Usage unit (e.g., Hours, GB)")
    cost_amount: float = Field(..., ge=0, description="Cost amount")
    currency: str = Field(..., min_length=1, description="Currency code (e.g., USD)")

    @field_validator("currency")
    @classmethod
    def _upper_currency(cls, v: str) -> str:
        return v.upper()


class BulkItemsPayloadRaw(BaseModel):
    """Raw payload wrapper that allows per-item validation inside route handlers."""

    items: List[dict] = Field(..., description="Array of ingestion rows (validated item-by-item)")


class BulkIngestError(BaseModel):
    """Error information for a single invalid row."""

    index: int = Field(..., description="Index of the item in the request")
    message: str = Field(..., description="Validation error message")


class BulkIngestResponse(BaseModel):
    """Response summarizing inserted/updated rows and any validation errors."""

    inserted: int = Field(..., ge=0, description="Number of rows inserted")
    updated: int = Field(..., ge=0, description="Number of rows updated")
    errors: List[BulkIngestError] = Field(default_factory=list, description="List of per-row errors")
