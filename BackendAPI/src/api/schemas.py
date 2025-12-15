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


# ---------- Query/Response Schemas for Feature Endpoints ----------

class ResourceOut(BaseModel):
    """Resource representation returned by list endpoints."""

    id: str = Field(..., description="Internal resource UUID")
    organization_id: str = Field(..., description="Owning organization UUID")
    cloud_account_id: str = Field(..., description="Cloud account UUID")
    provider: ProviderEnum = Field(..., description="Cloud provider")
    resource_id: str = Field(..., description="Provider resource identifier")
    resource_type: str = Field(..., description="Resource type")
    region: str = Field(..., description="Cloud region")
    state: str = Field(..., description="Resource state")
    tags: Dict[str, str] = Field(default_factory=dict, description="Key-value tags")
    cost_daily: Optional[float] = Field(None, description="Approximate daily cost")
    cost_monthly: Optional[float] = Field(None, description="Approximate monthly cost")
    created_at: datetime = Field(..., description="Resource creation timestamp")


class ResourceListResponse(BaseModel):
    """Paginated resource list response."""

    items: List[ResourceOut] = Field(..., description="List of resources in the current page")
    total: int = Field(..., description="Total number of matching resources")
    page: int = Field(..., description="Current page (1-based)")
    size: int = Field(..., description="Page size")


class CostSummary(BaseModel):
    """Aggregated cost summary for a period."""

    total_cost: float = Field(..., description="Total cost across all dimensions")
    by_provider: Dict[str, float] = Field(default_factory=dict, description="Costs grouped by provider")
    by_region: Dict[str, float] = Field(default_factory=dict, description="Costs grouped by region")
    period: str = Field(..., description="Aggregation period (daily|monthly)")


class RecommendationOut(BaseModel):
    """Recommendation representation."""

    id: str = Field(..., description="Recommendation UUID")
    organization_id: str = Field(..., description="Owning organization UUID")
    resource_id: Optional[str] = Field(None, description="Associated resource UUID, if any")
    recommendation_type: str = Field(..., description="Type/category of recommendation")
    priority: str = Field(..., description="Priority: low|medium|high|critical")
    potential_savings_monthly: Optional[float] = Field(None, description="Estimated monthly savings")
    description: Optional[str] = Field(None, description="Human-readable description")
    action_items: Optional[List[str]] = Field(None, description="Action items for remediation")
    created_at: datetime = Field(..., description="Creation timestamp")


class AutomationRuleIn(BaseModel):
    """Payload to create an automation rule."""

    organization_id: UUID4 = Field(..., description="Owning organization UUID")
    name: str = Field(..., min_length=1, description="Rule name")
    rule_type: str = Field(..., min_length=1, description="Rule type/category")
    is_enabled: Optional[bool] = Field(True, description="Whether the rule is enabled")
    match_criteria: Optional[dict] = Field(None, description="Match criteria object")
    action_type: Optional[str] = Field(None, description="Action type to perform")
    cron_schedule: Optional[str] = Field(None, description="Cron schedule expression")


class AutomationRuleOut(BaseModel):
    """Automation rule returned by API."""

    id: str = Field(..., description="Rule UUID")
    organization_id: str = Field(..., description="Owning organization UUID")
    name: str = Field(..., description="Rule name")
    rule_type: str = Field(..., description="Rule type/category")
    is_enabled: bool = Field(..., description="Whether the rule is enabled")
    match_criteria: Optional[dict] = Field(None, description="Match criteria object")
    action_type: Optional[str] = Field(None, description="Action type")
    cron_schedule: Optional[str] = Field(None, description="Cron schedule")
    created_at: datetime = Field(..., description="Creation timestamp")
