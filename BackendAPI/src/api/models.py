from __future__ import annotations

import uuid
from datetime import datetime, date
from sqlalchemy import (
    Column,
    String,
    DateTime,
    Date,
    UniqueConstraint,
    Index,
    Numeric,
    JSON,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column
from src.api.db import Base


def _uuid_str() -> str:
    return str(uuid.uuid4())


class Resource(Base):
    """ORM model representing a cloud resource."""

    __tablename__ = "resources"

    # Using string UUIDs to keep compatibility across SQLite/Postgres without dialect-specific types
    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid_str)

    organization_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    cloud_account_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    provider: Mapped[str] = mapped_column(String, nullable=False, index=True)
    resource_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    resource_type: Mapped[str] = mapped_column(String, nullable=False)
    region: Mapped[str] = mapped_column(String, nullable=False, index=True)
    state: Mapped[str] = mapped_column(String, nullable=False)
    tags: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    cost_daily = Column(Numeric(18, 6), nullable=True)
    cost_monthly = Column(Numeric(18, 6), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), onupdate=func.current_timestamp(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("organization_id", "provider", "resource_id", name="uq_resource_key"),
        Index("ix_resources_org_provider_resid", "organization_id", "provider", "resource_id"),
    )


class Cost(Base):
    """ORM model representing cost/usage by service and date."""

    __tablename__ = "costs"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid_str)

    organization_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    cloud_account_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    provider: Mapped[str] = mapped_column(String, nullable=False, index=True)
    service_name: Mapped[str] = mapped_column(String, nullable=False)
    region: Mapped[str] = mapped_column(String, nullable=False, index=True)
    cost_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)

    usage_quantity = Column(Numeric(18, 6), nullable=False)
    usage_unit: Mapped[str] = mapped_column(String, nullable=False)
    cost_amount = Column(Numeric(18, 6), nullable=False)
    currency: Mapped[str] = mapped_column(String, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            "cloud_account_id",
            "provider",
            "service_name",
            "region",
            "cost_date",
            name="uq_cost_key",
        ),
        Index(
            "ix_costs_org_acct_provider_service_region_date",
            "organization_id",
            "cloud_account_id",
            "provider",
            "service_name",
            "region",
            "cost_date",
        ),
    )
