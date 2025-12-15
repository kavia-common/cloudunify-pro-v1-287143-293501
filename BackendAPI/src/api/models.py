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
    Boolean,
    Text,
    ForeignKey,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column
from src.api.db import Base


def _uuid_str() -> str:
    return str(uuid.uuid4())


# --- Identity and Organizations ---


class User(Base):
    """ORM model representing a platform user."""

    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid_str)
    email: Mapped[str] = mapped_column(String, nullable=False, index=True, unique=True)
    username: Mapped[str | None] = mapped_column(String, nullable=True, index=True, unique=True)
    hashed_password: Mapped[str | None] = mapped_column(String, nullable=True)
    # global role for simplicity; per-org role lives in UserOrgMember
    role: Mapped[str] = mapped_column(String, default="user", nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), onupdate=func.current_timestamp(), nullable=False
    )


class Organization(Base):
    """ORM model representing an organization (tenant)."""

    __tablename__ = "organizations"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid_str)
    name: Mapped[str] = mapped_column(String, nullable=False)
    slug: Mapped[str] = mapped_column(String, nullable=False, index=True, unique=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), onupdate=func.current_timestamp(), nullable=False
    )


class UserOrgMember(Base):
    """Membership linking a User to an Organization with a per-organization role."""

    __tablename__ = "user_org_members"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid_str)

    user_id: Mapped[str] = mapped_column(String, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    organization_id: Mapped[str] = mapped_column(
        String, ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False
    )
    # owner|admin|member|viewer
    role: Mapped[str] = mapped_column(String, default="member", nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), onupdate=func.current_timestamp(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("user_id", "organization_id", name="uq_user_org_member"),
        Index("ix_user_org_members_user_org", "user_id", "organization_id", unique=True),
    )


class CloudAccount(Base):
    """Cloud account registered to an organization (e.g., an AWS account)."""

    __tablename__ = "cloud_accounts"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid_str)
    organization_id: Mapped[str] = mapped_column(
        String, ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True
    )
    provider: Mapped[str] = mapped_column(String, nullable=False)
    account_id: Mapped[str | None] = mapped_column(String, nullable=True)
    account_name: Mapped[str | None] = mapped_column(String, nullable=True)
    # Secure credential/config placeholder; actual encryption handled at service layer
    connection_config: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), onupdate=func.current_timestamp(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("organization_id", "provider", "account_id", name="uq_cloud_account_key"),
        Index("ix_cloud_accounts_org_provider", "organization_id", "provider"),
    )


# --- Inventory and Costs ---


class Resource(Base):
    """ORM model representing a cloud resource."""

    __tablename__ = "resources"

    # Using string UUIDs to keep compatibility across SQLite/Postgres without dialect-specific types
    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid_str)

    organization_id: Mapped[str] = mapped_column(
        String, ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True
    )
    cloud_account_id: Mapped[str] = mapped_column(
        String, ForeignKey("cloud_accounts.id", ondelete="SET NULL"), nullable=False, index=True
    )
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
        DateTime(timezone=True),
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("organization_id", "provider", "resource_id", name="uq_resource_key"),
        Index("ix_resources_org_provider_resid", "organization_id", "provider", "resource_id"),
    )


class Cost(Base):
    """ORM model representing cost/usage by service and date."""

    __tablename__ = "costs"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid_str)

    organization_id: Mapped[str] = mapped_column(
        String, ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True
    )
    cloud_account_id: Mapped[str] = mapped_column(
        String, ForeignKey("cloud_accounts.id", ondelete="SET NULL"), nullable=False, index=True
    )
    provider: Mapped[str] = mapped_column(String, nullable=False, index=True)
    service_name: Mapped[str] = mapped_column(String, nullable=False)
    region: Mapped[str] = mapped_column(String, nullable=False, index=True)
    cost_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)

    usage_quantity = Column(Numeric(18, 6), nullable=False)
    usage_unit: Mapped[str] = mapped_column(String, nullable=False)
    cost_amount = Column(Numeric(18, 6), nullable=False)
    currency: Mapped[str] = mapped_column(String, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

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


class CostBreakdown(Base):
    """Aggregated costs by dimension (e.g., service, region) for analytics."""

    __tablename__ = "cost_breakdowns"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid_str)

    organization_id: Mapped[str] = mapped_column(
        String, ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True
    )
    cloud_account_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("cloud_accounts.id", ondelete="SET NULL"), nullable=True, index=True
    )
    provider: Mapped[str] = mapped_column(String, nullable=False)
    dimension: Mapped[str] = mapped_column(String, nullable=False)  # e.g., 'service','region','tag:<k>'
    dimension_value: Mapped[str] = mapped_column(String, nullable=False)
    period: Mapped[str] = mapped_column(String, nullable=False, default="monthly")  # daily|monthly
    cost_date: Mapped[date] = mapped_column(Date, nullable=False)
    cost_amount = Column(Numeric(18, 6), nullable=False)
    currency: Mapped[str] = mapped_column(String, nullable=False, default="USD")

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), onupdate=func.current_timestamp(), nullable=False
    )

    __table_args__ = (
        Index("ix_cost_breakdowns_org_dim_date", "organization_id", "dimension", "dimension_value", "period", "cost_date"),
    )


# --- Recommendations, Automation, Activity ---


class Recommendation(Base):
    """Optimization recommendation produced by analysis."""

    __tablename__ = "recommendations"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid_str)
    organization_id: Mapped[str] = mapped_column(
        String, ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True
    )
    resource_id: Mapped[str | None] = mapped_column(String, ForeignKey("resources.id", ondelete="SET NULL"), nullable=True)
    recommendation_type: Mapped[str] = mapped_column(String, nullable=False)
    priority: Mapped[str] = mapped_column(String, nullable=False, default="medium")  # low|medium|high|critical
    potential_savings_monthly = Column(Numeric(18, 6), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    action_items: Mapped[dict | None] = mapped_column(JSON, nullable=True)  # array-like JSON

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), onupdate=func.current_timestamp(), nullable=False
    )

    __table_args__ = (Index("ix_recommendations_org", "organization_id"),)


class AutomationRule(Base):
    """Automation rule that can match resources and perform actions."""

    __tablename__ = "automation_rules"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid_str)
    organization_id: Mapped[str] = mapped_column(
        String, ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    rule_type: Mapped[str] = mapped_column(String, nullable=False)
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    match_criteria: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    action_type: Mapped[str | None] = mapped_column(String, nullable=True)
    cron_schedule: Mapped[str | None] = mapped_column(String, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), onupdate=func.current_timestamp(), nullable=False
    )

    __table_args__ = (Index("ix_automation_rules_org_name", "organization_id", "name", unique=True),)


class ActivityLog(Base):
    """Audit/activity log for user and system actions."""

    __tablename__ = "activity_logs"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid_str)
    organization_id: Mapped[str] = mapped_column(
        String, ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True
    )
    actor_user_id: Mapped[str | None] = mapped_column(String, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    action: Mapped[str] = mapped_column(String, nullable=False)
    resource_id: Mapped[str | None] = mapped_column(String, ForeignKey("resources.id", ondelete="SET NULL"), nullable=True)
    details: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), nullable=False
    )

    __table_args__ = (Index("ix_activity_logs_org_created_at", "organization_id", "created_at"),)
