"""Initial schema for CloudUnify Pro MVP

Revision ID: 7b3f7e5e3a1b
Revises: 
Create Date: 2025-12-15 08:30:00.000000
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "7b3f7e5e3a1b"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Users
    op.create_table(
        "users",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("email", sa.String(), nullable=False),
        sa.Column("username", sa.String(), nullable=True),
        sa.Column("hashed_password", sa.String(), nullable=True),
        sa.Column("role", sa.String(), nullable=False, server_default="user"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
    )
    op.create_index("ix_users_email", "users", ["email"], unique=True)
    op.create_index("ix_users_username", "users", ["username"], unique=True)

    # Organizations
    op.create_table(
        "organizations",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("slug", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
    )
    op.create_index("ix_organizations_slug", "organizations", ["slug"], unique=True)

    # UserOrgMember
    op.create_table(
        "user_org_members",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("user_id", sa.String(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("organization_id", sa.String(), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("role", sa.String(), nullable=False, server_default="member"),  # owner|admin|member|viewer
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("user_id", "organization_id", name="uq_user_org_member"),
    )
    op.create_index("ix_user_org_members_user_org", "user_org_members", ["user_id", "organization_id"], unique=True)

    # Cloud accounts
    op.create_table(
        "cloud_accounts",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("organization_id", sa.String(), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("provider", sa.String(), nullable=False),
        sa.Column("account_id", sa.String(), nullable=True),
        sa.Column("account_name", sa.String(), nullable=True),
        sa.Column("connection_config", sa.JSON(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("organization_id", "provider", "account_id", name="uq_cloud_account_key"),
    )
    op.create_index("ix_cloud_accounts_org_provider", "cloud_accounts", ["organization_id", "provider"])

    # Resources
    op.create_table(
        "resources",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("organization_id", sa.String(), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("cloud_account_id", sa.String(), sa.ForeignKey("cloud_accounts.id", ondelete="SET NULL"), nullable=False),
        sa.Column("provider", sa.String(), nullable=False),
        sa.Column("resource_id", sa.String(), nullable=False),
        sa.Column("resource_type", sa.String(), nullable=False),
        sa.Column("region", sa.String(), nullable=False),
        sa.Column("state", sa.String(), nullable=False),
        sa.Column("tags", sa.JSON(), nullable=True),
        sa.Column("cost_daily", sa.Numeric(18, 6), nullable=True),
        sa.Column("cost_monthly", sa.Numeric(18, 6), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("organization_id", "provider", "resource_id", name="uq_resource_key"),
    )
    op.create_index("ix_resources_org_provider_resid", "resources", ["organization_id", "provider", "resource_id"])

    # Raw Costs (per-day service cost)
    op.create_table(
        "costs",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("organization_id", sa.String(), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("cloud_account_id", sa.String(), sa.ForeignKey("cloud_accounts.id", ondelete="SET NULL"), nullable=False),
        sa.Column("provider", sa.String(), nullable=False),
        sa.Column("service_name", sa.String(), nullable=False),
        sa.Column("region", sa.String(), nullable=False),
        sa.Column("cost_date", sa.Date(), nullable=False),
        sa.Column("usage_quantity", sa.Numeric(18, 6), nullable=False),
        sa.Column("usage_unit", sa.String(), nullable=False),
        sa.Column("cost_amount", sa.Numeric(18, 6), nullable=False),
        sa.Column("currency", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint(
            "organization_id",
            "cloud_account_id",
            "provider",
            "service_name",
            "region",
            "cost_date",
            name="uq_cost_key",
        ),
    )
    op.create_index(
        "ix_costs_org_acct_provider_service_region_date",
        "costs",
        ["organization_id", "cloud_account_id", "provider", "service_name", "region", "cost_date"],
    )

    # Cost Breakdowns (aggregated)
    op.create_table(
        "cost_breakdowns",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("organization_id", sa.String(), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("cloud_account_id", sa.String(), sa.ForeignKey("cloud_accounts.id", ondelete="SET NULL"), nullable=True),
        sa.Column("provider", sa.String(), nullable=False),
        sa.Column("dimension", sa.String(), nullable=False),  # e.g., 'service','region','tag:<k>'
        sa.Column("dimension_value", sa.String(), nullable=False),
        sa.Column("period", sa.String(), nullable=False, server_default="monthly"),  # daily|monthly
        sa.Column("cost_date", sa.Date(), nullable=False),
        sa.Column("cost_amount", sa.Numeric(18, 6), nullable=False),
        sa.Column("currency", sa.String(), nullable=False, server_default="USD"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
    )
    op.create_index(
        "ix_cost_breakdowns_org_dim_date",
        "cost_breakdowns",
        ["organization_id", "dimension", "dimension_value", "period", "cost_date"],
    )

    # Recommendations
    op.create_table(
        "recommendations",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("organization_id", sa.String(), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("resource_id", sa.String(), sa.ForeignKey("resources.id", ondelete="SET NULL"), nullable=True),
        sa.Column("recommendation_type", sa.String(), nullable=False),
        sa.Column("priority", sa.String(), nullable=False, server_default="medium"),
        sa.Column("potential_savings_monthly", sa.Numeric(18, 6), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("action_items", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
    )
    op.create_index("ix_recommendations_org", "recommendations", ["organization_id"])

    # Automation Rules
    op.create_table(
        "automation_rules",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("organization_id", sa.String(), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("rule_type", sa.String(), nullable=False),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("match_criteria", sa.JSON(), nullable=True),
        sa.Column("action_type", sa.String(), nullable=True),
        sa.Column("cron_schedule", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
    )
    op.create_index("ix_automation_rules_org_name", "automation_rules", ["organization_id", "name"], unique=True)

    # Activity Logs
    op.create_table(
        "activity_logs",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("organization_id", sa.String(), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("actor_user_id", sa.String(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("action", sa.String(), nullable=False),
        sa.Column("resource_id", sa.String(), sa.ForeignKey("resources.id", ondelete="SET NULL"), nullable=True),
        sa.Column("details", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
    )
    op.create_index("ix_activity_logs_org_created_at", "activity_logs", ["organization_id", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_activity_logs_org_created_at", table_name="activity_logs")
    op.drop_table("activity_logs")

    op.drop_index("ix_automation_rules_org_name", table_name="automation_rules")
    op.drop_table("automation_rules")

    op.drop_index("ix_recommendations_org", table_name="recommendations")
    op.drop_table("recommendations")

    op.drop_index("ix_cost_breakdowns_org_dim_date", table_name="cost_breakdowns")
    op.drop_table("cost_breakdowns")

    op.drop_index("ix_costs_org_acct_provider_service_region_date", table_name="costs")
    op.drop_table("costs")

    op.drop_index("ix_resources_org_provider_resid", table_name="resources")
    op.drop_table("resources")

    op.drop_index("ix_cloud_accounts_org_provider", table_name="cloud_accounts")
    op.drop_table("cloud_accounts")

    op.drop_index("ix_user_org_members_user_org", table_name="user_org_members")
    op.drop_table("user_org_members")

    op.drop_index("ix_organizations_slug", table_name="organizations")
    op.drop_table("organizations")

    op.drop_index("ix_users_email", table_name="users")
    op.drop_index("ix_users_username", table_name="users")
    op.drop_table("users")
