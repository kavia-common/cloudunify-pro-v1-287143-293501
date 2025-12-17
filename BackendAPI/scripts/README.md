# CloudUnify Pro - One-off Dataset Ingestion Utility

This folder contains a standalone ingestion script to load Excel datasets into the CloudUnify Pro backend database.

What it does:
- Scans an input directory for `.xlsx` files (default: repo_root/attachments)
- Parses resource files (AWS/Azure/GCP) and a recommendations file
- Validates and upserts rows into PostgreSQL (Neon) with idempotency
  - Resources uniqueness: (organization_id, provider, resource_id)
  - Recommendations uniqueness: primary key `id` populated from the `recommendation_id` in the file
- Enforces SSL and channel binding for Neon connections
- Prints per-file and overall summary of inserted/updated/skipped rows

Prerequisites:
- Python 3.11+ (3.12 recommended)
- Install dependencies for BackendAPI:
  pip install -r ../requirements.txt

Environment:
- DATABASE_URL (optional): postgresql://... (If not set, the script uses an internal fallback DSN. Replace it with your Neon DSN.)
  The script enforces `sslmode=require` and `channel_binding=require` in the connection parameters.

Usage examples:
- Basic, providing org and one cloud account for all providers:
  python scripts/ingest_datasets.py --org <ORG_UUID> --cloud-account-id <ACCOUNT_UUID>

- Provider-specific cloud accounts:
  python scripts/ingest_datasets.py --org <ORG_UUID> --account-aws <AWS_ACCOUNT_UUID> --account-azure <AZURE_ACCOUNT_UUID> --account-gcp <GCP_ACCOUNT_UUID>

- Custom input directory:
  python scripts/ingest_datasets.py --org <ORG_UUID> --cloud-account-id <ACCOUNT_UUID> --input-dir /path/to/excels

- Dry run (no writes):
  python scripts/ingest_datasets.py --org <ORG_UUID> --account-aws <AWS_ACCOUNT_UUID> --dry-run

Notes:
- If you omit `--org`, the script will attempt to auto-detect the single organization in the DB.
- If you omit a provider cloud account, the script will try to auto-detect a single active cloud account for the org+provider.
- The script expects the Excel headers to contain fields like:
  - Resources: resource_id, resource_type, region, state, cost_daily, cost_monthly, tags, launch_time (or created_at)
  - Recommendations: recommendation_id, resource_id, recommendation_type, priority, potential_savings_monthly, description, cloud_provider
- Recommendations will link to a resource if the (organization_id, provider, resource_id) match an existing resource; otherwise the association is left empty.

Security:
- Do not commit production database credentials in source control.
- Use the DATABASE_URL environment variable to configure the DB connection at runtime.
