# CloudUnify Pro - One-off Dataset Ingestion Utility

This folder contains a standalone ingestion script to load Excel datasets into the CloudUnify Pro backend database.

## What it does
- Scans an input directory for `.xlsx` **and** `.json` files (default: repo_root/attachments)
- Parses resource datasets (AWS/Azure/GCP) and recommendations datasets
- Ensures/creates lookup rows as needed:
  - `organizations` (defaults when auto-creating: **CloudUnify Demo** / **cloudunify-demo**)
  - `cloud_accounts` (defaults when auto-creating: **AWS Main**, **Azure Main**, **GCP Main**)
- Validates and upserts rows into PostgreSQL (Neon) with idempotency
  - Resources uniqueness: `(organization_id, provider, resource_id)`
  - Recommendations uniqueness: primary key `id` populated from `recommendation_id` (or stable UUIDv5 fallback if missing)
- Optionally upserts into `resource_costs_daily` **if that table exists** in the target DB
  - Uses internal `resources.id` as FK and a stable UUIDv5 for the `resource_costs_daily.id`
- Enforces SSL and channel binding for Neon connections
- Prints per-file and overall summary of inserted/updated/skipped rows (concise import report)
- Optionally writes a **structured JSON import report** via `--report-path`
- Supports ingesting only a subset of attachments via `--attachments-prefix` (e.g., `20251220_`)

## Prerequisites
- Python 3.11+ (3.12 recommended)
- Install dependencies for BackendAPI:
  ```bash
  pip install -r ../requirements.txt
  ```

## Database connection (precedence)
The script resolves the Postgres DSN in this order:
1. `--database-url`
2. `db_connection.txt` (if present; supports `psql postgresql://...` or bare `postgresql://...`)
3. `DATABASE_URL` environment variable
4. Fallback placeholder DSN (must be replaced or import will fail)

Neon requirements:
- The script enforces `sslmode=require` and `channel_binding=require` in the DSN.

## Usage examples (direct)
- Basic, providing org and one cloud account for all providers:
  ```bash
  python scripts/ingest_datasets.py --org <ORG_UUID> --cloud-account-id <ACCOUNT_UUID>
  ```

- Provider-specific cloud accounts:
  ```bash
  python scripts/ingest_datasets.py --org <ORG_UUID> --account-aws <AWS_ACCOUNT_UUID> --account-azure <AZURE_ACCOUNT_UUID> --account-gcp <GCP_ACCOUNT_UUID>
  ```

- Custom input directory:
  ```bash
  python scripts/ingest_datasets.py --org <ORG_UUID> --cloud-account-id <ACCOUNT_UUID> --input-dir /path/to/excels
  ```

- Dry run (no writes):
  ```bash
  python scripts/ingest_datasets.py --org <ORG_UUID> --cloud-account-id <ACCOUNT_UUID> --dry-run
  ```

## Usage examples (Makefile)
From `BackendAPI/`:
- Import:
  ```bash
  make ingest-excels ORG_ID=<ORG_UUID> CLOUD_ACCOUNT_ID=<ACCOUNT_UUID>
  ```

- Dry run:
  ```bash
  make ingest-excels-dry-run ORG_ID=<ORG_UUID> CLOUD_ACCOUNT_ID=<ACCOUNT_UUID>
  ```

## Notes
- If you omit `--org`, the script will attempt to auto-detect a single organization in the DB; if none exist, it can auto-create one (unless `--no-create-lookups`).
- If you omit a provider cloud account, the script will try to auto-detect a single active cloud account for the org+provider; if none exist, it can auto-create one (unless `--no-create-lookups`).
- Recommendations link to a resource if `(organization_id, provider, resource_id)` matches an existing resource; otherwise the association is left null.
- Security: do not commit production DB credentials in source control; prefer `DATABASE_URL` or `db_connection.txt` outside version control.
"""
