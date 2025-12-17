# Dataset Ingestion Plan for CloudUnify Pro-v1 (Neon PostgreSQL)

## Objectives
This document describes how to ingest the five provided Excel datasets into the CloudUnify Pro-v1 backend database running on Neon PostgreSQL. It covers the mapping of each input file to the target tables, the schema assumptions derived from API models and migrations, the ingestion workflow (validation, normalization, idempotent upsert, transactions, error logging, performance considerations), environment and security requirements (including Neon SSL and channel binding), exact run commands, detailed column mappings, post-ingestion verification queries, rollback/re-run guidance, and expected outputs/logs.

The ingestion is performed using the one-off script at BackendAPI/scripts/ingest_datasets.py. No database schema changes are performed by this script.

## Data Sources and Target Tables
The ingestion script auto-discovers .xlsx files from an input folder (defaults to repo_root/attachments) and classifies any file with “recommendation” in its name as recommendations; all others are treated as resource datasets.

Target table mapping:
- attachments/20251217_144808_mock_aws_resources_1.xlsx → resources (provider inferred: aws)
- attachments/20251217_144809_mock_aws_resources.xlsx → resources (provider inferred: aws)
- attachments/20251217_144810_mock_azure_resources.xlsx → resources (provider inferred: azure)
- attachments/20251217_144811_mock_gcp_resources.xlsx → resources (provider inferred: gcp)
- attachments/20251217_144812_mock_recommendations.xlsx → recommendations

Uniqueness and idempotency:
- resources: unique key (organization_id, provider, resource_id)
- recommendations: primary key id (populated from recommendation_id in the file)

Note: Costs are not part of these Excel datasets and therefore not handled by this script. Costs ingestion is supported by the REST API /costs/bulk endpoint when needed.

## Schema Assumptions (from API Models and Migrations)
The following assumptions come from src/api/models.py and alembic/versions/7b3f7e5e3a1b_initial.py.

### Resources table (resources)
- Columns: id (PK, UUID-as-string), organization_id, cloud_account_id, provider, resource_id, resource_type, region, state, tags (JSON), cost_daily (numeric), cost_monthly (numeric), created_at (timestamptz, default now), updated_at (timestamptz)
- Uniqueness: (organization_id, provider, resource_id)
- On conflict (upsert): all updatable fields except created_at are replaced; updated_at set to current timestamp
- Notes: cloud_account_id must exist; tags is JSON map

### Recommendations table (recommendations)
- Columns: id (PK), organization_id, resource_id (nullable FK to resources.id), recommendation_type, priority (low|medium|high|critical), potential_savings_monthly (numeric, nullable), description (text, nullable), action_items (JSON array-like), created_at, updated_at
- Uniqueness: id (string; set from recommendation_id in the input)
- Resource link: if (organization_id, provider, external resource_id) resolves to an existing resource, recommendations.resource_id is set to that internal resource UUID; else left null

### Costs table (costs) [context only]
- Not used by the Excel script; included for completeness:
- Unique key: (organization_id, cloud_account_id, provider, service_name, region, cost_date)
- Replace semantics on conflict for API-based ingestion

## Ingestion Workflow
### 1) Discovery and classification
- The script scans the input directory for .xlsx files (default: repo_root/attachments).
- Files with “recommendation” in the name are classified as recommendations; others are resources.

### 2) Validation
- Resources: required fields per row: resource_id (or id/name), resource_type, region, state. Missing any of these results in the row being skipped (logged at debug level).
- Recommendations: required incoming recommendation_id (or id). If missing, the row is skipped.

### 3) Parsing and normalization
- Header normalization: headers are lowercased and spaces replaced with underscores.
- Provider normalization: aws/azure/gcp inferred from filename; if unavailable, attempts to parse a “provider” or “cloud_provider” column and normalize (e.g., “AWS”, “Amazon” → “aws”).
- Tags parsing: strings like "Environment:production,Team:backend" are converted into a JSON object { "Environment": "production", "Team": "backend" }.
- Numbers: cost_daily, cost_monthly, potential_savings_monthly parsed to decimals; blanks or invalid values become NULL.
- Datetime: launch_time or created_at parsed as ISO-8601; a trailing "Z" is treated as UTC; if none is provided for a resource, created_at defaults to now (UTC).
- Priority normalization: converted to one of low|medium|high|critical; defaults to medium.
- Recommendation action_items: if “action_items” is a delimited string, it is split on commas/semicolons into an array; if absent but “impact” exists, action_items becomes [impact].

### 4) Idempotent upsert strategy
- Resources: INSERT with ON CONFLICT (organization_id, provider, resource_id) DO UPDATE, replacing fields except created_at, refreshing updated_at. Inserted vs updated is determined via RETURNING (xmax = 0) AS inserted.
- Recommendations: INSERT with ON CONFLICT (id) DO UPDATE, replacing all mutable columns and refreshing updated_at. The incoming recommendation_id is used as the primary key id.

### 5) Transaction handling
- A single psycopg connection is opened with autocommit=False.
- Each file is processed in a transaction and committed at the end; errors during a file’s ingestion prevent commit for that file only.
- Dry run mode (--dry-run) validates and simulates inserted/updated counts without writing.

### 6) Error logging
- Logging to stdout at INFO level summarizing inserted/updated/skipped per file, plus a final ingestion summary.
- Skipped rows include debug logs with row index and reason (missing required fields).

### 7) Performance considerations
- The script performs row-by-row upserts. This is adequate for the provided datasets and small batch runs.
- For very large files, consider batching or COPY-based loaders in future iterations. Current scope does not change schema or code.

## Environment and Security
- Database URL: The script uses --database-url CLI, or the DATABASE_URL environment variable; otherwise it falls back to an internal DSN placeholder.
- Neon security: The script ensures sslmode=require and channel_binding=require are set in the DSN; it also normalizes sqlalchemy-style schemes (postgresql+asyncpg) to psycopg’s postgresql://.
- Secrets: Do not commit credentials to version control. Provide DATABASE_URL at runtime (e.g., via environment variables or secrets manager).
- Schema: No schema changes are performed by this script. Ensure migrations have been applied (alembic upgrade head) before ingestion.

## How to Run
### Prerequisites
- Python 3.11+ (3.12 recommended)
- Install BackendAPI dependencies:
```
cd cloudunify-pro-v1-287143-293501/BackendAPI
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```
- Database connectivity:
  - Set DATABASE_URL to your Neon DSN with sslmode=require and channel_binding=require, for example:
```
export DATABASE_URL="postgresql://<USER>:<PASSWORD>@<HOST>:<PORT>/<DBNAME>?sslmode=require&channel_binding=require"
```
  - Or pass via --database-url.

- Organization and cloud accounts:
  - Provide --org <ORG_UUID>.
  - Provide --cloud-account-id to use one account for all providers, or provider-specific overrides: --account-aws, --account-azure, --account-gcp.
  - If not provided, the script will attempt to auto-detect when exactly one active account exists per provider for the organization.

### Commands
- Basic (default input directory auto-detected as repo_root/attachments):
```
cd cloudunify-pro-v1-287143-293501/BackendAPI
python scripts/ingest_datasets.py --org <ORG_UUID> --cloud-account-id <ACCOUNT_UUID>
```

- Provider-specific accounts:
```
python scripts/ingest_datasets.py --org <ORG_UUID> --account-aws <AWS_ACCOUNT_UUID> --account-azure <AZURE_ACCOUNT_UUID> --account-gcp <GCP_ACCOUNT_UUID>
```

- Custom input directory:
```
python scripts/ingest_datasets.py --org <ORG_UUID> --cloud-account-id <ACCOUNT_UUID> --input-dir /absolute/path/to/attachments
```

- Dry run (validate and simulate without writes):
```
python scripts/ingest_datasets.py --org <ORG_UUID> --cloud-account-id <ACCOUNT_UUID> --dry-run
```

## Column-to-Column Mapping and Key Constraints
This section details the exact mapping used by the script.

### A) Resource datasets (AWS/Azure/GCP) → resources
- Files: 
  - attachments/20251217_144808_mock_aws_resources_1.xlsx
  - attachments/20251217_144809_mock_aws_resources.xlsx
  - attachments/20251217_144810_mock_azure_resources.xlsx
  - attachments/20251217_144811_mock_gcp_resources.xlsx

- Provider: inferred from filename (aws|azure|gcp). If not inferable, the script attempts to normalize an in-file provider value.

- Mapping table:
| Excel column         | DB column                   | Transform/Notes |
|----------------------|-----------------------------|-----------------|
| resource_id (or id/name) | resources.resource_id     | Required; part of uniqueness key |
| resource_type        | resources.resource_type      | Required |
| region               | resources.region             | Required |
| state                | resources.state              | Required |
| tags                 | resources.tags (JSON)        | Parse "k:v,k2:v2" → {"k":"v","k2":"v2"}; optional |
| cost_daily           | resources.cost_daily (NUM)   | Decimal; optional |
| cost_monthly         | resources.cost_monthly (NUM) | Decimal; optional |
| launch_time or created_at | resources.created_at     | ISO-8601 parse; default now(UTC) if missing |
| (derived) provider   | resources.provider           | From filename or normalized column |
| (CLI/auto-detected) cloud_account_id | resources.cloud_account_id | Must be supplied/detected per provider |
| Unused: instance_type, cpu_utilization, memory_utilization | — | Ignored in this ingestion plan |

- Key constraint for idempotency:
  - Unique key: (organization_id, provider, resource_id)
  - On conflict: cloud_account_id, resource_type, region, state, tags, cost_daily, cost_monthly replaced; created_at preserved; updated_at refreshed

### B) Recommendations dataset → recommendations
- File: attachments/20251217_144812_mock_recommendations.xlsx

- Mapping table:
| Excel column                 | DB column                           | Transform/Notes |
|------------------------------|-------------------------------------|-----------------|
| recommendation_id (or id)    | recommendations.id                  | Required; primary key for idempotency |
| organization_id (implicit)   | recommendations.organization_id     | From CLI/auto-detection |
| resource_id                  | recommendations.resource_id         | Resolved by lookup of internal resource UUID via (organization_id, provider, external resource_id); provider from cloud_provider or filename; null if not found |
| cloud_provider (or provider) | —                                   | Used only for provider normalization in resource lookup; not stored |
| recommendation_type (or type)| recommendations.recommendation_type | Default "General" if missing |
| priority                     | recommendations.priority            | Normalized to low|medium|high|critical; default medium |
| potential_savings_monthly    | recommendations.potential_savings_monthly | Decimal; optional |
| description                  | recommendations.description         | Optional |
| action_items                 | recommendations.action_items (JSON) | If string, split by comma/semicolon; if absent but impact present, action_items becomes [impact] |
| impact                       | recommendations.action_items        | Used only if action_items missing |
| (derived) created_at         | recommendations.created_at          | Set to now(UTC) at ingestion |

- Key constraint for idempotency:
  - Unique on id
  - On conflict: organization_id, resource_id, recommendation_type, priority, potential_savings_monthly, description, action_items replaced; updated_at refreshed

## Post-Ingestion Verification Queries
Run these as read-only checks after ingestion. Replace placeholders with real UUIDs/values.

- Count resources by provider for the organization:
```
SELECT provider, COUNT(*) 
FROM resources 
WHERE organization_id = '<ORG_UUID>' 
GROUP BY provider 
ORDER BY provider;
```

- Validate uniqueness (should return zero):
```
SELECT organization_id, provider, resource_id, COUNT(*)
FROM resources
GROUP BY organization_id, provider, resource_id
HAVING COUNT(*) > 1;
```

- Count recommendations for the organization:
```
SELECT COUNT(*) 
FROM recommendations 
WHERE organization_id = '<ORG_UUID>';
```

- How many recommendations are linked to an internal resource:
```
SELECT 
  SUM(CASE WHEN resource_id IS NOT NULL THEN 1 ELSE 0 END) AS linked,
  SUM(CASE WHEN resource_id IS NULL THEN 1 ELSE 0 END) AS unlinked
FROM recommendations
WHERE organization_id = '<ORG_UUID>';
```

- Spot check a resource from the AWS files:
```
SELECT id, organization_id, provider, resource_id, resource_type, region, state, cost_daily, cost_monthly, created_at
FROM resources
WHERE organization_id = '<ORG_UUID>' AND provider = 'aws' AND resource_id = 'i-0123456789abcdef0';
```

## Rollback and Re-Run Guidance
- Safe re-run: The workflow is idempotent. Re-running the script with the same files will upsert rows and not create duplicates (thanks to unique keys).
- Dry run first: Use --dry-run to validate counts and simulate inserted/updated before writing.
- Targeted rollback (manual):
  - Resources inserted or updated by a specific dataset can be removed using the known natural keys. For example, if the AWS dataset should be rolled back:
```
-- Delete a specific resource by natural key
DELETE FROM resources 
WHERE organization_id = '<ORG_UUID>' AND provider = 'aws' AND resource_id = 'i-0123456789abcdef0';
```
  - For multiple resources, generate a list from the Excel file and use an IN (...) clause.
  - Recommendations can be rolled back by their primary keys:
```
DELETE FROM recommendations 
WHERE organization_id = '<ORG_UUID>' AND id IN ('rec-001','rec-002','rec-003');
```
- Point-in-time restore: For broad rollbacks, prefer Neon’s PITR/branching features rather than manual deletes.
- No schema changes: Since no DDL is performed, rollback focuses solely on targeted DML deletes.

## Expected Outputs and Logs
The script logs to stdout at INFO level. Examples:

- Per-file results:
```
INFO: Resources file 20251217_144809_mock_aws_resources.xlsx: inserted=3 updated=0 skipped=0
INFO: Recommendations file 20251217_144812_mock_recommendations.xlsx: inserted=4 updated=1 skipped=0
```

- Final summary:
```
INFO: ==== Ingestion Summary ====
INFO: Resources: inserted=5 updated=2 skipped=1
INFO: Recommendations: inserted=7 updated=3 skipped=0
```

- Connection and auto-detection messages:
```
INFO: Connecting to database...
INFO: Auto-detected single organization: aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
INFO: Auto-detected cloud account for aws: bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb
```

## Appendix: CLI Examples
- Minimal (single cloud account for all providers):
```
python scripts/ingest_datasets.py \
  --org aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa \
  --cloud-account-id bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb
```

- Provider-specific cloud accounts:
```
python scripts/ingest_datasets.py \
  --org aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa \
  --account-aws 11111111-1111-1111-1111-111111111111 \
  --account-azure 22222222-2222-2222-2222-222222222222 \
  --account-gcp 33333333-3333-3333-3333-333333333333
```

- Custom input directory and dry run:
```
python scripts/ingest_datasets.py \
  --org aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa \
  --cloud-account-id bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb \
  --input-dir /absolute/path/to/attachments \
  --dry-run
```

- With DATABASE_URL passed inline (Neon):
```
DATABASE_URL="postgresql://user:pass@host:5432/db?sslmode=require&channel_binding=require" \
python scripts/ingest_datasets.py \
  --org aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa \
  --cloud-account-id bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb
```

Notes:
- Make sure migrations are applied before ingestion (e.g., alembic upgrade head via the application init_db flow or CI).
- Replace the fallback DSN constant in scripts/ingest_datasets.py with your actual Neon DSN if you do not provide DATABASE_URL at runtime.
