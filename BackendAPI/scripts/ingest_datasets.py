#!/usr/bin/env python3
"""
CloudUnify Pro - One-off dataset ingestion utility

This script ingests attached Excel datasets (AWS, Azure, GCP resources, and recommendations)
into the Postgres database used by CloudUnify Pro.

Highlights:
- Detects and parses all .xlsx files in the provided input directory (default: repo_root/attachments)
- Maps resource columns to 'resources' and recommendation columns to 'recommendations'
- Performs idempotent upserts:
    - resources keyed by (organization_id, provider, resource_id)
    - recommendations keyed by 'id' using the provided 'recommendation_id'
- Validates required fields and coerces data types (dates, numbers, enums)
- Requires SSL and channel binding in DB connection (Neon compatible)
- Logs concise summary of inserted/updated/skipped per file and per table
- Run locally:
    python scripts/ingest_datasets.py --org <ORG_UUID> --account-aws <ACCOUNT_UUID> --account-azure <ACCOUNT_UUID> --account-gcp <ACCOUNT_UUID>

Environment:
- DATABASE_URL (optional): If set, used verbatim (sslmode/channel_binding enforced/added if missing).
  If not set, the script uses a fallback DSN compiled in the code (replace with the provided Neon DSN).

Basic schema assumptions (do NOT alter DB schema):
- resources(unique: organization_id, provider, resource_id)
- recommendations(primary key 'id' is string; we use incoming recommendation_id as that id)

Note:
- This script is a data-load utility and does not require the preview system.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

# Dependencies: psycopg (v3), openpyxl, python-dotenv (already in project requirements)
try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Json
except Exception:  # pragma: no cover
    print("ERROR: psycopg is required to run this script. Please install dependencies.", file=sys.stderr)
    raise

try:
    from openpyxl import load_workbook
except Exception:  # pragma: no cover
    print("ERROR: openpyxl is required to parse Excel files. Please install dependencies.", file=sys.stderr)
    raise

try:
    from dotenv import load_dotenv  # provided by project requirements
except Exception:
    def load_dotenv(*args: Any, **kwargs: Any) -> None:
        return


# ---------- Logging ----------

logger = logging.getLogger("cloudunify.scripts.ingest")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)


# ---------- Constants and helpers ----------

# Replace this with the provided Neon connection string if DATABASE_URL is not set.
# Do not commit secrets to version control in real deployments.
FALLBACK_NEON_DSN = "postgresql://USER:PASSWORD@HOST:PORT/DBNAME?sslmode=require"

PROVIDER_SLUGS = {"aws", "azure", "gcp"}
PRIORITY_ALLOWED = {"low", "medium", "high", "critical"}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_priority(v: Any) -> str:
    if v is None:
        return "medium"
    s = str(v).strip().lower()
    return s if s in PRIORITY_ALLOWED else "medium"


def _normalize_provider(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip().lower()
    # Allow AWS/Azure/GCP variants
    if s in PROVIDER_SLUGS:
        return s
    if s == "amazon" or s == "aws":
        return "aws"
    if s == "microsoft" or s == "azure":
        return "azure"
    if s == "google" or s == "gcp":
        return "gcp"
    return None


def _provider_from_filename(name: str) -> Optional[str]:
    lower = name.lower()
    for prov in PROVIDER_SLUGS:
        if prov in lower:
            return prov
    # handle 'mock_aws_resources_1.xlsx' pattern already covered
    return None


def _parse_tags(s: Any) -> Dict[str, str]:
    """
    Parse tag string like "Environment:production,Team:backend" into dict.
    Accepts dict (pass-through), None (empty dict), or string.
    """
    if not s:
        return {}
    if isinstance(s, dict):
        # ensure values are strings
        return {str(k): str(v) for k, v in s.items()}
    if isinstance(s, (list, tuple)):
        # list of "k:v" strings
        out: Dict[str, str] = {}
        for item in s:
            parts = str(item).split(":", 1)
            if len(parts) == 2:
                k, v = parts
                out[k.strip()] = v.strip()
        return out
    text = str(s).strip()
    if not text:
        return {}
    out: Dict[str, str] = {}
    for pair in text.split(","):
        if not pair.strip():
            continue
        if ":" in pair:
            k, v = pair.split(":", 1)
            out[k.strip()] = v.strip()
        else:
            # single word tag -> value "true"
            out[pair.strip()] = "true"
    return out


def _to_decimal_or_none(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return v
    s = str(v).strip()
    if s == "" or s.lower() == "nan":
        return None
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def _parse_datetime_or_none(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        # Return as-is and ensure timezone awareness; assume UTC if naive
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    s = str(v).strip()
    if not s:
        return None
    # normalize a trailing Z to UTC
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _snake(s: str) -> str:
    return s.strip().lower().replace(" ", "_")


def _ensure_ssl_and_channel_binding(dsn: str) -> str:
    """
    Ensure DSN has sslmode=require and channel_binding=require.
    Also normalize scheme for psycopg if SQLAlchemy async URLs are passed (e.g., postgresql+asyncpg://).
    """
    # Normalize scheme if using sqlalchemy URLs
    if dsn.startswith("postgresql+"):
        dsn = "postgresql://" + dsn.split("postgresql+", 1)[1]
    parts = urlparse(dsn)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    if q.get("sslmode") is None:
        q["sslmode"] = "require"
    if q.get("channel_binding") is None:
        q["channel_binding"] = "require"
    # Build new DSN
    new_parts = parts._replace(query=urlencode(q))
    return urlunparse(new_parts)


@dataclass(frozen=True)
class IngestConfig:
    organization_id: str
    account_by_provider: Dict[str, str]
    dsn: str
    input_dir: Path
    dry_run: bool = False


# PUBLIC_INTERFACE
def discover_input_files(input_dir: Path) -> Tuple[List[Path], List[Path]]:
    """Discover Excel files under input_dir and split into resource files vs recommendation files."""
    xlsx_files = sorted([p for p in input_dir.glob("*.xlsx") if p.is_file()])
    resource_files: List[Path] = []
    recommendation_files: List[Path] = []
    for p in xlsx_files:
        if "recommendation" in p.name.lower():
            recommendation_files.append(p)
        else:
            resource_files.append(p)
    return resource_files, recommendation_files


def _read_excel_rows(path: Path) -> List[Dict[str, Any]]:
    wb = load_workbook(filename=str(path), data_only=True, read_only=True)
    try:
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return []
        headers = [(_snake(str(c)) if c is not None else "") for c in (rows[0] or [])]
        # strip trailing empty headers
        while headers and headers[-1] == "":
            headers.pop()
        out: List[Dict[str, Any]] = []
        for r in rows[1:]:
            if r is None:
                continue
            values = list(r[: len(headers)])
            row = {headers[i]: values[i] for i in range(len(headers))}
            # skip fully empty rows
            if all(v is None or (isinstance(v, str) and v.strip() == "") for v in row.values()):
                continue
            out.append(row)
        return out
    finally:
        wb.close()


def _resolve_repo_root(start: Path) -> Optional[Path]:
    # Walk upward to find a directory that has 'attachments' as a child
    cur = start
    for _ in range(6):
        if (cur / "attachments").is_dir():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return None


# ---------- Database helpers ----------

def _fetch_one_scalar(conn: psycopg.Connection, query: str, params: Tuple[Any, ...]) -> Optional[Any]:
    with conn.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
        return row[0] if row and len(row) >= 1 else None


def _resolve_org_and_accounts(conn: psycopg.Connection, args: argparse.Namespace, providers_needed: Iterable[str]) -> Tuple[str, Dict[str, str]]:
    org_id = args.organization_id
    if not org_id:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT id FROM organizations")
            rows = cur.fetchall()
            if len(rows) == 1:
                org_id = rows[0]["id"]
                logger.info("Auto-detected single organization: %s", org_id)
            else:
                raise SystemExit("ERROR: --org/--organization-id is required (cannot auto-detect).")

    accounts: Dict[str, str] = {}
    # Provider-specific args
    explicit_map = {
        "aws": args.account_aws or args.cloud_account_id,
        "azure": args.account_azure or args.cloud_account_id,
        "gcp": args.account_gcp or args.cloud_account_id,
    }
    for prov in providers_needed:
        if explicit_map.get(prov):
            accounts[prov] = explicit_map[prov]  # type: ignore[assignment]
            continue
        # Try auto-detect single cloud account for org+provider
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT id FROM cloud_accounts WHERE organization_id = %s AND provider = %s AND is_active = TRUE",
                (org_id, prov),
            )
            rows = cur.fetchall()
            if len(rows) == 1:
                accounts[prov] = rows[0]["id"]
                logger.info("Auto-detected cloud account for %s: %s", prov, accounts[prov])
            else:
                raise SystemExit(
                    f"ERROR: Cloud account for provider '{prov}' required. "
                    f"Pass --account-{prov} or --cloud-account-id, or ensure exactly one active account exists."
                )
    return org_id, accounts


# ---------- Upsert SQL (PostgreSQL) ----------

SQL_UPSERT_RESOURCE = """
INSERT INTO resources (
    id, organization_id, cloud_account_id, provider, resource_id,
    resource_type, region, state, tags, cost_daily, cost_monthly, created_at
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (organization_id, provider, resource_id) DO UPDATE SET
    cloud_account_id = EXCLUDED.cloud_account_id,
    resource_type = EXCLUDED.resource_type,
    region = EXCLUDED.region,
    state = EXCLUDED.state,
    tags = EXCLUDED.tags,
    cost_daily = EXCLUDED.cost_daily,
    cost_monthly = EXCLUDED.cost_monthly,
    updated_at = NOW()
RETURNING (xmax = 0) AS inserted;
"""

SQL_FIND_RESOURCE_INTERNAL_ID = """
SELECT id FROM resources
WHERE organization_id = %s AND provider = %s AND resource_id = %s
LIMIT 1;
"""

SQL_UPSERT_RECOMMENDATION = """
INSERT INTO recommendations (
    id, organization_id, resource_id, recommendation_type, priority,
    potential_savings_monthly, description, action_items, created_at
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s
)
ON CONFLICT (id) DO UPDATE SET
    organization_id = EXCLUDED.organization_id,
    resource_id = EXCLUDED.resource_id,
    recommendation_type = EXCLUDED.recommendation_type,
    priority = EXCLUDED.priority,
    potential_savings_monthly = EXCLUDED.potential_savings_monthly,
    description = EXCLUDED.description,
    action_items = EXCLUDED.action_items,
    updated_at = NOW()
RETURNING (xmax = 0) AS inserted;
"""


# ---------- Ingestion pipelines ----------

def _ingest_resources_file(conn: psycopg.Connection, cfg: IngestConfig, path: Path) -> Tuple[int, int, int]:
    rows = _read_excel_rows(path)
    if not rows:
        logger.info("No rows found in %s", path.name)
        return (0, 0, 0)

    provider = _provider_from_filename(path.name)
    if not provider:
        # Try derive from column if present (rare)
        for r in rows:
            provider = _normalize_provider(r.get("provider"))
            if provider:
                break
    if not provider:
        logger.warning("Provider not inferred from filename or data for %s; skipping file.", path.name)
        return (0, 0, len(rows))

    cloud_account_id = cfg.account_by_provider.get(provider)
    if not cloud_account_id:
        logger.warning("No cloud account configured for provider=%s; skipping file %s", provider, path.name)
        return (0, 0, len(rows))

    inserted = 0
    updated = 0
    skipped = 0

    with conn.cursor() as cur:
        for idx, r in enumerate(rows):
            resource_id = (r.get("resource_id") or r.get("id") or r.get("name"))
            resource_type = r.get("resource_type")
            region = r.get("region")
            state = r.get("state")
            if resource_id is None or resource_type is None or region is None or state is None:
                skipped += 1
                logger.debug(
                    "Skipping row %d in %s: missing required fields (resource_id/resource_type/region/state). Row=%s",
                    idx, path.name, r
                )
                continue

            tags = _parse_tags(r.get("tags"))
            cost_daily = _to_decimal_or_none(r.get("cost_daily"))
            cost_monthly = _to_decimal_or_none(r.get("cost_monthly"))
            created_at = _parse_datetime_or_none(r.get("launch_time")) or _parse_datetime_or_none(r.get("created_at")) or _now_utc()

            # Generate a new UUID for first insert; on conflict, this ID is ignored
            res_id = str(uuid.uuid4())

            params = (
                res_id,
                cfg.organization_id,
                cloud_account_id,
                provider,
                str(resource_id),
                str(resource_type),
                str(region),
                str(state),
                Json(tags),
                cost_daily,
                cost_monthly,
                created_at,
            )

            if cfg.dry_run:
                # Simulate existence: select to determine inserted vs updated
                existing = _fetch_one_scalar(
                    conn,
                    "SELECT 1 FROM resources WHERE organization_id=%s AND provider=%s AND resource_id=%s",
                    (cfg.organization_id, provider, str(resource_id)),
                )
                if existing:
                    updated += 1
                else:
                    inserted += 1
                continue

            cur.execute(SQL_UPSERT_RESOURCE, params)
            ret = cur.fetchone()
            was_inserted = bool(ret and ret[0])
            if was_inserted:
                inserted += 1
            else:
                updated += 1

    if not cfg.dry_run:
        conn.commit()
    logger.info("Resources file %s: inserted=%s updated=%s skipped=%s", path.name, inserted, updated, skipped)
    return inserted, updated, skipped


def _ingest_recommendations_file(conn: psycopg.Connection, cfg: IngestConfig, path: Path) -> Tuple[int, int, int]:
    rows = _read_excel_rows(path)
    if not rows:
        logger.info("No rows found in %s", path.name)
        return (0, 0, 0)

    inserted = 0
    updated = 0
    skipped = 0

    with conn.cursor() as cur:
        for idx, r in enumerate(rows):
            # Expect an external recommendation_id we use as primary key
            rid = r.get("recommendation_id") or r.get("id")
            if not rid:
                skipped += 1
                logger.debug("Skipping rec row %d in %s: missing recommendation_id. Row=%s", idx, path.name, r)
                continue

            provider = _normalize_provider(r.get("cloud_provider") or r.get("provider"))
            # map resource foreign key if we can
            resource_provider = provider or _provider_from_filename(path.name) or "aws"  # sensible default
            external_res_id = r.get("resource_id")
            internal_resource_id: Optional[str] = None
            if external_res_id:
                cur.execute(SQL_FIND_RESOURCE_INTERNAL_ID, (cfg.organization_id, resource_provider, str(external_res_id)))
                ir = cur.fetchone()
                if ir:
                    internal_resource_id = ir[0]

            recommendation_type = r.get("recommendation_type") or r.get("type") or "General"
            priority = _normalize_priority(r.get("priority"))
            potential_savings = _to_decimal_or_none(r.get("potential_savings_monthly"))
            description = r.get("description")
            # action_items: accept a column 'action_items' as CSV or fall back to 'impact' as a single action item
            action_items_raw = r.get("action_items")
            if action_items_raw is None and r.get("impact"):
                action_items: Any = [str(r.get("impact"))]
            elif isinstance(action_items_raw, str):
                # split on semicolon or comma
                parts = [p.strip() for p in action_items_raw.replace(";", ",").split(",") if p.strip()]
                action_items = parts
            else:
                action_items = action_items_raw  # pass-through (could be list already)

            params = (
                str(rid),
                cfg.organization_id,
                internal_resource_id,
                str(recommendation_type),
                priority,
                potential_savings,
                str(description) if description is not None else None,
                Json(action_items) if action_items is not None else None,
                _now_utc(),
            )

            if cfg.dry_run:
                existing = _fetch_one_scalar(conn, "SELECT 1 FROM recommendations WHERE id=%s", (str(rid),))
                if existing:
                    updated += 1
                else:
                    inserted += 1
                continue

            cur.execute(SQL_UPSERT_RECOMMENDATION, params)
            ret = cur.fetchone()
            was_inserted = bool(ret and ret[0])
            if was_inserted:
                inserted += 1
            else:
                updated += 1

    if not cfg.dry_run:
        conn.commit()
    logger.info("Recommendations file %s: inserted=%s updated=%s skipped=%s", path.name, inserted, updated, skipped)
    return inserted, updated, skipped


# PUBLIC_INTERFACE
def run() -> int:
    """Entry point for CLI execution."""
    load_dotenv()

    parser = argparse.ArgumentParser(description="CloudUnify Pro dataset ingestion tool")
    parser.add_argument("--database-url", dest="database_url", help="Database URL (postgresql://...). Defaults to DATABASE_URL env or fallback DSN.")
    parser.add_argument("--org", "--organization-id", dest="organization_id", help="Organization UUID to assign data to.")
    parser.add_argument("--cloud-account-id", dest="cloud_account_id", help="Cloud account UUID to use for all providers (if provider-specific not provided).")

    # Provider-specific cloud account overrides
    parser.add_argument("--account-aws", dest="account_aws", help="Cloud account UUID for AWS provider.")
    parser.add_argument("--account-azure", dest="account_azure", help="Cloud account UUID for Azure provider.")
    parser.add_argument("--account-gcp", dest="account_gcp", help="Cloud account UUID for GCP provider.")

    parser.add_argument("--input-dir", dest="input_dir", default="", help="Directory containing .xlsx files. Defaults to repo_root/attachments")
    parser.add_argument("--dry-run", dest="dry_run", action="store_true", help="Validate and report without writing to DB.")

    args = parser.parse_args()

    # Resolve input directory
    input_dir = Path(args.input_dir) if args.input_dir else None
    if input_dir is None or not input_dir.exists():
        # derive from repo root by walking upward from this script
        script_dir = Path(__file__).resolve().parent
        repo_root = _resolve_repo_root(script_dir.parent) or script_dir.parent.parent.parent
        default_dir = repo_root / "attachments"
        input_dir = default_dir
    if not input_dir.exists():
        logger.error("Input directory does not exist: %s", input_dir)
        return 2

    # Resolve DSN
    raw_dsn = args.database_url or os.getenv("DATABASE_URL") or FALLBACK_NEON_DSN
    dsn = _ensure_ssl_and_channel_binding(raw_dsn)

    # Discover files
    resource_files, recommendation_files = discover_input_files(input_dir)
    if not resource_files and not recommendation_files:
        logger.warning("No Excel files found in %s", input_dir)
        return 0

    # Determine providers present in resource files
    providers_in_files: set[str] = set()
    for p in resource_files:
        prov = _provider_from_filename(p.name)
        if prov:
            providers_in_files.add(prov)
    # If recommendations solely present and provider resolvable from data, we'll attempt mapping later; here only for cloud account resolution

    # Connect to DB (Neon: sslmode/channel_binding enforced)
    logger.info("Connecting to database...")
    with psycopg.connect(dsn, autocommit=False) as conn:
        # Resolve org and cloud accounts
        org_id, accounts = _resolve_org_and_accounts(conn, args, sorted(providers_in_files or list(PROVIDER_SLUGS)))
        cfg = IngestConfig(
            organization_id=org_id,
            account_by_provider=accounts,
            dsn=dsn,
            input_dir=input_dir,
            dry_run=bool(args.dry_run),
        )

        total_inserted_resources = 0
        total_updated_resources = 0
        total_skipped_resources = 0

        total_inserted_recs = 0
        total_updated_recs = 0
        total_skipped_recs = 0

        # Process resource files first to ensure recommendation FK lookups can succeed
        for rf in resource_files:
            ins, upd, sk = _ingest_resources_file(conn, cfg, rf)
            total_inserted_resources += ins
            total_updated_resources += upd
            total_skipped_resources += sk

        for recf in recommendation_files:
            ins, upd, sk = _ingest_recommendations_file(conn, cfg, recf)
            total_inserted_recs += ins
            total_updated_recs += upd
            total_skipped_recs += sk

        # Summary
        logger.info("==== Ingestion Summary ====")
        logger.info("Resources: inserted=%s updated=%s skipped=%s", total_inserted_resources, total_updated_resources, total_skipped_resources)
        logger.info("Recommendations: inserted=%s updated=%s skipped=%s", total_inserted_recs, total_updated_recs, total_skipped_recs)

    return 0


if __name__ == "__main__":
    sys.exit(run())
