from __future__ import annotations

import argparse
import asyncio
import os
from dataclasses import dataclass

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.api.db import get_database_url, init_db
from src.api.models import User
from src.api.security import get_password_hash, verify_password


def _normalize_async_db_url(url: str) -> str:
    """
    Normalize a DB URL into an async SQLAlchemy URL compatible with create_async_engine().

    - postgresql://...      -> postgresql+asyncpg://...
    - sqlite:///...         -> sqlite+aiosqlite:///...
    - leaves already-async URLs unchanged
    """
    u = (url or "").strip()
    if not u:
        return u
    if u.startswith("postgresql://"):
        return u.replace("postgresql://", "postgresql+asyncpg://", 1)
    if u.startswith("sqlite:///"):
        return u.replace("sqlite:///", "sqlite+aiosqlite:///", 1)
    return u


@dataclass(frozen=True)
class SeedResult:
    created: bool
    updated: bool
    user_id: str


async def _ensure_user(
    session: AsyncSession,
    *,
    email: str,
    password: str,
    role: str,
    is_active: bool,
    allow_plaintext_migration: bool,
) -> SeedResult:
    """
    Ensure a user exists. If user exists:
      - update role/is_active
      - ensure password hash verifies; optionally migrate plaintext/legacy to bcrypt.

    IMPORTANT:
      - Plaintext migration is only performed when allow_plaintext_migration=True.
    """
    email_norm = email.strip().lower()
    res = await session.execute(select(User).where(func.lower(User.email) == email_norm))
    user = res.scalar_one_or_none()

    if user is None:
        user = User(
            email=email_norm,
            hashed_password=get_password_hash(password),
            role=role,
            is_active=is_active,
        )
        session.add(user)
        await session.commit()
        await session.refresh(user)
        return SeedResult(created=True, updated=False, user_id=user.id)

    updated = False

    if user.email != email_norm:
        user.email = email_norm
        updated = True

    if user.role != role:
        user.role = role
        updated = True

    if user.is_active is not is_active:
        user.is_active = is_active
        updated = True

    # Verify password; if it fails, optionally migrate plaintext/legacy hashes.
    hash_ok = False
    if user.hashed_password:
        hash_ok = verify_password(password, user.hashed_password)

    if not hash_ok:
        # Optional plaintext/legacy migration:
        # If the stored value equals the plaintext password (or is a legacy non-bcrypt string),
        # and migration is allowed, replace it with a bcrypt hash.
        if allow_plaintext_migration and (user.hashed_password == password or not str(user.hashed_password or "").startswith("$2")):
            user.hashed_password = get_password_hash(password)
            updated = True
        else:
            # If user exists but password doesn't verify and we won't migrate, still update the hash
            # only when explicitly allowed; otherwise leave unchanged.
            pass

    if updated:
        await session.commit()

    return SeedResult(created=False, updated=updated, user_id=user.id)


# PUBLIC_INTERFACE
def main() -> None:
    """Create/ensure a demo user in the configured database.

    This utility is intended for development/staging setups where a demo login is needed.

    Env vars (optional):
      - DATABASE_URL: database connection string (if not set, backend defaults apply)
      - DEMO_EMAIL / DEMO_PASSWORD / DEMO_ROLE / DEMO_ACTIVE
      - ALLOW_PLAINTEXT_PASSWORD_MIGRATION=1 to upgrade plaintext/legacy password storage to bcrypt

    Examples:
      python scripts/seed_demo_user.py
      DEMO_EMAIL=admin@example.com DEMO_PASSWORD=Demo123! python scripts/seed_demo_user.py
      make seed-demo-user
    """
    parser = argparse.ArgumentParser(description="Seed a demo user into the configured DB.")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL") or get_database_url(), help="Database URL")
    parser.add_argument("--email", default=os.getenv("DEMO_EMAIL", "admin@example.com"), help="Demo user email")
    parser.add_argument("--password", default=os.getenv("DEMO_PASSWORD", "Demo123!"), help="Demo user password")
    parser.add_argument("--role", default=os.getenv("DEMO_ROLE", "admin"), help="Demo user role (admin|user|viewer)")
    parser.add_argument(
        "--active",
        default=os.getenv("DEMO_ACTIVE", "1"),
        help="Whether user is active (1/0, true/false).",
    )
    args = parser.parse_args()

    active_raw = str(args.active).strip().lower()
    is_active = active_raw in {"1", "true", "yes", "on"}

    allow_plaintext_migration = str(os.getenv("ALLOW_PLAINTEXT_PASSWORD_MIGRATION", "0")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    # Ensure init_db() uses the same DATABASE_URL as this script.
    # We set env var for the duration of this run.
    os.environ["DATABASE_URL"] = args.database_url
    db_url_async = _normalize_async_db_url(args.database_url)

    async def _run() -> None:
        await init_db()

        engine = create_async_engine(db_url_async, future=True, pool_pre_ping=True)
        session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

        async with session_factory() as session:
            result = await _ensure_user(
                session,
                email=args.email,
                password=args.password,
                role=args.role.strip().lower() or "admin",
                is_active=is_active,
                allow_plaintext_migration=allow_plaintext_migration,
            )

        await engine.dispose()

        action = "CREATED" if result.created else ("UPDATED" if result.updated else "UNCHANGED")
        print(f"{action}: demo user {args.email.strip().lower()} (id={result.user_id}, role={args.role}, active={is_active})")

    asyncio.run(_run())


if __name__ == "__main__":
    main()
