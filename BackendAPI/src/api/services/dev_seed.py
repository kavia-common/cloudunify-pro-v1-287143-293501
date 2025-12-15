from __future__ import annotations

import logging
import os
from typing import Optional

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.db import get_session, get_database_url
from src.api.models import User
from src.api.security import get_password_hash, verify_password

logger = logging.getLogger("cloudunify.dev_seed")


def _truthy_env(value: Optional[str], default: bool = False) -> bool:
    """
    Interpret common truthy values in environment variables.

    Accepts: '1', 'true', 'yes', 'on' (case-insensitive).
    """
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _should_seed_default() -> bool:
    """
    Decide if seeding should occur by default when DEV_SEED_USERS is not explicitly set.

    Default to True when using SQLite (dev/test) or when NODE_ENV/REACT_APP_NODE_ENV is 'development'.
    """
    node_env = os.getenv("NODE_ENV") or os.getenv("REACT_APP_NODE_ENV")
    if (node_env or "").strip().lower() == "development":
        return True
    # Seed by default on SQLite to make dev/test work out of the box
    db_url = get_database_url()
    return db_url.startswith("sqlite")


async def _ensure_user(session: AsyncSession, email: str, password: str, role: str, is_active: bool = True) -> None:
    """
    Ensure a user with the given email exists. If not, create it using the provided password and role.
    If it exists, align role/active flags and ensure password hash matches provided password (dev convenience).
    """
    email_norm = email.strip().lower()
    res = await session.execute(select(User).where(func.lower(User.email) == email_norm))
    user = res.scalar_one_or_none()
    if user:
        changed = False
        # Ensure normalized email is stored
        if user.email != email_norm:
            user.email = email_norm
            changed = True
        if user.role != role:
            user.role = role
            changed = True
        if user.is_active is not is_active:
            user.is_active = is_active
            changed = True
        # In development, if the stored hash does not match the configured password, update it
        try:
            hash_ok = bool(user.hashed_password) and verify_password(password, user.hashed_password or "")
        except Exception:
            hash_ok = False
        if not hash_ok:
            user.hashed_password = get_password_hash(password)
            changed = True
            logger.info("Refreshed password hash for dev seed user %s", email_norm)
        if changed:
            await session.commit()
            logger.info("Updated dev seed user %s (role=%s, active=%s)", email_norm, role, is_active)
        return

    user = User(
        email=email_norm,
        hashed_password=get_password_hash(password),
        role=role,
        is_active=is_active,
    )
    session.add(user)
    await session.commit()
    logger.info("Created dev seed user %s (role=%s)", email_norm, role)


# PUBLIC_INTERFACE
async def maybe_seed_dev_users() -> None:
    """
    Seed default development users if enabled by environment configuration.

    Behavior:
    - Controlled by DEV_SEED_USERS (truthy enables, falsy disables).
    - If DEV_SEED_USERS is not set, defaults to seeding when using SQLite or
      when NODE_ENV or REACT_APP_NODE_ENV is 'development'.
    - Creates (if missing) an admin and a regular user with emails/passwords configurable via:
        DEV_ADMIN_EMAIL, DEV_ADMIN_PASSWORD, DEV_USER_EMAIL, DEV_USER_PASSWORD

    Defaults (for development only):
        DEV_ADMIN_EMAIL=admin@cloudunify.pro
        DEV_ADMIN_PASSWORD=Admin123!
        DEV_USER_EMAIL=user@cloudunify.pro
        DEV_USER_PASSWORD=User123!
    """
    seed_flag_env = os.getenv("DEV_SEED_USERS")
    do_seed = _truthy_env(seed_flag_env, default=_should_seed_default())
    if not do_seed:
        return

    admin_email = os.getenv("DEV_ADMIN_EMAIL", "admin@cloudunify.pro").strip()
    admin_password = os.getenv("DEV_ADMIN_PASSWORD", "Admin123!")
    user_email = os.getenv("DEV_USER_EMAIL", "user@cloudunify.pro").strip()
    user_password = os.getenv("DEV_USER_PASSWORD", "User123!")

    # Use a session via existing dependency to avoid duplicating engine setup
    async for session in get_session():
        try:
            await _ensure_user(session, admin_email, admin_password, role="admin", is_active=True)
            await _ensure_user(session, user_email, user_password, role="user", is_active=True)
        finally:
            break  # get_session is a generator; take a single session then exit
