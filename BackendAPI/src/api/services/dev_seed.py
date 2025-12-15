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

    Default to True when using SQLite (dev/test) or when ENV/NODE_ENV/REACT_APP_NODE_ENV is 'development'.
    """
    node_env = os.getenv("NODE_ENV") or os.getenv("REACT_APP_NODE_ENV") or os.getenv("ENV")
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
    - Controlled by DEV_SEED_USERS (truthy enables, falsy disables). Alias: DEV_SEED_ENABLED.
    - If neither flag is set, defaults to seeding when using SQLite or when
      ENV / NODE_ENV / REACT_APP_NODE_ENV is 'development'.
    - Always ensures (if enabled) an admin and a regular user, configurable via:
        DEV_ADMIN_EMAIL, DEV_ADMIN_PASSWORD, DEV_USER_EMAIL, DEV_USER_PASSWORD
    - Additionally, can seed a custom demo user when DEV_SEED_EMAIL and DEV_SEED_PASSWORD are set,
      with optional DEV_SEED_ROLE (default 'user') and DEV_SEED_ACTIVE (default truthy).

    Defaults (for development only):
        DEV_ADMIN_EMAIL=admin@cloudunify.pro
        DEV_ADMIN_PASSWORD=Admin123!
        DEV_USER_EMAIL=user@cloudunify.pro
        DEV_USER_PASSWORD=User123!

    Security notes:
        - Email values are normalized to lowercase on storage and lookup.
        - If a user exists but the configured password doesn't match, the password hash is refreshed
          to ensure login works in development (never do this in production).
    """
    # Primary flag and aliases
    seed_flag_env = os.getenv("DEV_SEED_USERS")
    if seed_flag_env is None:
        seed_flag_env = os.getenv("DEV_SEED_ENABLED")
    if seed_flag_env is None:
        # Also support 'DEV_SEED' as an additional alias
        seed_flag_env = os.getenv("DEV_SEED")

    do_seed = _truthy_env(seed_flag_env, default=_should_seed_default())
    if not do_seed:
        logger.debug("Dev seeding disabled (set DEV_SEED_USERS=1 to enable).")
        return

    admin_email = os.getenv("DEV_ADMIN_EMAIL", "admin@cloudunify.pro").strip()
    admin_password = os.getenv("DEV_ADMIN_PASSWORD", "Admin123!")
    user_email = os.getenv("DEV_USER_EMAIL", "user@cloudunify.pro").strip()
    user_password = os.getenv("DEV_USER_PASSWORD", "User123!")

    # Optional custom seed user (e.g., for demo credentials)
    extra_email = (os.getenv("DEV_SEED_EMAIL") or "").strip()
    extra_password = os.getenv("DEV_SEED_PASSWORD")
    extra_role = os.getenv("DEV_SEED_ROLE", "user").strip().lower() or "user"
    extra_active = _truthy_env(os.getenv("DEV_SEED_ACTIVE"), default=True)

    # Log startup seeding context without revealing any passwords
    flags_snapshot = {
        "DEV_SEED_USERS": os.getenv("DEV_SEED_USERS"),
        "DEV_SEED_ENABLED": os.getenv("DEV_SEED_ENABLED"),
        "DEV_SEED": os.getenv("DEV_SEED"),
    }
    logger.info(
        "Dev seeding enabled. Flags=%s; custom_email=%s; forced_demo=%s",
        flags_snapshot,
        (extra_email or "").lower() or "(none)",
        "kishore@kavia.ai",
    )

    async for session in get_session():
        try:
            await _ensure_user(session, admin_email, admin_password, role="admin", is_active=True)
            await _ensure_user(session, user_email, user_password, role="user", is_active=True)

            # Seed custom user when both email and password provided
            if extra_email and extra_password:
                await _ensure_user(session, extra_email, extra_password, role=extra_role, is_active=extra_active)
                logger.info("Ensured custom dev seed user %s (role=%s, active=%s)", extra_email.lower(), extra_role, extra_active)

            # Always ensure the reported demo credentials exist in dev when seeding is enabled
            # Email is normalized and password hashed using the same bcrypt scheme as in login
            await _ensure_user(session, "kishore@kavia.ai", "kishore15404", role="user", is_active=True)
            logger.info("Ensured dev demo user kishore@kavia.ai")
        finally:
            # get_session is a generator; take a single session then exit
            break
