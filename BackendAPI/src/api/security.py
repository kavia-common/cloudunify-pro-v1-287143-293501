from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional

import jwt
from fastapi import Depends, HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.db import get_session
from src.api.models import User

# Password hashing context: bcrypt
_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# JWT settings from environment (with safe defaults)
_SECRET_KEY = os.getenv("SECRET_KEY", "CHANGE_ME_DEVELOPMENT_ONLY")
_ALGORITHM = os.getenv("ALGORITHM", "HS256")
_ACCESS_MIN = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))
_REFRESH_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))

# HTTP Bearer security scheme
http_bearer = HTTPBearer(auto_error=False)


class Role(str, Enum):
    """Platform user roles."""
    admin = "admin"
    user = "user"
    viewer = "viewer"


# PUBLIC_INTERFACE
def get_password_hash(password: str) -> str:
    """Hash a plaintext password using bcrypt."""
    return _pwd_context.hash(password)


# PUBLIC_INTERFACE
def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a plaintext password against a bcrypt hash."""
    try:
        return _pwd_context.verify(plain_password, hashed_password)
    except Exception:
        return False


def _jwt_now() -> datetime:
    return datetime.now(tz=timezone.utc)


# PUBLIC_INTERFACE
def create_access_token(user_id: str, role: str) -> str:
    """Create a signed JWT access token for a user."""
    expire = _jwt_now() + timedelta(minutes=_ACCESS_MIN)
    payload = {
        "sub": user_id,
        "role": role,
        "type": "access",
        "exp": expire,
        "iat": _jwt_now(),
    }
    return jwt.encode(payload, _SECRET_KEY, algorithm=_ALGORITHM)


# PUBLIC_INTERFACE
def create_refresh_token(user_id: str, role: str) -> str:
    """Create a signed JWT refresh token for a user."""
    expire = _jwt_now() + timedelta(days=_REFRESH_DAYS)
    payload = {
        "sub": user_id,
        "role": role,
        "type": "refresh",
        "exp": expire,
        "iat": _jwt_now(),
    }
    return jwt.encode(payload, _SECRET_KEY, algorithm=_ALGORITHM)


def _decode_token(token: str) -> dict:
    """Decode and validate a JWT, raising HTTP 401 on failure."""
    try:
        return jwt.decode(token, _SECRET_KEY, algorithms=[_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


# PUBLIC_INTERFACE
def decode_access_token(token: str) -> dict:
    """Decode a signed access JWT and ensure token type is 'access'.

    Args:
        token: Encoded JWT bearer token.

    Returns:
        The decoded payload dict.

    Raises:
        HTTPException 401 for expired/invalid tokens or wrong type.
    """
    payload = _decode_token(token)
    if payload.get("type") != "access":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token type")
    return payload


# PUBLIC_INTERFACE
async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(http_bearer),
    session: AsyncSession = Depends(get_session),
) -> dict:
    """Validate bearer access token and return user principal.

    Returns:
        dict with keys: id, role.

    Raises:
        HTTPException 401 if missing/invalid credentials or user inactive.
    """
    if not credentials or not credentials.scheme or credentials.scheme.lower() != "bearer":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")

    token = credentials.credentials
    payload = _decode_token(token)
    if payload.get("type") != "access":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token type")

    user_id = payload.get("sub")
    role = payload.get("role")
    if not user_id or not role:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token claims")

    # Verify user exists and is active
    result = await session.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Inactive or missing user")

    return {"id": user_id, "role": role}


# PUBLIC_INTERFACE
def require_roles(*roles: Role):
    """Dependency factory that enforces one of the specified roles.

    Usage:
        @router.get("/admin-only")
        async def admin_only(_=Depends(require_roles(Role.admin))):
            return {"ok": True}
    """

    def _inner(user=Depends(get_current_user)):
        if user["role"] not in {r.value for r in roles}:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
        return user

    return _inner
