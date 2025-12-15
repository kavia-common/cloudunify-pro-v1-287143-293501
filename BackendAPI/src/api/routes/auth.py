from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, EmailStr
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.db import get_session
from src.api.models import User
from src.api.security import (
    verify_password,
    create_access_token,
    create_refresh_token,
    require_roles,
    Role,
    get_current_user,
)

router = APIRouter(prefix="/auth", tags=["Auth"])


class LoginRequest(BaseModel):
    """Login payload with user credentials."""
    email: EmailStr = Field(..., description="User email address")
    password: str = Field(..., description="User password (plaintext)")


class LoginResponse(BaseModel):
    """JWT token pair response."""
    access_token: str = Field(..., description="Short-lived access token (JWT)")
    refresh_token: str = Field(..., description="Long-lived refresh token (JWT)")
    token_type: str = Field("bearer", description="Token type (bearer)")


class UserPublic(BaseModel):
    """Public view of a user."""
    id: str = Field(..., description="User ID")
    email: EmailStr = Field(..., description="User email")
    role: str = Field(..., description="User global role")
    is_active: bool = Field(..., description="Whether the user is active")


# PUBLIC_INTERFACE
@router.post(
    "/login",
    summary="User login",
    response_model=LoginResponse,
    responses={
        200: {"description": "Login successful"},
        401: {"description": "Invalid credentials"},
    },
)
async def login(payload: LoginRequest, session: AsyncSession = Depends(get_session)) -> LoginResponse:
    """Authenticate a user and return an access/refresh token pair.

    Parameters:
        payload: LoginRequest containing email and password.
        session: Async database session (injected).

    Returns:
        LoginResponse with access_token, refresh_token, and token_type.

    Raises:
        HTTPException 401 if credentials are invalid or user is inactive.
    """
    # Fetch user by email
    result = await session.execute(select(User).where(User.email == payload.email))
    user: Optional[User] = result.scalar_one_or_none()
    if not user or not user.hashed_password or not verify_password(payload.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )
    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User is inactive")

    access_token = create_access_token(user_id=user.id, role=user.role)
    refresh_token = create_refresh_token(user_id=user.id, role=user.role)
    return LoginResponse(access_token=access_token, refresh_token=refresh_token, token_type="bearer")


# PUBLIC_INTERFACE
@router.get("/me", summary="Get current user", response_model=UserPublic)
async def get_me(
    user=Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> UserPublic:
    """Return the currently authenticated user's public profile."""
    # Load email fresh to avoid stale token data
    result = await session.execute(select(User).where(User.id == user["id"]))
    db_user = result.scalar_one_or_none()
    if not db_user:
        # Should not occur if get_current_user already validated; defensive
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    return UserPublic(id=db_user.id, email=db_user.email, role=db_user.role, is_active=db_user.is_active)


# PUBLIC_INTERFACE
@router.get("/admin-ping", summary="Admin-only test endpoint")
async def admin_ping(_=Depends(require_roles(Role.admin))):
    """Simple endpoint to verify RBAC. Accessible only by admin role."""
    return {"ok": True, "message": "admin-access-confirmed"}
