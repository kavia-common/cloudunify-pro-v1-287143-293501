from __future__ import annotations

from typing import Optional

from fastapi import Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

# HTTP Bearer security scheme (placeholder).
http_bearer = HTTPBearer(auto_error=False)

# PUBLIC_INTERFACE
async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(http_bearer),
) -> Optional[dict]:
    """Placeholder auth dependency.

    Accepts any request; returns None for now. To enforce JWT later,
    validate the bearer token here and return a user principal.

    TODO: Implement real JWT validation and RBAC.
    """
    return None
