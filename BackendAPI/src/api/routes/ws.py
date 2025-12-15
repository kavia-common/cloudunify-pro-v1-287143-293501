from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi import WebSocket, WebSocketDisconnect
from src.api.security import decode_access_token
from src.api.services.activity_stream import activity_manager

router = APIRouter(tags=["Realtime"])

logger = logging.getLogger("cloudunify.routes.ws")


def _extract_token_from_headers(headers) -> Optional[str]:
    """Extract Bearer token from the 'authorization' header if present."""
    auth = headers.get("authorization") or headers.get("Authorization")
    if not auth:
        return None
    parts = auth.split(" ", 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1]
    return None


# PUBLIC_INTERFACE
@router.get(
    "/ws/activity-stream/{organization_id}",
    summary="Real-time activity stream (WebSocket)",
    description=(
        "WebSocket endpoint for real-time activity updates.\n\n"
        "Connect using a WebSocket client to the same path.\n"
        "Authentication: provide a JWT access token using one of the following:\n"
        "- Authorization header: 'Authorization: Bearer <token>'\n"
        "- Query string: '?token=<token>'\n\n"
        "Messages:\n"
        "- Server may periodically send {\"type\":\"ping\"}; clients can ignore or reply with {\"type\":\"pong\"}.\n"
        "- Activity events are concise JSON objects documenting ingestion and other activities.\n"
    ),
    responses={200: {"description": "Usage information for WebSocket endpoint"}},
)
async def websocket_activity_usage(organization_id: str, token: Optional[str] = Query(default=None, description="JWT access token")):
    """Return usage details for connecting to the WebSocket activity stream."""
    # Static help response (does not validate token)
    return {
        "endpoint": f"/ws/activity-stream/{organization_id}",
        "auth": {
            "header": "Authorization: Bearer <access_token>",
            "query": "token=<access_token>",
        },
        "notes": [
            "On connect, server sends a 'connected' event.",
            "Server may send keepalive 'ping' messages.",
            "Events are organization-scoped.",
        ],
    }


# PUBLIC_INTERFACE
@router.websocket("/ws/activity-stream/{organization_id}")
async def websocket_activity_stream(websocket: WebSocket, organization_id: str):
    """WebSocket handler for real-time activity events within an organization.

    Authentication:
    - Provide a JWT access token via 'Authorization: Bearer <token>' header OR
      'token' query string parameter.

    On connect:
    - Sends a 'connected' event.
    - Periodically sends 'ping' keepalive messages.

    On messages:
    - If client sends a JSON message with {'type': 'pong'} the server ignores it.
    """
    # Extract token from Authorization header or query string
    token = _extract_token_from_headers(websocket.headers) or websocket.query_params.get("token")
    if not token:
        # Policy violation
        await websocket.close(code=1008)
        return

    try:
        payload = decode_access_token(token)
    except HTTPException:
        await websocket.close(code=1008)
        return

    user_id: Optional[str] = payload.get("sub")
    role: Optional[str] = payload.get("role")
    client_id: Optional[str] = websocket.query_params.get("client_id")

    meta = None
    try:
        # Accept and register
        meta = await activity_manager.connect(
            websocket,
            organization_id=organization_id,
            user_id=user_id,
            role=role,
            client_id=client_id,
        )

        # Simple receive loop: handle client pongs, ignore other messages
        while True:
            try:
                data = await websocket.receive_json()
            except WebSocketDisconnect:
                break
            except Exception:
                # Non-JSON or other error - ignore and continue
                continue

            if isinstance(data, dict) and data.get("type") == "pong":
                # ignore
                continue
            # Optionally echo back ack
            # await websocket.send_json({"type": "ack"})

    finally:
        if meta:
            await activity_manager.disconnect(meta)
