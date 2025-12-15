from __future__ import annotations

import asyncio

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import WebSocket
from starlette.websockets import WebSocketState

from src.api.schemas import ActivityEvent

logger = logging.getLogger("cloudunify.services.activity_stream")


@dataclass
class _ConnMeta:
    """Internal metadata about a websocket connection."""
    websocket: WebSocket
    organization_id: str
    user_id: Optional[str] = None
    role: Optional[str] = None
    client_id: Optional[str] = None
    ping_task: Optional[asyncio.Task] = None


class ActivityConnectionManager:
    """Manage WebSocket connections and broadcast events per-organization."""

    def __init__(self) -> None:
        # Map org_id -> set of connections
        self._org_conns: dict[str, set[_ConnMeta]] = defaultdict(set)
        # Map (org_id, user_id) -> _ConnMeta for reconnect-safe (deduplicate)
        self._by_org_user: dict[tuple[str, Optional[str]], _ConnMeta] = {}
        # General lock to protect maps
        self._lock = asyncio.Lock()

    async def _start_ping(self, conn: _ConnMeta, interval: int = 25) -> None:
        """Send periodic ping messages to keep the connection alive."""
        async def _pinger() -> None:
            try:
                while True:
                    # If client closes, let send_json raise and we'll clean up in caller.
                    await asyncio.sleep(interval)
                    if conn.websocket.application_state != WebSocketState.CONNECTED:
                        break
                    try:
                        await conn.websocket.send_json({"type": "ping", "ts": datetime.now(timezone.utc).isoformat()})
                    except Exception:
                        # Sending failed; break prompting cleanup
                        break
            except asyncio.CancelledError:
                # Normal task cancellation upon disconnect
                pass

        conn.ping_task = asyncio.create_task(_pinger())

    # PUBLIC_INTERFACE
    async def connect(self, websocket: WebSocket, organization_id: str, *, user_id: Optional[str], role: Optional[str], client_id: Optional[str] = None) -> _ConnMeta:
        """Accept a WebSocket connection and register it for an organization.

        If an existing connection for the same (organization_id, user_id) exists, it is closed
        to allow reconnect-safe semantics.
        """
        await websocket.accept()
        meta = _ConnMeta(websocket=websocket, organization_id=organization_id, user_id=user_id, role=role, client_id=client_id)

        async with self._lock:
            # Reconnect-safe: close any existing for same org+user
            key = (organization_id, user_id)
            if key in self._by_org_user:
                try:
                    old = self._by_org_user[key]
                    if old.websocket is not websocket and old.websocket.application_state == WebSocketState.CONNECTED:
                        await old.websocket.close(code=1000)
                except Exception:
                    pass
                # Remove old mapping from org set
                try:
                    self._org_conns[organization_id].discard(old)
                except Exception:
                    pass

            self._org_conns[organization_id].add(meta)
            self._by_org_user[key] = meta

        await self._start_ping(meta)
        # Send initial connected event
        await self._safe_send(meta.websocket, {
            "type": "connected",
            "organization_id": organization_id,
            "user_id": user_id,
            "role": role,
            "ts": datetime.now(timezone.utc).isoformat(),
        })
        logger.info("WebSocket connected org=%s user=%s", organization_id, user_id)
        return meta

    async def _safe_send(self, websocket: WebSocket, message: Dict[str, Any]) -> None:
        """Send a JSON message with safety checks."""
        if websocket.application_state != WebSocketState.CONNECTED:
            return
        try:
            await websocket.send_json(message)
        except Exception:
            # Ignore, the caller will handle disconnect
            pass

    # PUBLIC_INTERFACE
    async def disconnect(self, meta: _ConnMeta) -> None:
        """Remove the given connection from the manager and cancel ping task."""
        try:
            if meta.ping_task:
                meta.ping_task.cancel()
        except Exception:
            pass

        async with self._lock:
            try:
                self._org_conns.get(meta.organization_id, set()).discard(meta)
            except Exception:
                pass
            try:
                key = (meta.organization_id, meta.user_id)
                if self._by_org_user.get(key) is meta:
                    self._by_org_user.pop(key, None)
            except Exception:
                pass
            # Clean empty org sets
            if not self._org_conns.get(meta.organization_id):
                self._org_conns.pop(meta.organization_id, None)

        try:
            if meta.websocket.application_state == WebSocketState.CONNECTED:
                await meta.websocket.close(code=1000)
        except Exception:
            pass

        logger.info("WebSocket disconnected org=%s user=%s", meta.organization_id, meta.user_id)

    # PUBLIC_INTERFACE
    async def broadcast(self, organization_id: str, message: Dict[str, Any]) -> int:
        """Broadcast a JSON message to all clients connected for the organization.

        Returns:
            Number of connections that were attempted to receive the message.
        """
        async with self._lock:
            recipients = list(self._org_conns.get(organization_id, set()))

        dead: list[_ConnMeta] = []
        for meta in recipients:
            try:
                await self._safe_send(meta.websocket, message)
            except Exception:
                dead.append(meta)

        # Cleanup dead connections
        for meta in dead:
            await self.disconnect(meta)

        return len(recipients)

    # PUBLIC_INTERFACE
    async def broadcast_event(self, event: ActivityEvent) -> int:
        """Broadcast an ActivityEvent (pydantic) to the organization."""
        payload = event.model_dump(mode="json")
        return await self.broadcast(event.organization_id, payload)

    # PUBLIC_INTERFACE
    def make_event(self, *, event_type: str, organization_id: str, payload: Dict[str, Any]) -> ActivityEvent:
        """Convenience builder for activity events."""
        return ActivityEvent(
            type=event_type,
            organization_id=organization_id,
            ts=datetime.now(timezone.utc),
            payload=payload or {},
        )


# Singleton manager
activity_manager = ActivityConnectionManager()
