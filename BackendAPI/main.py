"""Uvicorn import shim for CloudUnify Pro Backend API.

This module exposes 'app' so you can run:
    uvicorn main:app --host 0.0.0.0 --port 3001

It delegates to the canonical application defined in src.api.main:app.
"""
from __future__ import annotations

import os

# Re-export the FastAPI app from the canonical module path
try:
    from src.api.main import app as app  # noqa: F401
except Exception as exc:  # pragma: no cover
    # Provide a more helpful import error if something goes wrong
    raise ImportError(
        "Failed to import 'app' from src.api.main. "
        "Ensure the 'src' package is importable and dependencies are installed."
    ) from exc


# PUBLIC_INTERFACE
def main() -> None:
    """Run the FastAPI application using uvicorn.

    Environment variables:
        HOST: Host interface to bind (default: 0.0.0.0)
        REACT_APP_PORT or PORT: Port to bind (default: 3001)
        RELOAD: "1" to enable auto-reload (development only)
        REACT_APP_LOG_LEVEL: Uvicorn log level (default: "info")
    """
    import uvicorn

    host = os.getenv("HOST", "0.0.0.0")
    port_str = os.getenv("REACT_APP_PORT") or os.getenv("PORT") or "3001"
    try:
        port = int(port_str)
    except ValueError:
        port = 3001
    reload_flag = os.getenv("RELOAD", "0") == "1"
    log_level = os.getenv("REACT_APP_LOG_LEVEL", "info")

    # Use module path to ensure proper import resolution
    uvicorn.run("src.api.main:app", host=host, port=port, reload=reload_flag, log_level=log_level)


if __name__ == "__main__":
    main()
