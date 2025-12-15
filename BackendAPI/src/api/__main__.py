"""
Module entrypoint to run the CloudUnify Pro FastAPI server directly using:
    python -m src.api

Respects the following environment variables:
- REACT_APP_PORT or PORT: Port to bind (default 3001)
- HOST: Host interface to bind (default 0.0.0.0)
- RELOAD: "1" to enable auto-reload (dev only)
- REACT_APP_LOG_LEVEL: Uvicorn log level (default "info")
"""
import os
import uvicorn


# PUBLIC_INTERFACE
def main():
    """Start the FastAPI application using uvicorn with sane defaults for container environments."""
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
