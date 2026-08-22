"""FastAPI application: the read API the dashboard talks to.

Scope, stated once so it stays true: this service reads the gold layer and
serves JSON. It has no write path, no auth, no database of its own and no
knowledge of how gold was produced. The ETL runs as a separate Container Apps
*Job* on a schedule; this is a long-running *App*. Keeping them apart is what
lets the pipeline fail, be retried, or be rewritten without a dashboard outage —
the API keeps serving the last gold the flow successfully wrote.

Two things here are environment-driven rather than hardcoded, both because the
same image runs on a laptop and behind Azure ingress:

* CORS_ORIGINS — the browser refuses a cross-origin XHR unless this server says
  otherwise, and the dashboard is served from a different origin in every
  environment (Vite on :5173 locally, a Static Web App in Azure). A wildcard
  would be the lazy fix; an explicit list is the difference between a
  configuration and a hole.
* PORT — Container Apps injects it and routes ingress to it. A literal port here
  would be ignored by the platform and present as an app that starts fine and is
  unreachable, which is a genuinely annoying failure to diagnose.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.app.deps import Warehouse
from backend.app.routes import router

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
log = logging.getLogger("backend.app")

DEFAULT_CORS_ORIGINS = "http://localhost:5173,http://localhost:3000"


def cors_origins() -> list[str]:
    """Parse CORS_ORIGINS, a comma-separated allow-list.

    Empty entries are dropped so a trailing comma in a Container Apps env var —
    easy to leave behind when editing a list in a YAML block — cannot register
    an empty-string origin that matches nothing and hides the real mistake.
    """
    raw = os.getenv("CORS_ORIGINS", DEFAULT_CORS_ORIGINS)
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Own the DuckDB connection for the life of the process.

    Startup work belongs here rather than at import time: uvicorn's reloader
    imports this module in the parent process too, and a connection opened at
    import would be created there and inherited across a fork. The lifespan
    handler runs once per real server, and its teardown half is what closes the
    connection on SIGTERM instead of leaking it.

    `Warehouse.open` never raises — see backend/app/deps.py for why a missing
    gold layer must not stop this server from starting.
    """
    app.state.warehouse = Warehouse.open()
    if app.state.warehouse.connection is not None:
        log.info("Warehouse ready (lake_backend=%s)", app.state.warehouse.settings.lake_backend)
    try:
        yield
    finally:
        app.state.warehouse.close()
        log.info("Warehouse closed")


app = FastAPI(
    title="Fleet Telemetry API",
    version="1.0.0",
    summary="Read-only JSON over the gold layer of the fleet telemetry lakehouse.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins(),
    # The API is read-only and unauthenticated, so there is nothing for a cookie
    # to carry and no reason to let a browser attach one. Leaving credentials
    # off also keeps the allow-list meaningful — with credentials enabled a
    # wildcard is rejected by browsers anyway, and this is the honest version.
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(router)


@app.get("/", include_in_schema=False)
def root() -> dict[str, str]:
    """Signpost for anyone who opens the bare host in a browser."""
    return {
        "service": "fleet-telemetry-api",
        "docs": "/docs",
        "health": "/api/health",
    }


if __name__ == "__main__":  # pragma: no cover
    # `python -m backend.app.main` as a convenience. The container does not use
    # this path — it execs uvicorn directly so that uvicorn is PID 1 and can
    # handle SIGTERM itself (see Dockerfile.backend).
    import uvicorn

    uvicorn.run(
        "backend.app.main:app",
        host=os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "8000")),
    )
