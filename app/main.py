"""FastAPI application factory + entrypoint.

Wires the module routers, renders the single error hierarchy into the
service-wide error schema (01 §5), sets up observability, and manages datastore
pools via lifespan. Stateless app tier — all state lives in Postgres and Redis.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.audit import router as audit_router
from app.channels import register_defaults
from app.config import get_settings
from app.db import dispose_engine
from app.errors import AppError, ValidationError
from app.ingestion import dedup_router
from app.ingestion import router as ingestion_router
from app.observability import configure_logging, configure_tracing, get_logger, metrics_router
from app.recipients import rate_limit_router, subscriptions_router
from app.recipients import router as recipients_router
from app.redis_client import close_redis

log = get_logger(__name__)


def _trace_id(request: Request) -> str:
    """Reuse an inbound request id if present, else mint one."""
    return request.headers.get("x-request-id") or uuid.uuid4().hex


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    register_defaults()
    log.info("app.startup", env=get_settings().env)
    yield
    await close_redis()
    await dispose_engine()
    log.info("app.shutdown")


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="alert-notification-service",
        version="0.1.0",
        description="Alert ingestion, dedup, rate-limiting and multi-channel fanout.",
        lifespan=lifespan,
    )

    @app.exception_handler(AppError)
    async def app_error_handler(request: Request, exc: AppError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict(_trace_id(request)))

    @app.exception_handler(RequestValidationError)
    async def validation_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        # Map Pydantic/FastAPI 422s into the service error schema as 400s (01 §5).
        first = exc.errors()[0] if exc.errors() else {}
        loc = [str(p) for p in first.get("loc", []) if p not in ("body", "header", "query")]
        err = ValidationError(first.get("msg", "invalid request"), field=".".join(loc) or None)
        return JSONResponse(status_code=err.http_status, content=err.to_dict(_trace_id(request)))

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict:
        return {"status": "ok"}

    @app.get("/readyz", include_in_schema=False)
    async def readyz() -> dict:
        # TODO: ping Redis + Postgres before reporting ready.
        return {"status": "ready"}

    app.include_router(ingestion_router)
    app.include_router(dedup_router)
    app.include_router(recipients_router)
    app.include_router(subscriptions_router)
    app.include_router(rate_limit_router)
    app.include_router(audit_router)
    if settings.metrics_enabled:
        app.include_router(metrics_router)

    configure_tracing(app)
    return app


app = create_app()


def run() -> None:
    """Console-script entrypoint (``ans-api``)."""
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=get_settings().debug)


if __name__ == "__main__":
    run()
