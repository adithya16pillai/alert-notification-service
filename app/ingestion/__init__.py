"""Ingestion module (01): validation, idempotency, durable write, enqueue."""

from app.ingestion.router import dedup_router, router

__all__ = ["dedup_router", "router"]
