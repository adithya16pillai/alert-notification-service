"""Ingestion module (01): validation, idempotency, durable write, enqueue."""

from app.ingestion.router import router

__all__ = ["router"]
