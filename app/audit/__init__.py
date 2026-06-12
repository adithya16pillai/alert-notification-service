"""Audit module (04): delivery-attempt log + status queries + DLQ."""

from app.audit.router import router

__all__ = ["router"]
