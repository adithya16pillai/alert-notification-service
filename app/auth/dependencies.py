"""API-key auth dependency.

v1 uses static API keys (header ``X-API-Key``) loaded from settings. The
abstraction is a FastAPI dependency so it can be swapped for JWT/mTLS without
touching route handlers.
"""

from __future__ import annotations

from dataclasses import dataclass

from fastapi import Header

from app.config import get_settings
from app.errors import AuthError


@dataclass(frozen=True)
class Principal:
    """The authenticated caller."""

    api_key: str
    producer: str


async def require_api_key(x_api_key: str | None = Header(default=None)) -> Principal:
    settings = get_settings()
    if not settings.api_keys:
        # Local/dev convenience: no keys configured => allow an anonymous producer.
        if settings.env == "local":
            return Principal(api_key="local", producer="local")
        raise AuthError("no API keys configured")
    if x_api_key is None or x_api_key not in settings.api_keys:
        raise AuthError("invalid or missing X-API-Key")
    return Principal(api_key=x_api_key, producer=x_api_key[:8])
