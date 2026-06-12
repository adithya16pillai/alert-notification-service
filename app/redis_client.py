"""Shared async Redis connection pool + Lua script loader.

Redis backs three CP-leaning concerns (see 00 §7.3): idempotency (SET NX EX),
per-recipient token-bucket rate limiting, and the priority queue ZSETs.
"""

from __future__ import annotations

from pathlib import Path

import redis.asyncio as redis

from app.config import get_settings

_LUA_DIR = Path(__file__).parent / "queue" / "lua"

_client: redis.Redis | None = None


def get_redis() -> redis.Redis:
    global _client
    if _client is None:
        settings = get_settings()
        _client = redis.from_url(settings.redis_url, decode_responses=True)
    return _client


def load_lua(name: str) -> str:
    """Read a Lua script body by filename (without extension)."""
    return (_LUA_DIR / f"{name}.lua").read_text(encoding="utf-8")


async def close_redis() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
