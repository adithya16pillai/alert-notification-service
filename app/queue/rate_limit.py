"""Per-recipient, per-channel token-bucket rate limiting (00 §3 goal 4).

Backed by ``token_bucket.lua`` for atomicity. The bucket key uses a Redis
cluster hash-tag (``{recipient}``) so all of a recipient's channel buckets pin
to one slot — keeping the rate-limit decision linearizable (00 §7.3).
"""

from __future__ import annotations

import time

from redis.commands.core import AsyncScript

from app.config import get_settings
from app.observability.metrics import rate_limit_denials_total
from app.redis_client import get_redis, load_lua

_bucket_script: AsyncScript | None = None


def _bucket_key(recipient_id: str, channel: str) -> str:
    return f"rl:{{{recipient_id}}}:{channel}"


async def allow(
    recipient_id: str,
    channel: str,
    *,
    cost: int = 1,
    capacity: int | None = None,
    refill_per_sec: float | None = None,
) -> tuple[bool, float]:
    """Return ``(allowed, remaining_tokens)`` for one delivery attempt.

    ``capacity`` / ``refill_per_sec`` override the global defaults, letting the
    dispatcher pass a resolved per-(recipient, channel) policy (05 §3, §7). The
    Lua script stays generic — the override is just two different ARGV values.
    """
    global _bucket_script
    redis = get_redis()
    if _bucket_script is None:
        # register_script uses EVALSHA and transparently falls back to EVAL on
        # NOSCRIPT (Redis restart / cache flush), re-caching the body (05 §6).
        _bucket_script = redis.register_script(load_lua("token_bucket"))
    settings = get_settings()
    allowed, remaining = await _bucket_script(
        keys=[_bucket_key(recipient_id, channel)],
        args=[
            settings.rate_limit_capacity if capacity is None else capacity,
            settings.rate_limit_refill_per_sec if refill_per_sec is None else refill_per_sec,
            time.time(),
            cost,
        ],
    )
    if not allowed:
        rate_limit_denials_total.labels(channel=channel).inc()
    return bool(allowed), float(remaining)
