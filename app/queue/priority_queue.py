"""Redis priority queue: one ZSET per severity, drained highest-first.

Score = ``submitted_at_unix_ms`` so each severity tier is FIFO while a higher
severity always preempts a lower one (00 §7.4, steps 4 & 6).
"""

from __future__ import annotations

from redis.commands.core import AsyncScript

from app.config import get_settings
from app.ingestion.schemas import Severity
from app.observability.metrics import queue_depth
from app.redis_client import get_redis, load_lua

_pop_script: AsyncScript | None = None


def _queue_key(severity: Severity) -> str:
    return f"{get_settings().queue_key_prefix}:{severity.value}"


async def enqueue_alert(alert_id: str, severity: Severity, *, score: int) -> None:
    redis = get_redis()
    await redis.zadd(_queue_key(severity), {alert_id: score})


async def pop_priority(batch_size: int) -> list[str]:
    """Pop up to ``batch_size`` alert ids, highest severity first, atomically."""
    global _pop_script
    redis = get_redis()
    if _pop_script is None:
        _pop_script = redis.register_script(load_lua("pop_priority"))
    settings = get_settings()
    # settings.severities is critical-first; the Lua script drains KEYS in order,
    # so pass them critical -> info (highest priority popped first).
    keys = [f"{settings.queue_key_prefix}:{s}" for s in settings.severities]
    return await _pop_script(keys=keys, args=[batch_size])


async def refresh_queue_depth_metrics() -> None:
    redis = get_redis()
    settings = get_settings()
    for sev in settings.severities:
        depth = await redis.zcard(f"{settings.queue_key_prefix}:{sev}")
        queue_depth.labels(severity=sev).set(depth)
