"""Deferred-retry queue: rate-limited deliveries are parked, not dropped (05 §7).

When the token bucket is empty we don't drop the alert — dropping silently is the
worst outcome (the customer never sees it). Instead the delivery is parked in a
per-severity ZSET ``queue:retry:{severity}`` scored by its next-attempt time. A
retry worker drains due items (``score <= now``), re-checks the limit, and either
sends or re-parks — until the total deferral exceeds the cap, after which the
attempt is abandoned to the DLQ (05 §7, §8).

Per-severity keys mean the retry worker honours priority too: a parked
``critical`` is retried before a parked ``info``.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass

from redis.commands.core import AsyncScript

from app.config import get_settings
from app.observability.metrics import retry_queue_depth
from app.redis_client import get_redis, load_lua

_pop_script: AsyncScript | None = None


def _retry_key(severity: str) -> str:
    return f"{get_settings().retry_queue_key_prefix}:{severity}"


@dataclass(frozen=True)
class DeferredDelivery:
    """A delivery parked on the per-severity retry ZSET. One queue carries both
    reasons (07 §7 "one retry queue, one DLQ"):

    - ``reason="rate_limit"`` — parked because the token bucket was empty (05 §7).
      ``first_deferred_ms`` is preserved across re-parks so the cap measures total
      time deferred; ``attempt_no``/history stay zero so the member is stable and
      re-parking just updates the score instead of duplicating.
    - ``reason="retry"`` — a transient delivery failure rescheduled with backoff
      (07 §3.3). ``attempt_no`` is the count of attempts already made and
      ``attempt_history`` accumulates per-attempt summaries for the DLQ.

    Keying the queue by severity (not channel, cf. 07 §3.3) is deliberate: a
    parked ``critical`` is retried before a parked ``info``, preserving priority
    on the retry path too.
    """

    alert_id: str
    tenant: str
    recipient_id: str
    channel: str
    target: str
    severity: str
    first_deferred_ms: int
    config: dict | None = None
    attempt_no: int = 0
    reason: str = "rate_limit"
    last_error: str | None = None
    attempt_history: tuple[dict, ...] = ()

    def to_member(self) -> str:
        # Sorted keys => a re-park of the same logical delivery produces an
        # identical member, so ZADD updates the score instead of duplicating.
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_member(cls, raw: str) -> DeferredDelivery:
        data = json.loads(raw)
        # JSON arrays decode to lists; keep the field a tuple so a round-trip is
        # value-stable (the member string is what ZADD dedupes on).
        data["attempt_history"] = tuple(data.get("attempt_history", ()))
        return cls(**data)


async def defer(delivery: DeferredDelivery, *, due_ms: int) -> None:
    """Park (or re-park) a delivery to be retried at ``due_ms`` (unix ms)."""
    await get_redis().zadd(_retry_key(delivery.severity), {delivery.to_member(): due_ms})


async def pop_due_retries(now_ms: int, limit: int) -> list[DeferredDelivery]:
    """Atomically remove and return up to ``limit`` due deliveries, highest
    severity first. Two workers never receive the same parked delivery."""
    global _pop_script
    redis = get_redis()
    if _pop_script is None:
        _pop_script = redis.register_script(load_lua("pop_retry"))
    settings = get_settings()
    keys = [_retry_key(s) for s in settings.severities]  # critical-first
    members = await _pop_script(keys=keys, args=[now_ms, limit])
    return [DeferredDelivery.from_member(m) for m in members]


def now_ms() -> int:
    return int(time.time() * 1000)


async def refresh_retry_depth_metrics() -> None:
    redis = get_redis()
    for sev in get_settings().severities:
        retry_queue_depth.labels(severity=sev).set(await redis.zcard(_retry_key(sev)))
