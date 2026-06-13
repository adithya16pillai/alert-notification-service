"""Redis priority queue: one ZSET per severity, drained highest-first with a
starvation guard and a visibility (in-flight) set (02 §5, §6).

Score = ``received_at`` Unix milliseconds, so each severity tier is FIFO while a
higher severity always preempts a lower one (00 §7.4, steps 4 & 6). A single Lua
script does the atomic pop: it INCRs a shared counter and, on every Nth pop,
drains lowest-severity-first so no severity is starved (02 §3). Every popped id
is moved into an in-flight ZSET keyed by a visibility deadline; the dispatcher
acks on success and the reaper re-queues anything still 'accepted' past its
deadline (02 §6).
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, cast

from redis.commands.core import AsyncScript
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.observability import get_logger
from app.observability.metrics import inflight_depth, inflight_reaped_total, queue_depth
from app.redis_client import get_redis, load_lua

if TYPE_CHECKING:  # import only for typing — avoids an app.ingestion import cycle
    from app.ingestion.schemas import Severity

log = get_logger(__name__)

_pop_script: AsyncScript | None = None


def _queue_key(severity: Severity | str) -> str:
    # Duck-type Severity (a StrEnum) vs a plain string so this module never has
    # to import app.ingestion at load time (would cycle via the ingestion router).
    value = severity.value if hasattr(severity, "value") else severity
    return f"{get_settings().queue_key_prefix}:{value}"


async def enqueue_alert(alert_id: str, severity: Severity | str, *, score: int) -> None:
    redis = get_redis()
    await redis.zadd(_queue_key(severity), {alert_id: score})


async def pop_priority(batch_size: int) -> list[str]:
    """Pop up to ``batch_size`` alert ids, highest severity first, atomically.

    Every returned id is also recorded in-flight with a visibility deadline; the
    caller must ``ack_inflight`` once the alert is durably handled, or the reaper
    will re-queue it (02 §6).
    """
    global _pop_script
    redis = get_redis()
    if _pop_script is None:
        _pop_script = redis.register_script(load_lua("pop_priority"))
    settings = get_settings()
    # settings.severities is critical-first; the Lua script drains KEYS in order,
    # so pass them critical -> info (highest priority popped first).
    keys = [_queue_key(s) for s in settings.severities]
    popped = await _pop_script(
        keys=keys,
        args=[
            batch_size,
            settings.starvation_counter_key,
            settings.queue_starvation_factor,
            settings.inflight_key,
            int(time.time() * 1000),
            settings.inflight_ttl_seconds * 1000,
        ],
    )
    return cast("list[str]", popped)  # decode_responses=True => str members


async def ack_inflight(alert_ids: list[str]) -> None:
    """Drop durably-handled alerts from the in-flight set so they aren't reaped."""
    if not alert_ids:
        return
    await get_redis().zrem(get_settings().inflight_key, *alert_ids)


async def reap_inflight(session: AsyncSession, *, limit: int = 500) -> int:
    """Re-queue alerts whose visibility deadline passed (worker died mid-batch).

    Postgres is the source of truth: only rows still ``status='accepted'`` are
    re-enqueued (a worker that made progress flips them to ``dispatched`` or a
    terminal state before its deadline). Each expired member is then cleared from
    the in-flight set, so a finished-but-unacked alert is simply forgotten rather
    than re-delivered. Re-enqueue uses the original ``received_at`` so the alert
    lands at the front of its severity (lowest score = oldest), per 02 §6.
    """
    from app.ingestion.models import Alert  # local import avoids an import cycle

    redis = get_redis()
    settings = get_settings()
    now_ms = int(time.time() * 1000)
    expired = cast(
        "list[str]",
        await redis.zrangebyscore(settings.inflight_key, 0, now_ms, start=0, num=limit),
    )
    if not expired:
        return 0

    rows = (await session.execute(select(Alert).where(Alert.id.in_(expired)))).scalars().all()
    by_id = {row.id: row for row in rows}

    requeued = 0
    for alert_id in expired:
        row = by_id.get(alert_id)
        if row is not None and row.status == "accepted":
            # row.severity is the StrEnum value; _queue_key duck-types it.
            await enqueue_alert(
                row.id, row.severity, score=int(row.received_at.timestamp() * 1000)
            )
            requeued += 1
        await redis.zrem(settings.inflight_key, alert_id)

    if requeued:
        inflight_reaped_total.inc(requeued)
        log.info("queue.reaped", count=requeued)
    return requeued


async def queue_depth_for(severity: Severity | str) -> int:
    """Current backlog of a single severity ZSET (used for backpressure, 02 §6)."""
    return await get_redis().zcard(_queue_key(severity))


async def refresh_queue_depth_metrics() -> None:
    redis = get_redis()
    settings = get_settings()
    for sev in settings.severities:
        depth = await redis.zcard(_queue_key(sev))
        queue_depth.labels(severity=sev).set(depth)
    inflight_depth.set(await redis.zcard(settings.inflight_key))
