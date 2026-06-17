"""DLQ stream operations: push, list, get, replay, delete (07 §5).

The DLQ is a Redis stream (``dlq:alerts``) capped with ``MAXLEN ~`` so it
self-trims to roughly the retention window; older entries are exported to S3 by a
nightly job (07 §5.2). Each entry carries everything needed to *understand* the
failure (reason + truncated last error + per-attempt history) and to *replay* it
(the full target tuple), so replay reconstructs a delivery without touching the
original alert's fanout.

Entry reasons (07 §5.1), one queue for all three (07 §7):
  - ``exhausted_retries``  — transient failures past the channel's retry budget.
  - ``permanent_failure``  — a 4xx / bad-address / auth error; retrying is futile.
  - ``rate_limit_expired`` — parked for rate limiting longer than the defer cap.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from app.audit.service import record_attempt
from app.config import get_settings
from app.errors import NotFoundError
from app.observability import get_logger
from app.observability.metrics import dlq_depth, dlq_pushed_total, dlq_replays_total
from app.queue.retry_queue import DeferredDelivery, defer, now_ms
from app.redis_client import get_redis

log = get_logger(__name__)


@dataclass(frozen=True)
class DlqEntry:
    stream_id: str
    alert_id: str
    recipient_id: str
    channel: str
    target: str
    severity: str
    tenant: str
    reason: str
    last_error: str
    attempt_history: list[dict]
    config: dict


def _stream() -> str:
    return get_settings().dlq_stream


async def _refresh_depth() -> None:
    dlq_depth.set(await get_redis().xlen(_stream()))


async def push(
    *,
    alert_id: str,
    recipient_id: str,
    channel: str,
    target: str,
    severity: str,
    tenant: str,
    reason: str,
    last_error: str | None,
    attempt_history: list[dict] | None = None,
    config: dict | None = None,
) -> str:
    """Append a terminal failure to the DLQ. Returns the stream entry id.

    ``last_error`` is truncated (07 §5.2: a runaway provider message must not
    bloat the stream) and the full per-attempt history is stored as JSON so the
    on-call sees *what went wrong* across every attempt, not just the last."""
    settings = get_settings()
    fields = {
        "alert_id": alert_id,
        "recipient_id": recipient_id,
        "channel": channel,
        "target": target,
        "severity": severity,
        "tenant": tenant,
        "reason": reason,
        "last_error": (last_error or "")[: settings.dlq_max_error_bytes],
        "attempt_history": json.dumps(attempt_history or [], separators=(",", ":")),
        "config": json.dumps(config or {}, separators=(",", ":")),
    }
    stream_id = await get_redis().xadd(
        settings.dlq_stream, fields, maxlen=settings.dlq_maxlen, approximate=True
    )
    dlq_pushed_total.labels(channel=channel, reason=reason).inc()
    await _refresh_depth()
    log.info("dlq.pushed", stream_id=stream_id, alert_id=alert_id, channel=channel, reason=reason)
    return stream_id


def _parse(stream_id: str, fields: dict) -> DlqEntry:
    return DlqEntry(
        stream_id=stream_id,
        alert_id=fields.get("alert_id", ""),
        recipient_id=fields.get("recipient_id", ""),
        channel=fields.get("channel", ""),
        target=fields.get("target", ""),
        severity=fields.get("severity", ""),
        tenant=fields.get("tenant", ""),
        reason=fields.get("reason", ""),
        last_error=fields.get("last_error", ""),
        attempt_history=json.loads(fields.get("attempt_history", "[]")),
        config=json.loads(fields.get("config", "{}")),
    )


async def list_entries(
    *, cursor: str | None = None, limit: int = 50
) -> tuple[list[DlqEntry], str | None]:
    """Newest-first page of DLQ entries with cursor pagination (03 §4 contract).

    The cursor is an exclusive stream-id bound; ``next_cursor`` is non-null only
    when more entries remain, so the client stops when it goes null."""
    redis = get_redis()
    # `(id` is an exclusive max in XREVRANGE — the cursor is the last id we showed.
    upper = f"({cursor}" if cursor else "+"
    raw = await redis.xrevrange(_stream(), max=upper, min="-", count=limit + 1)
    entries = [_parse(sid, fields) for sid, fields in raw]
    next_cursor = None
    if len(entries) > limit:
        entries = entries[:limit]
        next_cursor = entries[-1].stream_id
    return entries, next_cursor


async def get_entry(stream_id: str) -> DlqEntry | None:
    raw = await get_redis().xrange(_stream(), min=stream_id, max=stream_id, count=1)
    return _parse(raw[0][0], raw[0][1]) if raw else None


async def delete_entry(stream_id: str) -> bool:
    """Acknowledge / give up on an entry (07 §5.3). Returns False if it was
    already gone (idempotent for the operator)."""
    removed = await get_redis().xdel(_stream(), stream_id)
    await _refresh_depth()
    return bool(removed)


async def replay(session: AsyncSession, stream_id: str, *, actor: str) -> DlqEntry:
    """Re-attempt one abandoned delivery (07 §5.4):

    1. read the entry; 2. open a fresh ``delivery_attempts`` row (attempt 1);
    3. re-park the delivery on the retry queue, due now; 4. remove the DLQ entry
    so it can't replay twice; 5. audit who replayed what.

    We re-park only this (recipient, channel) delivery rather than re-ingesting
    the whole alert — replaying must not re-page every recipient. Step 4 uses
    ``XDEL`` (no consumer group is in play, so there is nothing to ``XACK``)."""
    entry = await get_entry(stream_id)
    if entry is None:
        raise NotFoundError(f"dlq entry {stream_id} not found")

    from uuid import UUID

    await record_attempt(
        session,
        alert_id=entry.alert_id,
        recipient_id=UUID(entry.recipient_id),
        channel=entry.channel,
        status="pending",
        retry_count=0,
    )
    await defer(
        DeferredDelivery(
            alert_id=entry.alert_id,
            tenant=entry.tenant,
            recipient_id=entry.recipient_id,
            channel=entry.channel,
            target=entry.target,
            severity=entry.severity,
            first_deferred_ms=now_ms(),
            config=entry.config,
            attempt_no=0,
            reason="retry",
        ),
        due_ms=now_ms(),
    )
    await delete_entry(stream_id)
    dlq_replays_total.inc()
    log.info("dlq.replayed", stream_id=stream_id, alert_id=entry.alert_id, actor=actor)
    return entry
