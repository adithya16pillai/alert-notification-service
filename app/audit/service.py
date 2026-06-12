"""Delivery-attempt persistence + DLQ push.

The DLQ is a Redis stream (00 §7.4 step 10) so failed jobs survive worker
restarts and can be inspected / replayed by an operator.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.audit.models import DeliveryAttempt
from app.observability.metrics import dlq_depth
from app.redis_client import get_redis

DLQ_STREAM = "dlq:delivery"


async def record_attempt(
    session: AsyncSession,
    *,
    alert_id: str,
    recipient_id: UUID,
    channel: str,
    status: str,
    retry_count: int = 0,
    provider_id: str | None = None,
    last_error: str | None = None,
) -> DeliveryAttempt:
    attempt = DeliveryAttempt(
        alert_id=alert_id,
        recipient_id=recipient_id,
        channel=channel,
        status=status,
        retry_count=retry_count,
        provider_id=provider_id,
        last_error=last_error,
    )
    session.add(attempt)
    await session.commit()
    await session.refresh(attempt)
    return attempt


async def list_attempts(session: AsyncSession, alert_id: str) -> list[DeliveryAttempt]:
    stmt = (
        select(DeliveryAttempt)
        .where(DeliveryAttempt.alert_id == alert_id)
        .order_by(DeliveryAttempt.created_at)
    )
    return list((await session.execute(stmt)).scalars().all())


async def push_to_dlq(alert_id: str, recipient_id: UUID, channel: str, error: str) -> None:
    redis = get_redis()
    await redis.xadd(
        DLQ_STREAM,
        {
            "alert_id": alert_id,
            "recipient_id": str(recipient_id),
            "channel": channel,
            "error": error,
        },
    )
    dlq_depth.set(await redis.xlen(DLQ_STREAM))
