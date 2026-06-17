"""Delivery-attempt persistence (the durable audit trail).

One row per (alert × recipient × channel) attempt — the "no alert silently lost"
evidence (00 §2). Terminal failures additionally land in the DLQ stream, which is
owned by :mod:`app.dlq` (07 §5).
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.audit.models import DeliveryAttempt


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
