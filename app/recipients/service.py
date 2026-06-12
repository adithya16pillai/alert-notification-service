"""Recipient + subscription resolution.

The dispatcher calls ``resolve_targets`` to expand an alert's (tenant, topic,
severity) into the concrete set of (recipient × channel) delivery targets.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.recipients.models import ChannelConfig, Recipient, Subscription
from app.recipients.schemas import ResolvedTarget


async def resolve_targets(
    session: AsyncSession, *, tenant: str, topic: str, severity: int
) -> list[ResolvedTarget]:
    """Find every enabled channel of every active recipient subscribed to this
    (tenant, topic) at or below the alert's severity threshold.
    """
    stmt = (
        select(ChannelConfig, Subscription.recipient_id)
        .join(Recipient, Recipient.id == ChannelConfig.recipient_id)
        .join(Subscription, Subscription.recipient_id == Recipient.id)
        .where(
            Recipient.active.is_(True),
            ChannelConfig.enabled.is_(True),
            Subscription.tenant == tenant,
            Subscription.topic == topic,
            Subscription.min_severity <= severity,
        )
    )
    rows = (await session.execute(stmt)).all()
    return [
        ResolvedTarget(
            recipient_id=cc.recipient_id,
            channel=cc.channel,
            target=cc.target,
            config=cc.config,
        )
        for cc, _ in rows
    ]
