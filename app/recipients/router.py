"""Recipients admin HTTP surface: ``/v1/recipients/*``.

v1 ships create + get; full CRUD and the self-service admin UI are post-MVP
(00 §2 customer-facing deliverables).
"""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import require_api_key
from app.auth.dependencies import Principal
from app.db import get_session
from app.errors import NotFoundError
from app.recipients.models import ChannelConfig, Recipient, Subscription
from app.recipients.schemas import RecipientIn, RecipientOut

router = APIRouter(prefix="/v1/recipients", tags=["recipients"])


@router.post("", status_code=status.HTTP_201_CREATED, response_model=RecipientOut)
async def create_recipient(
    body: RecipientIn,
    principal: Principal = Depends(require_api_key),
    session: AsyncSession = Depends(get_session),
) -> RecipientOut:
    recipient = Recipient(tenant=body.tenant, name=body.name)
    session.add(recipient)
    await session.flush()
    for ch in body.channels:
        session.add(
            ChannelConfig(
                recipient_id=recipient.id,
                channel=ch.channel,
                target=ch.target,
                config=ch.config,
                enabled=ch.enabled,
            )
        )
    for sub in body.subscriptions:
        session.add(
            Subscription(
                recipient_id=recipient.id,
                tenant=sub.tenant,
                topic=sub.topic,
                min_severity=sub.min_severity,
            )
        )
    await session.commit()
    return RecipientOut(
        id=recipient.id, tenant=recipient.tenant, name=recipient.name, active=recipient.active
    )


@router.get("/{recipient_id}", response_model=RecipientOut)
async def get_recipient(
    recipient_id: UUID,
    principal: Principal = Depends(require_api_key),
    session: AsyncSession = Depends(get_session),
) -> RecipientOut:
    recipient = (
        await session.execute(select(Recipient).where(Recipient.id == recipient_id))
    ).scalar_one_or_none()
    if recipient is None:
        raise NotFoundError(f"recipient {recipient_id} not found")
    return RecipientOut(
        id=recipient.id, tenant=recipient.tenant, name=recipient.name, active=recipient.active
    )
