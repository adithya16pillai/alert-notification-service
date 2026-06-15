"""Recipients admin HTTP surface (03 §6).

Two routers: ``/v1/recipients`` (recipients + their channels) and
``/v1/subscriptions``. Every route resolves the caller's tenant via
:func:`require_tenant` and passes it to the service, which scopes every query —
cross-tenant access surfaces as ``404`` (03 §9). List endpoints are cursor
paginated and reject ``limit > 200`` with ``400`` (03 §4).
"""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import require_tenant
from app.db import get_session
from app.recipients import service
from app.recipients.pagination import validate_limit
from app.recipients.schemas import (
    ChannelIn,
    ChannelOut,
    Page,
    RateLimitPolicyIn,
    RateLimitPolicyOut,
    RecipientIn,
    RecipientOut,
    RecipientPatch,
    SubscriptionIn,
    SubscriptionOut,
    SubscriptionPatch,
)

router = APIRouter(prefix="/v1/recipients", tags=["recipients"])
subscriptions_router = APIRouter(prefix="/v1/subscriptions", tags=["subscriptions"])
rate_limit_router = APIRouter(prefix="/v1/rate-limit-policies", tags=["rate-limit"])


# --------------------------------------------------------------------------- #
# Recipients
# --------------------------------------------------------------------------- #
@router.post("", status_code=status.HTTP_201_CREATED, response_model=RecipientOut)
async def create_recipient(
    body: RecipientIn,
    tenant: str = Depends(require_tenant),
    session: AsyncSession = Depends(get_session),
) -> RecipientOut:
    recipient = await service.create_recipient(
        session, tenant=tenant, name=body.name, timezone=body.timezone
    )
    return RecipientOut.model_validate(recipient)


@router.get("", response_model=Page[RecipientOut])
async def list_recipients(
    cursor: str | None = Query(default=None),
    limit: int | None = Query(default=None),
    tenant: str = Depends(require_tenant),
    session: AsyncSession = Depends(get_session),
) -> Page[RecipientOut]:
    items, next_cursor = await service.list_recipients(
        session, tenant=tenant, cursor=cursor, limit=validate_limit(limit)
    )
    return Page[RecipientOut](
        items=[RecipientOut.model_validate(r) for r in items], next_cursor=next_cursor
    )


@router.get("/{recipient_id}", response_model=RecipientOut)
async def get_recipient(
    recipient_id: UUID,
    tenant: str = Depends(require_tenant),
    session: AsyncSession = Depends(get_session),
) -> RecipientOut:
    recipient = await service.get_recipient(session, tenant=tenant, recipient_id=recipient_id)
    return RecipientOut.model_validate(recipient)


@router.patch("/{recipient_id}", response_model=RecipientOut)
async def patch_recipient(
    recipient_id: UUID,
    body: RecipientPatch,
    tenant: str = Depends(require_tenant),
    session: AsyncSession = Depends(get_session),
) -> RecipientOut:
    recipient = await service.patch_recipient(
        session, tenant=tenant, recipient_id=recipient_id, patch=body
    )
    return RecipientOut.model_validate(recipient)


@router.delete("/{recipient_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_recipient(
    recipient_id: UUID,
    tenant: str = Depends(require_tenant),
    session: AsyncSession = Depends(get_session),
) -> Response:
    await service.delete_recipient(session, tenant=tenant, recipient_id=recipient_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# --------------------------------------------------------------------------- #
# Channels (nested under a recipient)
# --------------------------------------------------------------------------- #
@router.get("/{recipient_id}/channels", response_model=Page[ChannelOut])
async def list_channels(
    recipient_id: UUID,
    cursor: str | None = Query(default=None),
    limit: int | None = Query(default=None),
    tenant: str = Depends(require_tenant),
    session: AsyncSession = Depends(get_session),
) -> Page[ChannelOut]:
    items, next_cursor = await service.list_channels(
        session,
        tenant=tenant,
        recipient_id=recipient_id,
        cursor=cursor,
        limit=validate_limit(limit),
    )
    return Page[ChannelOut](
        items=[ChannelOut.model_validate(c) for c in items], next_cursor=next_cursor
    )


@router.post(
    "/{recipient_id}/channels", status_code=status.HTTP_201_CREATED, response_model=ChannelOut
)
async def add_channel(
    recipient_id: UUID,
    body: ChannelIn,
    tenant: str = Depends(require_tenant),
    session: AsyncSession = Depends(get_session),
) -> ChannelOut:
    channel = await service.add_channel(
        session, tenant=tenant, recipient_id=recipient_id, body=body
    )
    return ChannelOut.model_validate(channel)


@router.delete(
    "/{recipient_id}/channels/{channel_id}", status_code=status.HTTP_204_NO_CONTENT
)
async def delete_channel(
    recipient_id: UUID,
    channel_id: UUID,
    tenant: str = Depends(require_tenant),
    session: AsyncSession = Depends(get_session),
) -> Response:
    await service.delete_channel(
        session, tenant=tenant, recipient_id=recipient_id, channel_id=channel_id
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# --------------------------------------------------------------------------- #
# Subscriptions
# --------------------------------------------------------------------------- #
@subscriptions_router.get("", response_model=Page[SubscriptionOut])
async def list_subscriptions(
    cursor: str | None = Query(default=None),
    limit: int | None = Query(default=None),
    tenant: str = Depends(require_tenant),
    session: AsyncSession = Depends(get_session),
) -> Page[SubscriptionOut]:
    items, next_cursor = await service.list_subscriptions(
        session, tenant=tenant, cursor=cursor, limit=validate_limit(limit)
    )
    return Page[SubscriptionOut](
        items=[SubscriptionOut.model_validate(s) for s in items], next_cursor=next_cursor
    )


@subscriptions_router.post("", status_code=status.HTTP_201_CREATED, response_model=SubscriptionOut)
async def create_subscription(
    body: SubscriptionIn,
    tenant: str = Depends(require_tenant),
    session: AsyncSession = Depends(get_session),
) -> SubscriptionOut:
    subscription = await service.create_subscription(session, tenant=tenant, body=body)
    return SubscriptionOut.model_validate(subscription)


@subscriptions_router.patch("/{subscription_id}", response_model=SubscriptionOut)
async def patch_subscription(
    subscription_id: UUID,
    body: SubscriptionPatch,
    tenant: str = Depends(require_tenant),
    session: AsyncSession = Depends(get_session),
) -> SubscriptionOut:
    subscription = await service.patch_subscription(
        session, tenant=tenant, subscription_id=subscription_id, patch=body
    )
    return SubscriptionOut.model_validate(subscription)


@subscriptions_router.delete("/{subscription_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_subscription(
    subscription_id: UUID,
    tenant: str = Depends(require_tenant),
    session: AsyncSession = Depends(get_session),
) -> Response:
    await service.delete_subscription(session, tenant=tenant, subscription_id=subscription_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# --------------------------------------------------------------------------- #
# Rate-limit policies (05 §2 config API)
# --------------------------------------------------------------------------- #
@rate_limit_router.get("", response_model=list[RateLimitPolicyOut])
async def list_policies(
    tenant: str = Depends(require_tenant),
    session: AsyncSession = Depends(get_session),
) -> list[RateLimitPolicyOut]:
    rows = await service.list_rate_limit_policies(session, tenant=tenant)
    return [RateLimitPolicyOut.model_validate(r) for r in rows]


@rate_limit_router.put("", response_model=RateLimitPolicyOut)
async def upsert_policy(
    body: RateLimitPolicyIn,
    tenant: str = Depends(require_tenant),
    session: AsyncSession = Depends(get_session),
) -> RateLimitPolicyOut:
    policy = await service.upsert_rate_limit_policy(session, tenant=tenant, body=body)
    return RateLimitPolicyOut.model_validate(policy)


@rate_limit_router.delete("/{policy_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_policy(
    policy_id: UUID,
    tenant: str = Depends(require_tenant),
    session: AsyncSession = Depends(get_session),
) -> Response:
    await service.delete_rate_limit_policy(session, tenant=tenant, policy_id=policy_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
