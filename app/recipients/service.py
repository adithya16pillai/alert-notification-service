"""Recipient / channel / subscription service layer (03).

Two responsibilities:

1. **Admin CRUD** — tenant-scoped, soft-deleting, cursor-paginated. Every read
   and write is filtered by ``tenant_id`` so one tenant can never see or touch
   another's data; a miss is reported as ``404`` (not ``403``) so we don't leak
   the existence of another tenant's resource (03 §9).
2. **Hot-path resolution** — :func:`resolve_targets` expands an alert into its
   delivery targets through the subscription cache, with no DB call on a cache
   hit (03 §7).

Routing-affecting writes (anything that changes which channels an alert reaches)
call :func:`cache.invalidate`. Writes that can't change routing — creating or
renaming a recipient, adding an as-yet-unreferenced channel — deliberately don't,
to avoid needless cache churn (a subscription that later references a new channel
invalidates at *that* write).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import Select, select, tuple_, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.errors import NotFoundError, ValidationError
from app.recipients.cache import get_snapshot, invalidate
from app.recipients.matching import collect_targets
from app.recipients.models import Channel, Recipient, Subscription
from app.recipients.pagination import decode_cursor, encode_cursor
from app.recipients.schemas import (
    ChannelIn,
    RecipientPatch,
    ResolvedTarget,
    SubscriptionIn,
    SubscriptionPatch,
)


# --------------------------------------------------------------------------- #
# Pagination
# --------------------------------------------------------------------------- #
async def _paginate(
    session: AsyncSession, stmt: Select[Any], model: Any, *, cursor: str | None, limit: int
) -> tuple[list[Any], str | None]:
    """Apply keyset pagination to a tenant/soft-delete-filtered statement.

    Fetches ``limit + 1`` rows to know whether a next page exists without an
    extra ``COUNT`` and without ever emitting an empty trailing page (03 §6).
    The ``(created_at, id) < (cursor_at, cursor_id)`` row-value comparison is the
    keyset predicate — total-ordered by the id tiebreaker, O(log n) on the index.
    """
    if cursor:
        created_at, id_ = decode_cursor(cursor)
        try:
            cursor_id = UUID(id_)
        except ValueError as exc:
            raise ValidationError("malformed cursor", field="cursor") from exc
        stmt = stmt.where(tuple_(model.created_at, model.id) < (created_at, cursor_id))
    stmt = stmt.order_by(model.created_at.desc(), model.id.desc()).limit(limit + 1)
    rows = list((await session.execute(stmt)).scalars().all())

    has_more = len(rows) > limit
    items = rows[:limit]
    next_cursor = (
        encode_cursor(items[-1].created_at, items[-1].id) if has_more and items else None
    )
    return items, next_cursor


# --------------------------------------------------------------------------- #
# Scoped lookups (cross-tenant => 404)
# --------------------------------------------------------------------------- #
async def _live_recipient(session: AsyncSession, tenant: str, recipient_id: UUID) -> Recipient:
    row = (
        await session.execute(
            select(Recipient).where(
                Recipient.id == recipient_id,
                Recipient.tenant_id == tenant,
                Recipient.deleted_at.is_(None),
            )
        )
    ).scalar_one_or_none()
    if row is None:
        raise NotFoundError(f"recipient {recipient_id} not found")
    return row


async def _live_subscription(
    session: AsyncSession, tenant: str, subscription_id: UUID
) -> Subscription:
    row = (
        await session.execute(
            select(Subscription).where(
                Subscription.id == subscription_id,
                Subscription.tenant_id == tenant,
                Subscription.deleted_at.is_(None),
            )
        )
    ).scalar_one_or_none()
    if row is None:
        raise NotFoundError(f"subscription {subscription_id} not found")
    return row


async def _validate_channels(
    session: AsyncSession, recipient_id: UUID, channel_ids: list[UUID]
) -> None:
    """Every channel id must be a live channel of this recipient.

    Enforces the subscription invariant and blocks referencing another tenant's
    or recipient's channel ids (03 §9 cross-tenant isolation).
    """
    found = set(
        (
            await session.execute(
                select(Channel.id).where(
                    Channel.id.in_(channel_ids),
                    Channel.recipient_id == recipient_id,
                    Channel.deleted_at.is_(None),
                )
            )
        )
        .scalars()
        .all()
    )
    missing = [str(c) for c in channel_ids if c not in found]
    if missing:
        raise ValidationError(
            f"unknown or non-recipient channel_ids: {missing}", field="channel_ids"
        )


# --------------------------------------------------------------------------- #
# Recipients CRUD
# --------------------------------------------------------------------------- #
async def create_recipient(
    session: AsyncSession, *, tenant: str, name: str, timezone: str
) -> Recipient:
    recipient = Recipient(tenant_id=tenant, name=name, timezone=timezone)
    session.add(recipient)
    await session.commit()
    await session.refresh(recipient)
    return recipient


async def get_recipient(session: AsyncSession, *, tenant: str, recipient_id: UUID) -> Recipient:
    return await _live_recipient(session, tenant, recipient_id)


async def list_recipients(
    session: AsyncSession, *, tenant: str, cursor: str | None, limit: int
) -> tuple[list[Recipient], str | None]:
    stmt = select(Recipient).where(
        Recipient.tenant_id == tenant, Recipient.deleted_at.is_(None)
    )
    return await _paginate(session, stmt, Recipient, cursor=cursor, limit=limit)


async def patch_recipient(
    session: AsyncSession, *, tenant: str, recipient_id: UUID, patch: RecipientPatch
) -> Recipient:
    recipient = await _live_recipient(session, tenant, recipient_id)
    if patch.name is not None:
        recipient.name = patch.name
    if patch.timezone is not None:
        recipient.timezone = patch.timezone
    await session.commit()
    await session.refresh(recipient)
    return recipient


async def delete_recipient(session: AsyncSession, *, tenant: str, recipient_id: UUID) -> None:
    """Soft delete a recipient and cascade to its channels and subscriptions.

    History is preserved (rows stay, ``deleted_at`` is stamped) so the audit
    trail survives (03 §8). Routing changes, so the cache is invalidated.
    """
    recipient = await _live_recipient(session, tenant, recipient_id)
    now = datetime.now(UTC)
    recipient.deleted_at = now
    await session.execute(
        update(Channel)
        .where(Channel.recipient_id == recipient_id, Channel.deleted_at.is_(None))
        .values(deleted_at=now)
    )
    await session.execute(
        update(Subscription)
        .where(Subscription.recipient_id == recipient_id, Subscription.deleted_at.is_(None))
        .values(deleted_at=now)
    )
    await session.commit()
    await invalidate(tenant)


# --------------------------------------------------------------------------- #
# Channels
# --------------------------------------------------------------------------- #
async def list_channels(
    session: AsyncSession, *, tenant: str, recipient_id: UUID, cursor: str | None, limit: int
) -> tuple[list[Channel], str | None]:
    await _live_recipient(session, tenant, recipient_id)  # scope + 404
    stmt = select(Channel).where(
        Channel.recipient_id == recipient_id, Channel.deleted_at.is_(None)
    )
    return await _paginate(session, stmt, Channel, cursor=cursor, limit=limit)


async def add_channel(
    session: AsyncSession, *, tenant: str, recipient_id: UUID, body: ChannelIn
) -> Channel:
    await _live_recipient(session, tenant, recipient_id)  # scope + 404
    channel = Channel(
        recipient_id=recipient_id, kind=body.kind, address=body.address, config=body.config
    )
    session.add(channel)
    await session.commit()
    await session.refresh(channel)
    return channel


async def delete_channel(
    session: AsyncSession, *, tenant: str, recipient_id: UUID, channel_id: UUID
) -> None:
    await _live_recipient(session, tenant, recipient_id)  # scope + 404
    channel = (
        await session.execute(
            select(Channel).where(
                Channel.id == channel_id,
                Channel.recipient_id == recipient_id,
                Channel.deleted_at.is_(None),
            )
        )
    ).scalar_one_or_none()
    if channel is None:
        raise NotFoundError(f"channel {channel_id} not found")
    channel.deleted_at = datetime.now(UTC)
    await session.commit()
    # A deleted channel may be referenced by live subscriptions; drop the cache so
    # the dispatcher stops resolving it within 1s (03 §7).
    await invalidate(tenant)


# --------------------------------------------------------------------------- #
# Subscriptions
# --------------------------------------------------------------------------- #
async def list_subscriptions(
    session: AsyncSession, *, tenant: str, cursor: str | None, limit: int
) -> tuple[list[Subscription], str | None]:
    stmt = select(Subscription).where(
        Subscription.tenant_id == tenant, Subscription.deleted_at.is_(None)
    )
    return await _paginate(session, stmt, Subscription, cursor=cursor, limit=limit)


async def create_subscription(
    session: AsyncSession, *, tenant: str, body: SubscriptionIn
) -> Subscription:
    await _live_recipient(session, tenant, body.recipient_id)  # scope + 404
    await _validate_channels(session, body.recipient_id, body.channel_ids)
    subscription = Subscription(
        recipient_id=body.recipient_id,
        tenant_id=tenant,
        topic_pattern=body.topic_pattern,
        min_severity=body.min_severity.value,
        channel_ids=[str(c) for c in body.channel_ids],
        enabled=body.enabled,
    )
    session.add(subscription)
    await session.commit()
    await session.refresh(subscription)
    await invalidate(tenant)
    return subscription


async def patch_subscription(
    session: AsyncSession, *, tenant: str, subscription_id: UUID, patch: SubscriptionPatch
) -> Subscription:
    subscription = await _live_subscription(session, tenant, subscription_id)
    if patch.channel_ids is not None:
        await _validate_channels(session, subscription.recipient_id, patch.channel_ids)
        subscription.channel_ids = [str(c) for c in patch.channel_ids]
    if patch.topic_pattern is not None:
        subscription.topic_pattern = patch.topic_pattern
    if patch.min_severity is not None:
        subscription.min_severity = patch.min_severity.value
    if patch.enabled is not None:
        subscription.enabled = patch.enabled
    await session.commit()
    await session.refresh(subscription)
    await invalidate(tenant)
    return subscription


async def delete_subscription(
    session: AsyncSession, *, tenant: str, subscription_id: UUID
) -> None:
    subscription = await _live_subscription(session, tenant, subscription_id)
    subscription.deleted_at = datetime.now(UTC)
    await session.commit()
    await invalidate(tenant)


# --------------------------------------------------------------------------- #
# Hot-path resolution (03 §7)
# --------------------------------------------------------------------------- #
async def resolve_targets(
    session: AsyncSession, *, tenant: str, topic: str, severity: str
) -> list[ResolvedTarget]:
    """Expand an alert into its deduped delivery targets via the cache.

    On a cache hit this performs no DB query at all; on a miss it rebuilds the
    tenant snapshot in two queries (03 §7, §9).
    """
    snapshot = await get_snapshot(session, tenant)
    return collect_targets(snapshot, topic=topic, severity=severity)
