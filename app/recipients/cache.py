"""Two-layer subscription cache + pub/sub invalidation (03 §7).

Layer 1 — a per-process in-memory dict (``_local``). Zero-latency, but each
worker process has its own copy, so it must be invalidated explicitly.
Layer 2 — Redis key ``subs:tenant:{tenant}`` holding the JSON snapshot, TTL 60s.

On a write to any ``recipients.*`` row we (a) delete the Redis key and (b)
publish the tenant id on ``cache:subs:invalidate``. Every worker subscribes and
drops its local entry, so a change is live across all workers in well under 1s
(03 §4). The 60s TTL is the hard upper bound on staleness if pub/sub is down.

The snapshot is built in **exactly two queries** (subscriptions, then the
channels they reference) — see :func:`build_snapshot` — so resolving one alert's
recipients-with-channels is never N+1 (03 §9).
"""

from __future__ import annotations

import time
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.observability import get_logger
from app.observability.metrics import (
    subscription_cache_invalidations_total,
    subscription_cache_ops_total,
)
from app.recipients.models import Channel, Recipient, Subscription
from app.recipients.snapshot import SnapChannel, SnapSubscription, TenantSnapshot
from app.redis_client import get_redis

log = get_logger(__name__)

# tenant -> (snapshot, monotonic_expiry). The local layer carries the same
# logical TTL as Redis so it self-heals even if a pub/sub message is missed.
_local: dict[str, tuple[TenantSnapshot, float]] = {}


def _key(tenant: str) -> str:
    return f"{get_settings().subs_cache_key_prefix}:{tenant}"


def _drop_local(tenant: str) -> None:
    _local.pop(tenant, None)


def clear_local() -> None:
    """Drop the entire local layer (test hook / hard reset)."""
    _local.clear()


async def build_snapshot(session: AsyncSession, tenant: str) -> TenantSnapshot:
    """Build a tenant's routing snapshot from Postgres in two queries (03 §9)."""
    # Query 1: live, enabled subscriptions for the tenant.
    subs = (
        (
            await session.execute(
                select(Subscription).where(
                    Subscription.tenant_id == tenant,
                    Subscription.enabled.is_(True),
                    Subscription.deleted_at.is_(None),
                )
            )
        )
        .scalars()
        .all()
    )

    wanted: list[str] = []
    seen: set[str] = set()
    for sub in subs:
        for cid in sub.channel_ids or []:
            if cid not in seen:
                seen.add(cid)
                wanted.append(cid)

    channels: dict[str, SnapChannel] = {}
    if wanted:
        # Query 2: the referenced channels, scoped to live channels of live
        # recipients in this tenant. The join is what keeps a soft-deleted
        # recipient from matching new alerts (03 §9 acceptance criteria).
        rows = (
            (
                await session.execute(
                    select(Channel)
                    .join(Recipient, Recipient.id == Channel.recipient_id)
                    .where(
                        Channel.id.in_([UUID(c) for c in wanted]),
                        Channel.deleted_at.is_(None),
                        Recipient.deleted_at.is_(None),
                        Recipient.tenant_id == tenant,
                    )
                )
            )
            .scalars()
            .all()
        )
        channels = {
            str(c.id): SnapChannel(
                recipient_id=c.recipient_id,
                kind=c.kind,
                address=c.address,
                config=c.config or {},
            )
            for c in rows
        }

    return TenantSnapshot(
        subscriptions=[
            SnapSubscription(
                topic_pattern=s.topic_pattern,
                min_severity=s.min_severity,
                channel_ids=list(s.channel_ids or []),
            )
            for s in subs
        ],
        channels=channels,
    )


async def get_snapshot(session: AsyncSession, tenant: str) -> TenantSnapshot:
    """Return a tenant's snapshot, hitting local -> Redis -> DB in that order."""
    settings = get_settings()
    now = time.monotonic()

    # Layer 1: local process cache.
    entry = _local.get(tenant)
    if entry is not None and entry[1] > now:
        subscription_cache_ops_total.labels(result="hit_local").inc()
        return entry[0]

    # Layer 2: Redis.
    raw = await get_redis().get(_key(tenant))
    if raw is not None:
        # decode_responses=True yields str, but the stub is bytes|str — normalise.
        snapshot = TenantSnapshot.from_json(raw if isinstance(raw, str) else raw.decode())
        _local[tenant] = (snapshot, now + settings.subs_cache_ttl_seconds)
        subscription_cache_ops_total.labels(result="hit_redis").inc()
        return snapshot

    # Miss: rebuild from Postgres and populate both layers.
    snapshot = await build_snapshot(session, tenant)
    await get_redis().set(
        _key(tenant), snapshot.to_json(), ex=settings.subs_cache_ttl_seconds
    )
    _local[tenant] = (snapshot, now + settings.subs_cache_ttl_seconds)
    subscription_cache_ops_total.labels(result="miss").inc()
    return snapshot


async def invalidate(tenant: str) -> None:
    """Invalidate a tenant's cache after a write: drop Redis key, publish, drop local."""
    settings = get_settings()
    redis = get_redis()
    await redis.delete(_key(tenant))
    await redis.publish(settings.subs_invalidate_channel, tenant)
    _drop_local(tenant)
    subscription_cache_invalidations_total.inc()
    log.info("subs.cache_invalidated", tenant=tenant)


async def listen_for_invalidations(*, ready: object = None) -> None:
    """Subscribe to the invalidation channel and drop local entries (run in workers).

    ``ready`` (optional ``asyncio.Event``) is set once the subscription is live —
    lets tests await readiness before publishing without racing.
    """
    settings = get_settings()
    pubsub = get_redis().pubsub()
    await pubsub.subscribe(settings.subs_invalidate_channel)
    if ready is not None and hasattr(ready, "set"):
        ready.set()
    try:
        async for message in pubsub.listen():
            if message.get("type") != "message":
                continue
            tenant = message["data"]
            if isinstance(tenant, bytes):
                tenant = tenant.decode("utf-8")
            _drop_local(tenant)
            log.info("subs.cache_dropped_local", tenant=tenant)
    finally:
        await pubsub.unsubscribe(settings.subs_invalidate_channel)
        await pubsub.aclose()
