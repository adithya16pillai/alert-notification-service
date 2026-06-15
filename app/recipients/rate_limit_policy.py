"""Resolve the token-bucket policy for a delivery, with caching (05 §3, §7).

A policy is the tuple ``(capacity, refill_per_sec, critical_bypass)``. The
effective policy for a ``(tenant, recipient, channel)`` delivery is the **most
specific** live row in ``recipients.rate_limit_policies``; if none match we fall
back to the global defaults in ``Settings``.

Specificity (highest wins)::

    (recipient, channel)  >  (recipient, *)  >  (*, channel)  >  (*, *)  >  default

The per-tenant policy set is cached two layers deep — a per-process dict over a
Redis JSON snapshot — exactly like the subscription cache, but TTL-only: policy
edits are far rarer than subscription edits, so the TTL is an acceptable
staleness bound and we skip the pub/sub machinery (05 §7).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass

from sqlalchemy import select

from app.config import get_settings
from app.db import get_sessionmaker
from app.observability import get_logger
from app.recipients.models import RateLimitPolicy

log = get_logger(__name__)


@dataclass(frozen=True)
class ResolvedRateLimit:
    capacity: int
    refill_per_sec: float
    critical_bypass: bool


def default_policy() -> ResolvedRateLimit:
    s = get_settings()
    return ResolvedRateLimit(
        capacity=s.rate_limit_capacity,
        refill_per_sec=s.rate_limit_refill_per_sec,
        critical_bypass=s.rate_limit_critical_bypass,
    )


# tenant -> (rows, monotonic_expiry). Each row is a plain dict (JSON round-trips).
_local: dict[str, tuple[list[dict], float]] = {}


def _key(tenant: str) -> str:
    return f"{get_settings().rate_limit_policy_cache_key_prefix}:{tenant}"


def clear_local() -> None:
    """Drop the entire local layer (test hook / hard reset)."""
    _local.clear()


def _row_to_dict(row: RateLimitPolicy) -> dict:
    return {
        "recipient_id": str(row.recipient_id) if row.recipient_id else None,
        "channel_kind": row.channel_kind,
        "capacity": row.capacity,
        "refill_per_sec": row.refill_per_sec,
        "critical_bypass": row.critical_bypass,
    }


async def _load_from_db(tenant: str) -> list[dict]:
    async with get_sessionmaker()() as session:
        rows = (
            (
                await session.execute(
                    select(RateLimitPolicy).where(
                        RateLimitPolicy.tenant_id == tenant,
                        RateLimitPolicy.deleted_at.is_(None),
                    )
                )
            )
            .scalars()
            .all()
        )
    return [_row_to_dict(r) for r in rows]


async def _tenant_policies(tenant: str) -> list[dict]:
    """Return the tenant's policy rows, hitting local -> Redis -> DB."""
    from app.redis_client import get_redis

    settings = get_settings()
    now = time.monotonic()

    entry = _local.get(tenant)
    if entry is not None and entry[1] > now:
        return entry[0]

    redis = get_redis()
    raw = await redis.get(_key(tenant))
    if raw is not None:
        rows = json.loads(raw if isinstance(raw, str) else raw.decode())
    else:
        rows = await _load_from_db(tenant)
        await redis.set(
            _key(tenant), json.dumps(rows), ex=settings.rate_limit_policy_cache_ttl_seconds
        )

    _local[tenant] = (rows, now + settings.rate_limit_policy_cache_ttl_seconds)
    return rows


def _specificity(row: dict, recipient_id: str, channel: str) -> int:
    """Score a row's match against the delivery, or -1 if it doesn't apply."""
    r, c = row["recipient_id"], row["channel_kind"]
    if r is not None and r != recipient_id:
        return -1
    if c is not None and c != channel:
        return -1
    return (2 if r is not None else 0) + (1 if c is not None else 0)


async def resolve_rate_limit(tenant: str, recipient_id: str, channel: str) -> ResolvedRateLimit:
    """The effective policy for one delivery. Zero DB calls on a cache hit."""
    rows = await _tenant_policies(tenant)
    best: dict | None = None
    best_score = -1
    for row in rows:
        score = _specificity(row, recipient_id, channel)
        if score > best_score:
            best, best_score = row, score
    if best is None:
        return default_policy()
    return ResolvedRateLimit(
        capacity=best["capacity"],
        refill_per_sec=best["refill_per_sec"],
        critical_bypass=best["critical_bypass"],
    )


async def invalidate_policies(tenant: str) -> None:
    """Drop a tenant's cached policies after a write (Redis key + local layer)."""
    from app.redis_client import get_redis

    await get_redis().delete(_key(tenant))
    _local.pop(tenant, None)
    log.info("rate_limit.policies_invalidated", tenant=tenant)
