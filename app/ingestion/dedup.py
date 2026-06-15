"""Content-level deduplication (06).

Distinct from API idempotency (01): idempotency catches a producer retrying the
*same request*; dedup catches *different requests describing the same event* —
the detection tier firing repeatedly during one incident (06 §2).

A fingerprint is a stable SHA-256 over only the fields that define "same event":
``(tenant, topic, selected labels)``. Timestamps, free-form ``body`` and
``payload`` are deliberately excluded — including them would defeat matching
(06 §5). The label set is per-tenant configurable because "same event" means
different things to different customers.

The dedup decision is one atomic Redis ``SET NX EX``: the first alert claims the
fingerprint key for the window; later alerts find it claimed and are recorded as
``deduped`` pointing at the original (06 §6). Policies are cached two layers deep
(local TTL over Redis), like the rate-limit policy cache, with a TTL that bounds
cross-worker staleness to under a minute (06 §8).
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import get_sessionmaker
from app.errors import NotFoundError
from app.ingestion.models import DedupPolicy
from app.ingestion.schemas import AlertIn, DedupPolicyIn, Severity
from app.observability import get_logger
from app.redis_client import get_redis

log = get_logger(__name__)


def compute_fingerprint(
    tenant_id: str, topic: str, labels: dict, dedup_fields: list[str]
) -> str:
    """Stable hash of the fields that define "same event" (06 §5).

    Sorted fields make the result order-independent; a missing label contributes
    an empty value so two alerts that both omit a field still collide.
    """
    parts = [tenant_id, topic]
    for field in sorted(dedup_fields):
        parts.append(f"{field}={labels.get(field, '')}")
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()


@dataclass(frozen=True)
class ResolvedDedupPolicy:
    dedup_fields: list[str]
    window_seconds: int
    critical_bypass: bool
    enabled: bool
    fingerprint_version: int


def default_dedup_policy() -> ResolvedDedupPolicy:
    s = get_settings()
    return ResolvedDedupPolicy(
        dedup_fields=list(s.dedup_default_fields),
        window_seconds=s.dedup_window_seconds,
        critical_bypass=s.dedup_critical_bypass,
        enabled=True,
        fingerprint_version=s.dedup_fingerprint_version,
    )


# --------------------------------------------------------------------------- #
# Per-tenant policy cache (local TTL over Redis), mirroring the rate-limit cache
# --------------------------------------------------------------------------- #
_local: dict[str, tuple[list[dict], float]] = {}


def _cache_key(tenant: str) -> str:
    return f"{get_settings().dedup_policy_cache_key_prefix}:{tenant}"


def clear_local() -> None:
    """Drop the whole local layer (test hook / hard reset)."""
    _local.clear()


def _row_to_dict(row: DedupPolicy) -> dict:
    return {
        "topic": row.topic,
        "dedup_fields": list(row.dedup_fields or []),
        "window_seconds": row.window_seconds,
        "critical_bypass": row.critical_bypass,
        "enabled": row.enabled,
    }


async def _load_from_db(tenant: str) -> list[dict]:
    async with get_sessionmaker()() as session:
        rows = (
            (
                await session.execute(
                    select(DedupPolicy).where(
                        DedupPolicy.tenant_id == tenant,
                        DedupPolicy.deleted_at.is_(None),
                    )
                )
            )
            .scalars()
            .all()
        )
    return [_row_to_dict(r) for r in rows]


async def _tenant_policies(tenant: str) -> list[dict]:
    settings = get_settings()
    now = time.monotonic()

    entry = _local.get(tenant)
    if entry is not None and entry[1] > now:
        return entry[0]

    redis = get_redis()
    raw = await redis.get(_cache_key(tenant))
    if raw is not None:
        rows = json.loads(raw if isinstance(raw, str) else raw.decode())
    else:
        rows = await _load_from_db(tenant)
        await redis.set(
            _cache_key(tenant), json.dumps(rows), ex=settings.dedup_policy_cache_ttl_seconds
        )

    _local[tenant] = (rows, now + settings.dedup_policy_cache_ttl_seconds)
    return rows


async def resolve_dedup_policy(tenant: str, topic: str) -> ResolvedDedupPolicy:
    """Effective policy for a (tenant, topic): exact-topic row wins, else the
    tenant default (topic NULL), else the global default. Zero DB on cache hit."""
    rows = await _tenant_policies(tenant)
    exact: dict | None = None
    tenant_default: dict | None = None
    for row in rows:
        if row["topic"] == topic:
            exact = row
        elif row["topic"] is None:
            tenant_default = row
    chosen = exact or tenant_default
    if chosen is None:
        return default_dedup_policy()
    return ResolvedDedupPolicy(
        dedup_fields=chosen["dedup_fields"],
        window_seconds=chosen["window_seconds"],
        critical_bypass=chosen["critical_bypass"],
        enabled=chosen["enabled"],
        fingerprint_version=get_settings().dedup_fingerprint_version,
    )


async def invalidate_dedup_policies(tenant: str) -> None:
    await get_redis().delete(_cache_key(tenant))
    _local.pop(tenant, None)
    log.info("dedup.policies_invalidated", tenant=tenant)


# --------------------------------------------------------------------------- #
# The dedup decision (atomic SET NX), called from the ingest path
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DedupOutcome:
    decision: str  # "first" | "duplicate" | "skipped"
    original_id: str | None
    fingerprint_version: int


async def evaluate(alert_in: AlertIn, new_id: str) -> DedupOutcome:
    """Decide whether ``alert_in`` is the first occurrence of its event or a
    duplicate within the window. ``"skipped"`` means dedup didn't apply (disabled
    or critical bypass) — the caller dispatches normally."""
    settings = get_settings()
    policy = await resolve_dedup_policy(alert_in.tenant_id, alert_in.topic)
    version = policy.fingerprint_version

    if not policy.enabled:
        return DedupOutcome("skipped", None, version)
    if alert_in.severity is Severity.critical and policy.critical_bypass:
        return DedupOutcome("skipped", None, version)

    fingerprint = compute_fingerprint(
        alert_in.tenant_id, alert_in.topic, alert_in.labels, policy.dedup_fields
    )
    # Version in the key namespace so bumping the algorithm can't collide with
    # keys written by the old one (06 §7).
    key = f"{settings.dedup_key_prefix}:v{version}:{fingerprint}"
    redis = get_redis()
    claimed = await redis.set(key, new_id, nx=True, ex=policy.window_seconds)
    if claimed:
        return DedupOutcome("first", None, version)
    original_id = await redis.get(key)
    return DedupOutcome("duplicate", original_id, version)


# --------------------------------------------------------------------------- #
# Policy config CRUD (06 §2)
# --------------------------------------------------------------------------- #
async def list_dedup_policies(session: AsyncSession, *, tenant: str) -> list[DedupPolicy]:
    stmt = (
        select(DedupPolicy)
        .where(DedupPolicy.tenant_id == tenant, DedupPolicy.deleted_at.is_(None))
        .order_by(DedupPolicy.created_at)
    )
    return list((await session.execute(stmt)).scalars().all())


async def upsert_dedup_policy(
    session: AsyncSession, *, tenant: str, body: DedupPolicyIn
) -> DedupPolicy:
    """Create or replace the policy for a (tenant, topic) scope."""
    existing = (
        await session.execute(
            select(DedupPolicy).where(
                DedupPolicy.tenant_id == tenant,
                DedupPolicy.topic == body.topic,
                DedupPolicy.deleted_at.is_(None),
            )
        )
    ).scalar_one_or_none()

    if existing is not None:
        existing.dedup_fields = body.dedup_fields
        existing.window_seconds = body.window_seconds
        existing.critical_bypass = body.critical_bypass
        existing.enabled = body.enabled
        policy = existing
    else:
        policy = DedupPolicy(
            tenant_id=tenant,
            topic=body.topic,
            dedup_fields=body.dedup_fields,
            window_seconds=body.window_seconds,
            critical_bypass=body.critical_bypass,
            enabled=body.enabled,
        )
        session.add(policy)

    await session.commit()
    await session.refresh(policy)
    await invalidate_dedup_policies(tenant)
    return policy


async def delete_dedup_policy(session: AsyncSession, *, tenant: str, policy_id: UUID) -> None:
    from datetime import UTC, datetime

    policy = (
        await session.execute(
            select(DedupPolicy).where(
                DedupPolicy.id == policy_id,
                DedupPolicy.tenant_id == tenant,
                DedupPolicy.deleted_at.is_(None),
            )
        )
    ).scalar_one_or_none()
    if policy is None:
        raise NotFoundError(f"dedup policy {policy_id} not found")
    policy.deleted_at = datetime.now(UTC)
    await session.commit()
    await invalidate_dedup_policies(tenant)
