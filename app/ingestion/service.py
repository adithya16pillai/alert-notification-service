"""Ingestion service: idempotency, durable write, enqueue (01 §7).

Two-layer idempotency:
  1. Fast path — Redis ``SET NX EX 86400 idem:{tenant}:{key}`` holds the ULID.
  2. Durable safety net — the ``uq_alerts_idempotency`` Postgres unique index,
     so duplicates are still rejected if Redis is wiped.

Order is write-ahead-log: Postgres commit first, then Redis enqueue. A failed
enqueue leaves a ``status='accepted'`` row that the janitor re-enqueues (01 §8).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from ulid import ULID

from app.config import get_settings
from app.errors import IdempotencyConflict
from app.ingestion.models import Alert
from app.ingestion.schemas import AlertIn
from app.observability import get_logger
from app.observability.metrics import alerts_ingested_total
from app.queue.priority_queue import enqueue_alert
from app.redis_client import get_redis

log = get_logger(__name__)


@dataclass(frozen=True)
class IngestResult:
    alert_id: str
    replay: bool  # True => idempotent replay (HTTP 200), False => new (HTTP 202)


def _fingerprint(a: AlertIn) -> str:
    """Stable hash of the request-derived fields, for conflict detection."""
    canonical = json.dumps(
        {
            "tenant_id": a.tenant_id,
            "source": a.source,
            "severity": a.severity.value,
            "topic": a.topic,
            "title": a.title,
            "body": a.body,
            "labels": a.labels,
            "payload": a.payload,
            "occurred_at": a.occurred_at.isoformat(),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def _row_fingerprint(row: Alert) -> str:
    canonical = json.dumps(
        {
            "tenant_id": row.tenant_id,
            "source": row.source,
            "severity": row.severity,
            "topic": row.topic,
            "title": row.title,
            "body": row.body,
            "labels": row.labels,
            "payload": row.payload,
            "occurred_at": row.occurred_at.isoformat(),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


async def _load_existing(session: AsyncSession, tenant_id: str, key: str) -> Alert | None:
    stmt = select(Alert).where(Alert.tenant_id == tenant_id, Alert.idempotency_key == key)
    return (await session.execute(stmt)).scalar_one_or_none()


async def ingest_alert(
    session: AsyncSession, alert_in: AlertIn, idempotency_key: str
) -> IngestResult:
    settings = get_settings()
    redis = get_redis()
    fingerprint = _fingerprint(alert_in)
    redis_key = f"idem:{alert_in.tenant_id}:{idempotency_key}"
    new_id = str(ULID())

    # --- Layer 1: Redis fast path (CP per key, single shard, atomic) ---
    try:
        claimed = await asyncio.wait_for(
            redis.set(redis_key, new_id, nx=True, ex=settings.idempotency_ttl_seconds),
            timeout=settings.channel_timeout_seconds,
        )
    except (TimeoutError, asyncio.TimeoutError):
        claimed = None  # fall through to the DB safety net

    if not claimed:
        existing = await _load_existing(session, alert_in.tenant_id, idempotency_key)
        if existing is not None:
            if _row_fingerprint(existing) != fingerprint:
                raise IdempotencyConflict(
                    f"idempotency key {idempotency_key} reused with a different payload",
                    field="Idempotency-Key",
                )
            log.info("ingest.idempotent_replay", alert_id=existing.id, key=idempotency_key)
            return IngestResult(alert_id=existing.id, replay=True)
        # Redis claimed by a sibling request still mid-insert; use its id.
        existing_id = await redis.get(redis_key)
        new_id = existing_id or new_id

    # --- Durable write (before the 202 — no fire-and-pray) ---
    alert = Alert(
        id=new_id,
        tenant_id=alert_in.tenant_id,
        source=alert_in.source,
        severity=alert_in.severity.value,
        topic=alert_in.topic,
        title=alert_in.title,
        body=alert_in.body,
        labels=alert_in.labels,
        payload=alert_in.payload,
        occurred_at=alert_in.occurred_at,
        status="accepted",
        idempotency_key=idempotency_key,
    )
    session.add(alert)
    try:
        await session.commit()
    except IntegrityError:
        # --- Layer 2: DB unique index caught a duplicate (Redis was cold) ---
        await session.rollback()
        existing = await _load_existing(session, alert_in.tenant_id, idempotency_key)
        if existing is None:
            raise
        if _row_fingerprint(existing) != fingerprint:
            raise IdempotencyConflict(
                f"idempotency key {idempotency_key} reused with a different payload",
                field="Idempotency-Key",
            ) from None
        return IngestResult(alert_id=existing.id, replay=True)

    # --- Enqueue (write-ahead-log: notification after durability) ---
    # The row is already durable, so we honour the 2xx contract even if enqueue
    # fails — the janitor re-enqueues stale 'accepted' rows within 30s (01 §8).
    try:
        await enqueue_alert(alert.id, alert_in.severity, score=int(time.time() * 1000))
    except Exception as exc:  # noqa: BLE001
        log.warning("ingest.enqueue_failed", alert_id=alert.id, error=str(exc))

    alerts_ingested_total.labels(severity=alert_in.severity.value).inc()
    log.info("ingest.accepted", alert_id=alert.id, severity=alert_in.severity.value)
    return IngestResult(alert_id=alert.id, replay=False)
