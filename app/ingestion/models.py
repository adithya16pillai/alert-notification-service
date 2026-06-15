"""ORM model for ``ingestion.alerts`` — durable system of record (01 §6).

IDs are ULIDs (sortable text): monotonic-ish ordering preserves B-tree index
locality, unlike uniformly-random UUIDv4. ``received_at`` is range-partitioned
by month for retention rollover (09 §3).
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base

_SEVERITIES = ("critical", "high", "medium", "low", "info")


class Alert(Base):
    __tablename__ = "alerts"
    __table_args__ = (
        CheckConstraint(
            "severity IN ('critical','high','medium','low','info')", name="ck_alerts_severity"
        ),
        # Partial indexes exclude soft-deleted rows.
        Index(
            "idx_alerts_tenant_received",
            "tenant_id",
            "received_at",
            postgresql_where=("deleted_at IS NULL"),
        ),
        Index(
            "idx_alerts_severity_received",
            "severity",
            "received_at",
            postgresql_where=("deleted_at IS NULL"),
        ),
        # Durable idempotency safety net (01 §7): unique per (tenant, key).
        Index(
            "uq_alerts_idempotency",
            "tenant_id",
            "idempotency_key",
            unique=True,
            postgresql_where=("idempotency_key IS NOT NULL"),
        ),
        # Find the duplicates suppressed against an original (06 §6).
        Index("idx_alerts_dedup_of", "dedup_of", postgresql_where="dedup_of IS NOT NULL"),
        {"schema": "ingestion"},
    )

    id: Mapped[str] = mapped_column(String(26), primary_key=True)  # ULID
    tenant_id: Mapped[str] = mapped_column(String(128))
    source: Mapped[str] = mapped_column(String(256))
    severity: Mapped[str] = mapped_column(String(16))
    topic: Mapped[str] = mapped_column(String(256))
    title: Mapped[str] = mapped_column(String(512))
    body: Mapped[str | None] = mapped_column(Text, nullable=True)
    labels: Mapped[dict] = mapped_column(JSONB, default=dict)
    payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    status: Mapped[str] = mapped_column(String(32), default="accepted")  # accepted|deduped|...
    idempotency_key: Mapped[str | None] = mapped_column(String(256), nullable=True)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Content dedup (06 §6): duplicates point at the original via dedup_of; the
    # original carries the running dedup_count. fingerprint_version lets us evolve
    # the hashing algorithm without colliding with old keys (06 §7).
    dedup_of: Mapped[str | None] = mapped_column(String(26), nullable=True)
    dedup_count: Mapped[int] = mapped_column(Integer, default=0)
    fingerprint_version: Mapped[int] = mapped_column(Integer, default=1)


class DedupPolicy(Base):
    """Per-(tenant, topic) content-dedup policy (06 §4, §5).

    A NULL ``topic`` is the tenant-wide default; an exact-topic row overrides it.
    ``dedup_fields`` are the label names that define "same event" — different
    tenants have different notions (``[host, region]`` vs ``[service, endpoint]``),
    so the set is configurable (06 §5).
    """

    __tablename__ = "dedup_policies"
    __table_args__ = (
        CheckConstraint("window_seconds >= 1", name="ck_dedup_window"),
        # One live policy per scope; NULLs-not-distinct makes the tenant default
        # (topic NULL) genuinely unique (the soft-delete + NULL-uniqueness trap).
        Index(
            "uq_dedup_scope",
            "tenant_id",
            "topic",
            unique=True,
            postgresql_where="deleted_at IS NULL",
            postgresql_nulls_not_distinct=True,
        ),
        {"schema": "ingestion"},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[str] = mapped_column(String(128))
    topic: Mapped[str | None] = mapped_column(String(256), nullable=True)  # NULL = tenant default
    dedup_fields: Mapped[list[str]] = mapped_column(ARRAY(String(128)), default=list)
    window_seconds: Mapped[int] = mapped_column(Integer)
    critical_bypass: Mapped[bool] = mapped_column(Boolean, default=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
