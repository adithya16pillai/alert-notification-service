"""ORM model for ``ingestion.alerts`` — durable system of record (01 §6).

IDs are ULIDs (sortable text): monotonic-ish ordering preserves B-tree index
locality, unlike uniformly-random UUIDv4. ``received_at`` is range-partitioned
by month for retention rollover (09 §3).
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Index,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
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
    status: Mapped[str] = mapped_column(String(32), default="accepted")  # accepted|dispatched|...
    idempotency_key: Mapped[str | None] = mapped_column(String(256), nullable=True)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
