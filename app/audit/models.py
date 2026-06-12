"""ORM model for ``audit.delivery_attempts`` — the defensible audit trail.

One row per (alert × recipient × channel) attempt. Used both for operational
retries and for the "no alert silently lost" success metric (00 §2).
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class DeliveryAttempt(Base):
    __tablename__ = "delivery_attempts"
    __table_args__ = (
        Index("ix_delivery_attempts_alert", "alert_id"),
        Index("ix_delivery_attempts_status", "status"),
        {"schema": "audit"},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    alert_id: Mapped[str] = mapped_column(String(26))  # ULID, references ingestion.alerts.id
    recipient_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    channel: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(32), default="pending")  # pending|sent|failed|dlq
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    provider_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
