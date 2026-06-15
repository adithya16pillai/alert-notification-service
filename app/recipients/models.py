"""ORM models for the ``recipients`` schema (03 §5).

The routing graph between an incoming alert and the humans/services notified:
``recipients`` own ``channels`` (where to deliver) and ``subscriptions`` (which
alerts to match). Everything is soft-deleted — ``deleted_at IS NULL`` is the
"live" predicate — so history and the audit trail survive a delete. Uniqueness
and the hot-path indexes are therefore *partial* indexes scoped to live rows
(03 §8: the soft-delete unique-constraint trap).
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


class Recipient(Base):
    __tablename__ = "recipients"
    __table_args__ = (
        # Hot-path lookup is by tenant; exclude soft-deleted rows (03 §5).
        Index(
            "idx_recipients_tenant",
            "tenant_id",
            postgresql_where="deleted_at IS NULL",
        ),
        {"schema": "recipients"},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[str] = mapped_column(String(128))
    name: Mapped[str] = mapped_column(String(256))
    timezone: Mapped[str] = mapped_column(String(64), default="UTC")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    channels: Mapped[list[Channel]] = relationship(back_populates="recipient")
    subscriptions: Mapped[list[Subscription]] = relationship(back_populates="recipient")


class Channel(Base):
    __tablename__ = "channels"
    __table_args__ = (
        CheckConstraint(
            "kind IN ('email','slack','webhook','sms')", name="ck_channels_kind"
        ),
        Index(
            "idx_channels_recipient",
            "recipient_id",
            postgresql_where="deleted_at IS NULL",
        ),
        # Soft-delete-safe uniqueness: one live channel per (recipient, kind,
        # address). A deleted row doesn't block re-adding the same address (03 §8).
        Index(
            "uq_channels_recipient_kind_address",
            "recipient_id",
            "kind",
            "address",
            unique=True,
            postgresql_where="deleted_at IS NULL",
        ),
        {"schema": "recipients"},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    recipient_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("recipients.recipients.id", ondelete="CASCADE")
    )
    kind: Mapped[str] = mapped_column(String(16))  # email | slack | webhook | sms
    address: Mapped[str] = mapped_column(String(512))  # email addr / slack id / URL / phone
    verified: Mapped[bool] = mapped_column(Boolean, default=False)
    # Secret *refs* only (e.g. 'secret://aws-sm/...'), never raw secrets (03 §8).
    config: Mapped[dict] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    recipient: Mapped[Recipient] = relationship(back_populates="channels")


class Subscription(Base):
    __tablename__ = "subscriptions"
    __table_args__ = (
        CheckConstraint(
            "min_severity IN ('critical','high','medium','low','info')",
            name="ck_subscriptions_min_severity",
        ),
        # Matching hot path scans live, enabled subs for a tenant (03 §7).
        Index(
            "idx_sub_tenant",
            "tenant_id",
            postgresql_where="deleted_at IS NULL AND enabled = TRUE",
        ),
        {"schema": "recipients"},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    recipient_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("recipients.recipients.id", ondelete="CASCADE")
    )
    tenant_id: Mapped[str] = mapped_column(String(128))
    topic_pattern: Mapped[str] = mapped_column(String(256))  # glob, e.g. 'auth.*'
    min_severity: Mapped[str] = mapped_column(String(16))  # severity label, not rank
    channel_ids: Mapped[list[str]] = mapped_column(ARRAY(String(36)), default=list)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    recipient: Mapped[Recipient] = relationship(back_populates="subscriptions")


class RateLimitPolicy(Base):
    """Per-scope token-bucket override (05 §7).

    Scope is the tuple ``(tenant_id, recipient_id, channel_kind)`` where a NULL
    ``recipient_id`` or ``channel_kind`` means "applies to all" — so a tenant can
    set a default, override it per channel, and override that again per recipient.
    The most specific live row wins (resolved in ``rate_limit_policy.py``).
    """

    __tablename__ = "rate_limit_policies"
    __table_args__ = (
        CheckConstraint(
            "channel_kind IS NULL OR channel_kind IN ('email','slack','webhook','sms')",
            name="ck_rlp_channel_kind",
        ),
        CheckConstraint("capacity >= 1", name="ck_rlp_capacity"),
        CheckConstraint("refill_per_sec > 0", name="ck_rlp_refill"),
        # One live policy per scope. NULLs-not-distinct (PG15+) makes the tenant
        # default (both NULL) genuinely unique — the classic NULL-uniqueness trap.
        Index(
            "uq_rlp_scope",
            "tenant_id",
            "recipient_id",
            "channel_kind",
            unique=True,
            postgresql_where="deleted_at IS NULL",
            postgresql_nulls_not_distinct=True,
        ),
        {"schema": "recipients"},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[str] = mapped_column(String(128))
    recipient_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )  # NULL = tenant-wide
    # NULL channel_kind = applies to all channels.
    channel_kind: Mapped[str | None] = mapped_column(String(16), nullable=True)
    capacity: Mapped[int] = mapped_column(Integer)
    refill_per_sec: Mapped[float] = mapped_column(Float)
    critical_bypass: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
