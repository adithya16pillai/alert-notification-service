"""ORM models for the ``recipients`` schema."""

from __future__ import annotations

import uuid

from sqlalchemy import Boolean, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


class Recipient(Base):
    __tablename__ = "recipients"
    __table_args__ = ({"schema": "recipients"},)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant: Mapped[str] = mapped_column(String(128), index=True)
    name: Mapped[str] = mapped_column(String(256))
    active: Mapped[bool] = mapped_column(Boolean, default=True)

    channels: Mapped[list[ChannelConfig]] = relationship(back_populates="recipient")
    subscriptions: Mapped[list[Subscription]] = relationship(back_populates="recipient")


class ChannelConfig(Base):
    __tablename__ = "channel_configs"
    __table_args__ = (
        UniqueConstraint("recipient_id", "channel", name="uq_recipient_channel"),
        {"schema": "recipients"},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    recipient_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("recipients.recipients.id", ondelete="CASCADE")
    )
    channel: Mapped[str] = mapped_column(String(32))  # email | slack | webhook | sms
    target: Mapped[str] = mapped_column(String(512))  # address / url / phone / channel-id
    config: Mapped[dict] = mapped_column(JSONB, default=dict)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)

    recipient: Mapped[Recipient] = relationship(back_populates="channels")


class Subscription(Base):
    __tablename__ = "subscriptions"
    __table_args__ = ({"schema": "recipients"},)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    recipient_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("recipients.recipients.id", ondelete="CASCADE")
    )
    tenant: Mapped[str] = mapped_column(String(128), index=True)
    topic: Mapped[str] = mapped_column(String(256), index=True)
    min_severity: Mapped[int] = mapped_column(default=0)

    recipient: Mapped[Recipient] = relationship(back_populates="subscriptions")
