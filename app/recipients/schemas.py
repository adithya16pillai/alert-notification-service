"""Pydantic schemas for the recipients admin surface (03 §6).

Request bodies use ``extra="forbid"`` so a typo'd field is a 400, not a silent
no-op. List endpoints return the cursor envelope ``{items, next_cursor}``.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.ingestion.schemas import Severity

_KIND = "^(email|slack|webhook|sms)$"


# --- Recipients ---
class RecipientIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=256)
    timezone: str = Field(default="UTC", max_length=64)


class RecipientPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=256)
    timezone: str | None = Field(default=None, max_length=64)


class RecipientOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    tenant_id: str
    name: str
    timezone: str
    created_at: datetime


# --- Channels ---
class ChannelIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: str = Field(pattern=_KIND)
    address: str = Field(min_length=1, max_length=512)
    config: dict = Field(default_factory=dict)


class ChannelOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    recipient_id: UUID
    kind: str
    address: str
    verified: bool
    config: dict
    created_at: datetime


# --- Subscriptions ---
class SubscriptionIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    recipient_id: UUID
    topic_pattern: str = Field(min_length=1, max_length=256)
    min_severity: Severity = Severity.info
    channel_ids: list[UUID] = Field(min_length=1)
    enabled: bool = True


class SubscriptionPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic_pattern: str | None = Field(default=None, min_length=1, max_length=256)
    min_severity: Severity | None = None
    channel_ids: list[UUID] | None = Field(default=None, min_length=1)
    enabled: bool | None = None


class SubscriptionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    recipient_id: UUID
    tenant_id: str
    topic_pattern: str
    min_severity: str
    channel_ids: list[UUID]
    enabled: bool
    created_at: datetime


# --- Cursor envelope (03 §6) ---
class Page[T](BaseModel):
    items: list[T]
    next_cursor: str | None = None


# --- Dispatcher hot path ---
class ResolvedTarget(BaseModel):
    """A (recipient, channel) pair the dispatcher should attempt delivery on."""

    recipient_id: UUID
    channel: str
    target: str
    config: dict = Field(default_factory=dict)
