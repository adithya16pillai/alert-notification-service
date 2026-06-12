"""Pydantic schemas for the recipients admin surface."""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, Field


class ChannelConfigIn(BaseModel):
    channel: str = Field(pattern="^(email|slack|webhook|sms)$")
    target: str = Field(min_length=1, max_length=512)
    config: dict = Field(default_factory=dict)
    enabled: bool = True


class SubscriptionIn(BaseModel):
    tenant: str
    topic: str
    min_severity: int = 0


class RecipientIn(BaseModel):
    tenant: str
    name: str
    channels: list[ChannelConfigIn] = Field(default_factory=list)
    subscriptions: list[SubscriptionIn] = Field(default_factory=list)


class RecipientOut(BaseModel):
    id: UUID
    tenant: str
    name: str
    active: bool


class ResolvedTarget(BaseModel):
    """A (recipient, channel) pair the dispatcher should attempt delivery on."""

    recipient_id: UUID
    channel: str
    target: str
    config: dict = Field(default_factory=dict)
