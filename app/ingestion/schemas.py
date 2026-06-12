"""Pydantic request/response schemas + shared Severity enum.

Strict mode (``extra="forbid"``) rejects unknown top-level fields so producer
typos surface as 400s instead of being silently dropped (01 §7).
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class Severity(StrEnum):
    """Severity label. ``priority`` drives which queue ZSET the alert lands in;
    the dispatcher drains highest priority first (00 §7.4 step 6).
    """

    critical = "critical"
    high = "high"
    medium = "medium"
    low = "low"
    info = "info"

    @property
    def label(self) -> str:
        return self.value

    @property
    def priority(self) -> int:
        return _PRIORITY[self]


_PRIORITY = {
    Severity.critical: 4,
    Severity.high: 3,
    Severity.medium: 2,
    Severity.low: 1,
    Severity.info: 0,
}


class AlertIn(BaseModel):
    """Inbound alert payload from a producer (01 §5)."""

    model_config = ConfigDict(extra="forbid")

    tenant_id: str = Field(min_length=1, max_length=128)
    source: str = Field(min_length=1, max_length=256)
    severity: Severity
    topic: str = Field(min_length=1, max_length=256)
    title: str = Field(min_length=1, max_length=512)
    body: str | None = Field(default=None)
    labels: dict = Field(default_factory=dict)
    payload: dict | None = None
    occurred_at: datetime


class AlertAccepted(BaseModel):
    """202 (new) / 200 (idempotent replay) response body (01 §5)."""

    alert_id: str
    status: str = "accepted"
