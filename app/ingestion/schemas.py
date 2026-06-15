"""Pydantic request/response schemas + shared Severity enum.

Strict mode (``extra="forbid"``) rejects unknown top-level fields so producer
typos surface as 400s instead of being silently dropped (01 §7).
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from uuid import UUID

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
    """202 (new / deduped) / 200 (idempotent replay) response body (01 §5, 06 §4)."""

    alert_id: str
    status: str = "accepted"  # accepted | deduped


# --- Dedup policy config API (06 §2) ---
class DedupPolicyIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic: str | None = Field(default=None, max_length=256)  # None = tenant default
    dedup_fields: list[str] = Field(default_factory=lambda: ["host", "region"], max_length=50)
    window_seconds: int = Field(ge=1, le=86_400)
    critical_bypass: bool = False
    enabled: bool = True


class DedupPolicyOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    tenant_id: str
    topic: str | None
    dedup_fields: list[str]
    window_seconds: int
    critical_bypass: bool
    enabled: bool
    created_at: datetime
