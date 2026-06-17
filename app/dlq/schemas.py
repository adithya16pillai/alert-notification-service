"""DLQ admin API response shapes (07 §5.3)."""

from __future__ import annotations

from pydantic import BaseModel

from app.dlq.service import DlqEntry


class DlqEntryOut(BaseModel):
    stream_id: str
    alert_id: str
    recipient_id: str
    channel: str
    target: str
    severity: str
    tenant: str
    reason: str
    last_error: str
    attempt_history: list[dict]

    @classmethod
    def from_entry(cls, e: DlqEntry) -> DlqEntryOut:
        # `config` (signing secrets etc.) is intentionally omitted from the API.
        return cls(
            stream_id=e.stream_id,
            alert_id=e.alert_id,
            recipient_id=e.recipient_id,
            channel=e.channel,
            target=e.target,
            severity=e.severity,
            tenant=e.tenant,
            reason=e.reason,
            last_error=e.last_error,
            attempt_history=e.attempt_history,
        )


class DlqListOut(BaseModel):
    items: list[DlqEntryOut]
    next_cursor: str | None = None  # null => no more pages (03 §4)
