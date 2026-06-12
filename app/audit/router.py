"""Audit HTTP surface: ``GET /v1/alerts/{id}/attempts`` (00 §7.2)."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Path
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.audit.service import list_attempts
from app.auth import require_api_key
from app.auth.dependencies import Principal
from app.db import get_session

router = APIRouter(prefix="/v1/alerts", tags=["audit"])


class AttemptOut(BaseModel):
    id: UUID
    recipient_id: UUID
    channel: str
    status: str
    retry_count: int
    provider_id: str | None
    last_error: str | None

    model_config = {"from_attributes": True}


@router.get("/{alert_id}/attempts", response_model=list[AttemptOut])
async def get_attempts(
    alert_id: str = Path(min_length=26, max_length=26),
    principal: Principal = Depends(require_api_key),
    session: AsyncSession = Depends(get_session),
) -> list[AttemptOut]:
    """Read-after-write on primary for status pages (00 §7.3 delivery queries)."""
    attempts = await list_attempts(session, alert_id)
    return [AttemptOut.model_validate(a) for a in attempts]
