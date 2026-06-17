"""DLQ admin HTTP surface (07 §5.3): inspect, replay, acknowledge.

The DLQ is an operational artifact — the endpoints exist so an on-call engineer
can see *what went wrong* and fix it (replay) or accept the loss (delete), rather
than messages silently disappearing. It is admin-scoped (API key), deliberately
cross-tenant: operators triage the whole queue.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import require_api_key
from app.auth.dependencies import Principal
from app.config import get_settings
from app.db import get_session
from app.dlq import service
from app.dlq.schemas import DlqEntryOut, DlqListOut
from app.errors import NotFoundError

router = APIRouter(prefix="/v1/dlq", tags=["dlq"])


@router.get("", response_model=DlqListOut)
async def list_dlq(
    cursor: str | None = Query(default=None),
    limit: int = Query(default=0, ge=0),
    _: Principal = Depends(require_api_key),
) -> DlqListOut:
    settings = get_settings()
    effective = min(limit or settings.list_default_limit, settings.list_max_limit)
    entries, next_cursor = await service.list_entries(cursor=cursor, limit=effective)
    return DlqListOut(
        items=[DlqEntryOut.from_entry(e) for e in entries], next_cursor=next_cursor
    )


@router.get("/{stream_id}", response_model=DlqEntryOut)
async def get_dlq(stream_id: str, _: Principal = Depends(require_api_key)) -> DlqEntryOut:
    entry = await service.get_entry(stream_id)
    if entry is None:
        raise NotFoundError(f"dlq entry {stream_id} not found")
    return DlqEntryOut.from_entry(entry)


@router.post("/{stream_id}/replay", response_model=DlqEntryOut)
async def replay_dlq(
    stream_id: str,
    principal: Principal = Depends(require_api_key),
    session: AsyncSession = Depends(get_session),
) -> DlqEntryOut:
    entry = await service.replay(session, stream_id, actor=principal.producer)
    return DlqEntryOut.from_entry(entry)


@router.delete("/{stream_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_dlq(stream_id: str, _: Principal = Depends(require_api_key)) -> Response:
    await service.delete_entry(stream_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
