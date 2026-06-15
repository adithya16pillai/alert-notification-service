"""Ingestion HTTP surface: ``POST /v1/alerts`` (01 §5)."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Header, Request, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import require_api_key, require_tenant
from app.auth.dependencies import Principal
from app.config import get_settings
from app.db import get_session
from app.errors import PayloadTooLargeError
from app.ingestion import dedup
from app.ingestion.schemas import AlertAccepted, AlertIn, DedupPolicyIn, DedupPolicyOut
from app.ingestion.service import ingest_alert

router = APIRouter(prefix="/v1/alerts", tags=["ingestion"])
dedup_router = APIRouter(prefix="/v1/dedup-policies", tags=["dedup"])


async def enforce_body_limit(request: Request) -> None:
    """Reject oversized bodies with 413 (01 §7: hard cap 256 KB)."""
    cap = get_settings().ingest_max_body_bytes
    content_length = request.headers.get("content-length")
    if content_length is not None and content_length.isdigit() and int(content_length) > cap:
        raise PayloadTooLargeError(f"body exceeds {cap} bytes", field="body")


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=AlertAccepted,
    dependencies=[Depends(enforce_body_limit)],
)
async def post_alert(
    alert: AlertIn,
    response: Response,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    principal: Principal = Depends(require_api_key),
    session: AsyncSession = Depends(get_session),
) -> AlertAccepted:
    """Accept an alert, persist it durably, enqueue it, return a tracking ID.

    Returns ``202`` for a new alert and ``200`` for an idempotent replay (01 §5).
    Target: p99 < 50ms server-side. The synchronous path ends here.
    """
    # TODO(08): derive tenant from the JWT ``tenant_id`` claim and reject a body
    # whose tenant_id disagrees, to prevent cross-tenant spoofing. v1 auth is
    # API-key based, so the body's tenant_id is trusted for now.
    _ = principal
    result = await ingest_alert(session, alert, idempotency_key)
    if result.replay:
        response.status_code = status.HTTP_200_OK
    # A deduped alert is still accepted (202) and recorded — the recipient just
    # won't be paged again (06 §4). The status tells the producer what happened.
    return AlertAccepted(
        alert_id=result.alert_id, status="deduped" if result.deduped else "accepted"
    )


# --------------------------------------------------------------------------- #
# Dedup policy config API (06 §2)
# --------------------------------------------------------------------------- #
@dedup_router.get("", response_model=list[DedupPolicyOut])
async def list_dedup_policies(
    tenant: str = Depends(require_tenant),
    session: AsyncSession = Depends(get_session),
) -> list[DedupPolicyOut]:
    rows = await dedup.list_dedup_policies(session, tenant=tenant)
    return [DedupPolicyOut.model_validate(r) for r in rows]


@dedup_router.put("", response_model=DedupPolicyOut)
async def upsert_dedup_policy(
    body: DedupPolicyIn,
    tenant: str = Depends(require_tenant),
    session: AsyncSession = Depends(get_session),
) -> DedupPolicyOut:
    policy = await dedup.upsert_dedup_policy(session, tenant=tenant, body=body)
    return DedupPolicyOut.model_validate(policy)


@dedup_router.delete("/{policy_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_dedup_policy(
    policy_id: UUID,
    tenant: str = Depends(require_tenant),
    session: AsyncSession = Depends(get_session),
) -> Response:
    await dedup.delete_dedup_policy(session, tenant=tenant, policy_id=policy_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
