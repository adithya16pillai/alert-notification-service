"""Janitor: recover alerts that were durably accepted but never enqueued.

If Redis dies between the Postgres commit and the queue enqueue (01 §7), the
alert row sits at ``status='accepted'``. This sweep re-enqueues any such row
older than the grace window so it still gets dispatched (acceptance: recovery
within 30s). The dispatcher flips rows to ``dispatched`` once drained, so a
healthy alert is swept at most once.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.ingestion.models import Alert
from app.ingestion.schemas import Severity
from app.observability import get_logger
from app.queue.priority_queue import enqueue_alert

log = get_logger(__name__)

GRACE_SECONDS = 30


async def requeue_stale_accepted(session: AsyncSession, *, limit: int = 500) -> int:
    cutoff = datetime.now(UTC) - timedelta(seconds=GRACE_SECONDS)
    stmt = (
        select(Alert)
        .where(
            Alert.status == "accepted",
            Alert.received_at < cutoff,
            Alert.deleted_at.is_(None),
        )
        .order_by(Alert.received_at)
        .limit(limit)
    )
    stale = list((await session.execute(stmt)).scalars().all())
    for alert in stale:
        await enqueue_alert(alert.id, Severity(alert.severity), score=int(time.time() * 1000))
    if stale:
        log.info("janitor.requeued", count=len(stale))
    return len(stale)
