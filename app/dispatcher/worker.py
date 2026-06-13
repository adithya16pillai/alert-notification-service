"""Dispatcher worker: the asynchronous half of the pipeline (00 §7.4 steps 6-10).

Loop:
  1. Pop a batch highest-severity-first (atomic Lua pop).
  2. For each alert: load it, resolve (recipient × channel) targets, mark it
     ``dispatched`` so the janitor won't re-enqueue a healthy alert.
  3. Apply per-recipient+channel token-bucket rate limit.
  4. For each passing target: call the channel adapter with retry, record the
     attempt, DLQ on terminal failure.

Periodically runs the ingestion janitor to recover alerts that were durably
accepted but never enqueued (01 §8). Run N of these as separate processes
(stateless; state is in Postgres/Redis).
"""

from __future__ import annotations

import asyncio
import time

from sqlalchemy import select

from app.audit.service import push_to_dlq, record_attempt
from app.channels import DeliveryRequest, get_channel, register_defaults
from app.config import get_settings
from app.db import get_sessionmaker
from app.ingestion.janitor import GRACE_SECONDS, requeue_stale_accepted
from app.ingestion.models import Alert
from app.ingestion.schemas import Severity
from app.observability import configure_logging, get_logger
from app.observability.metrics import critical_ttd_seconds, delivery_attempts_total
from app.queue import ack_inflight, allow, pop_priority, reap_inflight
from app.queue.priority_queue import refresh_queue_depth_metrics
from app.recipients.schemas import ResolvedTarget
from app.recipients.service import resolve_targets

log = get_logger(__name__)


class Dispatcher:
    def __init__(self) -> None:
        self.settings = get_settings()
        self._running = False
        self._last_janitor = 0.0

    async def run_forever(self) -> None:
        register_defaults()
        self._running = True
        log.info("dispatcher.start", batch_size=self.settings.worker_batch_size)
        while self._running:
            processed = await self.drain_once()
            await refresh_queue_depth_metrics()
            await self._maybe_run_maintenance()
            if processed == 0:
                await asyncio.sleep(self.settings.worker_poll_interval_ms / 1000)

    def stop(self) -> None:
        self._running = False

    async def drain_once(self) -> int:
        alert_ids = await pop_priority(self.settings.worker_batch_size)
        for alert_id in alert_ids:
            await self._process_alert(alert_id)
            # Ack one-by-one: an alert whose processing raised stays in-flight and
            # is re-queued by the reaper (at-least-once, 02 §6).
            await ack_inflight([alert_id])
        return len(alert_ids)

    async def _maybe_run_maintenance(self) -> None:
        """Periodically recover lost work: stale-accepted rows (01 §8) and
        expired in-flight items whose worker died mid-batch (02 §6)."""
        now = time.monotonic()
        if now - self._last_janitor < GRACE_SECONDS:
            return
        self._last_janitor = now
        async with get_sessionmaker()() as session:
            await requeue_stale_accepted(session)
            await reap_inflight(session)

    async def _process_alert(self, alert_id: str) -> None:
        async with get_sessionmaker()() as session:
            alert = (
                await session.execute(select(Alert).where(Alert.id == alert_id))
            ).scalar_one_or_none()
            if alert is None:
                log.warning("dispatcher.alert_missing", alert_id=alert_id)
                return
            # Critical time-to-dispatch: receipt -> worker pickup (target <1s, 02 §4).
            if alert.severity == Severity.critical.value:
                critical_ttd_seconds.observe(max(0.0, time.time() - alert.received_at.timestamp()))
            targets = await resolve_targets(
                session, tenant=alert.tenant_id, topic=alert.topic, severity=alert.severity
            )
            # Mark picked-up before fanning out so the janitor leaves it alone.
            alert.status = "dispatched"
            await session.commit()
            # Detach the values we need; the session closes after this block.
            snapshot = _AlertSnapshot(
                id=alert.id,
                title=alert.title,
                body=alert.body or "",
                severity=alert.severity,
            )

        for target in targets:
            allowed, _ = await allow(str(target.recipient_id), target.channel)
            if not allowed:
                log.info(
                    "dispatcher.rate_limited",
                    alert_id=snapshot.id,
                    recipient_id=str(target.recipient_id),
                    channel=target.channel,
                )
                continue
            await self._deliver(snapshot, target)

    async def _deliver(self, alert: _AlertSnapshot, target: ResolvedTarget) -> None:
        channel = get_channel(target.channel)
        req = DeliveryRequest(
            alert_id=alert.id,
            recipient_id=target.recipient_id,
            target=target.target,
            title=alert.title,
            body=alert.body,
            severity=Severity(alert.severity).value,
            config=target.config,
        )

        last_error = "unknown"
        for attempt_no in range(self.settings.channel_max_retries):
            result = await channel.send(req)
            if result.ok:
                await self._record("sent", alert.id, target, attempt_no, result.provider_id, None)
                delivery_attempts_total.labels(channel=target.channel, status="sent").inc()
                return
            last_error = result.error or "delivery failed"
            if not result.retryable:
                break
            await asyncio.sleep(min(2**attempt_no, 10) * 0.1)  # capped backoff

        await self._record(
            "dlq", alert.id, target, self.settings.channel_max_retries, None, last_error
        )
        delivery_attempts_total.labels(channel=target.channel, status="failed").inc()
        await push_to_dlq(alert.id, target.recipient_id, target.channel, last_error)

    async def _record(self, status, alert_id, target, retry_count, provider_id, error) -> None:
        async with get_sessionmaker()() as session:
            await record_attempt(
                session,
                alert_id=alert_id,
                recipient_id=target.recipient_id,
                channel=target.channel,
                status=status,
                retry_count=retry_count,
                provider_id=provider_id,
                last_error=error,
            )


class _AlertSnapshot:
    """Plain holder for the alert fields needed after the DB session closes."""

    __slots__ = ("id", "title", "body", "severity")

    def __init__(self, id: str, title: str, body: str, severity: str) -> None:
        self.id = id
        self.title = title
        self.body = body
        self.severity = severity


def run() -> None:
    """Console-script entrypoint (``ans-worker``)."""
    configure_logging()
    asyncio.run(Dispatcher().run_forever())


if __name__ == "__main__":
    run()
