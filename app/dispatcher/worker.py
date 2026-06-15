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
from app.channels import DeliveryRequest, close_all, get_channel, register_defaults
from app.channels.policy import backoff_delay
from app.channels.rendering import get_renderer
from app.channels.secrets import install_sighup_reload
from app.config import get_settings
from app.db import get_sessionmaker
from app.errors import CircuitOpenError
from app.ingestion.janitor import GRACE_SECONDS, requeue_stale_accepted
from app.ingestion.models import Alert
from app.ingestion.schemas import Severity
from app.observability import configure_logging, get_logger
from app.observability.metrics import (
    critical_ttd_seconds,
    delivery_attempts_total,
    delivery_errors_total,
    rate_limit_abandoned_total,
    rate_limit_deferred_total,
)
from app.queue import ack_inflight, allow, pop_priority, reap_inflight
from app.queue.priority_queue import refresh_queue_depth_metrics
from app.queue.retry_queue import (
    DeferredDelivery,
    defer,
    now_ms,
    pop_due_retries,
    refresh_retry_depth_metrics,
)
from app.recipients.cache import listen_for_invalidations
from app.recipients.rate_limit_policy import ResolvedRateLimit, resolve_rate_limit
from app.recipients.schemas import ResolvedTarget
from app.recipients.service import resolve_targets

log = get_logger(__name__)


class Dispatcher:
    def __init__(self) -> None:
        self.settings = get_settings()
        self._running = False
        self._last_janitor = 0.0
        self._invalidation_task: asyncio.Task | None = None

    async def run_forever(self) -> None:
        register_defaults()
        # Hot-reload provider credentials on SIGHUP, no restart (04 §9). No-op
        # on platforms without SIGHUP (e.g. Windows dev).
        install_sighup_reload()
        self._running = True
        # Subscribe to subscription-cache invalidations so a subscription change
        # made via the API drops this worker's local cache within ~1s (03 §7).
        self._invalidation_task = asyncio.create_task(listen_for_invalidations())
        log.info("dispatcher.start", batch_size=self.settings.worker_batch_size)
        try:
            while self._running:
                processed = await self.drain_once()
                # Retry any rate-limited deliveries whose deferral is due (05 §7).
                retried = await self.drain_retries()
                await refresh_queue_depth_metrics()
                await refresh_retry_depth_metrics()
                await self._maybe_run_maintenance()
                if processed == 0 and retried == 0:
                    await asyncio.sleep(self.settings.worker_poll_interval_ms / 1000)
        finally:
            if self._invalidation_task is not None:
                self._invalidation_task.cancel()
            await close_all()  # release shared HTTP clients (04 §9)

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

    async def drain_retries(self) -> int:
        """Re-attempt deferred deliveries whose retry time is due (05 §7)."""
        due = await pop_due_retries(now_ms(), self.settings.worker_batch_size)
        for delivery in due:
            await self._process_deferred(delivery)
        return len(due)

    async def _process_deferred(self, delivery: DeferredDelivery) -> None:
        """Retry one parked delivery: abandon if past the cap, else re-check the
        limit and either send or re-park (05 §7, §8)."""
        target = ResolvedTarget(
            recipient_id=delivery.recipient_id,
            channel=delivery.channel,
            target=delivery.target,
            config=delivery.config or {},
        )
        cap_ms = self.settings.rate_limit_max_defer_seconds * 1000
        if now_ms() - delivery.first_deferred_ms >= cap_ms:
            # Total deferral exceeded the cap: abandon to the DLQ (05 §8).
            await self._record(
                "abandoned", delivery.alert_id, target, 0, None, "rate_limit_deferral_expired"
            )
            rate_limit_abandoned_total.labels(channel=delivery.channel).inc()
            await push_to_dlq(
                delivery.alert_id,
                target.recipient_id,
                delivery.channel,
                "rate_limit_deferral_expired",
            )
            log.info("dispatcher.deferral_abandoned", alert_id=delivery.alert_id)
            return

        if not await self._rate_limit_allows(delivery.tenant, delivery.severity, target):
            await self._park(delivery)  # still limited -> re-park, keeping first_deferred_ms
            return

        snapshot = await self._load_snapshot(delivery.alert_id)
        if snapshot is None:
            log.warning("dispatcher.deferred_alert_missing", alert_id=delivery.alert_id)
            return
        await self._deliver(snapshot, target)

    async def _load_snapshot(self, alert_id: str) -> _AlertSnapshot | None:
        async with get_sessionmaker()() as session:
            alert = (
                await session.execute(select(Alert).where(Alert.id == alert_id))
            ).scalar_one_or_none()
            return _AlertSnapshot.from_row(alert) if alert is not None else None

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
            snapshot = _AlertSnapshot.from_row(alert)

        for target in targets:
            if await self._rate_limit_allows(snapshot.tenant, snapshot.severity, target):
                await self._deliver(snapshot, target)
            else:
                # Don't drop — park for retry. A silent drop is the worst outcome
                # (the customer never sees the alert); defer up to the cap (05 §7).
                await self._park(
                    DeferredDelivery(
                        alert_id=snapshot.id,
                        tenant=snapshot.tenant,
                        recipient_id=str(target.recipient_id),
                        channel=target.channel,
                        target=target.target,
                        severity=snapshot.severity,
                        first_deferred_ms=now_ms(),
                        config=target.config,
                    )
                )

    async def _rate_limit_allows(
        self, tenant: str, severity: str, target: ResolvedTarget
    ) -> bool:
        """Token-bucket decision for one target, with policy + critical bypass.

        Critical bypass is enforced *here*, not in the Lua script, so the script
        stays generic and reusable (05 §7). The resolved policy supplies the
        bucket's capacity/refill; a cache hit means zero DB on the hot path.
        """
        policy = await self._policy(tenant, target)
        if severity == Severity.critical.value and policy.critical_bypass:
            return True
        allowed, _ = await allow(
            str(target.recipient_id),
            target.channel,
            capacity=policy.capacity,
            refill_per_sec=policy.refill_per_sec,
        )
        return allowed

    async def _policy(self, tenant: str, target: ResolvedTarget) -> ResolvedRateLimit:
        return await resolve_rate_limit(tenant, str(target.recipient_id), target.channel)

    async def _park(self, delivery: DeferredDelivery) -> None:
        """Park a rate-limited delivery for retry, due after the configured delay."""
        await defer(delivery, due_ms=now_ms() + self.settings.rate_limit_retry_delay_ms)
        rate_limit_deferred_total.labels(channel=delivery.channel).inc()
        log.info(
            "dispatcher.rate_limited",
            alert_id=delivery.alert_id,
            recipient_id=delivery.recipient_id,
            channel=delivery.channel,
        )

    async def _deliver(self, alert: _AlertSnapshot, target: ResolvedTarget) -> None:
        channel = get_channel(target.channel)
        severity = Severity(alert.severity).value
        # Render the (channel × severity) template; missing fields degrade to
        # safe defaults — a render failure never loses the alert (04 §8).
        subject, body = get_renderer().render(
            kind=target.channel,
            severity=severity,
            tenant=alert.tenant,
            context={
                "id": alert.id,
                "title": alert.title,
                "body": alert.body,
                "severity": severity,
                "source": alert.source,
                "topic": alert.topic,
                "occurred_at": alert.occurred_at,
                "labels": alert.labels,
            },
        )
        req = DeliveryRequest(
            alert_id=alert.id,
            recipient_id=target.recipient_id,
            target=target.target,
            title=alert.title,
            body=alert.body,
            severity=severity,
            config=target.config,
            tenant=alert.tenant,
            rendered_subject=subject,
            rendered_body=body,
        )

        # Retries, backoff, and budget come from the per-channel policy (04 §5),
        # so Slack's tight retry budget never inherits email's, and vice versa.
        policy = channel.policy
        last_error = "unknown"
        for attempt_no in range(policy.max_retries):
            try:
                result = await channel.send(req)
            except CircuitOpenError:
                # Provider is degraded; immediate retry is pointless. DLQ for
                # later replay so other channels keep flowing (04 §9 isolation).
                last_error = "circuit_open"
                delivery_errors_total.labels(
                    channel=target.channel, classification="circuit_open"
                ).inc()
                break

            if result.ok:
                await self._record("sent", alert.id, target, attempt_no, result.provider_id, None)
                delivery_attempts_total.labels(channel=target.channel, status="sent").inc()
                return

            last_error = result.error or "delivery failed"
            delivery_errors_total.labels(
                channel=target.channel, classification=result.status.value
            ).inc()
            if not result.retryable:
                break  # permanent failure -> abandon to DLQ immediately (04 §6)
            if attempt_no + 1 < policy.max_retries:
                # Honour a provider's Retry-After over our computed backoff (04 §9).
                delay = (
                    result.retry_after_s
                    if result.retry_after_s is not None
                    else backoff_delay(policy, attempt_no)
                )
                await asyncio.sleep(delay)

        await self._record("dlq", alert.id, target, policy.max_retries, None, last_error)
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

    __slots__ = ("id", "title", "body", "severity", "tenant", "source", "topic",
                 "occurred_at", "labels")

    def __init__(
        self,
        id: str,
        title: str,
        body: str,
        severity: str,
        tenant: str = "",
        source: str = "",
        topic: str = "",
        occurred_at: str = "",
        labels: dict | None = None,
    ) -> None:
        self.id = id
        self.title = title
        self.body = body
        self.severity = severity
        self.tenant = tenant
        self.source = source
        self.topic = topic
        self.occurred_at = occurred_at
        self.labels = labels or {}

    @classmethod
    def from_row(cls, alert: Alert) -> _AlertSnapshot:
        return cls(
            id=alert.id,
            title=alert.title,
            body=alert.body or "",
            severity=alert.severity,
            tenant=alert.tenant_id,
            source=alert.source,
            topic=alert.topic,
            occurred_at=alert.occurred_at.isoformat() if alert.occurred_at else "",
            labels=dict(alert.labels or {}),
        )


def run() -> None:
    """Console-script entrypoint (``ans-worker``)."""
    configure_logging()
    asyncio.run(Dispatcher().run_forever())


if __name__ == "__main__":
    run()
