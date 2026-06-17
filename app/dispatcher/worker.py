"""Dispatcher worker: the asynchronous half of the pipeline (00 §7.4 steps 6-10).

Loop:
  1. Pop a batch highest-severity-first (atomic Lua pop).
  2. For each alert: load it, resolve (recipient × channel) targets, mark it
     ``dispatched`` so the janitor won't re-enqueue a healthy alert.
  3. Apply per-recipient+channel token-bucket rate limit.
  4. For each passing target: make ONE guarded delivery attempt, record it, and
     decide the next step (07 §3.3) — success is done; a transient failure is
     rescheduled onto the retry queue with backoff+jitter (the worker never
     blocks sleeping); a permanent failure or exhausted retries is abandoned to
     the DLQ. A rate-limited target is parked on the same retry queue (05 §7).

Each loop also drains due retries/deferrals off ``queue:retry:{severity}`` and
feeds them back through the same attempt path, so one queue serves both reasons
(07 §7). Periodically runs the ingestion janitor to recover alerts that were
durably accepted but never enqueued (01 §8). Run N of these as separate
processes (stateless; state is in Postgres/Redis).
"""

from __future__ import annotations

import asyncio
import time

from sqlalchemy import select

from app.audit.service import record_attempt
from app.channels import DeliveryRequest, close_all, get_channel, register_defaults
from app.channels.policy import backoff_delay
from app.channels.rendering import get_renderer
from app.channels.secrets import install_sighup_reload
from app.config import get_settings
from app.db import get_sessionmaker
from app.dlq import service as dlq
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
        """Drain one parked delivery. Both reasons ride this one queue (07 §7);
        the reason picks the handler."""
        if delivery.reason == "retry":
            await self._process_retry(delivery)
        else:
            await self._process_rate_limited(delivery)

    def _target_of(self, delivery: DeferredDelivery) -> ResolvedTarget:
        return ResolvedTarget(
            recipient_id=delivery.recipient_id,
            channel=delivery.channel,
            target=delivery.target,
            config=delivery.config or {},
        )

    async def _process_rate_limited(self, delivery: DeferredDelivery) -> None:
        """A rate-limit-deferred delivery: abandon if past the cap, else re-check
        the limit and either start delivery or re-park (05 §7, §8)."""
        target = self._target_of(delivery)
        cap_ms = self.settings.rate_limit_max_defer_seconds * 1000
        if now_ms() - delivery.first_deferred_ms >= cap_ms:
            # Total deferral exceeded the cap: abandon to the DLQ (05 §8, 07 §5.1).
            await self._record(
                "abandoned", delivery.alert_id, target, 0, None, "rate_limit_deferral_expired"
            )
            rate_limit_abandoned_total.labels(channel=delivery.channel).inc()
            await self._abandon(
                delivery.alert_id,
                target,
                severity=delivery.severity,
                tenant=delivery.tenant,
                reason="rate_limit_expired",
                error="rate_limit_deferral_expired",
                history=list(delivery.attempt_history),
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
        # The limit cleared: begin the delivery attempt sequence afresh.
        await self._attempt(snapshot, target, attempt_no=0, history=[])

    async def _process_retry(self, delivery: DeferredDelivery) -> None:
        """A transient-failure retry whose backoff has elapsed (07 §3.3): pick up
        where the attempt count left off."""
        snapshot = await self._load_snapshot(delivery.alert_id)
        if snapshot is None:
            log.warning("dispatcher.retry_alert_missing", alert_id=delivery.alert_id)
            return
        await self._attempt(
            snapshot,
            self._target_of(delivery),
            attempt_no=delivery.attempt_no,
            history=list(delivery.attempt_history),
        )

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
                await self._attempt(snapshot, target, attempt_no=0, history=[])
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

    def _build_request(self, alert: _AlertSnapshot, target: ResolvedTarget) -> DeliveryRequest:
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
        return DeliveryRequest(
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

    async def _attempt(
        self,
        alert: _AlertSnapshot,
        target: ResolvedTarget,
        *,
        attempt_no: int,
        history: list[dict],
    ) -> None:
        """Make exactly ONE delivery attempt, record it, and decide the next step
        (07 §3.3): success → done; transient → reschedule with backoff onto the
        retry queue (the worker is never blocked sleeping); permanent or
        retries-exhausted → abandon to the DLQ. ``attempt_no`` is the count of
        attempts already made, so this attempt is number ``attempt_no + 1`` and a
        consistently-failing channel produces exactly ``max_retries + 1`` rows and
        one DLQ entry (07 §6)."""
        channel = get_channel(target.channel)
        policy = channel.policy  # per-channel budget/backoff (04 §5)
        n = attempt_no + 1
        result = await channel.send(self._build_request(alert, target))

        if result.ok:
            await self._record("sent", alert.id, target, n, result.provider_id, None)
            delivery_attempts_total.labels(channel=target.channel, status="sent").inc()
            return

        delivery_errors_total.labels(
            channel=target.channel, classification=result.status.value
        ).inc()
        error = result.error or "delivery failed"
        history = [*history, {"attempt": n, "status": result.status.value, "error": error}]

        is_permanent = not result.retryable  # 4xx / bad address / auth (04 §6)
        exhausted = n > policy.max_retries
        if is_permanent or exhausted:
            # This failed attempt is the terminal row (status 'abandoned'); no
            # extra row is written, so the count stays at max_retries + 1.
            await self._record("abandoned", alert.id, target, n, None, error)
            delivery_attempts_total.labels(channel=target.channel, status="failed").inc()
            await self._abandon(
                alert.id,
                target,
                severity=alert.severity,
                tenant=alert.tenant,
                reason="permanent_failure" if is_permanent else "exhausted_retries",
                error=error,
                history=history,
            )
            return

        # Transient and budget remains: record the failed attempt and reschedule
        # with exponential backoff + jitter. Honour a provider's Retry-After over
        # our computed delay when present (04 §9).
        await self._record("failed", alert.id, target, n, None, error)
        delay = (
            result.retry_after_s
            if result.retry_after_s is not None
            else backoff_delay(policy, n - 1)
        )
        await self._reschedule(
            alert, target, attempt_no=n, last_error=error, history=history, delay_s=delay
        )

    async def _reschedule(
        self,
        alert: _AlertSnapshot,
        target: ResolvedTarget,
        *,
        attempt_no: int,
        last_error: str,
        history: list[dict],
        delay_s: float,
    ) -> None:
        """Park a transient failure back on the retry queue, due after ``delay_s``
        (07 §3.3). Jitter in the delay spreads concurrent retries so a recovering
        provider isn't hit by a synchronized herd (07 §3.2)."""
        await defer(
            DeferredDelivery(
                alert_id=alert.id,
                tenant=alert.tenant,
                recipient_id=str(target.recipient_id),
                channel=target.channel,
                target=target.target,
                severity=alert.severity,
                first_deferred_ms=now_ms(),
                config=target.config or {},
                attempt_no=attempt_no,
                reason="retry",
                last_error=last_error,
                attempt_history=tuple(history),
            ),
            due_ms=now_ms() + int(delay_s * 1000),
        )
        log.info(
            "dispatcher.retry_scheduled",
            alert_id=alert.id,
            channel=target.channel,
            attempt=attempt_no,
            delay_s=round(delay_s, 3),
        )

    async def _abandon(
        self,
        alert_id: str,
        target: ResolvedTarget,
        *,
        severity: str,
        tenant: str,
        reason: str,
        error: str,
        history: list[dict],
    ) -> None:
        """Push a terminal failure to the DLQ — every abandoned attempt has a
        corresponding entry, so nothing is silently lost (07 §2 success metric)."""
        await dlq.push(
            alert_id=alert_id,
            recipient_id=str(target.recipient_id),
            channel=target.channel,
            target=target.target,
            severity=severity,
            tenant=tenant,
            reason=reason,
            last_error=error,
            attempt_history=history,
            config=target.config or {},
        )
        log.info("dispatcher.abandoned", alert_id=alert_id, channel=target.channel, reason=reason)

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
