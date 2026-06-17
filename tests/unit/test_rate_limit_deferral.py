"""Dispatcher rate-limit behaviour: critical bypass + defer-not-drop (05 §7, §8).

These exercise the decision logic in isolation by stubbing the I/O collaborators
(token bucket, policy cache, retry queue, DLQ, delivery) the dispatcher imports.
"""

import app.dispatcher.worker as worker
from app.dispatcher.worker import Dispatcher, _AlertSnapshot
from app.queue.retry_queue import DeferredDelivery
from app.recipients.rate_limit_policy import ResolvedRateLimit
from app.recipients.schemas import ResolvedTarget

RID = "11111111-1111-1111-1111-111111111111"


def _target(channel="email"):
    return ResolvedTarget(recipient_id=RID, channel=channel, target="ops@x.test", config={})


def _deferred(channel="email", first_deferred_ms=0, severity="high"):
    return DeferredDelivery(
        alert_id="01ABC",
        tenant="t1",
        recipient_id=RID,
        channel=channel,
        target="ops@x.test",
        severity=severity,
        first_deferred_ms=first_deferred_ms,
        config={},
    )


# --------------------------------------------------------------------------- #
# Critical bypass
# --------------------------------------------------------------------------- #
async def test_critical_bypasses_the_limiter(monkeypatch):
    called = {"allow": False}

    async def fake_resolve(_t, _r, _c):
        return ResolvedRateLimit(capacity=1, refill_per_sec=1.0, critical_bypass=True)

    async def fake_allow(*a, **k):
        called["allow"] = True
        return (False, 0.0)

    monkeypatch.setattr(worker, "resolve_rate_limit", fake_resolve)
    monkeypatch.setattr(worker, "allow", fake_allow)

    d = Dispatcher()
    assert await d._rate_limit_allows("t1", "critical", _target()) is True
    assert called["allow"] is False  # limiter never consulted for critical


async def test_non_critical_consults_the_limiter(monkeypatch):
    async def fake_resolve(_t, _r, _c):
        return ResolvedRateLimit(capacity=1, refill_per_sec=1.0, critical_bypass=True)

    async def fake_allow(rid, ch, **k):
        return (False, 0.0)

    monkeypatch.setattr(worker, "resolve_rate_limit", fake_resolve)
    monkeypatch.setattr(worker, "allow", fake_allow)

    d = Dispatcher()
    # bypass is on, but severity is 'high' -> limiter applies and denies.
    assert await d._rate_limit_allows("t1", "high", _target()) is False


async def test_bypass_off_means_critical_is_limited(monkeypatch):
    async def fake_resolve(_t, _r, _c):
        return ResolvedRateLimit(capacity=1, refill_per_sec=1.0, critical_bypass=False)

    async def fake_allow(rid, ch, **k):
        return (False, 0.0)

    monkeypatch.setattr(worker, "resolve_rate_limit", fake_resolve)
    monkeypatch.setattr(worker, "allow", fake_allow)

    d = Dispatcher()
    assert await d._rate_limit_allows("t1", "critical", _target()) is False


# --------------------------------------------------------------------------- #
# Deferral retry processing
# --------------------------------------------------------------------------- #
async def test_deferred_past_cap_is_abandoned_to_dlq(monkeypatch):
    dlq, records = [], []

    async def fake_push(*, alert_id, channel, reason, last_error, **_kw):
        dlq.append((alert_id, channel, reason, last_error))

    monkeypatch.setattr(worker.dlq, "push", fake_push)

    d = Dispatcher()

    async def fake_record(status, alert_id, target, retry_count, provider_id, error):
        records.append((status, error))

    d._record = fake_record  # type: ignore[method-assign]

    # first_deferred_ms far enough in the past to exceed the 60s cap.
    expired = worker.now_ms() - (d.settings.rate_limit_max_defer_seconds * 1000 + 5000)
    await d._process_deferred(_deferred(first_deferred_ms=expired))

    assert dlq and dlq[0][2] == "rate_limit_expired"
    assert dlq[0][3] == "rate_limit_deferral_expired"
    assert ("abandoned", "rate_limit_deferral_expired") in records


async def test_deferred_still_limited_is_reparked(monkeypatch):
    parked = []

    async def fake_defer(delivery, *, due_ms):
        parked.append((delivery, due_ms))

    monkeypatch.setattr(worker, "defer", fake_defer)

    d = Dispatcher()

    async def deny(*a, **k):
        return False

    d._rate_limit_allows = deny  # type: ignore[method-assign]

    delivery = _deferred(first_deferred_ms=worker.now_ms())
    await d._process_deferred(delivery)

    assert len(parked) == 1
    # Re-park preserves the original first_deferred_ms so the cap measures total time.
    assert parked[0][0].first_deferred_ms == delivery.first_deferred_ms


async def test_deferred_now_allowed_is_delivered(monkeypatch):
    delivered = []

    d = Dispatcher()

    async def allow_now(*a, **k):
        return True

    async def fake_load(alert_id):
        return _AlertSnapshot(id=alert_id, title="t", body="b", severity="high", tenant="t1")

    async def fake_attempt(snapshot, target, *, attempt_no, history):
        delivered.append((snapshot.id, target.channel, attempt_no))

    d._rate_limit_allows = allow_now  # type: ignore[method-assign]
    d._load_snapshot = fake_load  # type: ignore[method-assign]
    d._attempt = fake_attempt  # type: ignore[method-assign]

    await d._process_deferred(_deferred(first_deferred_ms=worker.now_ms()))
    # A cleared limit starts the delivery attempt sequence from scratch (attempt 0).
    assert delivered == [("01ABC", "email", 0)]
