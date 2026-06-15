"""Rate limiting end-to-end against fakeredis[lua] (05 acceptance criteria).

Runs the real ``token_bucket.lua`` and ``pop_retry.lua`` in an in-memory Redis,
so these verify the burst/throttle behaviour, lazy refill, atomicity under
concurrency, the deferred-retry queue, and the EVALSHA->EVAL fallback.
"""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

import app.queue.rate_limit as rate_limit
import app.queue.retry_queue as retry_queue
import app.redis_client as redis_client
from app.queue.rate_limit import allow
from app.queue.retry_queue import DeferredDelivery, defer, pop_due_retries

fakeredis = pytest.importorskip("fakeredis.aioredis", reason="fakeredis[lua] not installed")
from fakeredis import FakeServer  # noqa: E402


@pytest_asyncio.fixture
async def fake_redis(monkeypatch):
    client = fakeredis.FakeRedis(server=FakeServer(), decode_responses=True)
    monkeypatch.setattr(redis_client, "_client", client)
    monkeypatch.setattr(rate_limit, "_bucket_script", None)
    monkeypatch.setattr(retry_queue, "_pop_script", None)
    try:
        yield client
    finally:
        await client.aclose()
        rate_limit._bucket_script = None
        retry_queue._pop_script = None


class _Clock:
    """Controllable clock for the rate-limit module (real refill is time-based)."""

    def __init__(self, t: float = 1_000_000.0) -> None:
        self.t = t

    def time(self) -> float:
        return self.t


@pytest.fixture
def clock(monkeypatch):
    c = _Clock()
    monkeypatch.setattr(rate_limit.time, "time", c.time)
    return c


# --------------------------------------------------------------------------- #
# Token bucket: burst then throttle, then refill
# --------------------------------------------------------------------------- #
async def test_burst_of_10_passes_then_throttles(fake_redis, clock):
    # AC: 20 requests to a fresh bucket (cap=10) at one instant -> first 10 pass.
    allowed = 0
    for _ in range(20):
        ok, _rem = await allow("rcpt", "email", capacity=10, refill_per_sec=1.0)
        allowed += int(ok)
    assert allowed == 10


async def test_refill_after_idle(fake_redis, clock):
    for _ in range(10):  # drain the bucket
        await allow("rcpt", "email", capacity=10, refill_per_sec=1.0)
    assert (await allow("rcpt", "email", capacity=10, refill_per_sec=1.0))[0] is False

    clock.t += 5.0  # AC: 5s later, 5 tokens have refilled
    passed = 0
    for _ in range(10):
        ok, _rem = await allow("rcpt", "email", capacity=10, refill_per_sec=1.0)
        passed += int(ok)
    assert passed == 5


async def test_buckets_are_independent_per_channel(fake_redis, clock):
    for _ in range(10):
        await allow("rcpt", "email", capacity=10, refill_per_sec=1.0)
    # Email is drained, but the recipient's Slack bucket is untouched.
    assert (await allow("rcpt", "email", capacity=10, refill_per_sec=1.0))[0] is False
    assert (await allow("rcpt", "slack", capacity=10, refill_per_sec=1.0))[0] is True


async def test_concurrent_workers_never_exceed_capacity(fake_redis, clock):
    # AC: 50 concurrent workers on one bucket (cap=10) -> exactly 10 accepted.
    results = await asyncio.gather(
        *(allow("rcpt", "email", capacity=10, refill_per_sec=1.0) for _ in range(50))
    )
    assert sum(int(ok) for ok, _ in results) == 10


async def test_bucket_expires_to_avoid_memory_leak(fake_redis, clock):
    await allow("rcpt", "email", capacity=10, refill_per_sec=1.0)
    ttl = await fake_redis.ttl("rl:{rcpt}:email")
    assert ttl > 0  # AC: idle buckets carry a TTL so they're garbage-collected


async def test_evalsha_falls_back_to_eval_on_noscript(fake_redis, clock):
    # AC: after the script cache is flushed, the next call still works (the
    # redis-py Script re-EVALs on NOSCRIPT and re-caches).
    assert (await allow("rcpt", "email", capacity=10, refill_per_sec=1.0))[0] is True
    await fake_redis.script_flush()
    assert (await allow("rcpt", "email", capacity=10, refill_per_sec=1.0))[0] is True


# --------------------------------------------------------------------------- #
# Deferred-retry queue
# --------------------------------------------------------------------------- #
def _d(alert_id, severity, first_ms=0):
    return DeferredDelivery(
        alert_id=alert_id,
        tenant="t1",
        recipient_id="r",
        channel="email",
        target="x@y",
        severity=severity,
        first_deferred_ms=first_ms,
    )


async def test_pop_due_returns_only_due_items(fake_redis):
    await defer(_d("due", "low"), due_ms=1000)
    await defer(_d("not-due", "low"), due_ms=5000)

    popped = await pop_due_retries(now_ms=2000, limit=10)
    ids = [p.alert_id for p in popped]
    assert ids == ["due"]
    # The not-yet-due item is still parked.
    assert (await pop_due_retries(now_ms=2000, limit=10)) == []


async def test_pop_due_drains_highest_severity_first(fake_redis):
    await defer(_d("low1", "low"), due_ms=100)
    await defer(_d("crit1", "critical"), due_ms=100)
    await defer(_d("high1", "high"), due_ms=100)

    popped = await pop_due_retries(now_ms=1000, limit=10)
    assert [p.alert_id for p in popped] == ["crit1", "high1", "low1"]


async def test_repark_updates_score_not_duplicates(fake_redis):
    d = _d("a", "low", first_ms=500)
    await defer(d, due_ms=1000)
    await defer(d, due_ms=9000)  # re-park the same logical delivery, later

    # Not due at t=2000 (score was bumped to 9000); exactly one entry exists.
    assert (await pop_due_retries(now_ms=2000, limit=10)) == []
    popped = await pop_due_retries(now_ms=9000, limit=10)
    assert [p.alert_id for p in popped] == ["a"]
