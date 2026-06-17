"""Redis-backed circuit breaker FSM (07 §4 acceptance criteria).

Runs the breaker's Lua against fakeredis. ``now_ms`` is injected so the
open-timeout transitions are deterministic without sleeping. A fresh breaker per
test registers its scripts against the test's fake client.
"""

from __future__ import annotations

import pytest

from app.channels.circuit import RedisCircuitBreaker
from app.config import get_settings

pytestmark = pytest.mark.usefixtures("fake_redis")

P = "slack"


def _breaker() -> RedisCircuitBreaker:
    return RedisCircuitBreaker()


async def _trip(cb: RedisCircuitBreaker, *, now: int) -> None:
    """Drive the breaker to OPEN with `threshold` failures inside the window."""
    threshold = get_settings().circuit_failure_threshold
    for _ in range(threshold):
        assert await cb.allow(P, now_ms=now) == "closed"
        await cb.record(P, ok=False, now_ms=now)


async def test_closed_below_threshold():
    cb = _breaker()
    threshold = get_settings().circuit_failure_threshold
    for _ in range(threshold - 1):
        await cb.record(P, ok=False, now_ms=1000)
    assert await cb.allow(P, now_ms=1000) == "closed"


async def test_opens_at_threshold_and_fast_fails():
    cb = _breaker()
    await _trip(cb, now=1000)
    # AC: during an open circuit, allow() returns "open" => the channel fast-fails.
    assert await cb.allow(P, now_ms=1000) == "open"


async def test_success_resets_failure_count():
    cb = _breaker()
    threshold = get_settings().circuit_failure_threshold
    for _ in range(threshold - 1):
        await cb.record(P, ok=False, now_ms=1000)
    await cb.record(P, ok=True, now_ms=1000)  # closes + clears the window
    for _ in range(threshold - 1):
        await cb.record(P, ok=False, now_ms=1000)
    assert await cb.allow(P, now_ms=1000) == "closed"


async def test_failures_outside_window_do_not_accumulate():
    cb = _breaker()
    window_ms = get_settings().circuit_failure_window_seconds * 1000
    threshold = get_settings().circuit_failure_threshold
    for _ in range(threshold - 1):
        await cb.record(P, ok=False, now_ms=1000)
    # One more failure, but after the window has rolled => count restarts at 1.
    await cb.record(P, ok=False, now_ms=1000 + window_ms + 1)
    assert await cb.allow(P, now_ms=1000 + window_ms + 1) == "closed"


async def test_half_open_after_timeout_grants_single_probe():
    cb = _breaker()
    await _trip(cb, now=1000)
    open_ms = get_settings().circuit_open_timeout_seconds * 1000
    after = 1000 + open_ms
    # First caller past the cooldown gets the lone probe; concurrent callers wait.
    assert await cb.allow(P, now_ms=after) == "probe"
    assert await cb.allow(P, now_ms=after) == "open"


async def test_probe_success_closes_the_circuit():
    cb = _breaker()
    await _trip(cb, now=1000)
    after = 1000 + get_settings().circuit_open_timeout_seconds * 1000
    assert await cb.allow(P, now_ms=after) == "probe"
    await cb.record(P, ok=True, now_ms=after)
    assert await cb.allow(P, now_ms=after) == "closed"


async def test_probe_failure_reopens_the_circuit():
    cb = _breaker()
    await _trip(cb, now=1000)
    open_ms = get_settings().circuit_open_timeout_seconds * 1000
    after = 1000 + open_ms
    assert await cb.allow(P, now_ms=after) == "probe"
    await cb.record(P, ok=False, now_ms=after)  # probe failed => re-open
    assert await cb.allow(P, now_ms=after) == "open"
    # ...and a fresh cooldown must elapse before the next probe.
    assert await cb.allow(P, now_ms=after + open_ms) == "probe"


async def test_breaker_is_per_provider():
    cb = _breaker()
    await _trip(cb, now=1000)
    assert await cb.allow("slack", now_ms=1000) == "open"
    # A different provider is unaffected — the whole point of per-provider (§4.2).
    assert await cb.allow("email", now_ms=1000) == "closed"


async def test_state_is_read_only_and_does_not_consume_the_probe():
    cb = _breaker()
    await _trip(cb, now=1000)
    after = 1000 + get_settings().circuit_open_timeout_seconds * 1000
    # state() reports half_open without taking the probe...
    assert await cb.state(P, now_ms=after) == "half_open"
    # ...so the probe is still available to a real allow() call.
    assert await cb.allow(P, now_ms=after) == "probe"
