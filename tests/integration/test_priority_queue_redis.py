"""Behavioural tests for the severity priority queue (02 acceptance criteria).

Run against fakeredis[lua] executing the real Lua script, so they verify the
atomic pop, strict priority, FIFO-within-severity, the 1-in-N starvation guard,
and the visibility (in-flight) set end to end.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.config import get_settings
from app.ingestion.schemas import Severity
from app.queue.priority_queue import (
    ack_inflight,
    enqueue_alert,
    pop_priority,
    queue_depth_for,
    reap_inflight,
)

pytestmark = pytest.mark.usefixtures("fake_redis")


async def _seed(severity: Severity, prefix: str, n: int, *, base_score: int = 1000) -> list[str]:
    ids = [f"{prefix}{i}" for i in range(n)]
    for i, alert_id in enumerate(ids):
        await enqueue_alert(alert_id, severity, score=base_score + i)
    return ids


async def test_critical_jumps_a_large_info_backlog():
    # AC: a critical enqueued behind 10k info items is consumed first.
    await _seed(Severity.info, "info", 10_000)
    await enqueue_alert("crit-1", Severity.critical, score=999_999)

    batch = await pop_priority(50)

    assert "crit-1" in batch
    # Nothing lower-priority sneaks ahead: a normal pop returns one severity only.
    assert all(b.startswith("crit") for b in batch)


async def test_fifo_within_a_severity():
    await enqueue_alert("a", Severity.high, score=300)
    await enqueue_alert("b", Severity.high, score=100)  # oldest
    await enqueue_alert("c", Severity.high, score=200)

    assert await pop_priority(3) == ["b", "c", "a"]  # ascending score = FIFO


async def test_starvation_guard_surfaces_low_within_n_pops(fake_redis):
    # Critical stays permanently full; a lone low item must still drain by the
    # Nth pop (default N=10) thanks to the starvation tick.
    await _seed(Severity.critical, "c", 200, base_score=6000)
    await enqueue_alert("low-1", Severity.low, score=1)
    await fake_redis.set(get_settings().starvation_counter_key, 0)

    popped_low_at = None
    for pop_no in range(1, get_settings().queue_starvation_factor + 1):
        if "low-1" in await pop_priority(1):
            popped_low_at = pop_no
            break

    assert popped_low_at == get_settings().queue_starvation_factor


async def test_no_duplicate_ids_under_concurrent_pops():
    # AC: concurrent workers never both process the same alert id.
    import asyncio

    await _seed(Severity.medium, "m", 100)

    async def worker() -> list[str]:
        got: list[str] = []
        for _ in range(40):
            got += await pop_priority(3)
        return got

    results = await asyncio.gather(*(worker() for _ in range(4)))
    drained = [alert_id for r in results for alert_id in r]

    assert sorted(drained) == sorted(f"m{i}" for i in range(100))  # all, no dupes


async def test_pop_tracks_inflight_and_ack_clears_it(fake_redis):
    await enqueue_alert("x", Severity.high, score=1)
    inflight_key = get_settings().inflight_key

    batch = await pop_priority(10)
    assert batch == ["x"]
    assert await fake_redis.zscore(inflight_key, "x") is not None  # visible in-flight

    await ack_inflight(["x"])
    assert await fake_redis.zscore(inflight_key, "x") is None  # cleared on ack


# --- Reaper: worker crash mid-batch -> re-queue after the visibility timeout ---


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return self

    def all(self):
        return self._rows


class _FakeSession:
    """Minimal stand-in for AsyncSession.execute().scalars().all()."""

    def __init__(self, rows):
        self._rows = rows

    async def execute(self, _stmt):
        return _FakeResult(self._rows)


class _Row:
    def __init__(self, alert_id, severity, status):
        self.id = alert_id
        self.severity = severity
        self.status = status
        self.received_at = datetime(2026, 6, 12, tzinfo=UTC)


async def test_reaper_requeues_expired_accepted_only(fake_redis):
    inflight_key = get_settings().inflight_key
    # Two popped-but-unacked alerts with a deadline already in the past.
    await fake_redis.zadd(inflight_key, {"still-accepted": 1, "already-done": 1})

    session = _FakeSession(
        [
            _Row("still-accepted", "high", "accepted"),  # worker died -> re-queue
            _Row("already-done", "high", "dispatched"),  # finished -> just forget
        ]
    )

    requeued = await reap_inflight(session)

    assert requeued == 1
    assert await queue_depth_for(Severity.high) == 1  # the accepted one is back
    # Both are cleared from in-flight regardless of outcome.
    assert await fake_redis.zcard(inflight_key) == 0
