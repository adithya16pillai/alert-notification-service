"""Integration fixtures: an in-memory Redis that actually runs the Lua scripts.

``fakeredis[lua]`` executes the real ``pop_priority.lua`` against an in-memory
server, so these tests exercise the production code paths (``enqueue_alert`` ->
``pop_priority`` -> ``ack_inflight``) including atomicity and the starvation
guard — without needing a Docker Redis. The fixture swaps the shared client and
clears the cached registered script so the Lua re-registers against the fake.
"""

from __future__ import annotations

import pytest
import pytest_asyncio

import app.queue.priority_queue as pq
import app.redis_client as redis_client

fakeredis = pytest.importorskip("fakeredis.aioredis", reason="fakeredis[lua] not installed")
from fakeredis import FakeServer  # noqa: E402  (after importorskip)


@pytest_asyncio.fixture
async def fake_redis(monkeypatch):
    # A dedicated server per test => full isolation (fresh counters, queues).
    client = fakeredis.FakeRedis(server=FakeServer(), decode_responses=True)
    monkeypatch.setattr(redis_client, "_client", client)
    monkeypatch.setattr(pq, "_pop_script", None)  # force re-register vs the fake
    try:
        yield client
    finally:
        await client.aclose()
        pq._pop_script = None
