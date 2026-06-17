"""Shared fixtures.

``fake_redis`` runs an in-memory Redis (``fakeredis[lua]``) that actually executes
the production Lua — priority pop, retry pop, and the circuit-breaker FSM — so
tests exercise the real code paths without a Docker Redis. It swaps the shared
client and clears every cached registered ``Script`` so they re-register against
the fresh fake (each test gets an isolated server).
"""

from __future__ import annotations

import pytest
import pytest_asyncio

import app.channels.circuit as circuit
import app.queue.priority_queue as pq
import app.queue.retry_queue as rq
import app.redis_client as redis_client

fakeredis = pytest.importorskip("fakeredis.aioredis", reason="fakeredis[lua] not installed")
from fakeredis import FakeServer  # noqa: E402  (after importorskip)


@pytest_asyncio.fixture
async def fake_redis(monkeypatch):
    client = fakeredis.FakeRedis(server=FakeServer(), decode_responses=True)
    monkeypatch.setattr(redis_client, "_client", client)
    # Force every cached Script to re-register against this test's fake client.
    monkeypatch.setattr(pq, "_pop_script", None)
    monkeypatch.setattr(rq, "_pop_script", None)
    monkeypatch.setattr(circuit._breaker, "_allow_script", None)
    monkeypatch.setattr(circuit._breaker, "_record_script", None)
    try:
        yield client
    finally:
        await client.aclose()
        pq._pop_script = None
        rq._pop_script = None
        circuit._breaker._allow_script = None
        circuit._breaker._record_script = None
