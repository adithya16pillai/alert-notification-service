"""Subscription cache behaviour against fakeredis (03 §7).

Exercises the two-layer cache (local + Redis), the TTL, and — most importantly —
that an invalidation published by the API process drops a *worker* process's
local cache via pub/sub, which is what makes a subscription change go live across
all workers within ~1s (03 §4 acceptance criteria).

``build_snapshot`` (the DB query) is stubbed so these tests need only Redis; the
real two-query build is covered in tests/unit/test_subscription_snapshot.py.
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

import app.recipients.cache as cache
from app.config import get_settings
from app.recipients.snapshot import SnapChannel, SnapSubscription, TenantSnapshot

pytestmark = pytest.mark.usefixtures("fake_redis")


@pytest.fixture(autouse=True)
def _clear_local():
    cache.clear_local()
    yield
    cache.clear_local()


def _make_snapshot(address: str = "a@x.com") -> TenantSnapshot:
    cid = str(uuid4())
    return TenantSnapshot(
        subscriptions=[SnapSubscription("auth.*", "info", [cid])],
        channels={cid: SnapChannel(uuid4(), "email", address, {})},
    )


def _counting_builder(snapshot: TenantSnapshot):
    calls = {"n": 0}

    async def _build(_session, _tenant):
        calls["n"] += 1
        return snapshot

    return _build, calls


async def test_miss_builds_then_local_hit_skips_rebuild(monkeypatch):
    build, calls = _counting_builder(_make_snapshot())
    monkeypatch.setattr(cache, "build_snapshot", build)

    first = await cache.get_snapshot(None, "t1")   # miss -> build
    second = await cache.get_snapshot(None, "t1")  # local hit -> no build

    assert calls["n"] == 1
    assert first == second


async def test_redis_hit_after_local_cleared(monkeypatch, fake_redis):
    build, calls = _counting_builder(_make_snapshot())
    monkeypatch.setattr(cache, "build_snapshot", build)

    await cache.get_snapshot(None, "t1")  # miss populates redis + local
    cache.clear_local()                   # simulate a fresh worker process

    snap = await cache.get_snapshot(None, "t1")  # served from redis, no rebuild
    assert calls["n"] == 1
    assert snap.subscriptions[0].topic_pattern == "auth.*"
    # And the redis key carries the bounded TTL.
    ttl = await fake_redis.ttl(cache._key("t1"))
    assert 0 < ttl <= get_settings().subs_cache_ttl_seconds


async def test_invalidate_clears_both_layers_and_forces_rebuild(monkeypatch, fake_redis):
    build, calls = _counting_builder(_make_snapshot())
    monkeypatch.setattr(cache, "build_snapshot", build)

    await cache.get_snapshot(None, "t1")
    await cache.invalidate("t1")

    assert await fake_redis.get(cache._key("t1")) is None  # redis dropped
    assert "t1" not in cache._local                        # local dropped

    await cache.get_snapshot(None, "t1")  # must rebuild
    assert calls["n"] == 2


async def test_local_expiry_falls_back_to_redis_not_db(monkeypatch):
    build, calls = _counting_builder(_make_snapshot())
    monkeypatch.setattr(cache, "build_snapshot", build)

    await cache.get_snapshot(None, "t1")  # build #1, populates redis + local
    # Force the local entry to look expired without sleeping.
    snap, _ = cache._local["t1"]
    cache._local["t1"] = (snap, 0.0)

    await cache.get_snapshot(None, "t1")  # local expired -> redis, still no rebuild
    assert calls["n"] == 1


async def test_pubsub_invalidation_drops_local_cache_in_a_listener(monkeypatch, fake_redis):
    """A publish on the invalidate channel must drop another process's local cache."""
    build, _ = _counting_builder(_make_snapshot())
    monkeypatch.setattr(cache, "build_snapshot", build)

    # Prime this process's local cache.
    await cache.get_snapshot(None, "t1")
    assert "t1" in cache._local

    ready = asyncio.Event()
    listener = asyncio.create_task(cache.listen_for_invalidations(ready=ready))
    try:
        await asyncio.wait_for(ready.wait(), timeout=2.0)
        # Simulate a *different* process invalidating: publish directly so we test
        # the listener path, not the local in-process drop in invalidate().
        await fake_redis.publish(get_settings().subs_invalidate_channel, "t1")

        async def _dropped() -> bool:
            for _ in range(200):
                if "t1" not in cache._local:
                    return True
                await asyncio.sleep(0.01)
            return False

        assert await _dropped()
    finally:
        listener.cancel()
        with pytest.raises(asyncio.CancelledError):
            await listener
