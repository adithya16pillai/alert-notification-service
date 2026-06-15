"""Content-dedup behaviour (06 acceptance criteria).

The dedup decision (``evaluate``) runs against fakeredis with the policy stubbed,
so it needs no Postgres. The full ingest path (records + ``dedup_count``) is
Postgres-gated and skips when the DB is unreachable.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

import app.ingestion.dedup as dedup
from app.ingestion.dedup import ResolvedDedupPolicy, evaluate
from app.ingestion.schemas import AlertIn, Severity

pytestmark = pytest.mark.usefixtures("fake_redis")  # swaps the Redis client

_BASE_POLICY = ResolvedDedupPolicy(
    dedup_fields=["host", "region"],
    window_seconds=300,
    critical_bypass=False,
    enabled=True,
    fingerprint_version=1,
)


def _use_policy(monkeypatch, **over):
    policy = replace(_BASE_POLICY, **over)

    async def fake(_tenant, _topic):
        return policy

    monkeypatch.setattr(dedup, "resolve_dedup_policy", fake)


def _alert(severity=Severity.high, host="web-01", region="eu"):
    return AlertIn(
        tenant_id="acme",
        source="siem",
        severity=severity,
        topic="auth.brute_force",
        title="failed logins",
        labels={"host": host, "region": region},
        occurred_at=datetime(2026, 6, 15, tzinfo=UTC),
    )


async def test_first_occurrence_is_not_deduped(monkeypatch):
    _use_policy(monkeypatch)
    out = await evaluate(_alert(), "id-1")
    assert out.decision == "first"
    assert out.original_id is None


async def test_second_identical_is_a_duplicate_of_the_first(monkeypatch):
    _use_policy(monkeypatch)
    await evaluate(_alert(), "id-1")
    out = await evaluate(_alert(), "id-2")
    assert out.decision == "duplicate"
    assert out.original_id == "id-1"  # points at the original


async def test_100_identical_yield_one_first_and_99_duplicates(monkeypatch):
    # AC §8: 100 identical alerts -> 1 dispatched + 99 duplicates of the original.
    _use_policy(monkeypatch)
    decisions = [(await evaluate(_alert(), f"id-{i}")) for i in range(100)]
    firsts = [d for d in decisions if d.decision == "first"]
    dupes = [d for d in decisions if d.decision == "duplicate"]
    assert len(firsts) == 1
    assert len(dupes) == 99
    assert all(d.original_id == "id-0" for d in dupes)


async def test_different_event_is_independent(monkeypatch):
    _use_policy(monkeypatch)
    await evaluate(_alert(host="web-01"), "id-1")
    out = await evaluate(_alert(host="web-02"), "id-2")  # different host => different event
    assert out.decision == "first"


async def test_window_expiry_allows_a_new_dispatch(monkeypatch, fake_redis):
    # AC §8: after the window expires, the same payload is a new dispatch.
    _use_policy(monkeypatch, window_seconds=300)
    await evaluate(_alert(), "id-1")
    await fake_redis.flushdb()  # simulate the dedup key's TTL elapsing
    out = await evaluate(_alert(), "id-2")
    assert out.decision == "first"


async def test_dedup_key_carries_the_window_ttl(monkeypatch, fake_redis):
    _use_policy(monkeypatch, window_seconds=123)
    await evaluate(_alert(), "id-1")
    keys = [k async for k in fake_redis.scan_iter(match="dedup:v1:*")]
    assert keys
    assert 0 < await fake_redis.ttl(keys[0]) <= 123


async def test_critical_bypass_skips_dedup(monkeypatch):
    # AC §8: with bypass on, two identical critical alerts both dispatch.
    _use_policy(monkeypatch, critical_bypass=True)
    a = await evaluate(_alert(severity=Severity.critical), "id-1")
    b = await evaluate(_alert(severity=Severity.critical), "id-2")
    assert a.decision == "skipped"
    assert b.decision == "skipped"


async def test_critical_is_deduped_when_bypass_off(monkeypatch):
    # PRD §4 default: critical IS deduped.
    _use_policy(monkeypatch, critical_bypass=False)
    await evaluate(_alert(severity=Severity.critical), "id-1")
    out = await evaluate(_alert(severity=Severity.critical), "id-2")
    assert out.decision == "duplicate"


async def test_disabled_policy_skips_dedup(monkeypatch):
    _use_policy(monkeypatch, enabled=False)
    out = await evaluate(_alert(), "id-1")
    assert out.decision == "skipped"
