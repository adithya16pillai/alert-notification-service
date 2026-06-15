"""Dedup policy resolution: exact topic > tenant default > global (06 §4)."""

import app.ingestion.dedup as dedup
from app.ingestion.dedup import default_dedup_policy, resolve_dedup_policy


def _row(topic, *, fields=None, window=300, bypass=False, enabled=True):
    return {
        "topic": topic,
        "dedup_fields": fields or ["host", "region"],
        "window_seconds": window,
        "critical_bypass": bypass,
        "enabled": enabled,
    }


async def test_exact_topic_wins_over_tenant_default(monkeypatch):
    rows = [_row(None, window=300), _row("auth.brute_force", window=60)]

    async def fake(_tenant):
        return rows

    monkeypatch.setattr(dedup, "_tenant_policies", fake)
    assert (await resolve_dedup_policy("t", "auth.brute_force")).window_seconds == 60
    # A topic with no exact row falls back to the tenant default.
    assert (await resolve_dedup_policy("t", "disk.full")).window_seconds == 300


async def test_no_rows_uses_global_default(monkeypatch):
    async def fake(_tenant):
        return []

    monkeypatch.setattr(dedup, "_tenant_policies", fake)
    resolved = await resolve_dedup_policy("t", "auth.x")
    default = default_dedup_policy()
    assert resolved.dedup_fields == default.dedup_fields
    assert resolved.window_seconds == default.window_seconds
    assert resolved.critical_bypass == default.critical_bypass


async def test_default_dedupes_critical(monkeypatch):
    # PRD §4: dedupe critical by default (bypass off).
    assert default_dedup_policy().critical_bypass is False
