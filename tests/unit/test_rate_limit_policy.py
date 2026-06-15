"""Policy resolution: most-specific scope wins, else the global default (05 §7)."""


import app.recipients.rate_limit_policy as rlp
from app.queue.retry_queue import DeferredDelivery
from app.recipients.rate_limit_policy import (
    _specificity,
    default_policy,
    resolve_rate_limit,
)

RID = "11111111-1111-1111-1111-111111111111"
OTHER = "22222222-2222-2222-2222-222222222222"


def _row(recipient_id, channel_kind, capacity=5, refill=2.0, bypass=True):
    return {
        "recipient_id": recipient_id,
        "channel_kind": channel_kind,
        "capacity": capacity,
        "refill_per_sec": refill,
        "critical_bypass": bypass,
    }


def test_specificity_ordering():
    assert _specificity(_row(RID, "email"), RID, "email") == 3
    assert _specificity(_row(RID, None), RID, "email") == 2
    assert _specificity(_row(None, "email"), RID, "email") == 1
    assert _specificity(_row(None, None), RID, "email") == 0


def test_specificity_non_match_is_negative():
    assert _specificity(_row(OTHER, "email"), RID, "email") == -1
    assert _specificity(_row(RID, "slack"), RID, "email") == -1


async def test_resolve_picks_most_specific(monkeypatch):
    rows = [
        _row(None, None, capacity=10),  # tenant default
        _row(None, "email", capacity=20),  # channel default
        _row(RID, "email", capacity=99),  # most specific
    ]

    async def fake(_tenant):
        return rows

    monkeypatch.setattr(rlp, "_tenant_policies", fake)
    resolved = await resolve_rate_limit("t1", RID, "email")
    assert resolved.capacity == 99


async def test_resolve_falls_back_to_channel_default(monkeypatch):
    rows = [_row(None, None, capacity=10), _row(None, "email", capacity=20)]

    async def fake(_tenant):
        return rows

    monkeypatch.setattr(rlp, "_tenant_policies", fake)
    # No recipient-specific row for the SMS channel -> tenant default (10).
    assert (await resolve_rate_limit("t1", RID, "sms")).capacity == 10
    # Email has a channel default (20).
    assert (await resolve_rate_limit("t1", RID, "email")).capacity == 20


async def test_resolve_no_rows_uses_global_default(monkeypatch):
    async def fake(_tenant):
        return []

    monkeypatch.setattr(rlp, "_tenant_policies", fake)
    assert await resolve_rate_limit("t1", RID, "email") == default_policy()


def test_deferred_delivery_member_is_stable_for_reparks():
    # A re-park of the same logical delivery must produce an identical member so
    # ZADD updates the score instead of creating a duplicate entry (05 §7).
    d1 = DeferredDelivery("a", "t", "r", "email", "x@y", "low", 100, {"k": "v"})
    d2 = DeferredDelivery("a", "t", "r", "email", "x@y", "low", 100, {"k": "v"})
    assert d1.to_member() == d2.to_member()
    assert DeferredDelivery.from_member(d1.to_member()) == d1
