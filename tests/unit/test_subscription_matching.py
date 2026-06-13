"""Subscription matching edge cases — the dispatcher hot path (03 §7).

Covers severity floors, glob topic matching, and target collection (dedupe,
order, missing-channel tolerance).
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from app.recipients.matching import (
    collect_targets,
    meets_min_severity,
    severity_rank,
    subscription_matches,
    topic_matches,
)
from app.recipients.snapshot import SnapChannel, SnapSubscription, TenantSnapshot

# --- severity ranking ------------------------------------------------------ #


def test_severity_rank_orders_critical_most_severe():
    order = ["critical", "high", "medium", "low", "info"]
    ranks = [severity_rank(s) for s in order]
    assert ranks == sorted(ranks)  # critical lowest -> most severe
    assert severity_rank("critical") == 0
    assert severity_rank("info") == 4


def test_severity_rank_rejects_unknown_label():
    with pytest.raises(ValueError):
        severity_rank("emergency")


@pytest.mark.parametrize(
    "alert,floor,expected",
    [
        ("critical", "high", True),   # more severe than floor
        ("high", "high", True),       # exactly the floor
        ("low", "high", False),       # below the floor
        ("critical", "info", True),   # floor at the bottom catches everything
        ("info", "info", True),
        ("info", "critical", False),  # floor at the top catches only critical
    ],
)
def test_meets_min_severity(alert, floor, expected):
    assert meets_min_severity(alert, floor) is expected


# --- glob topic matching --------------------------------------------------- #


@pytest.mark.parametrize(
    "pattern,topic,expected",
    [
        ("auth.*", "auth.login", True),
        ("auth.*", "auth.user.created", True),  # '*' spans dots
        ("auth.*", "auth.", True),              # trailing empty segment
        ("auth.*", "auth", False),              # the literal '.' is required
        ("auth.*", "billing.charge", False),
        ("auth.login", "auth.login", True),     # exact pattern
        ("auth.login", "auth.logout", False),
        ("*", "anything.at.all", True),         # match-all
        ("Auth.*", "auth.login", False),        # case-sensitive (fnmatchcase)
    ],
)
def test_topic_matches(pattern, topic, expected):
    assert topic_matches(pattern, topic) is expected


def test_subscription_matches_requires_both_topic_and_severity():
    assert subscription_matches("auth.*", "high", topic="auth.login", severity="critical")
    # topic matches but severity below floor
    assert not subscription_matches("auth.*", "high", topic="auth.login", severity="low")
    # severity ok but topic mismatch
    assert not subscription_matches("auth.*", "low", topic="billing.x", severity="critical")


# --- target collection ----------------------------------------------------- #


def _snapshot(subs, channels):
    return TenantSnapshot(subscriptions=subs, channels=channels)


def test_collect_targets_maps_kind_and_address():
    rid = uuid4()
    cid = str(uuid4())
    snap = _snapshot(
        [SnapSubscription("auth.*", "info", [cid])],
        {cid: SnapChannel(rid, "email", "a@x.com", {"k": "v"})},
    )
    targets = collect_targets(snap, topic="auth.login", severity="high")
    assert len(targets) == 1
    t = targets[0]
    assert t.recipient_id == rid
    assert t.channel == "email"     # kind -> channel
    assert t.target == "a@x.com"    # address -> target
    assert t.config == {"k": "v"}


def test_collect_targets_dedupes_same_channel_across_subscriptions():
    cid = str(uuid4())
    ch = {cid: SnapChannel(uuid4(), "slack", "C123", {})}
    snap = _snapshot(
        [
            SnapSubscription("auth.*", "info", [cid]),
            SnapSubscription("*", "info", [cid]),  # also matches, same channel
        ],
        ch,
    )
    targets = collect_targets(snap, topic="auth.login", severity="critical")
    assert len(targets) == 1  # one delivery, not two


def test_collect_targets_skips_unknown_channel_ids():
    # A channel id referenced by a sub but absent from the snapshot (deleted after
    # build, before invalidation) is silently skipped, not an error.
    present = str(uuid4())
    snap = _snapshot(
        [SnapSubscription("auth.*", "info", [str(uuid4()), present])],
        {present: SnapChannel(uuid4(), "sms", "+15550000", {})},
    )
    targets = collect_targets(snap, topic="auth.x", severity="info")
    assert [t.target for t in targets] == ["+15550000"]


def test_collect_targets_filters_by_topic_and_severity():
    cid = str(uuid4())
    snap = _snapshot(
        [
            SnapSubscription("auth.*", "critical", [cid]),  # severity too strict
            SnapSubscription("billing.*", "info", [cid]),   # topic mismatch
        ],
        {cid: SnapChannel(uuid4(), "email", "a@x.com", {})},
    )
    assert collect_targets(snap, topic="auth.login", severity="high") == []
