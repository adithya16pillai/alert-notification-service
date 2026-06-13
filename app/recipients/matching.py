"""Subscription matching — the dispatcher hot path (03 §7).

Pure functions over a :class:`~app.recipients.snapshot.TenantSnapshot`: given an
alert's ``(topic, severity)`` they decide which subscriptions fire and expand the
matched subscriptions' ``channel_ids`` into a deduped list of delivery targets.
No I/O lives here, so the matching rules are exhaustively unit-testable.

Severity semantics: a subscription's ``min_severity`` is a *floor*. An alert
matches when it is at least as severe. Following 03 §7 we phrase this as
``severity_rank(alert) <= severity_rank(sub.min_severity)`` where a *lower* rank
means *more* severe (critical = 0 … info = 4).
"""

from __future__ import annotations

from fnmatch import fnmatchcase

from app.ingestion.schemas import Severity
from app.recipients.schemas import ResolvedTarget
from app.recipients.snapshot import TenantSnapshot

# critical = 0 (most severe) … info = 4 (least severe). Derived from the queue's
# priority ordering (critical = 4 … info = 0) so the two never drift apart.
_MAX_PRIORITY = max(s.priority for s in Severity)


def severity_rank(severity: str) -> int:
    """Rank where lower == more severe (03 §7). Raises on an unknown label."""
    return _MAX_PRIORITY - Severity(severity).priority


def meets_min_severity(alert_severity: str, min_severity: str) -> bool:
    """True when the alert is at least as severe as the subscription's floor."""
    return severity_rank(alert_severity) <= severity_rank(min_severity)


def topic_matches(pattern: str, topic: str) -> bool:
    """Case-sensitive glob match (``auth.*`` matches ``auth.login``).

    ``fnmatchcase`` (not ``fnmatch``) so behaviour is identical across OSes —
    plain ``fnmatch`` normalises case on some platforms.
    """
    return fnmatchcase(topic, pattern)


def subscription_matches(
    topic_pattern: str, min_severity: str, *, topic: str, severity: str
) -> bool:
    return topic_matches(topic_pattern, topic) and meets_min_severity(severity, min_severity)


def collect_targets(
    snapshot: TenantSnapshot, *, topic: str, severity: str
) -> list[ResolvedTarget]:
    """Expand matching subscriptions into deduped ``(recipient × channel)`` targets.

    Dedup is by channel id (03 §7 step 4): two subscriptions routing to the same
    channel produce one delivery, and order is stable (first match wins). A
    channel id that isn't in the snapshot (deleted/soft-deleted after the snapshot
    was built but before invalidation propagated) is silently skipped.
    """
    seen: set[str] = set()
    targets: list[ResolvedTarget] = []
    for sub in snapshot.subscriptions:
        if not subscription_matches(
            sub.topic_pattern, sub.min_severity, topic=topic, severity=severity
        ):
            continue
        for cid in sub.channel_ids:
            if cid in seen:
                continue
            channel = snapshot.channels.get(cid)
            if channel is None:
                continue
            seen.add(cid)
            targets.append(
                ResolvedTarget(
                    recipient_id=channel.recipient_id,
                    channel=channel.kind,
                    target=channel.address,
                    config=channel.config,
                )
            )
    return targets
