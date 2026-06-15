"""Per-channel delivery policy: timeout, retry budget, and backoff (04 §5).

These are channel *constants*, not per-deployment tunables — the values come
straight from the per-channel constraints table in 04 §5 and reflect each
provider's published behaviour. Keeping them here (rather than in ``Settings``)
makes the table the single source of truth and keeps the env surface small.

Backoff is exponential with full ±25% jitter (04 §5, 07). Pure exponential
backoff causes a thundering herd: every client retries on the same boundary, so
a provider that just recovered is immediately hammered again. Jitter spreads the
retries out.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

#: Fraction of jitter applied symmetrically around the computed backoff (±25%).
JITTER_RATIO = 0.25


@dataclass(frozen=True)
class ChannelPolicy:
    """Reliability envelope for one channel kind."""

    timeout_s: float
    max_retries: int
    backoff_base_s: float
    backoff_cap_s: float


# 04 §5 — per-channel constraints. Edit the table, edit one line here.
POLICIES: dict[str, ChannelPolicy] = {
    "email": ChannelPolicy(timeout_s=10.0, max_retries=5, backoff_base_s=2.0, backoff_cap_s=300.0),
    "slack": ChannelPolicy(timeout_s=5.0, max_retries=3, backoff_base_s=1.0, backoff_cap_s=60.0),
    "webhook": ChannelPolicy(timeout_s=8.0, max_retries=5, backoff_base_s=2.0, backoff_cap_s=300.0),
    "sms": ChannelPolicy(timeout_s=8.0, max_retries=3, backoff_base_s=5.0, backoff_cap_s=300.0),
}

#: Conservative fallback for an unknown channel kind (mirrors Settings defaults).
DEFAULT_POLICY = ChannelPolicy(
    timeout_s=5.0, max_retries=3, backoff_base_s=1.0, backoff_cap_s=60.0
)


def get_policy(kind: str) -> ChannelPolicy:
    return POLICIES.get(kind, DEFAULT_POLICY)


def backoff_delay(
    policy: ChannelPolicy, attempt: int, *, rng: random.Random | None = None
) -> float:
    """Seconds to sleep before retry ``attempt`` (0-indexed).

    ``min(base * 2**attempt, cap)`` with full ±``JITTER_RATIO`` jitter. ``rng`` is
    injectable so tests can pin the jitter; production uses the module RNG.
    """

    raw = min(policy.backoff_base_s * (2**attempt), policy.backoff_cap_s)
    draw = rng.random() if rng is not None else random.random()
    jitter = raw * JITTER_RATIO * (2 * draw - 1)  # uniform in [-ratio, +ratio]·raw
    return float(max(0.0, raw + jitter))
