"""Per-channel backoff: exponential, capped, with ±25% jitter (04 §5)."""

import random

from app.channels.policy import JITTER_RATIO, ChannelPolicy, backoff_delay, get_policy


def test_policy_table_matches_spec():
    # 04 §5 per-channel constraints table.
    assert get_policy("email") == ChannelPolicy(10.0, 5, 2.0, 300.0)
    assert get_policy("slack") == ChannelPolicy(5.0, 3, 1.0, 60.0)
    assert get_policy("webhook") == ChannelPolicy(8.0, 5, 2.0, 300.0)
    assert get_policy("sms") == ChannelPolicy(8.0, 3, 5.0, 300.0)


def test_unknown_channel_gets_default_policy():
    assert get_policy("carrier-pigeon").max_retries == 3


def test_backoff_is_exponential_before_cap():
    p = ChannelPolicy(timeout_s=5, max_retries=5, backoff_base_s=2.0, backoff_cap_s=300.0)
    # Pin jitter to 0 (rng.random() == 0.5 -> 2*0.5-1 == 0).
    rng = random.Random()
    rng.random = lambda: 0.5  # type: ignore[method-assign]
    assert backoff_delay(p, 0, rng=rng) == 2.0
    assert backoff_delay(p, 1, rng=rng) == 4.0
    assert backoff_delay(p, 2, rng=rng) == 8.0


def test_backoff_respects_cap():
    p = ChannelPolicy(timeout_s=5, max_retries=10, backoff_base_s=2.0, backoff_cap_s=10.0)
    rng = random.Random()
    rng.random = lambda: 0.5  # type: ignore[method-assign]
    assert backoff_delay(p, 8, rng=rng) == 10.0  # 2*256 capped to 10


def test_jitter_stays_within_band():
    p = ChannelPolicy(timeout_s=5, max_retries=5, backoff_base_s=4.0, backoff_cap_s=300.0)
    rng = random.Random(1234)
    raw = 4.0  # attempt 0
    for _ in range(1000):
        d = backoff_delay(p, 0, rng=rng)
        assert raw * (1 - JITTER_RATIO) <= d <= raw * (1 + JITTER_RATIO)


def test_jitter_extremes():
    p = ChannelPolicy(timeout_s=5, max_retries=5, backoff_base_s=4.0, backoff_cap_s=300.0)
    low = random.Random()
    low.random = lambda: 0.0  # type: ignore[method-assign]
    high = random.Random()
    high.random = lambda: 1.0  # type: ignore[method-assign]
    assert backoff_delay(p, 0, rng=low) == 4.0 * (1 - JITTER_RATIO)
    assert backoff_delay(p, 0, rng=high) == 4.0 * (1 + JITTER_RATIO)


def test_jitter_spreads_concurrent_retries():
    # 07 §6: 100 clients retrying the same attempt must not land on one instant.
    # Jitter spreads them across a band, so the recovering provider isn't hit by a
    # synchronized herd (07 §3.2). Compare to the herd a no-jitter backoff produces.
    p = ChannelPolicy(timeout_s=5, max_retries=5, backoff_base_s=4.0, backoff_cap_s=300.0)
    rng = random.Random(42)
    delays = [backoff_delay(p, 2, rng=rng) for _ in range(100)]
    assert len(set(delays)) > 90  # near-all distinct => no lockstep
    raw = min(4.0 * (2**2), 300.0)
    # The jitter band spans raw·2·JITTER_RATIO (= 0.5·raw here); 100 samples should
    # fill most of it rather than clustering on one instant.
    assert max(delays) - min(delays) > 0.3 * raw
