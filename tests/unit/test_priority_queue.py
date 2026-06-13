"""Pure (no-datastore) checks for the priority-queue wiring (02 §5).

These assert the contract the Lua script depends on — keys are built per
severity and handed to the script highest-priority-first — and that the
starvation/visibility tunables have sane defaults. Behavioural tests that need
Redis + Lua live in tests/integration/test_priority_queue_redis.py.
"""

from app.config import get_settings
from app.ingestion.schemas import Severity
from app.queue.priority_queue import _queue_key


def test_queue_key_is_prefixed_per_severity():
    prefix = get_settings().queue_key_prefix
    assert _queue_key(Severity.critical) == f"{prefix}:critical"
    assert _queue_key("info") == f"{prefix}:info"


def test_severities_are_ordered_critical_first():
    # The Lua script drains KEYS in order, so this tuple *is* the priority order.
    assert get_settings().severities == ("critical", "high", "medium", "low", "info")


def test_starvation_and_visibility_defaults():
    s = get_settings()
    assert s.queue_starvation_factor == 10  # 1-in-N, default N=10 (02 §3)
    assert s.inflight_ttl_seconds == 60  # visibility timeout (02 §6)
    assert s.info_shed_enabled is False  # backpressure is opt-in; critical never shed
