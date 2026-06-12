"""Circuit breaker opens after the failure threshold and recovers on success."""

import pytest

from app.channels.base import CircuitBreaker
from app.errors import CircuitOpenError


def test_stays_closed_below_threshold():
    cb = CircuitBreaker()
    for _ in range(cb._threshold - 1):
        cb.record(ok=False)
    cb.check()  # must not raise


def test_opens_at_threshold():
    cb = CircuitBreaker()
    for _ in range(cb._threshold):
        cb.record(ok=False)
    with pytest.raises(CircuitOpenError):
        cb.check()


def test_success_resets_failures():
    cb = CircuitBreaker()
    for _ in range(cb._threshold - 1):
        cb.record(ok=False)
    cb.record(ok=True)
    for _ in range(cb._threshold - 1):
        cb.record(ok=False)
    cb.check()  # reset means we're back below threshold
