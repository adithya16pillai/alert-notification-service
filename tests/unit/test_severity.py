"""Severity is a StrEnum whose ``priority`` drives queue ordering."""

from app.ingestion.schemas import Severity


def test_severity_values_are_the_labels():
    assert Severity.critical.value == "critical"
    assert Severity("high") is Severity.high


def test_priority_orders_high_to_low():
    order = sorted(Severity, key=lambda s: s.priority, reverse=True)
    assert order == [
        Severity.critical,
        Severity.high,
        Severity.medium,
        Severity.low,
        Severity.info,
    ]


def test_critical_is_highest_priority():
    assert max(Severity, key=lambda s: s.priority) is Severity.critical
