"""Idempotency conflict detection rests on the payload fingerprint (01 §8)."""

from datetime import datetime

from app.ingestion.schemas import AlertIn, Severity
from app.ingestion.service import _fingerprint


def _alert(**overrides) -> AlertIn:
    base = dict(
        tenant_id="acme",
        source="siem.splunk",
        severity=Severity.critical,
        topic="auth.brute_force",
        title="10+ failed logins",
        body="source ip ...",
        labels={"host": "web-01"},
        payload={"raw": "x"},
        occurred_at=datetime(2026, 5, 17, 9, 12, 33),
    )
    base.update(overrides)
    return AlertIn(**base)


def test_same_payload_same_fingerprint():
    assert _fingerprint(_alert()) == _fingerprint(_alert())


def test_label_order_does_not_change_fingerprint():
    a = _alert(labels={"host": "web-01", "region": "eu"})
    b = _alert(labels={"region": "eu", "host": "web-01"})
    assert _fingerprint(a) == _fingerprint(b)


def test_different_payload_changes_fingerprint():
    assert _fingerprint(_alert()) != _fingerprint(_alert(title="different title"))


def test_strict_mode_rejects_unknown_fields():
    import pytest
    from pydantic import ValidationError as PydValidationError

    with pytest.raises(PydValidationError):
        _alert(unexpected="boom")
