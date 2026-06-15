"""Content-dedup fingerprint: what defines "same event" (06 §5, §8)."""

import time

from app.ingestion.dedup import compute_fingerprint

FIELDS = ["host", "region"]


def _fp(labels, *, tenant="acme", topic="auth.brute_force", fields=None):
    return compute_fingerprint(tenant, topic, labels, fields or FIELDS)


def test_same_event_same_fingerprint():
    assert _fp({"host": "web-01", "region": "eu"}) == _fp({"host": "web-01", "region": "eu"})


def test_label_order_does_not_matter():
    assert _fp({"host": "web-01", "region": "eu"}) == _fp({"region": "eu", "host": "web-01"})


def test_non_dedup_labels_are_ignored():
    # A label not in dedup_fields (and the volatile bits not even passed in:
    # timestamp, body, payload) must not change the fingerprint (06 §5).
    base = {"host": "web-01", "region": "eu"}
    assert _fp(base) == _fp({**base, "request_id": "abc", "ts": "now"})


def test_changing_a_dedup_field_changes_fingerprint():
    assert _fp({"host": "web-01", "region": "eu"}) != _fp({"host": "web-02", "region": "eu"})


def test_missing_label_still_collides():
    # Two alerts that both omit a dedup field are the "same event".
    assert _fp({"region": "eu"}) == _fp({"region": "eu"})
    # but differ from one that has the field set.
    assert _fp({"region": "eu"}) != _fp({"host": "web-01", "region": "eu"})


def test_tenant_and_topic_are_part_of_identity():
    labels = {"host": "web-01", "region": "eu"}
    assert _fp(labels, tenant="acme") != _fp(labels, tenant="other")
    assert _fp(labels, topic="auth.x") != _fp(labels, topic="auth.y")


def test_configurable_fields_change_what_matches():
    labels = {"service": "api", "endpoint": "/v1/x", "host": "web-01"}
    # With (service, endpoint) two alerts on the same service+endpoint collide
    # even if host differs.
    a = compute_fingerprint("t", "topic", labels, ["service", "endpoint"])
    b = compute_fingerprint("t", "topic", {**labels, "host": "web-99"}, ["service", "endpoint"])
    assert a == b


def test_fingerprint_is_fast_for_20_labels():
    # AC §8: <1ms p99 for a 20-label alert. Time 1000 computations; the per-call
    # average must be comfortably sub-millisecond.
    labels = {f"k{i}": f"v{i}" for i in range(20)}
    fields = [f"k{i}" for i in range(20)]
    start = time.perf_counter()
    for _ in range(1000):
        compute_fingerprint("acme", "auth.x", labels, fields)
    avg_ms = (time.perf_counter() - start) / 1000 * 1000
    assert avg_ms < 1.0
