"""Contract tests for the rate-limit-policy config API (05 §2).

Only paths that fail validation *before* the handler touches Postgres, so no
datastores are needed (mirrors the recipients contract tests). Happy-path upsert
+ resolution lives in the Redis/Postgres integration tests.
"""

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_capacity_must_be_at_least_one():
    r = client.put("/v1/rate-limit-policies", json={"capacity": 0, "refill_per_sec": 1.0})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "validation_error"


def test_refill_must_be_positive():
    r = client.put("/v1/rate-limit-policies", json={"capacity": 10, "refill_per_sec": 0})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "validation_error"


def test_bad_channel_kind_is_rejected():
    r = client.put(
        "/v1/rate-limit-policies",
        json={"capacity": 10, "refill_per_sec": 1.0, "channel_kind": "carrier-pigeon"},
    )
    assert r.status_code == 400
    assert r.json()["error"]["field"] == "channel_kind"


def test_unknown_field_is_rejected():
    r = client.put(
        "/v1/rate-limit-policies",
        json={"capacity": 10, "refill_per_sec": 1.0, "bogus": True},
    )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "validation_error"
