"""Contract tests for POST /v1/alerts request validation + error schema (01 §5).

These exercise paths that fail *before* the handler touches Postgres/Redis, so
they need no datastores. Happy-path + idempotency live in integration tests.
"""

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)

VALID = {
    "tenant_id": "acme",
    "source": "siem.splunk",
    "severity": "critical",
    "topic": "auth.brute_force",
    "title": "10+ failed logins for user admin",
    "occurred_at": "2026-05-17T09:12:33Z",
}


def _post(body, **headers):
    return client.post("/v1/alerts", json=body, headers={"Idempotency-Key": "k1", **headers})


def test_unknown_top_level_field_is_rejected():
    r = _post({**VALID, "bogus": 1})
    assert r.status_code == 400
    err = r.json()["error"]
    assert err["code"] == "validation_error"
    assert err["field"] == "bogus"


def test_invalid_severity_is_rejected():
    r = _post({**VALID, "severity": "apocalyptic"})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "validation_error"


def test_idempotency_key_header_is_required():
    r = client.post("/v1/alerts", json=VALID)
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "validation_error"


def test_error_schema_always_carries_trace_id():
    r = _post({**VALID, "severity": "nope"})
    err = r.json()["error"]
    assert set(err) == {"code", "message", "field", "trace_id"}
    assert err["trace_id"]
