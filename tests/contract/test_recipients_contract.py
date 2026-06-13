"""Contract tests for the recipients/subscriptions admin surface (03 §4, §6).

Like the ingestion contract tests, these only hit paths that fail *before* the
handler touches Postgres — page-size policy and cursor parsing — so they need no
datastores. Tenant scoping and CRUD happy paths live in
tests/integration/test_recipients_db.py (Postgres-gated).

Env defaults to ``local``, so auth + tenant resolution fall back to anonymous /
``local`` (see app/auth/dependencies.py).
"""

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_limit_over_200_is_rejected_with_400():
    r = client.get("/v1/recipients", params={"limit": 201})
    assert r.status_code == 400
    err = r.json()["error"]
    assert err["code"] == "validation_error"
    assert err["field"] == "limit"


def test_limit_zero_is_rejected():
    r = client.get("/v1/recipients", params={"limit": 0})
    assert r.status_code == 400
    assert r.json()["error"]["field"] == "limit"


def test_subscriptions_list_also_enforces_limit():
    r = client.get("/v1/subscriptions", params={"limit": 5000})
    assert r.status_code == 400
    assert r.json()["error"]["field"] == "limit"


def test_malformed_cursor_is_rejected_with_400():
    r = client.get("/v1/recipients", params={"cursor": "not-base64!!!"})
    assert r.status_code == 400
    err = r.json()["error"]
    assert err["code"] == "validation_error"
    assert err["field"] == "cursor"


def test_create_recipient_rejects_unknown_field():
    r = client.post("/v1/recipients", json={"name": "x", "bogus": 1})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "validation_error"


def test_create_subscription_requires_at_least_one_channel():
    # channel_ids has min_length=1; an empty list fails validation before any DB.
    r = client.post(
        "/v1/subscriptions",
        json={"recipient_id": "00000000-0000-0000-0000-000000000000",
              "topic_pattern": "auth.*", "channel_ids": []},
    )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "validation_error"
