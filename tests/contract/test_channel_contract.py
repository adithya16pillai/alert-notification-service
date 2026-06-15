"""Channel adapter contract suite (04 §10).

Every adapter must, behind the common interface, handle: success, transient
failure (retryable), permanent failure (abandon), timeout, and 429 with
``Retry-After``. We drive the HTTP adapters with an ``httpx.MockTransport`` and
the email adapter by stubbing its blocking SMTP send.
"""

from __future__ import annotations

import hashlib
import hmac
import smtplib
from uuid import uuid4

import httpx
import pytest

from app.channels.base import DeliveryRequest
from app.channels.classification import Outcome
from app.channels.email.adapter import EmailChannel, SmtpDowngradeError
from app.channels.slack.adapter import SlackChannel
from app.channels.sms.adapter import SmsChannel
from app.channels.webhook.adapter import WebhookChannel


def make_req(*, target: str = "https://example.test/hook", config=None) -> DeliveryRequest:
    return DeliveryRequest(
        alert_id="01ABCDEF",
        recipient_id=uuid4(),
        target=target,
        title="Disk full",
        body="root volume at 98%",
        severity="critical",
        config=config or {},
        rendered_subject="[CRITICAL] Disk full",
        rendered_body="root volume at 98%",
    )


def with_transport(adapter, handler):
    """Swap an HTTP adapter's shared client for one backed by a mock transport."""
    adapter._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return adapter


# --------------------------------------------------------------------------- #
# Webhook
# --------------------------------------------------------------------------- #
async def test_webhook_success_and_signature():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["sig"] = request.headers.get("X-Signature")
        captured["body"] = request.content
        return httpx.Response(200)

    a = with_transport(WebhookChannel(), handler)
    res = await a.send(make_req(config={"signing_secret": "s3cr3t"}))
    assert res.status is Outcome.SENT
    # Signature verifies against the exact bytes sent (04 §9, §10).
    expected = hmac.new(b"s3cr3t", captured["body"], hashlib.sha256).hexdigest()
    assert captured["sig"] == f"sha256={expected}"


async def test_webhook_5xx_is_transient():
    a = with_transport(WebhookChannel(), lambda r: httpx.Response(503))
    res = await a.send(make_req(config={"signing_secret": "x"}))
    assert res.status is Outcome.TRANSIENT_FAILURE


async def test_webhook_4xx_is_permanent():
    a = with_transport(WebhookChannel(), lambda r: httpx.Response(404))
    res = await a.send(make_req(config={"signing_secret": "x"}))
    assert res.status is Outcome.PERMANENT_FAILURE


async def test_webhook_429_carries_retry_after():
    a = with_transport(
        WebhookChannel(), lambda r: httpx.Response(429, headers={"Retry-After": "7"})
    )
    res = await a.send(make_req(config={"signing_secret": "x"}))
    assert res.status is Outcome.TRANSIENT_FAILURE
    assert res.retry_after_s == 7.0


async def test_webhook_timeout_is_transient():
    def handler(request):
        raise httpx.TimeoutException("boom")

    a = with_transport(WebhookChannel(), handler)
    res = await a.send(make_req(config={"signing_secret": "x"}))
    assert res.status is Outcome.TRANSIENT_FAILURE


# --------------------------------------------------------------------------- #
# Slack
# --------------------------------------------------------------------------- #
@pytest.fixture
def slack_token(monkeypatch):
    monkeypatch.setattr("app.channels.slack.adapter.get_secret", lambda *a, **k: "xoxb-test")


async def test_slack_success(slack_token):
    a = with_transport(
        SlackChannel(), lambda r: httpx.Response(200, json={"ok": True, "ts": "1.23"})
    )
    res = await a.send(make_req(target="C123"))
    assert res.status is Outcome.SENT
    assert res.provider_id == "1.23"


async def test_slack_app_error_is_permanent(slack_token):
    body = {"ok": False, "error": "channel_not_found"}
    a = with_transport(SlackChannel(), lambda r: httpx.Response(200, json=body))
    res = await a.send(make_req(target="C123"))
    assert res.status is Outcome.PERMANENT_FAILURE


async def test_slack_429_carries_retry_after(slack_token):
    a = with_transport(
        SlackChannel(), lambda r: httpx.Response(429, headers={"Retry-After": "30"})
    )
    res = await a.send(make_req(target="C123"))
    assert res.status is Outcome.TRANSIENT_FAILURE
    assert res.retry_after_s == 30.0


async def test_slack_unfurl_disabled(slack_token):
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        captured.update(json.loads(request.content))
        return httpx.Response(200, json={"ok": True, "ts": "1"})

    a = with_transport(SlackChannel(), handler)
    await a.send(make_req(target="C123"))
    assert captured["unfurl_links"] is False


# --------------------------------------------------------------------------- #
# SMS (Twilio)
# --------------------------------------------------------------------------- #
@pytest.fixture
def twilio_creds(monkeypatch):
    monkeypatch.setattr("app.channels.sms.adapter.get_secret", lambda *a, **k: "AC_or_token")


async def test_sms_success(twilio_creds):
    a = with_transport(SmsChannel(), lambda r: httpx.Response(201, json={"sid": "SM1"}))
    res = await a.send(make_req(target="+15551234567", config={"from": "+15550000000"}))
    assert res.status is Outcome.SENT
    assert res.provider_id == "SM1"


async def test_sms_invalid_number_is_permanent(twilio_creds):
    a = with_transport(SmsChannel(), lambda r: httpx.Response(400, json={"code": 21211}))
    res = await a.send(make_req(target="+1", config={"from": "+15550000000"}))
    assert res.status is Outcome.PERMANENT_FAILURE


async def test_sms_429_transient(twilio_creds):
    a = with_transport(
        SmsChannel(), lambda r: httpx.Response(429, headers={"Retry-After": "1"})
    )
    res = await a.send(make_req(target="+15551234567", config={"from": "+15550000000"}))
    assert res.status is Outcome.TRANSIENT_FAILURE
    assert res.retry_after_s == 1.0


# --------------------------------------------------------------------------- #
# Email (SMTP) — classification driven by stubbing the blocking send
# --------------------------------------------------------------------------- #
@pytest.fixture
def email_creds(monkeypatch):
    monkeypatch.setattr("app.channels.email.adapter.get_secret", lambda *a, **k: "smtp-cred")


async def test_email_success(email_creds, monkeypatch):
    monkeypatch.setattr(EmailChannel, "_send_sync", staticmethod(lambda **kw: None))
    res = await EmailChannel().send(make_req(target="ops@example.test"))
    assert res.status is Outcome.SENT


async def test_email_auth_failure_is_permanent(email_creds, monkeypatch):
    def boom(**kw):
        raise smtplib.SMTPAuthenticationError(535, b"bad creds")

    monkeypatch.setattr(EmailChannel, "_send_sync", staticmethod(boom))
    res = await EmailChannel().send(make_req(target="ops@example.test"))
    assert res.status is Outcome.PERMANENT_FAILURE


async def test_email_4yz_is_transient(email_creds, monkeypatch):
    def boom(**kw):
        raise smtplib.SMTPResponseException(450, b"mailbox busy")

    monkeypatch.setattr(EmailChannel, "_send_sync", staticmethod(boom))
    res = await EmailChannel().send(make_req(target="ops@example.test"))
    assert res.status is Outcome.TRANSIENT_FAILURE


async def test_email_5yz_is_permanent(email_creds, monkeypatch):
    def boom(**kw):
        raise smtplib.SMTPResponseException(550, b"no such user")

    monkeypatch.setattr(EmailChannel, "_send_sync", staticmethod(boom))
    res = await EmailChannel().send(make_req(target="ops@example.test"))
    assert res.status is Outcome.PERMANENT_FAILURE


async def test_email_starttls_downgrade_rejected(email_creds, monkeypatch):
    def boom(**kw):
        raise SmtpDowngradeError("no starttls")

    monkeypatch.setattr(EmailChannel, "_send_sync", staticmethod(boom))
    res = await EmailChannel().send(make_req(target="ops@example.test"))
    assert res.status is Outcome.PERMANENT_FAILURE


async def test_email_timeout_is_transient(email_creds, monkeypatch):
    def boom(**kw):
        raise TimeoutError("slow")

    monkeypatch.setattr(EmailChannel, "_send_sync", staticmethod(boom))
    res = await EmailChannel().send(make_req(target="ops@example.test"))
    assert res.status is Outcome.TRANSIENT_FAILURE


# --------------------------------------------------------------------------- #
# Per-channel isolation (04 §9): breakers are independent
# --------------------------------------------------------------------------- #
async def test_circuit_breakers_are_independent():
    slack = SlackChannel()
    email = EmailChannel()
    # Trip Slack's breaker directly.
    for _ in range(slack._breaker._threshold):
        slack._breaker.record(ok=False)
    # Slack is open, email is untouched.
    assert await slack.health_check() is False
    assert await email.health_check() is True
