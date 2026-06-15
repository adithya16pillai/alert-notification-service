"""Generic webhook adapter — HTTPS POST of a signed JSON envelope (04 §9).

Reliability shape shared by all HTTP adapters: one shared client, a bounded
timeout from the channel policy, and provider responses mapped to the three-way
classification (04 §6) — never a raw exception to the dispatcher.

Authenticity: the body is HMAC-SHA256 signed with the recipient's secret and the
signature sent in ``X-Signature: sha256=...`` so the receiver can verify it
wasn't tampered with or replayed by a third party (04 §9, §11).
"""

from __future__ import annotations

import hashlib
import hmac
import json

import httpx

from app.channels.base import DeliveryRequest, DeliveryResult, HttpChannel, parse_retry_after
from app.channels.classification import Outcome, classify_http_status
from app.channels.secrets import get_secret
from app.config import get_settings
from app.observability import get_logger

log = get_logger(__name__)


class WebhookChannel(HttpChannel):
    name = "webhook"

    def _signing_secret(self, req: DeliveryRequest) -> str | None:
        # Per-recipient secret: prefer the channel config, fall back to the
        # secrets backend keyed by recipient. Never logged.
        secret = req.config.get("signing_secret")
        if secret:
            return str(secret)
        return get_secret(f"webhook_signing_{req.recipient_id}", required=False)

    async def _deliver(self, req: DeliveryRequest) -> DeliveryResult:
        payload = {
            "alert_id": str(req.alert_id),
            "severity": req.severity,
            "title": req.title,
            "summary": req.message,
            "body": req.body,
        }
        # Sign the exact bytes we send so the receiver verifies the same blob.
        raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        headers = {"Content-Type": "application/json"}

        if get_settings().webhook_signing_enabled:
            secret = self._signing_secret(req)
            if secret:
                sig = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()
                headers["X-Signature"] = f"sha256={sig}"
            else:
                log.warning("channel.webhook.unsigned", recipient_id=str(req.recipient_id))

        try:
            resp = await self.client.post(req.target, content=raw, headers=headers)
        except httpx.TimeoutException:
            return DeliveryResult.transient("timeout")
        except httpx.HTTPError as exc:
            return DeliveryResult.transient(f"transport error: {exc!s}")

        outcome = classify_http_status(resp.status_code)
        if outcome is Outcome.SENT:
            return DeliveryResult.sent(provider_id=f"webhook:{resp.status_code}")
        if outcome is Outcome.TRANSIENT_FAILURE:
            return DeliveryResult.transient(
                f"http {resp.status_code}",
                retry_after_s=parse_retry_after(resp.headers.get("Retry-After")),
            )
        return DeliveryResult.permanent(f"http {resp.status_code}")
