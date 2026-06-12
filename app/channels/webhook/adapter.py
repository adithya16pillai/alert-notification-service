"""Generic webhook adapter — real httpx POST with timeout, as a worked example.

Other adapters are stubs; this one shows the expected shape: bounded timeout,
map transport/HTTP errors to a (retryable) DeliveryResult, never leak raw
exceptions to the dispatcher.
"""

from __future__ import annotations

import httpx

from app.channels.base import Channel, DeliveryRequest, DeliveryResult
from app.config import get_settings
from app.observability import get_logger

log = get_logger(__name__)


class WebhookChannel(Channel):
    name = "webhook"

    async def _deliver(self, req: DeliveryRequest) -> DeliveryResult:
        timeout = get_settings().channel_timeout_seconds
        payload = {
            "alert_id": str(req.alert_id),
            "severity": req.severity,
            "title": req.title,
            "body": req.body,
        }
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(req.target, json=payload)
            if resp.is_success:
                return DeliveryResult(ok=True, provider_id=f"webhook:{resp.status_code}")
            # 5xx is worth retrying; 4xx (except 429) is the caller's config problem.
            retryable = resp.status_code >= 500 or resp.status_code == 429
            return DeliveryResult(ok=False, error=f"http {resp.status_code}", retryable=retryable)
        except httpx.TimeoutException:
            return DeliveryResult(ok=False, error="timeout", retryable=True)
        except httpx.HTTPError as exc:
            return DeliveryResult(ok=False, error=str(exc), retryable=True)
