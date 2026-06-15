"""SMS channel adapter — Twilio Messages API (04 §9).

Specifics from the spec:
  - Account SID + auth token resolved from the secrets backend (HTTP basic auth).
  - 160 chars per segment, hard cap of 3 segments per message; truncate the body
    to fit, never the subject (SMS has no subject — the body is everything).
  - Twilio returns 201 on accept; 4xx (e.g. invalid number) is permanent, 429/5xx
    transient.
"""

from __future__ import annotations

import httpx

from app.channels.base import DeliveryRequest, DeliveryResult, HttpChannel, parse_retry_after
from app.channels.classification import Outcome, classify_http_status
from app.channels.secrets import get_secret
from app.config import get_settings
from app.observability import get_logger

log = get_logger(__name__)

_SEGMENT_CHARS = 160
_MAX_SEGMENTS = 3
_MAX_CHARS = _SEGMENT_CHARS * _MAX_SEGMENTS  # 480


def truncate_to_segments(text: str) -> str:
    """Clip to the segment budget, marking truncation with an ellipsis."""
    if len(text) <= _MAX_CHARS:
        return text
    return text[: _MAX_CHARS - 1].rstrip() + "…"


class SmsChannel(HttpChannel):
    name = "sms"

    async def _deliver(self, req: DeliveryRequest) -> DeliveryResult:
        settings = get_settings()
        sid = get_secret(settings.twilio_sid_secret)
        token = get_secret(settings.twilio_token_secret)
        url = f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json"
        form = {
            "To": req.target,
            "From": req.config.get("from") or settings.twilio_from_number,
            "Body": truncate_to_segments(req.message),
        }

        try:
            resp = await self.client.post(url, data=form, auth=(sid or "", token or ""))
        except httpx.TimeoutException:
            return DeliveryResult.transient("timeout")
        except httpx.HTTPError as exc:
            return DeliveryResult.transient(f"transport error: {exc!s}")

        outcome = classify_http_status(resp.status_code)
        if outcome is Outcome.SENT:
            provider_id = None
            try:
                provider_id = resp.json().get("sid")
            except ValueError:
                pass
            return DeliveryResult.sent(provider_id=provider_id)
        if outcome is Outcome.TRANSIENT_FAILURE:
            return DeliveryResult.transient(
                f"http {resp.status_code}",
                retry_after_s=parse_retry_after(resp.headers.get("Retry-After")),
            )
        return DeliveryResult.permanent(f"http {resp.status_code}")
