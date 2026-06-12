"""SMS channel adapter (stub) — e.g. Twilio."""

from __future__ import annotations

from app.channels.base import Channel, DeliveryRequest, DeliveryResult
from app.observability import get_logger

log = get_logger(__name__)


class SmsChannel(Channel):
    name = "sms"

    async def _deliver(self, req: DeliveryRequest) -> DeliveryResult:
        # TODO: integrate a real SMS provider with httpx + timeout.
        log.info("channel.sms.send", alert_id=str(req.alert_id), target=req.target)
        return DeliveryResult(ok=True, provider_id=f"sms-stub:{req.alert_id}")
