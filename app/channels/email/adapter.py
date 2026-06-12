"""Email channel adapter (stub).

Replace ``_deliver`` with a real provider call (SES / SendGrid / SMTP). The
contract — timeout, return a DeliveryResult, never raise for ordinary provider
failures — is what the dispatcher relies on.
"""

from __future__ import annotations

from app.channels.base import Channel, DeliveryRequest, DeliveryResult
from app.observability import get_logger

log = get_logger(__name__)


class EmailChannel(Channel):
    name = "email"

    async def _deliver(self, req: DeliveryRequest) -> DeliveryResult:
        # TODO: integrate a real email provider with httpx + timeout.
        log.info("channel.email.send", alert_id=str(req.alert_id), target=req.target)
        return DeliveryResult(ok=True, provider_id=f"email-stub:{req.alert_id}")
