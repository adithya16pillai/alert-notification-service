"""Slack channel adapter (stub) — posts to an incoming-webhook URL."""

from __future__ import annotations

from app.channels.base import Channel, DeliveryRequest, DeliveryResult
from app.observability import get_logger

log = get_logger(__name__)


class SlackChannel(Channel):
    name = "slack"

    async def _deliver(self, req: DeliveryRequest) -> DeliveryResult:
        # TODO: POST to req.target (webhook URL) with httpx + timeout.
        log.info("channel.slack.send", alert_id=str(req.alert_id), target=req.target)
        return DeliveryResult(ok=True, provider_id=f"slack-stub:{req.alert_id}")
