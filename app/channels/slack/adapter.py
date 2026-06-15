"""Slack channel adapter — ``chat.postMessage`` via the Web API (04 §9).

Specifics from the spec:
  - Bot token resolved from the secrets backend, never from code/logs.
  - ``unfurl_links=false`` so alert links don't spam previews into the channel.
  - Respect the ``Retry-After`` header on 429 (Slack's documented rate-limit).
  - Slack returns HTTP 200 even for application errors, with ``{"ok": false,
    "error": ...}`` in the body — so we classify on the body, not just the status.
"""

from __future__ import annotations

import httpx

from app.channels.base import DeliveryRequest, DeliveryResult, HttpChannel, parse_retry_after
from app.channels.classification import Outcome, classify_http_status
from app.channels.secrets import get_secret
from app.config import get_settings
from app.observability import get_logger

log = get_logger(__name__)

_API_URL = "https://slack.com/api/chat.postMessage"

# Slack application errors that won't succeed on retry (config/auth problems).
_PERMANENT_SLACK_ERRORS = frozenset(
    {
        "channel_not_found",
        "not_in_channel",
        "is_archived",
        "invalid_auth",
        "account_inactive",
        "token_revoked",
        "no_permission",
        "msg_too_long",
    }
)


class SlackChannel(HttpChannel):
    name = "slack"

    async def _deliver(self, req: DeliveryRequest) -> DeliveryResult:
        token = get_secret(get_settings().slack_token_secret)
        body = {
            "channel": req.target,  # channel id / name from the recipient config
            "text": req.message,
            "unfurl_links": False,
            "unfurl_media": False,
        }
        headers = {"Authorization": f"Bearer {token}"}

        try:
            resp = await self.client.post(_API_URL, json=body, headers=headers)
        except httpx.TimeoutException:
            return DeliveryResult.transient("timeout")
        except httpx.HTTPError as exc:
            return DeliveryResult.transient(f"transport error: {exc!s}")

        # Transport-level rate limit / outage.
        http_outcome = classify_http_status(resp.status_code)
        if http_outcome is Outcome.TRANSIENT_FAILURE:
            return DeliveryResult.transient(
                f"http {resp.status_code}",
                retry_after_s=parse_retry_after(resp.headers.get("Retry-After")),
            )
        if http_outcome is Outcome.PERMANENT_FAILURE:
            return DeliveryResult.permanent(f"http {resp.status_code}")

        # HTTP 200 — inspect the Slack application-level result.
        data = resp.json()
        if data.get("ok"):
            return DeliveryResult.sent(provider_id=data.get("ts"))
        err = str(data.get("error") or "unknown_slack_error")
        if err == "ratelimited":
            return DeliveryResult.transient(
                err, retry_after_s=parse_retry_after(resp.headers.get("Retry-After"))
            )
        if err in _PERMANENT_SLACK_ERRORS:
            return DeliveryResult.permanent(err)
        return DeliveryResult.transient(err)  # unknown -> retry conservatively
