"""Channel registry: maps a channel name to a singleton adapter instance.

Each adapter keeps its own circuit-breaker state, so adapters are created once
and reused for the worker's lifetime.
"""

from __future__ import annotations

from app.channels.base import Channel
from app.channels.email import EmailChannel
from app.channels.slack import SlackChannel
from app.channels.sms import SmsChannel
from app.channels.webhook import WebhookChannel
from app.errors import NotFoundError

_registry: dict[str, Channel] = {}


def register_defaults() -> None:
    if _registry:
        return
    for adapter in (EmailChannel(), SlackChannel(), WebhookChannel(), SmsChannel()):
        _registry[adapter.name] = adapter


def get_channel(name: str) -> Channel:
    if not _registry:
        register_defaults()
    try:
        return _registry[name]
    except KeyError as exc:
        raise NotFoundError(f"unknown channel {name!r}") from exc
