"""Channels module (04): pluggable delivery adapters behind one interface."""

from app.channels.base import Channel, DeliveryRequest, DeliveryResult, HttpChannel
from app.channels.classification import Outcome, classify_http_status
from app.channels.policy import ChannelPolicy, backoff_delay, get_policy
from app.channels.registry import close_all, get_channel, register_defaults
from app.channels.rendering import MessageRenderer, get_renderer

__all__ = [
    "Channel",
    "ChannelPolicy",
    "DeliveryRequest",
    "DeliveryResult",
    "HttpChannel",
    "MessageRenderer",
    "Outcome",
    "backoff_delay",
    "classify_http_status",
    "close_all",
    "get_channel",
    "get_policy",
    "get_renderer",
    "register_defaults",
]
