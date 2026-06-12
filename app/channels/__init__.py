"""Channels module (04): pluggable delivery adapters behind one interface."""

from app.channels.base import Channel, DeliveryRequest, DeliveryResult
from app.channels.registry import get_channel, register_defaults

__all__ = ["Channel", "DeliveryRequest", "DeliveryResult", "get_channel", "register_defaults"]
