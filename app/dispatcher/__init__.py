"""Dispatcher module (05): drains the queue and orchestrates fanout."""

from app.dispatcher.worker import Dispatcher

__all__ = ["Dispatcher"]
