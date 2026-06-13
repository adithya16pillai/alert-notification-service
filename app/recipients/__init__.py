"""Recipients module (03): recipients, subscriptions, channel configs."""

from app.recipients.router import router, subscriptions_router

__all__ = ["router", "subscriptions_router"]
