"""Dead letter queue (07 §5): the operational home for terminal failures.

Not "where messages die" but a structured, inspectable queue for human review
and replay. Backed by a Redis stream so entries survive worker restarts and carry
the full attempt history the on-call needs before deciding to replay.
"""

from app.dlq.router import router as dlq_router

__all__ = ["dlq_router"]
