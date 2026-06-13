"""Cursor pagination primitives (03 §6).

Keyset (cursor) pagination instead of ``OFFSET``: O(log n) on the
``(created_at, id)`` index and stable under concurrent inserts — a row added
mid-listing never causes a skipped or duplicated row across pages, which
``OFFSET`` cannot guarantee (03 §10).

``cursor = base64( "{created_at_iso}|{id}" )``. The id tie-breaker makes the
sort total even when two rows share a ``created_at``.
"""

from __future__ import annotations

import base64
import binascii
from datetime import datetime

from app.config import get_settings
from app.errors import ValidationError


def validate_limit(limit: int | None) -> int:
    """Clamp/validate a page size: default 50, reject ``> 200`` or ``< 1`` (03 §4)."""
    settings = get_settings()
    if limit is None:
        return settings.list_default_limit
    if limit > settings.list_max_limit:
        raise ValidationError(
            f"limit must be <= {settings.list_max_limit}", field="limit"
        )
    if limit < 1:
        raise ValidationError("limit must be >= 1", field="limit")
    return limit


def encode_cursor(created_at: datetime, id_: object) -> str:
    """Encode a ``(created_at, id)`` keyset position into an opaque token."""
    raw = f"{created_at.isoformat()}|{id_}"
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")


def decode_cursor(cursor: str) -> tuple[datetime, str]:
    """Decode a cursor back into ``(created_at, id)``.

    Raises :class:`ValidationError` (400) on any malformed token rather than
    leaking a 500 — the cursor is attacker-controlled input.
    """
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError, ValueError) as exc:
        raise ValidationError("malformed cursor", field="cursor") from exc

    iso, sep, id_ = raw.partition("|")
    if not sep or not id_:
        raise ValidationError("malformed cursor", field="cursor")
    try:
        created_at = datetime.fromisoformat(iso)
    except ValueError as exc:
        raise ValidationError("malformed cursor", field="cursor") from exc
    return created_at, id_
