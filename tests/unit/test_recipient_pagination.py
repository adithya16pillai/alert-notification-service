"""Cursor pagination + limit-validation edge cases (03 §4, §6).

These are the pure pieces of keyset pagination — encoding a position, decoding
attacker-controlled input safely, and enforcing the page-size policy. The SQL
stability property (no skips/dupes under concurrent insert) is exercised against
a real database in tests/integration/test_recipients_db.py.
"""

from __future__ import annotations

import base64
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from app.errors import ValidationError
from app.recipients.pagination import decode_cursor, encode_cursor, validate_limit


def _b64(raw: str) -> str:
    return base64.urlsafe_b64encode(raw.encode()).decode()


def test_cursor_round_trips_created_at_and_id():
    created_at = datetime(2026, 6, 13, 8, 30, 15, 123456, tzinfo=UTC)
    id_ = str(uuid4())
    decoded_at, decoded_id = decode_cursor(encode_cursor(created_at, id_))
    assert decoded_at == created_at
    assert decoded_id == id_


def test_cursor_preserves_timezone_offset():
    # isoformat carries the offset; base64url survives the '+' that would break a
    # raw query string.
    from datetime import timedelta, timezone

    created_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    decoded_at, _ = decode_cursor(encode_cursor(created_at, "x"))
    assert decoded_at == created_at


@pytest.mark.parametrize(
    "bad",
    [
        "not-base64!!!",          # invalid base64 alphabet
        "",                       # empty
        _b64("foobar"),           # valid base64 but no '|' separator
        _b64("2026-01-01T00:00|"),  # empty id after the separator
        _b64("|some-id"),         # empty datetime part
        _b64("not-a-date|abc"),   # unparseable datetime
    ],
)
def test_decode_rejects_malformed_cursor_as_400(bad):
    with pytest.raises(ValidationError) as exc:
        decode_cursor(bad)
    assert exc.value.http_status == 400
    assert exc.value.field == "cursor"


def test_validate_limit_default_when_missing():
    assert validate_limit(None) == 50


def test_validate_limit_allows_boundary():
    assert validate_limit(1) == 1
    assert validate_limit(50) == 50
    assert validate_limit(200) == 200


@pytest.mark.parametrize("bad", [0, -1, 201, 1000])
def test_validate_limit_rejects_out_of_range_as_400(bad):
    with pytest.raises(ValidationError) as exc:
        validate_limit(bad)
    assert exc.value.http_status == 400
    assert exc.value.field == "limit"
