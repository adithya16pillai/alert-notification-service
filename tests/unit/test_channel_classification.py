"""Error classification is the highest-cost logic in the channel layer (04 §6)."""

from app.channels.classification import Outcome, classify_http_status


def test_2xx_is_sent():
    for code in (200, 201, 202, 204):
        assert classify_http_status(code) is Outcome.SENT


def test_5xx_is_transient():
    for code in (500, 502, 503, 504):
        assert classify_http_status(code) is Outcome.TRANSIENT_FAILURE


def test_429_is_transient_not_permanent():
    # The subtle one: 429 must retry (with backoff), not abandon.
    assert classify_http_status(429) is Outcome.TRANSIENT_FAILURE


def test_other_4xx_is_permanent():
    for code in (400, 401, 403, 404, 422):
        assert classify_http_status(code) is Outcome.PERMANENT_FAILURE
