"""Error classification: the most safety-critical logic in the channel layer.

Every provider response collapses to one of three outcomes (04 §6):

  - ``SENT``               — 2xx / accepted; we're done.
  - ``TRANSIENT_FAILURE``  — 5xx, timeout, 429, connection reset; retry per policy.
  - ``PERMANENT_FAILURE``  — 4xx (except 429), invalid address, auth fail; DLQ now.

Misclassification is high-cost in both directions:
  - transient mistaken for permanent  -> we drop an alert that would have sent.
  - permanent mistaken for transient   -> retry storm against a hopeless request.

So this lives in its own module with dedicated unit tests (04 §11).
"""

from __future__ import annotations

from enum import StrEnum


class Outcome(StrEnum):
    SENT = "sent"
    TRANSIENT_FAILURE = "transient_failure"
    PERMANENT_FAILURE = "permanent_failure"


def classify_http_status(status: int) -> Outcome:
    """Map an HTTP status code to a delivery outcome.

    The one subtlety: 429 is *transient* (back off and retry, honouring
    ``Retry-After``), every other 4xx is *permanent* (the caller's request is
    malformed/unauthorised/addressed wrong — retrying changes nothing).
    """

    if 200 <= status < 300:
        return Outcome.SENT
    if status == 429 or status >= 500:
        return Outcome.TRANSIENT_FAILURE
    return Outcome.PERMANENT_FAILURE
