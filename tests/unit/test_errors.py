"""The single error hierarchy maps to HTTP + the service-wide error schema."""

from app.errors import (
    AppError,
    IdempotencyConflict,
    PayloadTooLargeError,
    RateLimitedError,
    ValidationError,
)


def test_all_errors_subclass_apperror():
    for exc in (ValidationError, RateLimitedError, IdempotencyConflict, PayloadTooLargeError):
        assert issubclass(exc, AppError)


def test_status_codes_match_spec():
    assert ValidationError().http_status == 400
    assert IdempotencyConflict().http_status == 409
    assert PayloadTooLargeError().http_status == 413
    assert RateLimitedError().http_status == 429


def test_to_dict_shape():
    payload = ValidationError("bad severity", field="severity").to_dict("trace-xyz")
    assert payload == {
        "error": {
            "code": "validation_error",
            "message": "bad severity",
            "field": "severity",
            "trace_id": "trace-xyz",
        }
    }
