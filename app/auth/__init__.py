"""Auth module (08): API-key based producer authentication."""

from app.auth.dependencies import require_api_key

__all__ = ["require_api_key"]
