"""Provider-credential resolution (04 §9, §10).

Hard requirements from the spec:
  - 100% of provider credentials come from a secrets manager at runtime — none
    in code, none in env-baked images for prod (04 §2 success metric).
  - Zero credential leakage in logs, traces, or error responses (04 §4, §10).
  - Rotated on ``SIGHUP`` with no process restart (04 §9).

Design: a small resolver with a pluggable backend. ``env`` backend (local/dev)
reads ``ANS_SECRET_<NAME>`` environment variables; ``aws`` backend reads from AWS
Secrets Manager under a configured prefix. Resolved values are cached in-memory;
``reload()`` clears the cache so the next read re-fetches (this is what the
SIGHUP handler calls). We never log a secret value — only its name and whether a
lookup hit or missed.
"""

from __future__ import annotations

import os
import signal
from abc import ABC, abstractmethod
from typing import Any

from app.config import get_settings
from app.errors import AppError
from app.observability import get_logger

log = get_logger(__name__)


class SecretNotFound(AppError):
    code = "secret_not_found"
    http_status = 500


class SecretsBackend(ABC):
    @abstractmethod
    def fetch(self, name: str) -> str | None:
        """Return the raw secret value for ``name``, or ``None`` if absent."""
        raise NotImplementedError


class EnvSecretsBackend(SecretsBackend):
    """Reads ``ANS_SECRET_<NAME>`` from the environment. For local/dev only."""

    prefix = "ANS_SECRET_"

    def fetch(self, name: str) -> str | None:
        return os.environ.get(self.prefix + name.upper())


class AwsSecretsBackend(SecretsBackend):
    """AWS Secrets Manager backend. Lazily constructs the boto3 client so the
    dependency is only required when this backend is actually selected."""

    def __init__(self, prefix: str) -> None:
        self._prefix = prefix
        self._client = None

    def _get_client(self) -> Any:  # pragma: no cover - exercised only with AWS configured
        if self._client is None:
            import boto3  # imported lazily; not a hard dependency for local/dev

            self._client = boto3.client("secretsmanager")
        return self._client

    def fetch(self, name: str) -> str | None:  # pragma: no cover - needs AWS
        client = self._get_client()
        secret_id = f"{self._prefix}{name}"
        try:
            resp = client.get_secret_value(SecretId=secret_id)
        except Exception:  # noqa: BLE001 - any AWS error => treat as missing
            return None
        value = resp.get("SecretString")
        return value if isinstance(value, str) else None


class SecretsResolver:
    """Caches resolved secrets in-memory; ``reload()`` invalidates the cache."""

    def __init__(self, backend: SecretsBackend) -> None:
        self._backend = backend
        self._cache: dict[str, str] = {}

    def get(self, name: str, *, required: bool = True) -> str | None:
        if name in self._cache:
            return self._cache[name]
        value = self._backend.fetch(name)
        if value is None:
            log.warning("secrets.miss", secret=name)  # name only, never the value
            if required:
                raise SecretNotFound(f"secret {name!r} not found")
            return None
        self._cache[name] = value
        log.info("secrets.hit", secret=name)
        return value

    def reload(self) -> None:
        """Drop the cache so the next ``get`` re-fetches (rotation)."""
        count = len(self._cache)
        self._cache.clear()
        log.info("secrets.reloaded", evicted=count)


def _build_backend() -> SecretsBackend:
    s = get_settings()
    if s.secrets_backend == "aws":
        return AwsSecretsBackend(prefix=s.aws_secrets_prefix)
    return EnvSecretsBackend()


_resolver: SecretsResolver | None = None


def get_resolver() -> SecretsResolver:
    global _resolver
    if _resolver is None:
        _resolver = SecretsResolver(_build_backend())
    return _resolver


def get_secret(name: str, *, required: bool = True) -> str | None:
    return get_resolver().get(name, required=required)


def install_sighup_reload() -> None:
    """Reload secrets on ``SIGHUP`` (no restart). No-op where SIGHUP is absent
    (e.g. Windows dev boxes), so callers don't need to guard the platform."""

    if not hasattr(signal, "SIGHUP"):
        return

    def _handler(_signum: int, _frame: object) -> None:
        get_resolver().reload()

    signal.signal(signal.SIGHUP, _handler)
    log.info("secrets.sighup_installed")
