"""Credential resolution: caching, reload-on-rotation, and required semantics."""

import pytest

from app.channels.secrets import (
    EnvSecretsBackend,
    SecretNotFound,
    SecretsBackend,
    SecretsResolver,
)


class DictBackend(SecretsBackend):
    def __init__(self, values):
        self.values = values
        self.calls = 0

    def fetch(self, name):
        self.calls += 1
        return self.values.get(name)


def test_env_backend_reads_prefixed_var(monkeypatch):
    monkeypatch.setenv("ANS_SECRET_SLACK_BOT_TOKEN", "xoxb-123")
    assert EnvSecretsBackend().fetch("slack_bot_token") == "xoxb-123"
    assert EnvSecretsBackend().fetch("absent") is None


def test_resolver_caches_until_reload():
    backend = DictBackend({"k": "v1"})
    r = SecretsResolver(backend)
    assert r.get("k") == "v1"
    assert r.get("k") == "v1"
    assert backend.calls == 1  # second read served from cache

    backend.values["k"] = "v2"
    assert r.get("k") == "v1"  # still cached
    r.reload()
    assert r.get("k") == "v2"  # re-fetched after rotation
    assert backend.calls == 2


def test_required_missing_raises():
    r = SecretsResolver(DictBackend({}))
    with pytest.raises(SecretNotFound):
        r.get("nope")


def test_optional_missing_returns_none():
    r = SecretsResolver(DictBackend({}))
    assert r.get("nope", required=False) is None
