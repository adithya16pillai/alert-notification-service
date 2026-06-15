"""Templating: fallback chain + missing-field safety (04 §8, §10)."""

from app.channels.rendering import MessageRenderer


def _ctx(**over):
    base = {
        "id": "01ABC",
        "title": "Disk full",
        "body": "root volume at 98%",
        "severity": "critical",
        "source": "prometheus",
        "topic": "infra.disk",
        "occurred_at": "2026-06-15T12:00:00Z",
        "labels": {"host": "db-1", "env": "prod"},
    }
    base.update(over)
    return base


def test_renders_severity_specific_template():
    r = MessageRenderer()
    subject, body = r.render(kind="email", severity="critical", context=_ctx())
    assert subject.startswith("[CRITICAL] Disk full")
    assert "db-1" in subject  # critical template includes host
    assert "root volume at 98%" in body
    assert "host: db-1" in body


def test_falls_back_to_default_template_for_unknown_severity():
    r = MessageRenderer()
    subject, _ = r.render(kind="email", severity="medium", context=_ctx(severity="medium"))
    # default.j2 prefixes with the severity, uppercased.
    assert subject.startswith("[MEDIUM] Disk full")


def test_missing_fields_never_raise_and_use_defaults():
    r = MessageRenderer()
    # No labels, no host, no source — must not raise and must use safe defaults.
    subject, body = r.render(
        kind="email", severity="critical", context={"title": "X", "severity": "critical"}
    )
    assert "unknown host" in subject
    assert "Source: unknown" in body


def test_empty_context_degrades_gracefully():
    r = MessageRenderer()
    subject, body = r.render(kind="email", severity="critical", context={})
    assert isinstance(subject, str) and isinstance(body, str)  # no exception


def test_autoescape_is_on():
    r = MessageRenderer()
    subject, _ = r.render(
        kind="email", severity="critical", context=_ctx(title="<script>&'\"")
    )
    assert "<script>" not in subject  # escaped


def test_unknown_channel_uses_builtin():
    r = MessageRenderer()
    subject, body = r.render(kind="carrier-pigeon", severity="low", context=_ctx(severity="low"))
    assert "[LOW]" in subject
    assert "Disk full" in subject
