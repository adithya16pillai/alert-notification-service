"""Message templating (04 §8).

Jinja2 templates live at ``app/channels/<kind>/templates/<severity>.j2``. Each
template defines two blocks — ``subject`` and ``body`` — so one file owns both
halves of a message and they can share context. We render the blocks separately.

Fallback chain (most specific first), per 04 §8::

    (tenant, kind, severity) -> (kind, severity) -> (kind, default) -> built-in

Safety (04 §8, §10):
  - ``SandboxedEnvironment`` — templates can't reach the filesystem, import, or
    call dangerous attributes.
  - ``autoescape`` on — rendered output is HTML-safe by default.
  - A lenient undefined plus a context with guaranteed keys means a missing
    field degrades to a blank/default, never an unhandled ``KeyError`` (§10).
"""

from __future__ import annotations

from pathlib import Path

from jinja2 import (
    ChainableUndefined,
    ChoiceLoader,
    DictLoader,
    FileSystemLoader,
    Template,
    select_autoescape,
)
from jinja2.runtime import Context
from jinja2.sandbox import SandboxedEnvironment

from app.observability import get_logger

log = get_logger(__name__)

_CHANNELS_ROOT = Path(__file__).parent

# Last-resort templates, used when no file template matches the fallback chain.
# Kept deliberately minimal and channel-agnostic.
_BUILTIN_TEMPLATES = {
    "_builtin/default.j2": (
        "{% block subject %}[{{ alert.severity | upper }}] "
        "{{ alert.title | default('alert') }}{% endblock %}"
        "{% block body %}{{ alert.body | default('') }}\n\n"
        "Severity: {{ alert.severity | default('unknown') }}\n"
        "Source: {{ alert.source | default('unknown') }}\n"
        "Alert ID: {{ alert.id | default('') }}{% endblock %}"
    ),
}


def _build_env() -> SandboxedEnvironment:
    env = SandboxedEnvironment(
        loader=ChoiceLoader(
            [
                FileSystemLoader(str(_CHANNELS_ROOT)),
                DictLoader(_BUILTIN_TEMPLATES),
            ]
        ),
        autoescape=select_autoescape(default=True, default_for_string=True),
        undefined=ChainableUndefined,  # `a.b.c` chains to undefined instead of raising
        trim_blocks=True,
        lstrip_blocks=True,
    )
    return env


class MessageRenderer:
    """Renders ``(subject, body)`` for a (kind, severity[, tenant]) message."""

    def __init__(self) -> None:
        self._env = _build_env()

    def _candidates(self, kind: str, severity: str, tenant: str | None) -> list[str]:
        names: list[str] = []
        if tenant:
            names.append(f"{kind}/templates/tenant/{tenant}/{severity}.j2")
        names.append(f"{kind}/templates/{severity}.j2")
        names.append(f"{kind}/templates/default.j2")
        names.append("_builtin/default.j2")
        return names

    def _select(self, kind: str, severity: str, tenant: str | None) -> Template:
        names = self._candidates(kind, severity, tenant)
        tmpl = self._env.select_template(names)
        if tmpl.name != names[0]:
            log.debug("renderer.fallback", kind=kind, severity=severity, chosen=tmpl.name)
        return tmpl

    def render(
        self,
        *,
        kind: str,
        severity: str,
        context: dict[str, object],
        tenant: str | None = None,
    ) -> tuple[str, str]:
        """Return ``(subject, body)``. Never raises on missing fields — falls
        back to the built-in template and, ultimately, the alert title."""

        # Pass the raw dict: a missing key resolves to ChainableUndefined, so
        # ``| default(...)`` fires and ``alert.a.b`` chains instead of raising.
        safe_ctx = {"alert": context}
        try:
            tmpl = self._select(kind, severity, tenant)
            ctx = tmpl.new_context(safe_ctx)
            subject = self._render_block(tmpl, "subject", ctx)
            body = self._render_block(tmpl, "body", ctx)
            return subject, body
        except Exception as exc:  # noqa: BLE001 - §10: a render failure must never lose an alert
            # A broken template must not lose the alert: emit a plain fallback.
            log.warning("renderer.error", kind=kind, severity=severity, error=str(exc))
            title = str(context.get("title") or "alert")
            return f"[{str(context.get('severity') or '').upper()}] {title}".strip(), str(
                context.get("body") or ""
            )

    @staticmethod
    def _render_block(tmpl: Template, name: str, ctx: Context) -> str:
        block = tmpl.blocks.get(name)
        if block is None:
            return ""
        return "".join(block(ctx)).strip()


_renderer: MessageRenderer | None = None


def get_renderer() -> MessageRenderer:
    global _renderer
    if _renderer is None:
        _renderer = MessageRenderer()
    return _renderer
