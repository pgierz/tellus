from __future__ import annotations

from typing import Any, Dict

try:
    from jinja2 import Environment, StrictUndefined
except Exception as e:  # pragma: no cover
    # Defer import errors until render invocation for clearer messaging
    Environment = None  # type: ignore
    StrictUndefined = None  # type: ignore
    _import_error = e
else:
    _import_error = None

_env = None

def _get_env():
    global _env
    if _env is None:
        if Environment is None:  # pragma: no cover
            raise RuntimeError(
                "Jinja2 is required for template rendering. Please install 'jinja2'."
            ) from _import_error
        _env = Environment(undefined=StrictUndefined, autoescape=False)
    return _env


def render_template(template: str, attrs: Dict[str, Any] | None) -> str:
    """Render a template string using only the provided attrs as context.

    - Uses Jinja2 with StrictUndefined so missing variables raise errors
      (callers may catch and handle as needed).
    - Values are stringified to keep path rendering predictable.
    """
    env = _get_env()
    tmpl = env.from_string(template or "")
    ctx = {str(k): str(v) for k, v in ((attrs or {}).items())}
    return tmpl.render(**ctx)
