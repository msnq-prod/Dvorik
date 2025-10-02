from __future__ import annotations

import os
import secrets
from dataclasses import dataclass
from typing import Any, Mapping

from flask import Flask, current_app, request
from itsdangerous import BadSignature, BadTimeSignature, URLSafeTimedSerializer
from markupsafe import Markup


CSRF_FIELD_NAME = "csrf_token"
_DEFAULT_TTL_SECONDS = 3600
_SERIALIZER_KEY = "dvorik_admin_csrf"


class CSRFError(RuntimeError):
    """Exception raised when CSRF validation fails."""


@dataclass(frozen=True, slots=True)
class _CSRFState:
    serializer: URLSafeTimedSerializer
    max_age: int


def init_csrf(app: Flask, *, secret_key: str | None = None, max_age: int | None = None) -> None:
    """Initialise CSRF protection helpers for the admin UI."""

    secret = secret_key or app.secret_key
    if not secret:
        raise RuntimeError("CSRF protection requires a configured secret key.")

    ttl = _resolve_ttl(max_age)
    state = _CSRFState(URLSafeTimedSerializer(secret_key=secret, salt="dvorik-admin-csrf"), ttl)
    app.extensions[_SERIALIZER_KEY] = state
    app.config.setdefault("DVORIK_ADMIN_CSRF_TTL", ttl)

    app.jinja_env.globals.update(
        csrf_token=generate_csrf_token,
        csrf_field=render_csrf_field,
    )


def generate_csrf_token() -> str:
    """Create a signed CSRF token for the current app."""

    serializer = _get_state().serializer
    payload = {"nonce": secrets.token_urlsafe(16)}
    return serializer.dumps(payload)


def render_csrf_field() -> Markup:
    """Render a hidden input element with a freshly minted CSRF token."""

    token = generate_csrf_token()
    return Markup(f'<input type="hidden" name="{CSRF_FIELD_NAME}" value="{token}">')


def validate_csrf_token(token: str | None) -> None:
    """Validate ``token`` and raise :class:`CSRFError` on failure."""

    if not token:
        raise CSRFError("Missing CSRF token.")

    state = _get_state()
    try:
        state.serializer.loads(token, max_age=state.max_age)
    except BadTimeSignature as exc:  # pragma: no cover - defensive guard
        raise CSRFError("CSRF token has expired.") from exc
    except BadSignature as exc:  # pragma: no cover - defensive guard
        raise CSRFError("Invalid CSRF token.") from exc


def validate_csrf_request() -> None:
    """Validate a CSRF token extracted from the current request."""

    token = request.form.get(CSRF_FIELD_NAME)
    if token is None:
        token = _first_present_header(request.headers, ("X-CSRFToken", "X-CSRF-Token"))
    validate_csrf_token(token)


def _resolve_ttl(value: int | None) -> int:
    if value is not None and value > 0:
        return value

    candidate = os.environ.get("DVORIK_ADMIN_CSRF_TTL")
    if candidate:
        try:
            parsed = int(candidate)
        except ValueError:  # pragma: no cover - defensive guard
            parsed = _DEFAULT_TTL_SECONDS
        if parsed > 0:
            return parsed
    return _DEFAULT_TTL_SECONDS


def _get_state() -> _CSRFState:
    extensions: Mapping[str, Any] = getattr(current_app, "extensions", {})
    state = extensions.get(_SERIALIZER_KEY)
    if not isinstance(state, _CSRFState):  # pragma: no cover - defensive guard
        raise RuntimeError("CSRF protection is not initialised for this application.")
    return state


def _first_present_header(headers: Mapping[str, str], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        if key in headers:
            return headers[key]
    return None


__all__ = [
    "CSRFError",
    "CSRF_FIELD_NAME",
    "generate_csrf_token",
    "init_csrf",
    "render_csrf_field",
    "validate_csrf_request",
    "validate_csrf_token",
]
