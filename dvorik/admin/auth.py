"""Authentication helpers and routes for the admin application."""

from __future__ import annotations

import hmac
import logging
import os
from functools import wraps
from typing import Callable, Mapping, ParamSpec, TypeVar, cast
from urllib.parse import urlsplit

from flask import (
    Blueprint,
    Flask,
    Response,
    current_app,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from flask.typing import ResponseReturnValue

from dvorik.core.config import Config

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")

_SESSION_KEY = "dvorik.superadmin"
_CONFIG_CREDENTIALS_KEY = "DVORIK_ADMIN_CREDENTIALS"

_USERNAME_ENV_KEYS: tuple[str, ...] = ("DVORIK_ADMIN_USERNAME", "SUPER_ADMIN_USERNAME")
_PASSWORD_ENV_KEYS: tuple[str, ...] = (
    "DVORIK_ADMIN_PASSWORD",
    "SUPER_ADMIN_PASSWORD",
)
_SECRET_ENV_KEYS: tuple[str, ...] = (
    "DVORIK_ADMIN_SECRET_KEY",
    "DVORIK_ADMIN_SECRET",
    "FLASK_SECRET_KEY",
    "SECRET_KEY",
)

blueprint = Blueprint("auth", __name__, url_prefix="/auth")


def init_app(app: Flask, *, config: Config) -> None:
    """Configure authentication for the admin application."""

    credentials = {
        "username": _resolve_username(config),
        "password": _resolve_password(),
    }

    secret_key = _resolve_secret_key()

    app.secret_key = secret_key
    app.config[_CONFIG_CREDENTIALS_KEY] = credentials

    app.register_blueprint(blueprint)


def is_superadmin_authenticated() -> bool:
    """Return ``True`` when the current session belongs to the superadmin."""

    return bool(session.get(_SESSION_KEY))


def ensure_superadmin() -> Response | None:
    """Abort the request with ``403`` when no authenticated superadmin is present."""

    if not is_superadmin_authenticated():
        return _forbidden_response()
    return None


def require_superadmin(func: Callable[P, R]) -> Callable[P, R | ResponseReturnValue]:
    """Decorator ensuring that the wrapped view is accessible to superadmins only."""

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R | ResponseReturnValue:
        failure = ensure_superadmin()
        if failure is not None:
            return failure
        return func(*args, **kwargs)

    return cast(Callable[P, R | ResponseReturnValue], wrapper)


@blueprint.get("/login")
def login_form() -> ResponseReturnValue:
    """Render the authentication form."""

    if is_superadmin_authenticated():
        return redirect(_safe_next_url(request.args.get("next")) or url_for("home.index"))

    context = _build_login_context()
    return render_template("auth/login.html", **context)


@blueprint.post("/login")
def login_submit() -> ResponseReturnValue:
    """Validate submitted credentials and initialise the session."""

    form_username = (request.form.get("username") or "").strip()
    form_password = request.form.get("password") or ""
    next_url = _safe_next_url(request.form.get("next") or request.args.get("next"))

    expected_username, expected_password = _expected_credentials()

    if _matches(form_username, expected_username) and _matches(form_password, expected_password):
        session[_SESSION_KEY] = expected_username
        session.permanent = True
        session.modified = True
        destination = next_url or url_for("home.index")
        logger.info("Superadmin authenticated", extra={"user_id": expected_username})
        return redirect(destination)

    logger.warning("Failed superadmin login attempt", extra={"user_id": form_username})

    context = _build_login_context(
        username=form_username,
        error="Invalid username or password. Please try again.",
    )
    context["next_url"] = next_url
    return render_template("auth/login.html", **context), 401


@blueprint.post("/logout")
def logout() -> ResponseReturnValue:
    """Terminate the authenticated session and redirect to the login form."""

    session.pop(_SESSION_KEY, None)
    session.modified = True
    next_url = _safe_next_url(request.form.get("next") or request.args.get("next"))
    destination = next_url or url_for("auth.login")
    return redirect(destination)


def _build_login_context(*, username: str | None = None, error: str | None = None) -> Mapping[str, object]:
    next_url = _safe_next_url(request.args.get("next"))
    return {
        "username": username or "",
        "error": error,
        "next_url": next_url,
    }


def _expected_credentials() -> tuple[str, str]:
    config_value = current_app.config.get(_CONFIG_CREDENTIALS_KEY)
    if not isinstance(config_value, Mapping):  # pragma: no cover - defensive guard
        raise RuntimeError("Admin credentials are not initialised")
    username = str(config_value.get("username") or "")
    password = str(config_value.get("password") or "")
    return username, password


def _resolve_username(config: Config) -> str:
    for key in _USERNAME_ENV_KEYS:
        value = os.environ.get(key)
        if value:
            return value.strip()
    return config.super_admin_username


def _resolve_password() -> str:
    for key in _PASSWORD_ENV_KEYS:
        value = os.environ.get(key)
        if value:
            return value
    raise RuntimeError(
        "Admin password environment variable is not set. "
        "Provide DVORIK_ADMIN_PASSWORD to enable the admin interface."
    )


def _resolve_secret_key() -> str:
    for key in _SECRET_ENV_KEYS:
        value = os.environ.get(key)
        if value:
            return value
    logger.warning("Falling back to an insecure development secret key")
    return "dev-insecure-admin-secret"


def _safe_next_url(candidate: str | None) -> str | None:
    if not candidate:
        return None
    parsed = urlsplit(candidate)
    if parsed.scheme or parsed.netloc:
        return None
    if candidate.startswith("//"):
        return None
    return candidate


def _matches(value: str, expected: str) -> bool:
    if not expected:
        return False
    return hmac.compare_digest(value, expected)


def _forbidden_response() -> Response:
    login_url = url_for("auth.login", next=request.url)
    response = render_template("auth/forbidden.html", login_url=login_url)
    return Response(response, status=403)


__all__ = [
    "blueprint",
    "ensure_superadmin",
    "init_app",
    "is_superadmin_authenticated",
    "require_superadmin",
]
