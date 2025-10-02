from __future__ import annotations

import logging
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Iterable

from flask import Blueprint, Flask, Response, g, request, session

from dvorik.core.config import Config, get_config
from dvorik.core.plugins import get_plugins, load_plugins
from dvorik.db import db, init_db

from dvorik.core.logging import bind_context, new_request_id, reset_context
from dvorik.core.plugins import load_plugins
from dvorik.db import init_db
from .csrf import init_csrf


logger = logging.getLogger(__name__)


def create_app(*, config: Config | None = None) -> Flask:
    """Create and configure the Flask application for the admin UI."""

    config = config or get_config()

    package_root = Path(__file__).resolve().parent
    app = Flask(
        "dvorik.admin",
        template_folder=str(package_root / "templates"),
        static_folder=str(package_root / "static"),
    )
    app.config["DVORIK_CONFIG"] = config

    from . import auth as auth_module

    auth_module.init_app(app, config=config)
    _initialise_csrf(app)

    _initialise_database()
    _register_builtin_components(config)
    _register_blueprints(app)

    @app.before_request
    def _prepare_logging_context() -> None:
        request_id = new_request_id()
        user_id = session.get("dvorik.superadmin")
        token = bind_context(
            request_id=request_id,
            user_id=user_id,
            http_method=request.method,
            path=request.path,
        )
        g.logging_context_token = token
        g.request_id = request_id

    @app.after_request
    def _cleanup_logging_context(response: Response) -> Response:
        token = getattr(g, "logging_context_token", None)
        if token is not None:
            reset_context(token)
            g.logging_context_token = None
        request_id = getattr(g, "request_id", None)
        if request_id:
            response.headers.setdefault("X-Request-ID", request_id)
        return response

    @app.teardown_request
    def _teardown_logging_context(_exc: BaseException | None) -> None:
        token = getattr(g, "logging_context_token", None)
        if token is not None:
            reset_context(token)
            g.logging_context_token = None

    @app.get("/health")
    def healthcheck() -> tuple[dict[str, str], int]:
        """Return a simple JSON payload confirming the service is alive."""

        return {"status": "ok"}, 200

    @app.get("/ready")
    def readiness() -> tuple[dict[str, object], int]:
        """Perform readiness checks covering the DB and plugin catalogue."""

        checks: dict[str, object] = {}

        try:
            with closing(db()) as conn:
                conn.execute("SELECT 1")
        except sqlite3.Error:
            logger.exception("Readiness check failed: database unavailable")
            checks["database"] = {"status": "error"}
            return {"status": "error", "checks": checks}, 503

        checks["database"] = {"status": "ok"}

        plugins = tuple(get_plugins())
        if not plugins:
            checks["plugins"] = {"status": "missing", "count": 0}
            logger.error("Readiness check failed: no plugins registered")
            return {"status": "error", "checks": checks}, 503

        checks["plugins"] = {
            "status": "ok",
            "count": len(plugins),
            "names": sorted(plugin.name for plugin in plugins),
        }

        return {"status": "ok", "checks": checks}, 200

    return app


def _initialise_csrf(app: Flask) -> None:
    """Configure CSRF protection for the admin application."""

    init_csrf(app)


def _initialise_database() -> None:
    """Ensure the application database schema is created."""

    try:
        init_db()
    except Exception:  # pragma: no cover - logged for visibility
        logger.exception("Failed to initialise database schema")
        raise


def _register_builtin_components(config: Config) -> None:
    """Load plugins and register built-in widgets."""

    if config.plugin_disabled:
        logger.info("Plugin loading disabled via configuration")
    else:
        load_plugins(*config.plugin_paths)

    try:
        from .widgets import register_builtin_widgets
    except Exception:  # pragma: no cover - defensive, logged
        logger.exception("Unable to import admin widgets")
        raise

    try:
        register_builtin_widgets()
    except Exception:  # pragma: no cover - defensive, logged
        logger.exception("Failed to register admin widgets")
        raise


def _register_blueprints(app: Flask) -> None:
    """Import and attach admin blueprints."""

    for blueprint in _iter_blueprints():
        app.register_blueprint(blueprint)


def _iter_blueprints() -> Iterable[Blueprint]:
    from .blueprints import home, menus, superadmin, tables, supply

    return (
        menus.blueprint,
        home.blueprint,
        superadmin.blueprint,
        tables.blueprint,
        supply.blueprint,
    )


def main() -> None:
    """Run the development server."""

    app = create_app()
    config: Config = app.config["DVORIK_CONFIG"]
    app.run(host="0.0.0.0", port=config.admin_port, debug=True)


__all__ = ["create_app", "main"]


if __name__ == "__main__":  # pragma: no cover - manual execution entry point
    main()
