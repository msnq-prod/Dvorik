from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

from flask import Blueprint, Flask

from dvorik.core.config import Config, get_config
from dvorik.core.plugins import load_plugins
from dvorik.db import init_db

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

    _initialise_database()
    _register_builtin_components()
    _register_blueprints(app)

    @app.get("/health")
    def healthcheck() -> tuple[dict[str, str], int]:
        """Return a simple JSON payload confirming the service is alive."""

        return {"status": "ok"}, 200

    return app


def _initialise_database() -> None:
    """Ensure the application database schema is created."""

    try:
        init_db()
    except Exception:  # pragma: no cover - logged for visibility
        logger.exception("Failed to initialise database schema")
        raise


def _register_builtin_components() -> None:
    """Load plugins and register built-in widgets."""

    load_plugins()

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
    from .blueprints import home, superadmin, tables, supply

    return (
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
