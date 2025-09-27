from __future__ import annotations

import os

from flask import Flask, jsonify, request
from werkzeug.exceptions import RequestEntityTooLarge

from app import db as adb
import admin_ui.context as context
from admin_ui.blueprints import cards as cards_bp
from admin_ui.blueprints import home as home_bp
from admin_ui.blueprints import inventory as inventory_bp
from admin_ui.blueprints import labels as labels_bp
from admin_ui.blueprints import reports as reports_bp
from admin_ui.blueprints import schedule as schedule_bp
from admin_ui.blueprints import supply as supply_bp
from admin_ui.blueprints import tables as tables_bp
from admin_ui.blueprints import utils as bp_utils


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "dev-local-admin"

    try:
        adb.init_db()
    except Exception:
        pass

    app.config.setdefault("MAX_CONTENT_LENGTH", 20 * 1024 * 1024)

    context.register(app)

    app.register_blueprint(home_bp.bp, url_prefix="")
    app.register_blueprint(labels_bp.bp, url_prefix="")
    app.register_blueprint(cards_bp.bp, url_prefix="")
    app.register_blueprint(inventory_bp.bp, url_prefix="")
    app.register_blueprint(tables_bp.bp, url_prefix="")
    app.register_blueprint(schedule_bp.bp, url_prefix="")
    app.register_blueprint(supply_bp.bp, url_prefix="")
    app.register_blueprint(reports_bp.bp, url_prefix="")

    @app.errorhandler(RequestEntityTooLarge)
    def handle_request_too_large(exc: RequestEntityTooLarge):
        max_len = app.config.get("MAX_CONTENT_LENGTH")
        message = "Файл слишком большой."
        extra = {}
        if max_len:
            size_mb = max_len / (1024 * 1024)
            if size_mb >= 10:
                size_str = f"{size_mb:.0f}"
            elif size_mb >= 1:
                size_str = f"{size_mb:.1f}".rstrip("0").rstrip(".")
            else:
                size_str = f"{size_mb:.2f}".rstrip("0").rstrip(".")
            message = f"Файл слишком большой. Максимальный размер: {size_str} МБ."
            extra["max_size"] = int(max_len)
        if bp_utils.wants_json_response() or request.path.startswith("/supply"):
            payload = {"success": False, "message": message}
            payload.update(extra)
            return jsonify(payload), 413
        return message, 413

    return app


if __name__ == "__main__":
    app = create_app()
    host = os.getenv("ADMIN_HOST", "127.0.0.1")
    try:
        port = int(os.getenv("ADMIN_PORT", "8000"))
    except Exception:
        port = 8000
    debug = os.getenv("FLASK_DEBUG", "").lower() in ("1", "true", "yes")
    app.run(host=host, port=port, debug=debug)
