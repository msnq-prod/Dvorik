from __future__ import annotations

import sqlite3
from dataclasses import replace

from dvorik.admin import auth as auth_module
from dvorik.admin import server as server_module
from dvorik.core import config as config_module


def _failing_get_plugins():
    raise AssertionError("get_plugins should not be called when plugins disabled")


def test_readiness_reports_disabled_plugins(monkeypatch, tmp_path):
    base_config = config_module.get_config(refresh=True)
    config = replace(
        base_config,
        plugin_disabled=True,
        db_path=tmp_path / "admin-readiness.sqlite3",
    )

    monkeypatch.setattr(auth_module, "init_app", lambda *_, **__: None)
    monkeypatch.setattr(server_module, "_initialise_csrf", lambda _app: None)
    monkeypatch.setattr(server_module, "_initialise_database", lambda: None)
    monkeypatch.setattr(server_module, "_register_builtin_components", lambda _cfg: None)
    monkeypatch.setattr(server_module, "_register_blueprints", lambda _app: None)
    monkeypatch.setattr(server_module, "db", lambda: sqlite3.connect(":memory:"))
    monkeypatch.setattr(server_module, "get_plugins", _failing_get_plugins)

    app = server_module.create_app(config=config)

    client = app.test_client()
    response = client.get("/ready")

    assert response.status_code == 200

    payload = response.get_json()
    assert payload == {
        "status": "ok",
        "checks": {
            "database": {"status": "ok"},
            "plugins": {"status": "disabled"},
        },
    }
