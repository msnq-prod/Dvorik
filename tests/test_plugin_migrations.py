"""Tests covering plugin migration discovery and execution."""

from __future__ import annotations

import sys
from types import ModuleType

import pytest

import dvorik.app as app_module
from dvorik.core import config as core_config
from dvorik.core import plugins as plugin_module
from dvorik.core.plugins import PluginDescriptor
from dvorik.core.version import API_VERSION as CORE_API_VERSION
from dvorik.db import db


@pytest.fixture(autouse=True)
def _reset_plugins(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure plugin registries are isolated between tests."""

    monkeypatch.setattr(plugin_module, "_PLUGINS", {})
    monkeypatch.setattr(plugin_module, "_MODULE_TO_PLUGIN", {})


def _create_test_config(tmp_path_factory: pytest.TempPathFactory) -> core_config.Config:
    base_dir = tmp_path_factory.mktemp("dvorik-config")

    data_dir = base_dir / "data"
    uploads_dir = data_dir / "uploads"
    normalized_dir = uploads_dir / "normalized"
    media_dir = base_dir / "media"
    photos_dir = media_dir / "photos"
    reports_dir = base_dir / "reports"

    for path in (data_dir, uploads_dir, normalized_dir, media_dir, photos_dir, reports_dir):
        path.mkdir(parents=True, exist_ok=True)

    db_path = data_dir / "test.sqlite3"

    return core_config.Config(
        bot_token="",
        super_admin_id=None,
        super_admin_username="@test",
        admin_port=8000,
        db_path=db_path,
        data_dir=data_dir,
        media_dir=media_dir,
        reports_dir=reports_dir,
        uploads_dir=uploads_dir,
        normalized_uploads_dir=normalized_dir,
        photos_dir=photos_dir,
        page_size=10,
        cards_page_size=20,
        stock_page_size=30,
        photo_quality=85,
    )


def test_load_plugins_exposes_migrate_callable(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    package_dir = tmp_path / "test_plugins"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "example.py").write_text(
        "\n".join(
            (
                "from dvorik.core.version import API_VERSION as CORE_API_VERSION",
                "API_VERSION = CORE_API_VERSION",
                "PLUGIN_NAME = 'example'",
                "def migrate(conn):",
                "    conn.execute('SELECT 1')",
            )
        ),
        encoding="utf-8",
    )

    monkeypatch.syspath_prepend(str(tmp_path))

    modules_to_cleanup = [name for name in sys.modules if name.startswith("test_plugins")]
    for module_name in modules_to_cleanup:
        sys.modules.pop(module_name)

    try:
        descriptors = plugin_module.load_plugins("test_plugins")
        assert descriptors, "expected plugin discovery to yield at least one plugin"

        discovered = next(
            (item for item in descriptors if item.module.__name__ == "test_plugins.example"),
            None,
        )
        assert discovered is not None, "expected the dummy plugin to be registered"
        assert callable(discovered.migrate)
    finally:
        for module_name in list(sys.modules):
            if module_name.startswith("test_plugins"):
                sys.modules.pop(module_name)


def test_create_system_runs_plugin_migrations(monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory) -> None:
    config = _create_test_config(tmp_path_factory)
    monkeypatch.setattr(core_config, "_CONFIG", config, raising=False)

    call_count = 0

    def migrate(conn) -> None:
        nonlocal call_count
        call_count += 1
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS plugin_flags(
                id INTEGER PRIMARY KEY CHECK(id = 1),
                enabled INTEGER NOT NULL
            )
            """
        )
        row = conn.execute("SELECT enabled FROM plugin_flags WHERE id = 1").fetchone()
        if row is None:
            conn.execute("INSERT INTO plugin_flags(id, enabled) VALUES (1, 1)")
        else:
            current = int(row["enabled"])
            new_value = 0 if current else 1
            conn.execute("UPDATE plugin_flags SET enabled = ? WHERE id = 1", (new_value,))

    module = ModuleType("test_plugin")
    module.migrate = migrate  # type: ignore[attr-defined]

    descriptor = PluginDescriptor(
        name="test-plugin",
        module=module,
        version="0.0.1",
        api_versions=(CORE_API_VERSION,),
        description="",
        migrate=migrate,
    )

    monkeypatch.setattr(app_module, "load_plugins", lambda dir="dvorik/plugins": (descriptor,))
    monkeypatch.setattr(app_module, "_register_admin_components", lambda: None)
    monkeypatch.setattr(app_module, "_register_bot_components", lambda: None)
    monkeypatch.setattr(app_module, "_register_scheduler_jobs", lambda: None)
    monkeypatch.setattr(app_module, "_register_notifications", lambda config: None)

    app_module.create_system(config=config)
    with db() as conn:
        row = conn.execute("SELECT enabled FROM plugin_flags WHERE id = 1").fetchone()
        assert row is not None
        assert int(row["enabled"]) == 1

    app_module.create_system(config=config)
    with db() as conn:
        row = conn.execute("SELECT enabled FROM plugin_flags WHERE id = 1").fetchone()
        assert row is not None
        assert int(row["enabled"]) == 0

    assert call_count == 2
