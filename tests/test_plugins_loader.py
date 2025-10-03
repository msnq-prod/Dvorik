import json
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from dvorik import app as app_module
from dvorik.core import config as config_module
from dvorik.core import plugins as plugins_module


@pytest.fixture
def reset_plugin_registry():
    plugins_module._PLUGINS.clear()
    plugins_module._MODULE_TO_PLUGIN.clear()
    yield
    plugins_module._PLUGINS.clear()
    plugins_module._MODULE_TO_PLUGIN.clear()


def _write_plugin_package(root: Path, package_name: str, modules: dict[str, str]) -> None:
    package_path = root / package_name
    package_path.mkdir(parents=True, exist_ok=True)
    (package_path / "__init__.py").write_text("", encoding="utf-8")
    for module_name, plugin_name in modules.items():
        (package_path / f"{module_name}.py").write_text(
            """
from dvorik.core.version import API_VERSION as CORE_API_VERSION

API_VERSION = CORE_API_VERSION
PLUGIN_NAME = "{name}"
            """.strip().format(name=plugin_name),
            encoding="utf-8",
        )


def _cleanup_modules(prefixes: list[str]) -> None:
    for prefix in prefixes:
        for module in list(sys.modules):
            if module == prefix or module.startswith(f"{prefix}."):
                sys.modules.pop(module, None)


def test_load_plugins_multiple_packages(tmp_path, monkeypatch, reset_plugin_registry):
    _write_plugin_package(tmp_path, "pkg_one", {"alpha": "alpha"})
    _write_plugin_package(tmp_path, "pkg_two", {"beta": "beta"})

    monkeypatch.syspath_prepend(str(tmp_path))

    try:
        descriptors = plugins_module.load_plugins("pkg_one", "pkg_two")
    finally:
        _cleanup_modules(["pkg_one", "pkg_two"])

    names = {descriptor.name for descriptor in descriptors}
    assert {"alpha", "beta"}.issubset(names)


def test_load_plugins_allow_and_skip(tmp_path, monkeypatch, reset_plugin_registry):
    _write_plugin_package(tmp_path, "pkg_combo", {"first": "first", "second": "second"})

    monkeypatch.syspath_prepend(str(tmp_path))

    try:
        descriptors = plugins_module.load_plugins("pkg_combo", allow=["first"], skip=["second"])
    finally:
        _cleanup_modules(["pkg_combo"])

    names = [descriptor.name for descriptor in descriptors]
    assert names.count("first") == 1
    assert "second" not in names


def test_load_config_reads_plugin_settings(tmp_path, monkeypatch):
    config_json = tmp_path / "config.json"
    config_json.write_text(
        json.dumps(
            {
                "PLUGIN_PATHS": ["custom/plugins", "other.plugins"],
                "PLUGIN_DISABLED": True,
            }
        ),
        encoding="utf-8",
    )

    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")

    config = config_module.load_config(
        json_path=config_json,
        env_path=env_file,
        environ={},
    )

    assert config.plugin_disabled is True
    assert set(config.plugin_paths) == {"custom/plugins", "other.plugins"}


def test_create_system_skips_plugins_when_disabled(tmp_path, monkeypatch, reset_plugin_registry):
    base_config = config_module.get_config(refresh=True)
    custom_paths = (str(tmp_path / "one"), str(tmp_path / "two"))
    config = replace(base_config, plugin_disabled=True, plugin_paths=custom_paths)

    calls: list[tuple] = []

    monkeypatch.setattr(app_module, "init_db", lambda: None)
    monkeypatch.setattr(app_module, "_register_admin_components", lambda: None)
    monkeypatch.setattr(app_module, "_register_bot_components", lambda: None)
    monkeypatch.setattr(app_module, "_register_scheduler_jobs", lambda: None)
    monkeypatch.setattr(app_module, "_register_notifications", lambda *_: None)

    def fake_load_plugins(*args, **kwargs):
        calls.append(args)
        return ()

    monkeypatch.setattr(app_module, "load_plugins", fake_load_plugins)

    app_module.create_system(config=config)

    assert calls == []


def test_create_system_uses_configured_plugin_paths(tmp_path, monkeypatch, reset_plugin_registry):
    base_config = config_module.get_config(refresh=True)
    custom_paths = (str(tmp_path / "alpha"), str(tmp_path / "beta"))
    config = replace(base_config, plugin_disabled=False, plugin_paths=custom_paths)

    captured: list[tuple] = []

    monkeypatch.setattr(app_module, "init_db", lambda: None)
    monkeypatch.setattr(app_module, "_register_admin_components", lambda: None)
    monkeypatch.setattr(app_module, "_register_bot_components", lambda: None)
    monkeypatch.setattr(app_module, "_register_scheduler_jobs", lambda: None)
    monkeypatch.setattr(app_module, "_register_notifications", lambda *_: None)

    def fake_load_plugins(*args, **kwargs):
        captured.append(args)
        return ()

    monkeypatch.setattr(app_module, "load_plugins", fake_load_plugins)

    app_module.create_system(config=config)

    assert len(captured) == 1
    assert captured[0] == config.plugin_paths
