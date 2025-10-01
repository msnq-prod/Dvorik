"""Unit tests for registry singletons."""

from __future__ import annotations

from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dvorik.core.registry import (
    BotRouterRegistry,
    JobRegistry,
    MenuRegistry,
    QueryRegistry,
    WidgetRegistry,
)


@pytest.fixture(autouse=True)
def reset_registries():
    """Ensure registry state isolation between tests."""

    for registry in (MenuRegistry, WidgetRegistry, BotRouterRegistry, JobRegistry):
        registry.clear()
    QueryRegistry.bind_to(None)
    QueryRegistry.clear()
    yield
    for registry in (MenuRegistry, WidgetRegistry, BotRouterRegistry, JobRegistry):
        registry.clear()
    QueryRegistry.bind_to(None)
    QueryRegistry.clear()


def test_widget_registry_register_and_get():
    WidgetRegistry.register("widget.home", {"name": "HomeWidget"})

    assert WidgetRegistry.get("widget.home") == {"name": "HomeWidget"}
    assert "widget.home" in WidgetRegistry
    assert len(WidgetRegistry) == 1

    with pytest.raises(KeyError):
        WidgetRegistry.register("widget.home", {"name": "Duplicate"})


def test_registry_ensure_supports_factories():
    value = WidgetRegistry.ensure("widget.dynamic", lambda: {"computed": True})

    assert value == {"computed": True}
    assert WidgetRegistry.get("widget.dynamic") is value


def test_query_registry_returns_defaults():
    QueryRegistry.register("stock.low", "SELECT 1")

    assert QueryRegistry.get("stock.low") == "SELECT 1"
    assert QueryRegistry.get("missing", "SELECT 2") == "SELECT 2"


def test_query_registry_bind_to_external_storage():
    external: dict[str, str] = {}

    QueryRegistry.bind_to(external)
    QueryRegistry.register("custom.sql", "SELECT * FROM product")

    assert external["custom.sql"] == "SELECT * FROM product"
    assert QueryRegistry.as_mapping() is external

    QueryRegistry.unregister("custom.sql")
    assert external == {}
