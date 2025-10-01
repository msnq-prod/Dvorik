from __future__ import annotations

import sqlite3
from dataclasses import dataclass

import pytest

from dvorik.core.registry import MenuRegistry
from dvorik.services.menu_catalog import get_fallback_menu, sync_menu_catalog


@pytest.fixture()
def conn():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE ui_menu(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT NOT NULL UNIQUE,
            title TEXT NOT NULL,
            url TEXT,
            icon TEXT,
            parent_id INTEGER,
            position INTEGER NOT NULL DEFAULT 0,
            target TEXT,
            visible INTEGER NOT NULL DEFAULT 1
        );
        """
    )
    yield connection
    connection.close()


@pytest.fixture(autouse=True)
def reset_menu_registry():
    MenuRegistry.clear()
    yield
    MenuRegistry.clear()


def test_sync_menu_catalog_seeds_fallback_entries(conn):
    result = sync_menu_catalog(connection=conn)

    rows = conn.execute(
        "SELECT slug, title, position FROM ui_menu ORDER BY position ASC, slug ASC"
    ).fetchall()
    fallback = get_fallback_menu()

    assert [row["slug"] for row in rows] == [definition.slug for definition in fallback]
    assert [row["title"] for row in rows] == [definition.title for definition in fallback]
    assert [row["position"] for row in rows] == [definition.position for definition in fallback]
    assert set(result) == {definition.slug for definition in fallback}


def test_sync_menu_catalog_persists_dataclass_entries(conn):
    @dataclass(slots=True)
    class PluginMenu:
        slug: str
        title: str
        url: str | None = None
        icon: str | None = None
        position: int = 55

    MenuRegistry.register("plugin.menu.custom", PluginMenu(slug="custom", title="Custom", url="/custom"))

    sync_menu_catalog(connection=conn)

    row = conn.execute("SELECT slug, url, position FROM ui_menu WHERE slug = ?", ("custom",)).fetchone()
    assert row["slug"] == "custom"
    assert row["url"] == "/custom"
    assert row["position"] == 55

    fallback_slugs = {definition.slug for definition in get_fallback_menu()}
    rows = conn.execute("SELECT slug FROM ui_menu").fetchall()
    assert fallback_slugs.union({"custom"}) == {row["slug"] for row in rows}
