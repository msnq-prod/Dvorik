from __future__ import annotations

import logging
import sqlite3
from dataclasses import asdict, dataclass, is_dataclass
from typing import Mapping, MutableMapping

from dvorik.core.registry import MenuRegistry
from dvorik.db.conn import db

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MenuDefinition:
    """Declarative description of a menu item for catalogue seeding."""

    key: str
    slug: str
    title: str
    url: str | None = None
    icon: str | None = None
    position: int = 0
    target: str | None = None
    visible: bool = True
    parent_slug: str | None = None

    def to_payload(self) -> Mapping[str, object]:
        """Return a mapping suitable for :class:`MenuRegistry`."""

        return {
            "slug": self.slug,
            "title": self.title,
            "url": self.url,
            "icon": self.icon,
            "position": self.position,
            "target": self.target,
            "visible": self.visible,
            "parent_slug": self.parent_slug,
        }


@dataclass(frozen=True, slots=True)
class _CatalogEntry:
    key: str
    slug: str
    title: str
    url: str | None
    icon: str | None
    position: int
    target: str | None
    visible: bool
    parent_slug: str | None


_FALLBACK_MENU: tuple[MenuDefinition, ...] = (
    MenuDefinition(
        key="builtin.menu.dashboard",
        slug="dashboard",
        title="Dashboard",
        url="/",
        position=10,
    ),
    MenuDefinition(
        key="builtin.menu.supply",
        slug="supply",
        title="Supply",
        url="/supply",
        position=20,
    ),
    MenuDefinition(
        key="builtin.menu.tables",
        slug="tables",
        title="Tables",
        url="/tables",
        position=30,
    ),
    MenuDefinition(
        key="builtin.menu.superadmin",
        slug="superadmin",
        title="Superadmin",
        url="/superadmin",
        position=40,
    ),
)


def get_fallback_menu() -> tuple[MenuDefinition, ...]:
    """Return fallback menu definitions used when the DB catalogue is empty."""

    return _FALLBACK_MENU


def ensure_fallback_menu_registered() -> None:
    """Ensure fallback menu definitions are present in :class:`MenuRegistry`."""

    for definition in _FALLBACK_MENU:
        MenuRegistry.ensure(definition.key, definition.to_payload())


def sync_menu_catalog(connection: sqlite3.Connection | None = None) -> Mapping[str, int]:
    """Persist registered menu entries into ``ui_menu``.

    Parameters
    ----------
    connection:
        Optional SQLite connection.  When ``None`` the default application
        connection is used.

    Returns
    -------
    Mapping[str, int]
        Mapping of menu ``slug`` to the corresponding database ``id``.
    """

    ensure_fallback_menu_registered()

    snapshot = tuple(MenuRegistry.items())
    if not snapshot:
        return {}

    entries = tuple(filter(None, (_normalise_entry(key, value) for key, value in snapshot)))
    if not entries:
        return {}

    owns_connection = connection is None
    conn = connection if connection is not None else db()
    ids: MutableMapping[str, int] = {}

    try:
        with conn:
            for entry in entries:
                conn.execute(
                    """
                    INSERT INTO ui_menu(slug, title, url, icon, parent_id, position, target, visible)
                    VALUES (?, ?, ?, ?, NULL, ?, ?, ?)
                    ON CONFLICT(slug) DO UPDATE SET
                        title = excluded.title,
                        url = excluded.url,
                        icon = excluded.icon,
                        position = excluded.position,
                        target = excluded.target,
                        visible = excluded.visible
                    """,
                    (
                        entry.slug,
                        entry.title,
                        entry.url,
                        entry.icon,
                        entry.position,
                        entry.target,
                        1 if entry.visible else 0,
                    ),
                )

            for entry in entries:
                row = conn.execute("SELECT id FROM ui_menu WHERE slug = ?", (entry.slug,)).fetchone()
                if row is None:
                    logger.warning("Menu entry '%s' was not persisted", entry.slug)
                    continue
                ids[entry.slug] = int(row["id"] if isinstance(row, sqlite3.Row) else row[0])

            for entry in entries:
                desired_parent_slug = entry.parent_slug
                parent_id = ids.get(desired_parent_slug) if desired_parent_slug else None
                conn.execute(
                    "UPDATE ui_menu SET parent_id = ? WHERE slug = ?",
                    (parent_id, entry.slug),
                )
    finally:
        if owns_connection:
            conn.close()

    return dict(ids)


def _normalise_entry(key: str, value: object) -> _CatalogEntry | None:
    mapping = _to_mapping(value)
    if mapping is None:
        logger.warning("Menu registry entry '%s' must be a mapping or dataclass", key)
        return None

    slug_raw = mapping.get("slug")
    title_raw = mapping.get("title")
    if not slug_raw or not title_raw:
        logger.warning("Menu registry entry '%s' is missing slug/title", key)
        return None

    slug = str(slug_raw).strip()
    title = str(title_raw).strip()
    if not slug or not title:
        logger.warning("Menu registry entry '%s' has empty slug/title", key)
        return None

    url_value = mapping.get("url")
    url = str(url_value).strip() if isinstance(url_value, str) else url_value if url_value is None else str(url_value)

    icon_value = mapping.get("icon")
    icon = str(icon_value).strip() if isinstance(icon_value, str) and icon_value.strip() else None

    target_value = mapping.get("target")
    target = str(target_value).strip() if isinstance(target_value, str) and target_value.strip() else None

    position_value = mapping.get("position", 0)
    try:
        position = int(position_value)
    except (TypeError, ValueError):
        logger.warning(
            "Menu registry entry '%s' provides invalid position %r; defaulting to 0",
            key,
            position_value,
        )
        position = 0

    visible = bool(mapping.get("visible", True))

    parent_slug_value = mapping.get("parent_slug") or mapping.get("parent")
    parent_slug = None
    if isinstance(parent_slug_value, str):
        parent_slug = parent_slug_value.strip() or None
    elif parent_slug_value is not None:
        parent_slug = str(parent_slug_value)

    return _CatalogEntry(
        key=key,
        slug=slug,
        title=title,
        url=url,
        icon=icon,
        position=position,
        target=target,
        visible=visible,
        parent_slug=parent_slug,
    )


def _to_mapping(value: object) -> Mapping[str, object] | None:
    if isinstance(value, Mapping):
        return value
    if is_dataclass(value):
        return asdict(value)
    return None


__all__ = [
    "MenuDefinition",
    "ensure_fallback_menu_registered",
    "get_fallback_menu",
    "sync_menu_catalog",
]
