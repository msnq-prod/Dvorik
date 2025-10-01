from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import logging
from typing import Iterable, Mapping

from flask import Blueprint
import sqlite3

from dvorik.db.conn import db
from dvorik.services.menu_catalog import get_fallback_menu

logger = logging.getLogger(__name__)

blueprint = Blueprint("menus", __name__)


@dataclass(frozen=True, slots=True)
class MenuEntry:
    """Serializable representation of a navigation menu item."""

    slug: str
    title: str
    url: str | None = None
    icon: str | None = None
    target: str | None = None
    children: tuple["MenuEntry", ...] = ()


@dataclass(slots=True)
class _MenuNode:
    """Internal mutable node used during tree construction."""

    id: int
    slug: str
    title: str
    url: str | None
    icon: str | None
    target: str | None
    parent_id: int | None


_FALLBACK_MENU: tuple[MenuEntry, ...] = tuple(
    MenuEntry(
        slug=definition.slug,
        title=definition.title,
        url=definition.url,
        icon=definition.icon,
        target=definition.target,
    )
    for definition in get_fallback_menu()
)


@blueprint.app_context_processor
def inject_menu() -> Mapping[str, object]:
    """Provide navigation menu entries to all templates."""

    menu_entries, is_dynamic = _load_menu_entries()
    return {
        "menu_entries": menu_entries,
        "menu_is_dynamic": is_dynamic,
    }


def _load_menu_entries() -> tuple[tuple[MenuEntry, ...], bool]:
    rows = _fetch_rows()
    if not rows:
        return _FALLBACK_MENU, False

    tree = _build_tree(rows)
    if not tree:
        logger.info("Menu table contains only hidden or invalid entries; using fallback")
        return _FALLBACK_MENU, False

    return tree, True


def _fetch_rows() -> list[sqlite3.Row]:
    conn = db()
    try:
        cursor = conn.execute(
            """
            SELECT
                id,
                slug,
                title,
                url,
                icon,
                parent_id,
                position,
                target
            FROM ui_menu
            WHERE visible = 1
            ORDER BY COALESCE(parent_id, 0) ASC, position ASC, title ASC, id ASC
            """
        )
        return list(cursor.fetchall())
    except sqlite3.Error:
        logger.exception("Failed to fetch menu entries from database")
        return []
    finally:
        conn.close()


def _build_tree(rows: Iterable[sqlite3.Row]) -> tuple[MenuEntry, ...]:
    nodes: dict[int, _MenuNode] = {}
    children: defaultdict[int | None, list[int]] = defaultdict(list)

    ids = {int(row["id"]) for row in rows}

    for row in rows:
        node = _MenuNode(
            id=int(row["id"]),
            slug=str(row["slug"]),
            title=str(row["title"]),
            url=str(row["url"]) if row["url"] is not None else None,
            icon=str(row["icon"]) if row["icon"] else None,
            target=str(row["target"]) if row["target"] else None,
            parent_id=int(row["parent_id"]) if row["parent_id"] is not None else None,
        )
        nodes[node.id] = node

        parent_id = node.parent_id
        if parent_id not in ids:
            parent_id = None
        children[parent_id].append(node.id)

    top_level_ids = children.get(None, [])
    if not top_level_ids:
        # No explicit top-level entries; treat all as top-level.
        top_level_ids = list(nodes.keys())

    visited: set[int] = set()

    def build_entry(node_id: int, trail: tuple[int, ...] = ()) -> MenuEntry:
        if node_id in trail:
            logger.warning("Detected cyclic menu relationship: trail=%s", trail + (node_id,))
            base = nodes[node_id]
            return MenuEntry(
                slug=base.slug,
                title=base.title,
                url=base.url,
                icon=base.icon,
                target=base.target,
                children=(),
            )

        base = nodes[node_id]
        child_entries = []
        next_trail = trail + (node_id,)
        for child_id in children.get(node_id, []):
            child_entries.append(build_entry(child_id, next_trail))
        visited.add(node_id)
        return MenuEntry(
            slug=base.slug,
            title=base.title,
            url=base.url,
            icon=base.icon,
            target=base.target,
            children=tuple(child_entries),
        )

    entries = [build_entry(node_id) for node_id in top_level_ids if node_id in nodes]

    # Include any orphaned nodes that were not reached via parent references.
    if len(visited) != len(nodes):
        for node_id in nodes:
            if node_id not in visited:
                entries.append(build_entry(node_id))

    return tuple(entries)


__all__ = ["blueprint", "MenuEntry"]
