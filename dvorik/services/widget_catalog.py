"""Helpers for synchronising the widget catalogue with the database."""

from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from typing import Iterable, Mapping, MutableMapping, Sequence

from dvorik.admin.widgets.api import Widget
from dvorik.core.registry import WidgetRegistry
from dvorik.db.conn import db

logger = logging.getLogger(__name__)

_DEFAULT_LAYOUTS: MutableMapping[str, list[str]] = defaultdict(list)
_WIDGET_ID_CACHE: MutableMapping[str, int] = {}


def register_default_widget_layout(
    zone: str, widgets: Sequence[str] | str, *, replace: bool = False
) -> None:
    """Declare widgets that should exist in ``zone`` by default.

    Parameters
    ----------
    zone:
        Dashboard zone identifier (e.g. ``"home.main"``).
    widgets:
        Sequence of widget registry keys. A single key may also be supplied.
    replace:
        When ``True`` the stored layout for ``zone`` is replaced entirely.
    """

    if isinstance(widgets, str):
        widget_keys = [widgets]
    else:
        widget_keys = list(widgets)

    if not widget_keys:
        return

    if replace or zone not in _DEFAULT_LAYOUTS:
        _DEFAULT_LAYOUTS[zone] = []

    layout = _DEFAULT_LAYOUTS[zone]
    if replace:
        layout.clear()

    for key in widget_keys:
        if key and key not in layout:
            layout.append(key)


def sync_widget_catalog(
    *, connection: sqlite3.Connection | None = None
) -> Mapping[str, int]:
    """Persist registered widgets and ensure default instances exist."""

    close_connection = False
    conn = connection
    if conn is None:
        conn = db()
        close_connection = True

    try:
        widgets = list(WidgetRegistry.items())
        catalog = _persist_widgets(conn, widgets)
        _ensure_default_instances(conn, catalog)
        _WIDGET_ID_CACHE.clear()
        _WIDGET_ID_CACHE.update(catalog)
        return dict(catalog)
    finally:
        if close_connection and conn is not None:
            conn.close()


def get_widget_catalog_ids() -> Mapping[str, int]:
    """Return a snapshot of the most recent key→ID mapping."""

    return dict(_WIDGET_ID_CACHE)


def _persist_widgets(
    conn: sqlite3.Connection, entries: Iterable[tuple[str, object]]
) -> dict[str, int]:
    ids: dict[str, int] = {}

    with conn:
        for key, candidate in entries:
            widget_cls = _coerce_widget_class(candidate, key)
            if widget_cls is None:
                continue

            module_name, widget_name = _split_widget_key(key, widget_cls)
            title = getattr(widget_cls, "title", widget_name)
            description = getattr(widget_cls, "description", None)
            config_schema = getattr(widget_cls, "config_schema", None)

            conn.execute(
                """
                INSERT INTO ui_widget(module, name, title, description, entrypoint, config_schema)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(module, name) DO UPDATE SET
                    title = excluded.title,
                    description = excluded.description,
                    entrypoint = excluded.entrypoint,
                    config_schema = excluded.config_schema
                """,
                (
                    module_name,
                    widget_name,
                    title,
                    description,
                    widget_cls.entrypoint(),
                    config_schema,
                ),
            )

        for key, candidate in entries:
            widget_cls = _coerce_widget_class(candidate, key)
            if widget_cls is None:
                continue

            module_name, widget_name = _split_widget_key(key, widget_cls)
            row = conn.execute(
                "SELECT id FROM ui_widget WHERE module = ? AND name = ?",
                (module_name, widget_name),
            ).fetchone()
            if row is None:
                logger.warning("Widget %s was not persisted in ui_widget", key)
                continue
            widget_id = int(row["id"] if isinstance(row, sqlite3.Row) else row[0])
            ids[key] = widget_id

    return ids


def _ensure_default_instances(
    conn: sqlite3.Connection, widget_ids: Mapping[str, int]
) -> None:
    if not _DEFAULT_LAYOUTS:
        return

    with conn:
        for zone, widget_keys in _DEFAULT_LAYOUTS.items():
            existing_ids = {
                int(row["widget_id"] if isinstance(row, sqlite3.Row) else row[0])
                for row in conn.execute(
                    "SELECT widget_id FROM ui_widget_instance WHERE zone = ?",
                    (zone,),
                ).fetchall()
            }

            position_row = conn.execute(
                "SELECT COALESCE(MAX(position), -1) FROM ui_widget_instance WHERE zone = ?",
                (zone,),
            ).fetchone()
            position = int(position_row[0]) if position_row else -1

            for key in widget_keys:
                widget_id = widget_ids.get(key)
                if widget_id is None:
                    logger.debug("Skipping default placement for unknown widget %s", key)
                    continue
                if widget_id in existing_ids:
                    continue

                position += 1
                conn.execute(
                    """
                    INSERT INTO ui_widget_instance(widget_id, zone, position, enabled)
                    VALUES (?, ?, ?, 1)
                    """,
                    (widget_id, zone, position),
                )
                existing_ids.add(widget_id)


def _coerce_widget_class(candidate: object, key: str) -> type[Widget] | None:
    if isinstance(candidate, type) and issubclass(candidate, Widget):
        return candidate

    logger.debug(
        "Widget registry entry %s is not a Widget subclass; skipping persistence", key
    )
    return None


def _split_widget_key(key: str, widget_cls: type[Widget]) -> tuple[str, str]:
    module_name, _, widget_name = key.partition(".")
    if not module_name or not widget_name:
        module_name = widget_cls.__module__
        widget_name = getattr(widget_cls, "slug", widget_cls.__name__)
    return module_name, widget_name


__all__ = [
    "get_widget_catalog_ids",
    "register_default_widget_layout",
    "sync_widget_catalog",
]

