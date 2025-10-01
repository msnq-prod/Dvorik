"""Built-in widgets bundled with the admin interface."""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from typing import Mapping, MutableMapping

from dvorik.core.plugins import register_widget
from dvorik.db.conn import db

from .api import Widget, WidgetContext

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _WidgetDefinition:
    module: str
    name: str
    title: str
    widget: type[Widget]
    description: str | None = None
    config_schema: str | None = None

    @property
    def key(self) -> str:
        return f"{self.module}.{self.name}"


class LowStockWidget(Widget):
    slug = "low_stock"
    title = "Low stock overview"
    description = "Highlights products that are running low on inventory."

    def render(self, context: WidgetContext | None = None) -> str:
        return """
        <section class="widget widget-low-stock">
            <header><h3>Low stock overview</h3></header>
            <p>No stock analytics are available yet.</p>
        </section>
        """.strip()


class ScheduleMiniWidget(Widget):
    slug = "schedule_mini"
    title = "Schedule snapshot"
    description = "Shows the current duty schedule summary."

    def render(self, context: WidgetContext | None = None) -> str:
        return """
        <section class="widget widget-schedule-mini">
            <header><h3>Schedule snapshot</h3></header>
            <p>Scheduling data is coming soon.</p>
        </section>
        """.strip()


class StockByLocationWidget(Widget):
    slug = "stock_by_location"
    title = "Stock by location"
    description = "Displays stock totals grouped by location."

    def render(self, context: WidgetContext | None = None) -> str:
        return """
        <section class="widget widget-stock-by-location">
            <header><h3>Stock by location</h3></header>
            <p>Location statistics are not yet implemented.</p>
        </section>
        """.strip()


_BUILTIN_WIDGETS: tuple[_WidgetDefinition, ...] = (
    _WidgetDefinition(
        module="builtin",
        name="low_stock",
        title=LowStockWidget.title,
        widget=LowStockWidget,
        description=LowStockWidget.description,
    ),
    _WidgetDefinition(
        module="builtin",
        name="schedule_mini",
        title=ScheduleMiniWidget.title,
        widget=ScheduleMiniWidget,
        description=ScheduleMiniWidget.description,
    ),
    _WidgetDefinition(
        module="builtin",
        name="stock_by_location",
        title=StockByLocationWidget.title,
        widget=StockByLocationWidget,
        description=StockByLocationWidget.description,
    ),
)

_DEFAULT_HOME_ORDER: tuple[str, ...] = tuple(defn.key for defn in _BUILTIN_WIDGETS)


def register_builtin_widgets() -> None:
    for definition in _BUILTIN_WIDGETS:
        register_widget(definition.key, definition.widget, replace=True)

    conn = db()
    try:
        widget_ids = _sync_catalog(conn)
        _seed_home_instances(conn, widget_ids)
    finally:
        conn.close()


def _sync_catalog(conn: sqlite3.Connection) -> Mapping[str, int]:
    ids: MutableMapping[str, int] = {}

    with conn:
        for definition in _BUILTIN_WIDGETS:
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
                    definition.module,
                    definition.name,
                    definition.title,
                    definition.description,
                    definition.widget.entrypoint(),
                    definition.config_schema,
                ),
            )

        for definition in _BUILTIN_WIDGETS:
            row = conn.execute(
                "SELECT id FROM ui_widget WHERE module = ? AND name = ?",
                (definition.module, definition.name),
            ).fetchone()
            if row is None:
                raise RuntimeError(f"Failed to persist widget {definition.key}")
            ids[definition.key] = int(row["id"] if isinstance(row, sqlite3.Row) else row[0])

    return ids


def _seed_home_instances(conn: sqlite3.Connection, widget_ids: Mapping[str, int]) -> None:
    cursor = conn.execute("SELECT COUNT(*) FROM ui_widget_instance")
    row = cursor.fetchone()
    existing = int(row[0]) if row is not None else 0
    if existing:
        return

    with conn:
        for position, key in enumerate(_DEFAULT_HOME_ORDER):
            widget_id = widget_ids.get(key)
            if widget_id is None:
                logger.warning("Widget %s missing from catalog; skipping seed", key)
                continue
            conn.execute(
                """
                INSERT INTO ui_widget_instance(widget_id, zone, position, enabled)
                VALUES (?, ?, ?, 1)
                """,
                (widget_id, "home.main", position),
            )


__all__ = [
    "LowStockWidget",
    "ScheduleMiniWidget",
    "StockByLocationWidget",
    "register_builtin_widgets",
]
