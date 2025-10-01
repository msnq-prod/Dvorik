"""Built-in widgets bundled with the admin interface."""

from __future__ import annotations

import datetime as dt
import logging
import sqlite3
from collections import defaultdict
from contextlib import closing
from dataclasses import dataclass
from html import escape
from itertools import groupby
from typing import Any, Iterable, Mapping, MutableMapping, Sequence

from dvorik.core.plugins import register_widget
from dvorik.db.conn import db
from dvorik.domain.models import LowStockRecord, StockSnapshot
from dvorik.repo.schedule_repo import SQLiteScheduleRepo
from dvorik.repo.stock_repo import SQLiteStockRepo

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
        threshold = float(_get_widget_option("threshold", 5, self.config, context))
        limit = int(_get_widget_option("limit", 10, self.config, context))

        with closing(db()) as conn:
            records = SQLiteStockRepo(conn).low_stock(threshold=threshold, limit=limit)

        rows = tuple(_render_low_stock_rows(records))
        body = (
            "<p>Всё в порядке, запасов достаточно.</p>"
            if not rows
            else _build_table(
                headings=("Товар", "Локация", "Остаток"),
                rows=rows,
            )
        )

        return (
            "<section class=\"widget widget-low-stock\">"
            "<header><h3>Low stock overview</h3></header>"
            f"<div class=\"widget-body\">{body}</div>"
            "</section>"
        )


class ScheduleMiniWidget(Widget):
    slug = "schedule_mini"
    title = "Schedule snapshot"
    description = "Shows the current duty schedule summary."

    def render(self, context: WidgetContext | None = None) -> str:
        horizon = int(_get_widget_option("days", 7, self.config, context))
        today = dt.date.today()
        end_date = today + dt.timedelta(days=max(horizon - 1, 0))

        with closing(db()) as conn:
            repo = SQLiteScheduleRepo(conn)
            assignments = repo.assignments(today.isoformat(), end_date.isoformat())
            days = {item.date: repo.day(item.date) for item in {a.date for a in assignments}}

        grouped: dict[str, list[int]] = defaultdict(list)
        for assignment in assignments:
            grouped[assignment.date].append(assignment.tg_id)

        rows: list[tuple[str, str, str]] = []
        for current_date in sorted(grouped.keys()):
            day = days.get(current_date)
            friendly_date = _format_date(current_date)
            status = "Открыто" if (day.is_open if day else True) else "Закрыто"
            notes = escape(day.notes) if day and day.notes else ""
            assignees = ", ".join(str(tg_id) for tg_id in grouped[current_date]) or "—"
            subtitle = f"<div class=\"notes\">{notes}</div>" if notes else ""
            rows.append(
                (
                    f"{friendly_date}{subtitle}",
                    status,
                    escape(assignees),
                )
            )

        body = (
            "<p>В ближайшие дни дежурства не назначены.</p>"
            if not rows
            else _build_table(
                headings=("Дата", "Статус", "Дежурные"),
                rows=rows,
            )
        )

        return (
            "<section class=\"widget widget-schedule-mini\">"
            "<header><h3>Schedule snapshot</h3></header>"
            f"<div class=\"widget-body\">{body}</div>"
            "</section>"
        )


class StockByLocationWidget(Widget):
    slug = "stock_by_location"
    title = "Stock by location"
    description = "Displays stock totals grouped by location."

    def render(self, context: WidgetContext | None = None) -> str:
        location_code = _get_widget_option("location", None, self.config, context)
        limit = int(_get_widget_option("limit", 15, self.config, context))

        with closing(db()) as conn:
            repo = SQLiteStockRepo(conn)
            snapshots = repo.stock_by_location(
                location_code=str(location_code) if location_code else None,
            )

        limited = list(snapshots)[:limit]
        rows = tuple(_render_location_rows(limited))
        body = (
            "<p>Нет данных по остаткам на выбранных локациях.</p>"
            if not rows
            else _build_table(
                headings=("Локация", "Товар", "Остаток"),
                rows=rows,
            )
        )

        return (
            "<section class=\"widget widget-stock-by-location\">"
            "<header><h3>Stock by location</h3></header>"
            f"<div class=\"widget-body\">{body}</div>"
            "</section>"
        )


def _get_widget_option(
    key: str,
    default: object,
    config: Mapping[str, Any],
    context: WidgetContext | None,
) -> object:
    if key in config:
        return config[key]
    if context and key in context.config:
        return context.config[key]
    return default


def _build_table(*, headings: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    head_html = "".join(f"<th>{escape(str(title))}</th>" for title in headings)
    body_html = "".join(
        "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>" for row in rows
    )
    return f"<table><thead><tr>{head_html}</tr></thead><tbody>{body_html}</tbody></table>"


def _render_low_stock_rows(records: Sequence[LowStockRecord]) -> Iterable[tuple[str, str, str]]:
    for record in records:
        product_name = escape(record.product.name or "Без названия")
        location_name = escape(record.location.title or record.location.code)
        qty_text = escape(_format_quantity(record.qty_pack, record.product.unit))
        yield (product_name, location_name, qty_text)


def _render_location_rows(snapshots: Sequence[StockSnapshot]) -> Iterable[tuple[str, str, str]]:
    for location_code, group in groupby(
        snapshots,
        key=lambda snap: snap.location.code,
    ):
        entries = list(group)
        location_title = entries[0].location.title or location_code
        for snapshot in entries:
            product = snapshot.product
            product_name = escape(product.name or "Без названия")
            location_label = escape(location_title)
            qty_text = escape(_format_quantity(snapshot.qty_pack, product.unit))
            yield (location_label, product_name, qty_text)


def _format_quantity(value: float, unit: str | None) -> str:
    qty = f"{value:.2f}".rstrip("0").rstrip(".")
    return f"{qty} {unit}".strip() if unit else qty


def _format_date(value: str) -> str:
    try:
        parsed = dt.date.fromisoformat(value)
    except ValueError:
        return escape(value)
    return parsed.strftime("%d.%m.%Y")


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
