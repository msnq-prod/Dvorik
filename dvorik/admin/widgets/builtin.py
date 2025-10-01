"""Built-in widgets bundled with the admin interface."""

from __future__ import annotations

from typing import Iterable

from dvorik.core.plugins import register_widget
from dvorik.services.widget_catalog import (
    register_default_widget_layout,
    sync_widget_catalog,
)

from .api import Widget, WidgetContext


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
_BUILTIN_WIDGETS: tuple[type[Widget], ...] = (
    LowStockWidget,
    ScheduleMiniWidget,
    StockByLocationWidget,
)


def _widget_key(widget: type[Widget]) -> str:
    slug = getattr(widget, "slug", widget.__name__)
    return f"builtin.{slug}"


def _builtin_widget_keys() -> Iterable[str]:
    for widget in _BUILTIN_WIDGETS:
        yield _widget_key(widget)


def register_builtin_widgets() -> None:
    for widget in _BUILTIN_WIDGETS:
        register_widget(_widget_key(widget), widget, replace=True)

    register_default_widget_layout("home.main", tuple(_builtin_widget_keys()), replace=True)
    sync_widget_catalog()


__all__ = [
    "LowStockWidget",
    "ScheduleMiniWidget",
    "StockByLocationWidget",
    "register_builtin_widgets",
]
