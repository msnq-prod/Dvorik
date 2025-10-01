"""Widget utilities for the admin interface."""

from .api import Widget, WidgetContext
from .builtin import (
    LowStockWidget,
    ScheduleMiniWidget,
    StockByLocationWidget,
    register_builtin_widgets,
)

__all__ = [
    "Widget",
    "WidgetContext",
    "LowStockWidget",
    "ScheduleMiniWidget",
    "StockByLocationWidget",
    "register_builtin_widgets",
]
