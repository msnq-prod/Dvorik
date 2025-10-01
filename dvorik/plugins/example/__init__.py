"""Example plugin showcasing integration with the plugin loader."""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from dvorik.admin.widgets.api import Widget, WidgetContext
from dvorik.core.plugins import (
    register_bot_router,
    register_menu,
    register_plugin,
    register_widget,
)
from dvorik.core.version import API_VERSION as CORE_API_VERSION
from dvorik.db.conn import db

logger = logging.getLogger(__name__)


PLUGIN_NAME = "example"
PLUGIN_VERSION = "0.1.0"
API_VERSION = CORE_API_VERSION
_PLUGIN_DESCRIPTION = "Example plugin bundled with the core distribution"


class TopSkusWidget(Widget):
    """Simple placeholder widget rendered on the admin dashboard."""

    slug = "top_skus"
    title = "Top SKUs"
    description = (
        "Highlights the top-performing products. Placeholder implementation "
        "until real analytics are wired in."
    )

    def render(self, context: WidgetContext | None = None) -> str:  # noqa: D401
        return """
        <section class=\"widget widget-top-skus\">
            <header><h3>Top SKUs</h3></header>
            <p>Аналитика по продажам появится после переноса сервиса статистики.</p>
            <ul>
                <li>Следите за обновлениями — данные скоро появятся.</li>
                <li>Виджет предоставлен примером плагина.</li>
            </ul>
        </section>
        """.strip()


router = Router(name="plugin_example")


@router.message(Command("topskus"))
async def handle_top_skus(message: Message) -> None:
    """Respond with a teaser about the new dashboard widget."""

    await message.answer(
        "В админке появился виджет \"Top SKUs\" из примерного плагина. "
        "Скоро он начнет показывать реальные данные!"
    )


@dataclass(frozen=True, slots=True)
class _MenuDefinition:
    slug: str
    title: str
    url: str
    icon: str | None = None
    position: int = 90


_MENU_ENTRY = _MenuDefinition(
    slug="example-top-skus",
    title="Top SKUs",
    url="/superadmin?section=widgets#example-top-skus",
    icon="bi-star",
    position=90,
)


def plugin_info() -> dict[str, str]:
    return {
        "name": PLUGIN_NAME,
        "version": PLUGIN_VERSION,
        "api_version": API_VERSION,
        "description": _PLUGIN_DESCRIPTION,
    }


def _ensure_widget_catalogued() -> None:
    conn = db()
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO ui_widget(module, name, title, description, entrypoint, config_schema)
                VALUES (?, ?, ?, ?, ?, NULL)
                ON CONFLICT(module, name) DO UPDATE SET
                    title = excluded.title,
                    description = excluded.description,
                    entrypoint = excluded.entrypoint,
                    config_schema = excluded.config_schema
                """,
                (
                    "example",
                    TopSkusWidget.slug,
                    TopSkusWidget.title,
                    TopSkusWidget.description,
                    TopSkusWidget.entrypoint(),
                ),
            )

            row = conn.execute(
                "SELECT id FROM ui_widget WHERE module = ? AND name = ?",
                ("example", TopSkusWidget.slug),
            ).fetchone()
            if row is None:
                return

            widget_id = int(row["id"] if isinstance(row, sqlite3.Row) else row[0])

            instance_row = conn.execute(
                """
                SELECT id FROM ui_widget_instance
                WHERE widget_id = ? AND zone = ?
                """,
                (widget_id, "home.main"),
            ).fetchone()
            if instance_row is None:
                position_row = conn.execute(
                    "SELECT COALESCE(MAX(position), -1) FROM ui_widget_instance WHERE zone = ?",
                    ("home.main",),
                ).fetchone()
                position = int(position_row[0]) + 1 if position_row else 0
                conn.execute(
                    """
                    INSERT INTO ui_widget_instance(widget_id, zone, position, enabled)
                    VALUES (?, ?, ?, 1)
                    """,
                    (widget_id, "home.main", position),
                )
    except sqlite3.Error:  # pragma: no cover - logged for visibility
        logger.exception("Failed to ensure example widget catalogue entry")
    finally:
        conn.close()


def _ensure_menu_entry() -> None:
    conn = db()
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO ui_menu(slug, title, url, icon, parent_id, position, target, visible)
                VALUES (?, ?, ?, ?, NULL, ?, NULL, 1)
                ON CONFLICT(slug) DO UPDATE SET
                    title = excluded.title,
                    url = excluded.url,
                    icon = excluded.icon,
                    position = excluded.position,
                    visible = excluded.visible
                """,
                (
                    _MENU_ENTRY.slug,
                    _MENU_ENTRY.title,
                    _MENU_ENTRY.url,
                    _MENU_ENTRY.icon,
                    _MENU_ENTRY.position,
                ),
            )
    except sqlite3.Error:  # pragma: no cover - logged for visibility
        logger.exception("Failed to persist example plugin menu entry")
    finally:
        conn.close()


def _register_components() -> None:
    info = plugin_info()
    register_plugin(
        info["name"],
        version=info["version"],
        api_version=info["api_version"],
        description=info["description"],
    )
    register_widget("example.top_skus", TopSkusWidget, replace=True)
    register_menu(
        "example.top_skus",
        {
            "slug": _MENU_ENTRY.slug,
            "title": _MENU_ENTRY.title,
            "url": _MENU_ENTRY.url,
            "icon": _MENU_ENTRY.icon,
        },
        replace=True,
    )
    register_bot_router("example.top_skus", router, replace=True)


_register_components()
_ensure_widget_catalogued()
_ensure_menu_entry()


__all__ = [
    "TopSkusWidget",
    "router",
]
