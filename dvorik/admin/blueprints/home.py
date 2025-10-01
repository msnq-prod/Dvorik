from __future__ import annotations

import html
import logging
from collections import defaultdict
from dataclasses import dataclass
from importlib import import_module
from typing import Any, Mapping

from flask import Blueprint, current_app, render_template

from dvorik.admin.widgets.api import Widget, WidgetContext
from dvorik.admin.widgets.validation import WidgetConfigError, validate_widget_config
from dvorik.core.config import Config
from dvorik.core.registry import WidgetRegistry
from dvorik.db.conn import db

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ZoneLayout:
    """Description of a layout zone displayed on the home page."""

    name: str
    css_class: str
    title: str | None = None


@dataclass(slots=True)
class RenderedWidget:
    """Widget instance rendered into HTML for display."""

    id: int
    key: str
    slug: str
    title: str
    html: str


HOME_LAYOUT: tuple[ZoneLayout, ...] = (
    ZoneLayout(name="home.main", css_class="zone-main"),
)

blueprint = Blueprint("home", __name__)


@blueprint.get("/")
def index() -> str:
    """Render the admin dashboard populated with widget zones."""

    config = _get_config()
    rendered_zones = _render_widget_zones(config)

    return render_template(
        "home.html",
        page_title="Dashboard",
        layout=HOME_LAYOUT,
        zones=rendered_zones,
    )


def _get_config() -> Config:
    value = current_app.config.get("DVORIK_CONFIG")
    if not isinstance(value, Config):  # pragma: no cover - defensive guard
        raise RuntimeError("DVORIK_CONFIG is not initialised")
    return value


def _render_widget_zones(config: Config) -> Mapping[str, tuple[RenderedWidget, ...]]:
    """Load widget instances from the database and render them to HTML."""

    conn = db()
    try:
        rows = conn.execute(
            """
            SELECT
                inst.id,
                inst.zone,
                inst.position,
                inst.config_json,
                widget.module,
                widget.name,
                widget.entrypoint,
                widget.config_schema
            FROM ui_widget_instance AS inst
            JOIN ui_widget AS widget ON widget.id = inst.widget_id
            WHERE inst.enabled = 1
            ORDER BY inst.zone ASC, inst.position ASC, inst.id ASC
            """
        ).fetchall()
    finally:
        conn.close()

    grouped: dict[str, list[RenderedWidget]] = defaultdict(list)

    for row in rows:
        zone = str(row["zone"])
        widget_id = int(row["id"])
        module = row["module"]
        name = row["name"]
        entrypoint = row["entrypoint"]
        key = _compose_widget_key(module, name) or (entrypoint or f"widget:{widget_id}")

        widget_cls = _resolve_widget_class(module, name, entrypoint)
        if widget_cls is None:
            logger.warning(
                "Widget %s (id=%s) is not registered; rendering placeholder", key, widget_id
            )
            grouped[zone].append(
                RenderedWidget(
                    id=widget_id,
                    key=key,
                    slug="unavailable",
                    title="Widget unavailable",
                    html=_render_error_markup("Widget unavailable", "Definition missing."),
                )
            )
            continue

        widget_title = str(getattr(widget_cls, "title", key))
        widget_slug = str(getattr(widget_cls, "slug", key))
        config_mapping = _decode_instance_config(
            row["config_json"],
            row["config_schema"],
            widget_id,
            key,
        )

        try:
            widget_obj = widget_cls(config=config_mapping)
        except Exception:
            logger.exception("Failed to initialise widget %s (id=%s)", key, widget_id)
            html_markup = _render_error_markup(widget_title, "Widget initialisation failed.")
        else:
            context = WidgetContext(
                config=config.as_dict(),
                extra={"zone": zone, "widget_id": widget_id, "widget_key": key},
            )
            try:
                html_markup = str(widget_obj.render(context))
            except Exception:
                logger.exception("Widget %s (id=%s) raised during render", key, widget_id)
                html_markup = _render_error_markup(widget_title, "Widget rendering failed.")

        grouped[zone].append(
            RenderedWidget(
                id=widget_id,
                key=key,
                slug=widget_slug,
                title=widget_title,
                html=html_markup,
            )
        )

    return {zone: tuple(items) for zone, items in grouped.items()}


def _compose_widget_key(module: Any, name: Any) -> str | None:
    if module and name:
        return f"{module}.{name}"
    return None


def _resolve_widget_class(
    module: Any,
    name: Any,
    entrypoint: Any,
) -> type[Widget] | None:
    key = _compose_widget_key(module, name)

    if key:
        try:
            candidate = WidgetRegistry.get(key)
        except KeyError:
            candidate = None
        else:
            if isinstance(candidate, type) and issubclass(candidate, Widget):
                return candidate
            logger.warning(
                "Widget registry entry %s is not a Widget subclass; falling back to entrypoint",
                key,
            )

    entrypoint_str = str(entrypoint) if entrypoint else None
    if not entrypoint_str:
        return None

    try:
        module_name, class_name = entrypoint_str.split(":", 1)
    except ValueError:
        logger.error("Invalid widget entrypoint '%s'", entrypoint_str)
        return None

    try:
        imported_module = import_module(module_name)
    except ModuleNotFoundError:
        logger.exception("Failed to import widget module %s", module_name)
        return None

    try:
        attr = getattr(imported_module, class_name)
    except AttributeError:
        logger.exception("Widget class %s missing in module %s", class_name, module_name)
        return None

    if not isinstance(attr, type) or not issubclass(attr, Widget):
        logger.error(
            "Widget entrypoint %s resolved to invalid object %r", entrypoint_str, attr
        )
        return None

    return attr


def _decode_instance_config(
    raw_config: Any,
    raw_schema: Any,
    widget_id: int,
    widget_key: str | None,
) -> Mapping[str, Any]:
    config_text = _normalise_json_text(raw_config, widget_id, widget_key, "configuration")
    schema_text = _normalise_json_text(raw_schema, widget_id, widget_key, "config schema")

    try:
        return validate_widget_config(config_text, schema_text)
    except WidgetConfigError as exc:
        label = widget_key or f"widget:{widget_id}"
        logger.warning(
            "Widget %s (id=%s) configuration failed validation: %s",
            label,
            widget_id,
            exc,
        )
        return {
            "__invalid_config__": True,
            "message": "Widget configuration invalid.",
        }


def _normalise_json_text(value: Any, widget_id: int, widget_key: str | None, kind: str) -> str | None:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="ignore")
    if isinstance(value, str):
        return value

    label = widget_key or f"widget:{widget_id}"
    logger.warning("Widget %s (id=%s) %s is not stored as text", label, widget_id, kind)
    return None


def _render_error_markup(title: str, message: str) -> str:
    escaped_title = html.escape(title or "Widget error")
    escaped_message = html.escape(message or "Widget failed to render.")
    return (
        "<section class=\"widget widget-error\">"
        "<header><h3>{title}</h3></header>"
        "<p class=\"widget-error__message\">{message}</p>"
        "</section>"
    ).format(title=escaped_title, message=escaped_message)


__all__ = ["blueprint", "HOME_LAYOUT", "ZoneLayout"]
