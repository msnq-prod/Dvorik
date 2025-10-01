from __future__ import annotations

import json
import logging
from typing import Any

import sqlite3

from flask import Blueprint, Response, has_request_context, redirect, render_template, request, url_for

from dvorik.db.conn import db

logger = logging.getLogger(__name__)

blueprint = Blueprint("superadmin", __name__, url_prefix="/superadmin")


@blueprint.get("/")
def dashboard() -> str:
    """Render the management console overview."""

    data = _fetch_dashboard_data()
    return render_template(
        "superadmin/dashboard.html",
        widgets=data["widgets"],
        widget_instances=data["widget_instances"],
        menu_entries=data["menu_entries"],
        queries=data["queries"],
        jobs=data["jobs"],
        schedule_types=("daily", "cron"),
    )


@blueprint.post("/widgets/save")
def save_widget() -> Response:
    """Create or update a widget definition."""

    widget_id = _parse_int(request.form.get("id"))
    module = _clean_text(request.form.get("module"))
    name = _clean_text(request.form.get("name"))
    title = _clean_text(request.form.get("title"))
    if not module or not name or not title:
        return _redirect_to_dashboard("widgets", status="error", error="Module, name and title are required.")

    description = _clean_text(request.form.get("description"))
    entrypoint = _clean_text(request.form.get("entrypoint"))
    config_schema = _clean_text(request.form.get("config_schema"))

    payload = {
        "module": module,
        "name": name,
        "title": title,
        "description": description,
        "entrypoint": entrypoint,
        "config_schema": config_schema,
    }

    conn = db()
    try:
        with conn:
            if widget_id is not None:
                cursor = conn.execute(
                    """
                    UPDATE ui_widget
                    SET module = ?, name = ?, title = ?, description = ?, entrypoint = ?, config_schema = ?
                    WHERE id = ?
                    """,
                    (module, name, title, description, entrypoint, config_schema, widget_id),
                )
                if cursor.rowcount == 0:
                    return _redirect_to_dashboard("widgets", status="error", error="Widget was not found.")
                _log_audit(conn, "update", "ui_widget", widget_id, payload)
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO ui_widget(module, name, title, description, entrypoint, config_schema)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (module, name, title, description, entrypoint, config_schema),
                )
                new_id = int(cursor.lastrowid)
                _log_audit(conn, "create", "ui_widget", new_id, payload)
    except sqlite3.Error as exc:  # pragma: no cover - defensive programming
        logger.exception("Failed to save widget definition")
        return _redirect_to_dashboard("widgets", status="error", error=_format_error(exc))
    finally:
        conn.close()

    return _redirect_to_dashboard("widgets", status="saved")


@blueprint.post("/widgets/delete")
def delete_widget() -> Response:
    """Delete a widget definition."""

    widget_id = _parse_int(request.form.get("id"))
    if widget_id is None:
        return _redirect_to_dashboard("widgets", status="error", error="Widget id is required for deletion.")

    conn = db()
    try:
        with conn:
            cursor = conn.execute("DELETE FROM ui_widget WHERE id = ?", (widget_id,))
            if cursor.rowcount == 0:
                return _redirect_to_dashboard("widgets", status="error", error="Widget was not found.")
            _log_audit(conn, "delete", "ui_widget", widget_id, {"deleted": True})
    except sqlite3.Error as exc:  # pragma: no cover - defensive programming
        logger.exception("Failed to delete widget definition")
        return _redirect_to_dashboard("widgets", status="error", error=_format_error(exc))
    finally:
        conn.close()

    return _redirect_to_dashboard("widgets", status="deleted")


@blueprint.post("/widget-instances/save")
def save_widget_instance() -> Response:
    """Create or update a widget instance."""

    instance_id = _parse_int(request.form.get("id"))
    widget_id = _parse_int(request.form.get("widget_id"))
    zone = _clean_text(request.form.get("zone"))
    if widget_id is None or not zone:
        return _redirect_to_dashboard(
            "widget-instances",
            status="error",
            error="Widget, zone and position are required.",
        )

    try:
        position = _parse_required_int(request.form.get("position"), field="position")
    except ValueError as exc:
        return _redirect_to_dashboard("widget-instances", status="error", error=str(exc))

    config_json = _clean_text(request.form.get("config_json"))
    enabled = 1 if request.form.get("enabled") else 0

    payload = {
        "widget_id": widget_id,
        "zone": zone,
        "position": position,
        "config_json": config_json,
        "enabled": bool(enabled),
    }

    conn = db()
    try:
        with conn:
            if instance_id is not None:
                cursor = conn.execute(
                    """
                    UPDATE ui_widget_instance
                    SET widget_id = ?, zone = ?, position = ?, config_json = ?, enabled = ?
                    WHERE id = ?
                    """,
                    (widget_id, zone, position, config_json, enabled, instance_id),
                )
                if cursor.rowcount == 0:
                    return _redirect_to_dashboard("widget-instances", status="error", error="Widget instance was not found.")
                _log_audit(conn, "update", "ui_widget_instance", instance_id, payload)
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO ui_widget_instance(widget_id, zone, position, config_json, enabled)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (widget_id, zone, position, config_json, enabled),
                )
                new_id = int(cursor.lastrowid)
                _log_audit(conn, "create", "ui_widget_instance", new_id, payload)
    except sqlite3.Error as exc:  # pragma: no cover - defensive programming
        logger.exception("Failed to save widget instance")
        return _redirect_to_dashboard("widget-instances", status="error", error=_format_error(exc))
    finally:
        conn.close()

    return _redirect_to_dashboard("widget-instances", status="saved")


@blueprint.post("/widget-instances/delete")
def delete_widget_instance() -> Response:
    """Remove a widget instance."""

    instance_id = _parse_int(request.form.get("id"))
    if instance_id is None:
        return _redirect_to_dashboard("widget-instances", status="error", error="Instance id is required for deletion.")

    conn = db()
    try:
        with conn:
            cursor = conn.execute("DELETE FROM ui_widget_instance WHERE id = ?", (instance_id,))
            if cursor.rowcount == 0:
                return _redirect_to_dashboard(
                    "widget-instances",
                    status="error",
                    error="Widget instance was not found.",
                )
            _log_audit(conn, "delete", "ui_widget_instance", instance_id, {"deleted": True})
    except sqlite3.Error as exc:  # pragma: no cover - defensive programming
        logger.exception("Failed to delete widget instance")
        return _redirect_to_dashboard("widget-instances", status="error", error=_format_error(exc))
    finally:
        conn.close()

    return _redirect_to_dashboard("widget-instances", status="deleted")


@blueprint.post("/menu/save")
def save_menu_entry() -> Response:
    """Create or update a menu entry."""

    entry_id = _parse_int(request.form.get("id"))
    slug = _clean_text(request.form.get("slug"))
    title = _clean_text(request.form.get("title"))
    if not slug or not title:
        return _redirect_to_dashboard("menu", status="error", error="Slug and title are required.")

    url_value = _clean_text(request.form.get("url"))
    icon = _clean_text(request.form.get("icon"))
    target = _clean_text(request.form.get("target"))
    try:
        position = _parse_required_int(request.form.get("position"), field="position")
    except ValueError as exc:
        return _redirect_to_dashboard("menu", status="error", error=str(exc))

    parent_id = _parse_int(request.form.get("parent_id"))
    visible = 1 if request.form.get("visible") else 0

    payload = {
        "slug": slug,
        "title": title,
        "url": url_value,
        "icon": icon,
        "target": target,
        "position": position,
        "parent_id": parent_id,
        "visible": bool(visible),
    }

    conn = db()
    try:
        with conn:
            if entry_id is not None:
                cursor = conn.execute(
                    """
                    UPDATE ui_menu
                    SET slug = ?, title = ?, url = ?, icon = ?, parent_id = ?, position = ?, target = ?, visible = ?
                    WHERE id = ?
                    """,
                    (slug, title, url_value, icon, parent_id, position, target, visible, entry_id),
                )
                if cursor.rowcount == 0:
                    return _redirect_to_dashboard("menu", status="error", error="Menu entry was not found.")
                _log_audit(conn, "update", "ui_menu", entry_id, payload)
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO ui_menu(slug, title, url, icon, parent_id, position, target, visible)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (slug, title, url_value, icon, parent_id, position, target, visible),
                )
                new_id = int(cursor.lastrowid)
                _log_audit(conn, "create", "ui_menu", new_id, payload)
    except sqlite3.Error as exc:  # pragma: no cover - defensive programming
        logger.exception("Failed to save menu entry")
        return _redirect_to_dashboard("menu", status="error", error=_format_error(exc))
    finally:
        conn.close()

    return _redirect_to_dashboard("menu", status="saved")


@blueprint.post("/menu/delete")
def delete_menu_entry() -> Response:
    """Remove a menu entry."""

    entry_id = _parse_int(request.form.get("id"))
    if entry_id is None:
        return _redirect_to_dashboard("menu", status="error", error="Entry id is required for deletion.")

    conn = db()
    try:
        with conn:
            cursor = conn.execute("DELETE FROM ui_menu WHERE id = ?", (entry_id,))
            if cursor.rowcount == 0:
                return _redirect_to_dashboard("menu", status="error", error="Menu entry was not found.")
            _log_audit(conn, "delete", "ui_menu", entry_id, {"deleted": True})
    except sqlite3.Error as exc:  # pragma: no cover - defensive programming
        logger.exception("Failed to delete menu entry")
        return _redirect_to_dashboard("menu", status="error", error=_format_error(exc))
    finally:
        conn.close()

    return _redirect_to_dashboard("menu", status="deleted")


@blueprint.post("/queries/save")
def save_query() -> Response:
    """Create or update a stored SQL query."""

    key = _clean_text(request.form.get("key"))
    sql_text = _clean_text(request.form.get("sql"))
    if not key or not sql_text:
        return _redirect_to_dashboard("queries", status="error", error="Key and SQL are required.")

    description = _clean_text(request.form.get("description"))
    original_key = _clean_text(request.form.get("original_key"))
    is_update = bool(original_key)
    if not is_update:
        original_key = key

    payload = {
        "key": key,
        "sql": sql_text,
        "description": description,
    }

    conn = db()
    try:
        with conn:
            if is_update:
                cursor = conn.execute(
                    """
                    UPDATE query_registry
                    SET key = ?, sql = ?, description = ?
                    WHERE key = ?
                    """,
                    (key, sql_text, description, original_key),
                )
                if cursor.rowcount == 0:
                    return _redirect_to_dashboard("queries", status="error", error="Query entry was not found.")
                _log_audit(conn, "update", "query_registry", key, payload)
            else:
                conn.execute(
                    """
                    INSERT INTO query_registry(key, sql, description)
                    VALUES (?, ?, ?)
                    """,
                    (key, sql_text, description),
                )
                _log_audit(conn, "create", "query_registry", key, payload)
    except sqlite3.Error as exc:  # pragma: no cover - defensive programming
        logger.exception("Failed to save query entry")
        return _redirect_to_dashboard("queries", status="error", error=_format_error(exc))
    finally:
        conn.close()

    return _redirect_to_dashboard("queries", status="saved")


@blueprint.post("/queries/delete")
def delete_query() -> Response:
    """Delete a stored SQL query."""

    key = _clean_text(request.form.get("key"))
    if not key:
        return _redirect_to_dashboard("queries", status="error", error="Key is required for deletion.")

    conn = db()
    try:
        with conn:
            cursor = conn.execute("DELETE FROM query_registry WHERE key = ?", (key,))
            if cursor.rowcount == 0:
                return _redirect_to_dashboard("queries", status="error", error="Query entry was not found.")
            _log_audit(conn, "delete", "query_registry", key, {"deleted": True})
    except sqlite3.Error as exc:  # pragma: no cover - defensive programming
        logger.exception("Failed to delete query entry")
        return _redirect_to_dashboard("queries", status="error", error=_format_error(exc))
    finally:
        conn.close()

    return _redirect_to_dashboard("queries", status="deleted")


@blueprint.post("/jobs/save")
def save_job() -> Response:
    """Create or update a scheduled job."""

    job_id = _parse_int(request.form.get("id"))
    name = _clean_text(request.form.get("name"))
    schedule_type = _clean_text(request.form.get("schedule_type"))
    if not name or not schedule_type:
        return _redirect_to_dashboard("jobs", status="error", error="Name and schedule type are required.")

    if schedule_type not in {"daily", "cron"}:
        return _redirect_to_dashboard("jobs", status="error", error="Schedule type must be either 'daily' or 'cron'.")

    schedule_expression = _clean_text(request.form.get("schedule_expression"))
    task_module = _clean_text(request.form.get("task_module"))
    task_name = _clean_text(request.form.get("task_name"))
    if not task_module or not task_name:
        return _redirect_to_dashboard("jobs", status="error", error="Task module and task name are required.")

    next_run_at = _clean_text(request.form.get("next_run_at"))
    last_run_at = _clean_text(request.form.get("last_run_at"))
    config_json = _clean_text(request.form.get("config_json"))
    enabled = 1 if request.form.get("enabled") else 0

    payload = {
        "name": name,
        "schedule_type": schedule_type,
        "schedule_expression": schedule_expression,
        "task_module": task_module,
        "task_name": task_name,
        "next_run_at": next_run_at,
        "last_run_at": last_run_at,
        "config_json": config_json,
        "enabled": bool(enabled),
    }

    conn = db()
    try:
        with conn:
            if job_id is not None:
                cursor = conn.execute(
                    """
                    UPDATE scheduled_job
                    SET name = ?, schedule_type = ?, schedule_expression = ?, next_run_at = ?, last_run_at = ?,
                        task_module = ?, task_name = ?, config_json = ?, enabled = ?
                    WHERE id = ?
                    """,
                    (
                        name,
                        schedule_type,
                        schedule_expression,
                        next_run_at,
                        last_run_at,
                        task_module,
                        task_name,
                        config_json,
                        enabled,
                        job_id,
                    ),
                )
                if cursor.rowcount == 0:
                    return _redirect_to_dashboard("jobs", status="error", error="Scheduled job was not found.")
                _log_audit(conn, "update", "scheduled_job", job_id, payload)
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO scheduled_job(
                        name, schedule_type, schedule_expression, next_run_at, last_run_at,
                        task_module, task_name, config_json, enabled
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        name,
                        schedule_type,
                        schedule_expression,
                        next_run_at,
                        last_run_at,
                        task_module,
                        task_name,
                        config_json,
                        enabled,
                    ),
                )
                new_id = int(cursor.lastrowid)
                _log_audit(conn, "create", "scheduled_job", new_id, payload)
    except sqlite3.Error as exc:  # pragma: no cover - defensive programming
        logger.exception("Failed to save scheduled job")
        return _redirect_to_dashboard("jobs", status="error", error=_format_error(exc))
    finally:
        conn.close()

    return _redirect_to_dashboard("jobs", status="saved")


@blueprint.post("/jobs/delete")
def delete_job() -> Response:
    """Delete a scheduled job."""

    job_id = _parse_int(request.form.get("id"))
    if job_id is None:
        return _redirect_to_dashboard("jobs", status="error", error="Job id is required for deletion.")

    conn = db()
    try:
        with conn:
            cursor = conn.execute("DELETE FROM scheduled_job WHERE id = ?", (job_id,))
            if cursor.rowcount == 0:
                return _redirect_to_dashboard("jobs", status="error", error="Scheduled job was not found.")
            _log_audit(conn, "delete", "scheduled_job", job_id, {"deleted": True})
    except sqlite3.Error as exc:  # pragma: no cover - defensive programming
        logger.exception("Failed to delete scheduled job")
        return _redirect_to_dashboard("jobs", status="error", error=_format_error(exc))
    finally:
        conn.close()

    return _redirect_to_dashboard("jobs", status="deleted")


def _fetch_dashboard_data() -> dict[str, list[sqlite3.Row]]:
    conn = db()
    try:
        widgets = list(
            conn.execute(
                """
                SELECT id, module, name, title, description, entrypoint, config_schema
                FROM ui_widget
                ORDER BY module ASC, name ASC
                """
            ).fetchall()
        )

        widget_instances = list(
            conn.execute(
                """
                SELECT
                    wi.id,
                    wi.widget_id,
                    wi.zone,
                    wi.position,
                    wi.config_json,
                    wi.enabled,
                    w.module AS widget_module,
                    w.name AS widget_name,
                    w.title AS widget_title
                FROM ui_widget_instance AS wi
                LEFT JOIN ui_widget AS w ON w.id = wi.widget_id
                ORDER BY wi.zone ASC, wi.position ASC, wi.id ASC
                """
            ).fetchall()
        )

        menu_entries = list(
            conn.execute(
                """
                SELECT id, slug, title, url, icon, parent_id, position, target, visible
                FROM ui_menu
                ORDER BY COALESCE(parent_id, 0) ASC, position ASC, id ASC
                """
            ).fetchall()
        )

        queries = list(
            conn.execute(
                """
                SELECT key, sql, description, updated_at
                FROM query_registry
                ORDER BY key ASC
                """
            ).fetchall()
        )

        jobs = list(
            conn.execute(
                """
                SELECT id, name, schedule_type, schedule_expression, next_run_at, last_run_at,
                       task_module, task_name, config_json, enabled
                FROM scheduled_job
                ORDER BY name ASC
                """
            ).fetchall()
        )
    finally:
        conn.close()

    return {
        "widgets": widgets,
        "widget_instances": widget_instances,
        "menu_entries": menu_entries,
        "queries": queries,
        "jobs": jobs,
    }


def _redirect_to_dashboard(anchor: str | None, *, status: str, error: str | None = None) -> Response:
    params: dict[str, str] = {"status": status}
    if error:
        params["error"] = _truncate(error)
    location = url_for("superadmin.dashboard", **params)
    if anchor:
        location = f"{location}#{anchor}"
    return redirect(location)


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _parse_required_int(value: str | None, *, field: str) -> int:
    if value is None:
        raise ValueError(f"{field.capitalize()} must be an integer.")
    value = value.strip()
    if not value:
        raise ValueError(f"{field.capitalize()} must be an integer.")
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{field.capitalize()} must be an integer.") from exc


def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _log_audit(conn: sqlite3.Connection, action: str, entity: str, entity_id: int | str | None, payload: dict[str, Any]) -> None:
    record: dict[str, Any] = dict(payload)
    if has_request_context():
        record.setdefault("remote_addr", request.remote_addr)
        actor_username = request.headers.get("X-Actor")
    else:  # pragma: no cover - defensive
        actor_username = None
    conn.execute(
        """
        INSERT INTO audit_log(actor_id, actor_username, action, entity, entity_id, payload_json)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            None,
            actor_username,
            action,
            entity,
            str(entity_id) if entity_id is not None else None,
            json.dumps(record, ensure_ascii=False),
        ),
    )


def _format_error(exc: Exception) -> str:
    message = str(exc).strip()
    if not message:
        message = exc.__class__.__name__
    return _truncate(message)


def _truncate(message: str, limit: int = 200) -> str:
    if len(message) <= limit:
        return message
    return f"{message[: limit - 1]}…"


__all__ = ["blueprint"]
