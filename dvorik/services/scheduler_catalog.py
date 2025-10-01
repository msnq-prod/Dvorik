from __future__ import annotations

import datetime as dt
import importlib
import json
import logging
import sqlite3
from collections.abc import Callable, Iterable, Mapping
from typing import Dict

from dvorik.core.registry import JobRegistry
from dvorik.core.scheduler import (
    ScheduledJob,
    register_cron,
    register_daily,
    unregister,
)
from dvorik.db.conn import db

logger = logging.getLogger(__name__)

_REGISTRY_PREFIX = "db.scheduled_job."


def _format_datetime(value: dt.datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc).isoformat(timespec="seconds")


def _parse_datetime(raw: str | None) -> dt.datetime | None:
    if not raw:
        return None
    try:
        parsed = dt.datetime.fromisoformat(raw)
    except ValueError:
        logger.warning("Invalid datetime stored for scheduled job", extra={"value": raw})
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _load_callback(module_name: str, attribute: str) -> Callable[..., object]:
    module = importlib.import_module(module_name)
    try:
        callback = getattr(module, attribute)
    except AttributeError as exc:
        raise AttributeError(f"Scheduled task '{module_name}.{attribute}' not found") from exc
    if not callable(callback):
        raise TypeError(f"Scheduled task '{module_name}.{attribute}' is not callable")
    return callback


def _make_state_updater(job_id: int) -> Callable[[dt.datetime | None, dt.datetime | None], None]:
    def _updater(last_run: dt.datetime | None, next_run: dt.datetime | None) -> None:
        conn = db()
        try:
            with conn:
                conn.execute(
                    """
                    UPDATE scheduled_job
                    SET last_run_at = ?, next_run_at = ?
                    WHERE id = ?
                    """,
                    (_format_datetime(last_run), _format_datetime(next_run), job_id),
                )
        except sqlite3.Error:
            logger.exception("Failed to persist scheduler state", extra={"job_id": job_id})
        finally:
            conn.close()

    return _updater


def _decode_config(config_json: str | None) -> Mapping[str, object] | None:
    if not config_json:
        return None
    try:
        data = json.loads(config_json)
    except json.JSONDecodeError:
        logger.warning("Failed to decode job configuration JSON", extra={"config": config_json})
        return None
    if not isinstance(data, Mapping):
        return None
    return data


def _registry_keys(prefix: str) -> Iterable[str]:
    return tuple(key for key in JobRegistry.keys() if key.startswith(prefix))


def _cleanup_previous_entries(prefix: str) -> None:
    for key in _registry_keys(prefix):
        try:
            job = JobRegistry.get(key)
        except KeyError:
            JobRegistry.unregister(key)
            continue
        unregister(job.name)
        JobRegistry.unregister(key)


def sync_jobs(conn: sqlite3.Connection | None = None) -> Dict[str, ScheduledJob]:
    """Synchronise DB-backed scheduled jobs with the in-memory scheduler."""

    _cleanup_previous_entries(_REGISTRY_PREFIX)

    owns_connection = conn is None
    connection = conn if conn is not None else db()
    try:
        try:
            rows = connection.execute(
                """
                SELECT id, name, schedule_type, schedule_expression,
                       next_run_at, last_run_at, task_module, task_name,
                       config_json, enabled
                FROM scheduled_job
                ORDER BY name ASC
                """
            ).fetchall()
        except sqlite3.Error:
            logger.exception("Failed to load scheduled jobs from database")
            return {}

        registered: Dict[str, ScheduledJob] = {}
        for row in rows:
            name = str(row["name"])
            schedule_type = str(row["schedule_type"])
            schedule_expression = row["schedule_expression"]
            task_module = str(row["task_module"])
            task_name = str(row["task_name"])
            enabled = bool(row["enabled"])
            next_run = _parse_datetime(row["next_run_at"])

            try:
                callback = _load_callback(task_module, task_name)
            except (ImportError, AttributeError, TypeError):
                logger.exception(
                    "Failed to resolve scheduled job callback",
                    extra={"module": task_module, "name": task_name},
                )
                continue

            state_updater = _make_state_updater(int(row["id"]))
            metadata: Dict[str, object] = {
                "id": int(row["id"]),
                "config": _decode_config(row["config_json"]),
                "config_json": row["config_json"],
                "last_run_at": row["last_run_at"],
            }

            try:
                if schedule_type == "daily":
                    if not schedule_expression:
                        raise ValueError("Daily schedule requires time expression")
                    job = register_daily(
                        name,
                        callback,
                        schedule_expression,
                        enabled=enabled,
                        metadata=metadata,
                        state_updater=state_updater,
                        next_run_at=next_run,
                    )
                elif schedule_type == "cron":
                    if not schedule_expression:
                        raise ValueError("Cron schedule requires expression")
                    job = register_cron(
                        name,
                        callback,
                        schedule_expression,
                        enabled=enabled,
                        metadata=metadata,
                        state_updater=state_updater,
                        next_run_at=next_run,
                    )
                else:
                    raise ValueError(f"Unsupported schedule type: {schedule_type}")
            except Exception:
                logger.exception(
                    "Failed to register scheduled job",
                    extra={"name": name, "type": schedule_type, "expression": schedule_expression},
                )
                continue

            registry_key = f"{_REGISTRY_PREFIX}{name}"
            JobRegistry.register(registry_key, job, replace=True)
            registered[name] = job

        if registered:
            logger.info("Synchronized %d scheduled job(s) from catalog", len(registered))
        else:
            logger.info("No scheduled jobs to synchronize")
        return registered
    finally:
        if owns_connection:
            connection.close()


__all__ = ["sync_jobs"]

