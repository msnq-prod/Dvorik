"""Logging utilities providing JSON output and contextual metadata support."""

from __future__ import annotations

import contextlib
import contextvars
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Iterator, Mapping

_LOG_CONTEXT: contextvars.ContextVar[Mapping[str, Any]] = contextvars.ContextVar(
    "dvorik_log_context",
    default={},
)

_BOOTSTRAPPED = False

_RESERVED_LOG_RECORD_KEYS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "message",
}


class JsonLogFormatter(logging.Formatter):
    """Render log records as JSON payloads with contextual metadata."""

    def format(self, record: logging.LogRecord) -> str:  # noqa: D401 - short summary
        record.message = record.getMessage()

        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc)
            .isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.message,
        }

        context = _LOG_CONTEXT.get({})
        if context:
            payload.update(context)

        extras = {
            key: value
            for key, value in record.__dict__.items()
            if key not in _RESERVED_LOG_RECORD_KEYS and not key.startswith("_")
        }
        if extras:
            payload.update(extras)

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack_info"] = record.stack_info

        return json.dumps(payload, ensure_ascii=False, default=_json_fallback)


def bootstrap_logging(*, level: str | None = None) -> None:
    """Configure root logging with the JSON formatter (idempotent)."""

    global _BOOTSTRAPPED
    if _BOOTSTRAPPED:
        return

    configured_level = (level or os.getenv("DVORIK_LOG_LEVEL") or "INFO").upper()
    try:
        numeric_level = int(configured_level)
    except ValueError:
        numeric_level = getattr(logging, configured_level, logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonLogFormatter())

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(numeric_level)

    logging.captureWarnings(True)

    _BOOTSTRAPPED = True


def bind_context(**values: Any) -> contextvars.Token[Mapping[str, Any]] | None:
    """Attach ``values`` to the current logging context."""

    filtered = {key: value for key, value in values.items() if value is not None}
    if not filtered:
        return None

    current = dict(_LOG_CONTEXT.get({}))
    current.update(filtered)
    return _LOG_CONTEXT.set(current)


def reset_context(token: contextvars.Token[Mapping[str, Any]] | None) -> None:
    """Restore the logging context referenced by ``token`` if present."""

    if token is None:
        return
    _LOG_CONTEXT.reset(token)


def get_context() -> Mapping[str, Any]:
    """Return a snapshot of the current logging context."""

    return dict(_LOG_CONTEXT.get({}))


@contextlib.contextmanager
def scoped_context(**values: Any) -> Iterator[Mapping[str, Any]]:
    """Temporarily bind values to the logging context within the scope."""

    token = bind_context(**values)
    try:
        yield get_context()
    finally:
        reset_context(token)


def new_request_id() -> str:
    """Generate a unique identifier for HTTP requests."""

    return uuid.uuid4().hex


def new_job_run_id(job_name: str | None = None) -> str:
    """Generate a unique identifier for job executions."""

    suffix = uuid.uuid4().hex
    if not job_name:
        return suffix
    return f"{job_name}-{suffix}"


def _json_fallback(value: Any) -> Any:
    """Best-effort JSON serializer returning ``repr`` for unsupported types."""

    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return repr(value)


__all__ = [
    "JsonLogFormatter",
    "bind_context",
    "bootstrap_logging",
    "get_context",
    "new_job_run_id",
    "new_request_id",
    "reset_context",
    "scoped_context",
]

