from __future__ import annotations

import logging
import sqlite3
from typing import Iterable

from .conn import db

logger = logging.getLogger(__name__)

# Placeholder collection for schema creation statements. Ticket 2.3 will
# populate the list with concrete DDL definitions.
_SCHEMA_SCRIPTS: Iterable[str] = ()


def _run_script(conn: sqlite3.Connection, script: str) -> None:
    """Execute a migration script handling common SQLite errors."""

    if not script.strip():
        return

    try:
        conn.executescript(script)
    except sqlite3.OperationalError as exc:  # pragma: no cover - defensive
        logger.warning("Skipping migration script due to sqlite error: %s", exc)


def init_db(connection: sqlite3.Connection | None = None) -> None:
    """Initialise the SQLite schema ensuring idempotency."""

    owns_connection = connection is None
    conn = connection if connection is not None else db()

    try:
        with conn:
            for script in _SCHEMA_SCRIPTS:
                _run_script(conn, script)
    finally:
        if owns_connection:
            conn.close()


__all__ = ["init_db"]
