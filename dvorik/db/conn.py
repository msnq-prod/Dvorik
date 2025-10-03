"""SQLite connection helpers for the rebuilt Dvorik project."""

from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import Iterable

from dvorik.core.config import get_config

_DETECT_TYPES = sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
_PRAGMAS: Iterable[str] = (
    "PRAGMA journal_mode=WAL",
    "PRAGMA foreign_keys=ON",
    "PRAGMA busy_timeout=5000",
)


def _ensure_parent_exists(path: Path) -> None:
    """Ensure the parent directory for ``path`` exists."""

    path.parent.mkdir(parents=True, exist_ok=True)


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    """Apply required PRAGMA statements to ``conn``."""

    for statement in _PRAGMAS:
        conn.execute(statement)


def db(path: str | Path | None = None) -> sqlite3.Connection:
    """Return a configured SQLite connection.

    Parameters
    ----------
    path:
        Optional explicit database path. When omitted, the path from the
        loaded configuration is used.
    """

    config = get_config()
    db_path = Path(path) if path is not None else config.db_path
    _ensure_parent_exists(db_path)

    conn = sqlite3.connect(
        db_path,
        detect_types=_DETECT_TYPES,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn)
    return conn


__all__ = ["db"]
