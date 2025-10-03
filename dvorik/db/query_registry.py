from __future__ import annotations

import sqlite3

__all__ = ["get_query", "set_query"]


def get_query(
    conn: sqlite3.Connection,
    key: str,
    default_sql: str,
) -> str:
    """Return the SQL text registered for ``key``.

    Parameters
    ----------
    conn:
        Active SQLite connection.
    key:
        Registry key used to look up the query.
    default_sql:
        SQL text returned when the key is not registered.
    """

    cursor = conn.execute(
        "SELECT sql FROM query_registry WHERE key = ?",
        (key,),
    )
    row = cursor.fetchone()
    if row is None:
        return default_sql

    value = row["sql"] if isinstance(row, sqlite3.Row) else row[0]
    return value if value is not None else default_sql


def set_query(
    conn: sqlite3.Connection,
    key: str,
    sql: str,
    description: str | None = None,
) -> None:
    """Insert or update a query registry entry."""

    with conn:
        conn.execute(
            """
            INSERT INTO query_registry(key, sql, description)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                sql = excluded.sql,
                description = COALESCE(excluded.description, query_registry.description),
                updated_at = datetime('now', 'localtime')
            """,
            (key, sql, description),
        )
