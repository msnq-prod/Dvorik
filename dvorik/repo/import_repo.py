"""SQLite-backed implementation of :class:`~dvorik.domain.ports.ImportLogRepo`."""

from __future__ import annotations

import sqlite3
from dataclasses import asdict
from typing import Any, Dict, Sequence

from dvorik.db.query_registry import get_query
from dvorik.domain.models import ImportLogEntry
from dvorik.domain.ports import ImportLogRepo


class SQLiteImportLogRepo(ImportLogRepo):
    """Repository managing import log records."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def get(self, import_id: int) -> ImportLogEntry | None:
        sql = get_query(
            self._conn,
            "repo.import.get",
            """
            SELECT
                id,
                original_name,
                stored_path,
                import_type,
                source_hash,
                normalized_csv,
                normalized_hash,
                supplier,
                invoice,
                items_count,
                items_json,
                reverted_at,
                created_at
            FROM import_log
            WHERE id = :import_id
            """,
        )
        cursor = self._conn.execute(sql, {"import_id": import_id})
        row = cursor.fetchone()
        if row is None:
            return None
        return _row_to_import_log(row)

    def latest(self, limit: int = 20) -> Sequence[ImportLogEntry]:
        sql = get_query(
            self._conn,
            "repo.import.latest",
            """
            SELECT
                id,
                original_name,
                stored_path,
                import_type,
                source_hash,
                normalized_csv,
                normalized_hash,
                supplier,
                invoice,
                items_count,
                items_json,
                reverted_at,
                created_at
            FROM import_log
            ORDER BY created_at DESC, id DESC
            LIMIT :limit
            """,
        )
        cursor = self._conn.execute(sql, {"limit": max(1, int(limit))})
        return [_row_to_import_log(row) for row in cursor.fetchall()]

    def add(self, entry: ImportLogEntry) -> ImportLogEntry:
        sql = get_query(
            self._conn,
            "repo.import.insert",
            """
            INSERT INTO import_log(
                original_name,
                stored_path,
                import_type,
                source_hash,
                normalized_csv,
                normalized_hash,
                supplier,
                invoice,
                items_count,
                items_json
            )
            VALUES (
                :original_name,
                :stored_path,
                :import_type,
                :source_hash,
                :normalized_csv,
                :normalized_hash,
                :supplier,
                :invoice,
                :items_count,
                :items_json
            )
            RETURNING *
            """,
        )
        params = _import_entry_to_params(entry)
        with self._conn:
            cursor = self._conn.execute(sql, params)
            row = cursor.fetchone()
        if row is None:  # pragma: no cover - SQLite should always return row
            raise RuntimeError("Failed to insert import log entry")
        return _row_to_import_log(row)

    def mark_reverted(self, import_id: int) -> None:
        sql = get_query(
            self._conn,
            "repo.import.mark_reverted",
            """
            UPDATE import_log
            SET reverted_at = COALESCE(reverted_at, datetime('now','localtime'))
            WHERE id = :import_id
            """,
        )
        with self._conn:
            self._conn.execute(sql, {"import_id": import_id})


def _row_to_import_log(row: sqlite3.Row) -> ImportLogEntry:
    return ImportLogEntry(
        id=row["id"],
        original_name=row["original_name"],
        stored_path=row["stored_path"],
        import_type=row["import_type"],
        source_hash=row["source_hash"],
        normalized_csv=row["normalized_csv"],
        normalized_hash=row["normalized_hash"],
        supplier=row["supplier"],
        invoice=row["invoice"],
        items_count=int(row["items_count"] or 0),
        items_json=row["items_json"],
        reverted_at=row["reverted_at"],
        created_at=row["created_at"],
    )


def _import_entry_to_params(entry: ImportLogEntry) -> Dict[str, Any]:
    data = asdict(entry)
    return {
        "original_name": data["original_name"],
        "stored_path": data["stored_path"],
        "import_type": data["import_type"],
        "source_hash": data["source_hash"],
        "normalized_csv": data.get("normalized_csv"),
        "normalized_hash": data.get("normalized_hash"),
        "supplier": data.get("supplier"),
        "invoice": data.get("invoice"),
        "items_count": data.get("items_count", 0),
        "items_json": data.get("items_json"),
    }


__all__ = ["SQLiteImportLogRepo"]
