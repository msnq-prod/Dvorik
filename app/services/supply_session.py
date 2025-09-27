from __future__ import annotations

import datetime as dt
import json
from typing import Any, Dict, Iterable, Optional

import sqlite3


def _now_utc() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)


def create(conn: sqlite3.Connection, token: str, data: Dict[str, Any]) -> None:
    payload = {
        "token": token,
        "created_at": data.get("created_at") or _now_utc().isoformat(),
        "stored_path": data.get("stored_path"),
        "source_hash": data.get("source_hash"),
        "import_type": data.get("import_type"),
        "preview_normalized_path": data.get("preview_normalized_path"),
        "base_name": data.get("base_name"),
        "initial_rows_json": json.dumps(data.get("initial_rows") or []),
        "sheet_pointer_json": json.dumps(data.get("sheet_pointer")),
        "needs_mapping": 1 if data.get("needs_mapping") else 0,
        "supplier": data.get("supplier"),
        "invoice": data.get("invoice"),
        "committed": 1 if data.get("committed") else 0,
    }
    with conn:
        conn.execute(
            """
            INSERT INTO import_session(
                token, created_at, stored_path, source_hash, import_type,
                preview_normalized_path, base_name, initial_rows_json,
                sheet_pointer_json, needs_mapping, supplier, invoice, committed
            ) VALUES (:token, :created_at, :stored_path, :source_hash, :import_type,
                      :preview_normalized_path, :base_name, :initial_rows_json,
                      :sheet_pointer_json, :needs_mapping, :supplier, :invoice, :committed)
            """,
            payload,
        )


def get(conn: sqlite3.Connection, token: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        "SELECT * FROM import_session WHERE token=?",
        (token,),
    ).fetchone()
    if not row:
        return None
    return _row_to_dict(row)


def update(conn: sqlite3.Connection, token: str, **fields: Any) -> None:
    if not fields:
        return
    data = dict(fields)
    if "initial_rows" in data:
        data["initial_rows_json"] = json.dumps(data.pop("initial_rows") or [])
    if "sheet_pointer" in data:
        data["sheet_pointer_json"] = json.dumps(data.pop("sheet_pointer"))
    if "needs_mapping" in data:
        data["needs_mapping"] = 1 if data["needs_mapping"] else 0
    if "committed" in data:
        data["committed"] = 1 if data["committed"] else 0
    assignments = ", ".join(f"{key}=?" for key in data.keys())
    params = list(data.values()) + [token]
    with conn:
        conn.execute(
            f"UPDATE import_session SET {assignments} WHERE token=?",
            params,
        )


def delete(conn: sqlite3.Connection, token: str) -> None:
    with conn:
        conn.execute("DELETE FROM import_session WHERE token=?", (token,))


def purge_expired(
    conn: sqlite3.Connection, ttl_seconds: int
) -> Iterable[Dict[str, Any]]:
    cutoff = (_now_utc() - dt.timedelta(seconds=ttl_seconds)).isoformat()
    rows = conn.execute(
        "SELECT * FROM import_session WHERE committed=0 AND created_at<?",
        (cutoff,),
    ).fetchall()
    tokens = [row["token"] for row in rows]
    if tokens:
        with conn:
            conn.execute(
                "DELETE FROM import_session WHERE token IN ({})".format(
                    ",".join(["?"] * len(tokens))
                ),
                tokens,
            )
    for row in rows:
        yield _row_to_dict(row)


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    data = dict(row)
    data["initial_rows"] = json.loads(data.get("initial_rows_json") or "[]")
    data.pop("initial_rows_json", None)
    pointer = data.get("sheet_pointer_json")
    data["sheet_pointer"] = json.loads(pointer) if pointer else None
    data.pop("sheet_pointer_json", None)
    data["needs_mapping"] = bool(data.get("needs_mapping"))
    data["committed"] = bool(data.get("committed"))
    return data


__all__ = [
    "create",
    "get",
    "update",
    "delete",
    "purge_expired",
]

