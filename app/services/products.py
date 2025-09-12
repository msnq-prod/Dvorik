from __future__ import annotations

import sqlite3


def has_incomplete(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT 1 FROM product WHERE local_name IS NULL OR (photo_file_id IS NULL AND COALESCE(photo_path,'')='') LIMIT 1"
    ).fetchone()
    return bool(row)
