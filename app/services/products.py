from __future__ import annotations

import sqlite3

from app.db import get_default_supplier_id


def has_incomplete(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM product
        WHERE archived=0 AND (local_name IS NULL OR (photo_file_id IS NULL AND COALESCE(photo_path,'')=''))
        LIMIT 1
        """
    ).fetchone()
    return bool(row)


def get_display_article(conn: sqlite3.Connection, product_id: int) -> str | None:
    supplier_id = get_default_supplier_id(conn)
    row = conn.execute(
        """
        SELECT COALESCE(
            (SELECT code FROM supplier_sku WHERE product_id=? AND supplier_id=? AND active=1 ORDER BY id LIMIT 1),
            (SELECT code FROM supplier_sku WHERE product_id=? AND active=1 ORDER BY id LIMIT 1),
            article
        ) AS code
        FROM product
        WHERE id=?
        """,
        (product_id, supplier_id, product_id, product_id),
    ).fetchone()
    return row["code"] if row else None
