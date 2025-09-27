from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Tuple

from app.db import get_default_supplier_id


def _article_expr(conn: sqlite3.Connection, use_supplier_codes: bool) -> Tuple[str, List[Any]]:
    if not use_supplier_codes:
        return "p.article", []
    supplier_id = get_default_supplier_id(conn)
    expr = (
        "COALESCE("
        "(SELECT code FROM supplier_sku WHERE product_id=p.id AND supplier_id=? AND active=1 ORDER BY id LIMIT 1),"
        "(SELECT code FROM supplier_sku WHERE product_id=p.id AND active=1 ORDER BY id LIMIT 1),"
        "p.article)"
    )
    return expr, [supplier_id]


def low_stock(conn: sqlite3.Connection, *, use_supplier_codes: bool = True, limit: int = 1000) -> List[Dict[str, Any]]:
    article_expr, params = _article_expr(conn, use_supplier_codes)
    sql = f"""
        SELECT {article_expr} AS article,
               COALESCE(p.local_name, p.name) AS name,
               IFNULL(SUM(s.qty_pack), 0) AS total
        FROM product p
        LEFT JOIN stock s ON s.product_id = p.id
        WHERE p.archived = 0
        GROUP BY p.id
        HAVING total > 0 AND total < 2
        ORDER BY total ASC, p.id DESC
        LIMIT ?
    """
    rows = conn.execute(sql, (*params, limit)).fetchall()
    return [
        {"article": row["article"], "name": row["name"], "total": float(row["total"])}
        for row in rows
    ]


def zero_stock(conn: sqlite3.Connection, *, use_supplier_codes: bool = True, limit: int = 5000) -> List[Dict[str, Any]]:
    article_expr, params = _article_expr(conn, use_supplier_codes)
    sql = f"""
        SELECT {article_expr} AS article,
               COALESCE(p.local_name, p.name) AS name
        FROM product p
        LEFT JOIN stock s ON s.product_id = p.id
        WHERE p.archived = 0
        GROUP BY p.id
        HAVING IFNULL(SUM(s.qty_pack), 0) = 0
        ORDER BY p.id DESC
        LIMIT ?
    """
    rows = conn.execute(sql, (*params, limit)).fetchall()
    return [
        {"article": row["article"], "name": row["name"]}
        for row in rows
    ]


def mid_stock(conn: sqlite3.Connection, *, use_supplier_codes: bool = True, limit: int = 1000) -> List[Dict[str, Any]]:
    article_expr, params = _article_expr(conn, use_supplier_codes)
    sql = f"""
        SELECT {article_expr} AS article,
               COALESCE(p.local_name, p.name) AS name,
               IFNULL(SUM(s.qty_pack), 0) AS total
        FROM product p
        LEFT JOIN stock s ON s.product_id = p.id
        WHERE p.archived = 0
        GROUP BY p.id
        HAVING total >= 3 AND total <= 5
        ORDER BY total DESC, name ASC
        LIMIT ?
    """
    rows = conn.execute(sql, (*params, limit)).fetchall()
    return [
        {"article": row["article"], "name": row["name"], "total": float(row["total"])}
        for row in rows
    ]


def all_stock(conn: sqlite3.Connection, *, use_supplier_codes: bool = True, limit: int = 2000) -> List[Dict[str, Any]]:
    article_expr, params = _article_expr(conn, use_supplier_codes)
    sql = f"""
        SELECT {article_expr} AS article,
               COALESCE(p.local_name, p.name) AS name,
               IFNULL(SUM(s.qty_pack), 0) AS total
        FROM product p
        LEFT JOIN stock s ON s.product_id = p.id
        WHERE p.archived = 0
        GROUP BY p.id
        ORDER BY name ASC
        LIMIT ?
    """
    rows = conn.execute(sql, (*params, limit)).fetchall()
    return [
        {"article": row["article"], "name": row["name"], "total": float(row["total"])}
        for row in rows
    ]


def archived_stock(conn: sqlite3.Connection, *, use_supplier_codes: bool = True, limit: int = 2000) -> List[Dict[str, Any]]:
    article_expr, params = _article_expr(conn, use_supplier_codes)
    sql = f"""
        SELECT {article_expr} AS article,
               COALESCE(p.local_name, p.name) AS name,
               p.archived_at,
               p.last_restock_at
        FROM product p
        WHERE p.archived = 1
        ORDER BY (p.archived_at IS NULL) ASC, p.archived_at DESC, name ASC
        LIMIT ?
    """
    rows = conn.execute(sql, (*params, limit)).fetchall()
    return [
        {
            "article": row["article"],
            "name": row["name"],
            "archived_at": row["archived_at"],
            "last_restock_at": row["last_restock_at"],
        }
        for row in rows
    ]


__all__ = [
    "low_stock",
    "zero_stock",
    "mid_stock",
    "all_stock",
    "archived_stock",
]
