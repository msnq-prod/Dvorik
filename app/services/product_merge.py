from __future__ import annotations

import json
import re
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import sqlite3

FIELD_KEYS = ("article", "name", "brand_country", "local_name")
MERGE_MODES = {"a", "b", "merge"}
PHOTO_MODES = {"a", "b", "merge"}


def normalize_name(value: Optional[str]) -> str:
    """Normalize product names for alias matching."""
    if value is None:
        return ""
    normalized = re.sub(r"\s+", " ", str(value)).strip().lower()
    return normalized


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _merge_values(values: Iterable[Tuple[str, Any]]) -> Tuple[str, str]:
    parts: List[str] = []
    seen: set[str] = set()
    last_mode = "merge"
    for mode, value in values:
        if not _has_value(value):
            continue
        text = _coerce_text(value).strip()
        norm = normalize_name(text)
        if norm in seen:
            if not parts:
                last_mode = mode
            continue
        seen.add(norm)
        parts.append(text)
        last_mode = mode
    if not parts:
        return "", "merge"
    if len(parts) == 1:
        return parts[0], last_mode
    return " / ".join(parts), "merge"


def _resolve_field(field: str, mode: str, a_value: Any, b_value: Any) -> Tuple[str, str]:
    if mode not in MERGE_MODES:
        mode = "a"
    if mode == "merge":
        merged, applied = _merge_values((("a", a_value), ("b", b_value)))
        return merged, applied
    if mode == "a":
        if _has_value(a_value):
            return _coerce_text(a_value), "a"
        if _has_value(b_value):
            return _coerce_text(b_value), "b"
        return "", "a"
    # mode == "b"
    if _has_value(b_value):
        return _coerce_text(b_value), "b"
    if _has_value(a_value):
        return _coerce_text(a_value), "a"
    return "", "b"


def _resolve_photo(mode: str, base: Dict[str, Any], other: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], str]:
    if mode not in PHOTO_MODES:
        mode = "a"
    a_file = base.get("photo_file_id")
    a_path = base.get("photo_path")
    b_file = other.get("photo_file_id")
    b_path = other.get("photo_path")

    def pick(source: str) -> Tuple[Optional[str], Optional[str]]:
        if source == "b":
            return b_file, b_path
        return a_file, a_path

    if mode == "merge":
        file_id, path = pick("a")
        applied = "a"
        if not (_has_value(file_id) or _has_value(path)):
            file_id, path = pick("b")
            if _has_value(file_id) or _has_value(path):
                applied = "b"
        return file_id if _has_value(file_id) else None, path if _has_value(path) else None, applied

    file_id, path = pick(mode)
    applied = mode
    if not (_has_value(file_id) or _has_value(path)):
        alt = "b" if mode == "a" else "a"
        file_id, path = pick(alt)
        if _has_value(file_id) or _has_value(path):
            applied = alt
    return file_id if _has_value(file_id) else None, path if _has_value(path) else None, applied


def _load_product(conn: sqlite3.Connection, pid: int) -> Dict[str, Any]:
    row = conn.execute("SELECT * FROM product WHERE id=?", (pid,)).fetchone()
    if not row:
        raise ValueError(f"Product #{pid} not found")
    return _row_to_dict(row)


def _load_stocks(conn: sqlite3.Connection, pid: int) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT location_code, qty_pack, name, local_name FROM stock WHERE product_id=?",
        (pid,),
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for row in rows:
        qty = row["qty_pack"] if row["qty_pack"] is not None else 0.0
        out.append(
            {
                "location_code": row["location_code"],
                "qty": float(qty),
                "name": row["name"],
                "local_name": row["local_name"],
            }
        )
    return out


def _placeholder_article(base_id: int, other_id: int) -> str:
    stamp = int(time.time())
    return f"MERGED-{base_id}-{other_id}-{stamp}"


def _compute_stocks(mode: str, base: List[Dict[str, Any]], other: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if mode not in {"a", "b", "merge"}:
        mode = "merge"
    if mode == "a":
        return [dict(item) for item in base]
    if mode == "b":
        return [dict(item) for item in other]
    combined: Dict[str, Dict[str, Any]] = {}
    for source in (base, other):
        for row in source:
            code = row["location_code"]
            qty = float(row.get("qty") or 0.0)
            if code not in combined:
                combined[code] = {
                    "location_code": code,
                    "qty": qty,
                    "name": row.get("name"),
                    "local_name": row.get("local_name"),
                }
            else:
                combined[code]["qty"] += qty
    return [combined[key] for key in sorted(combined.keys())]


def _insert_stocks(
    conn: sqlite3.Connection,
    product_id: int,
    stocks: List[Dict[str, Any]],
    name: Optional[str],
    local_name: Optional[str],
) -> None:
    for row in stocks:
        qty = float(row.get("qty") or 0.0)
        if abs(qty) < 1e-9:
            continue
        code = row.get("location_code")
        conn.execute(
            """
            INSERT INTO stock(product_id, location_code, qty_pack, name, local_name)
            VALUES (?,?,?,?,?)
            """,
            (
                product_id,
                code,
                qty,
                name,
                local_name,
            ),
        )


def _ensure_article_alias(
    conn: sqlite3.Connection,
    product_id: int,
    alias_article: Optional[str],
    source_product_id: Optional[int],
    merge_log_id: Optional[int],
) -> Optional[int]:
    alias = (alias_article or "").strip()
    if not alias:
        return None
    existing = conn.execute(
        "SELECT id, merge_log_id FROM product_article_alias WHERE alias_article=?",
        (alias,),
    ).fetchone()
    if existing:
        return None
    cur = conn.execute(
        """
        INSERT INTO product_article_alias(product_id, alias_article, source_product_id, merge_log_id)
        VALUES (?,?,?,?)
        """,
        (product_id, alias, source_product_id, merge_log_id),
    )
    return int(cur.lastrowid)


def _ensure_name_alias(
    conn: sqlite3.Connection,
    product_id: int,
    alias_name: Optional[str],
    source_product_id: Optional[int],
    merge_log_id: Optional[int],
) -> Optional[int]:
    if not alias_name:
        return None
    normalized = normalize_name(alias_name)
    if not normalized:
        return None
    existing = conn.execute(
        "SELECT id FROM product_name_alias WHERE normalized_name=?",
        (normalized,),
    ).fetchone()
    if existing:
        return None
    cur = conn.execute(
        """
        INSERT INTO product_name_alias(product_id, alias_name, normalized_name, source_product_id, merge_log_id)
        VALUES (?,?,?,?,?)
        """,
        (product_id, alias_name.strip(), normalized, source_product_id, merge_log_id),
    )
    return int(cur.lastrowid)


def _build_summary(base: Dict[str, Any], other: Dict[str, Any], result_article: str) -> str:
    left = base.get("article") or f"#{base.get('id')}"
    right = other.get("article") or f"#{other.get('id')}"
    return f"{left} + {right} → {result_article}"


def apply_merge(
    conn: sqlite3.Connection,
    source_a_id: int,
    source_b_id: int,
    *,
    field_modes: Optional[Dict[str, str]] = None,
    stock_mode: str = "merge",
) -> Dict[str, Any]:
    if source_a_id == source_b_id:
        raise ValueError("Нельзя объединять карточку саму с собой")
    field_modes = dict(field_modes or {})
    if stock_mode not in {"a", "b", "merge"}:
        stock_mode = "merge"

    with conn:
        base_before = _load_product(conn, source_a_id)
        other_before = _load_product(conn, source_b_id)
        if base_before.get("archived"):
            base_before["archived"] = int(base_before.get("archived") or 0)
        if other_before.get("archived"):
            other_before["archived"] = int(other_before.get("archived") or 0)
        base_stocks_before = _load_stocks(conn, source_a_id)
        other_stocks_before = _load_stocks(conn, source_b_id)

        applied_modes: Dict[str, str] = {}
        resolved: Dict[str, Any] = {}
        for key in FIELD_KEYS:
            default_mode = "merge" if key in {"name", "local_name"} else "a"
            mode = field_modes.get(key, default_mode)
            value, applied = _resolve_field(key, mode, base_before.get(key), other_before.get(key))
            resolved[key] = value
            applied_modes[key] = applied
        photo_mode = field_modes.get("photo", "a")
        file_id, path, applied_photo = _resolve_photo(photo_mode, base_before, other_before)
        resolved["photo_file_id"] = file_id
        resolved["photo_path"] = path
        applied_modes["photo"] = applied_photo

        final_article = resolved.get("article", "").strip()
        if not final_article:
            raise ValueError("У объединённой карточки должен быть артикул")

        placeholder = _placeholder_article(source_a_id, source_b_id)
        conn.execute(
            """
            UPDATE product
            SET article=?, archived=1, archived_at=datetime('now','localtime')
            WHERE id=?
            """,
            (placeholder, source_b_id),
        )
        other_after = _load_product(conn, source_b_id)

        conn.execute(
            """
            UPDATE product
            SET article=?, name=?, brand_country=?, local_name=?,
                photo_file_id=?, photo_path=?, archived=0, archived_at=NULL
            WHERE id=?
            """,
            (
                final_article,
                resolved.get("name"),
                resolved.get("brand_country"),
                resolved.get("local_name"),
                file_id,
                path,
                source_a_id,
            ),
        )
        base_after = _load_product(conn, source_a_id)

        final_stocks = _compute_stocks(stock_mode, base_stocks_before, other_stocks_before)
        conn.execute("DELETE FROM stock WHERE product_id=?", (source_a_id,))
        _insert_stocks(conn, source_a_id, final_stocks, base_after.get("name"), base_after.get("local_name"))
        conn.execute("DELETE FROM stock WHERE product_id=?", (source_b_id,))

        summary = _build_summary(base_before, other_before, final_article)
        changes: Dict[str, Any] = {
            "base_id": source_a_id,
            "other_id": source_b_id,
            "base_before": base_before,
            "other_before": other_before,
            "base_after": base_after,
            "other_after": other_after,
            "base_stocks_before": base_stocks_before,
            "other_stocks_before": other_stocks_before,
            "final_stocks": final_stocks,
            "stock_mode": stock_mode,
            "requested_field_modes": field_modes,
            "applied_field_modes": applied_modes,
        }

        cur = conn.execute(
            """
            INSERT INTO product_merge_log(
                source_a_id, source_b_id, result_id,
                field_modes, stock_mode, summary, changes_json
            ) VALUES (?,?,?,?,?,?,?)
            """,
            (
                source_a_id,
                source_b_id,
                source_a_id,
                json.dumps(applied_modes, ensure_ascii=False),
                stock_mode,
                summary,
                json.dumps(changes, ensure_ascii=False),
            ),
        )
        log_id = int(cur.lastrowid)

        alias_records: List[Dict[str, Any]] = []
        name_alias_records: List[Dict[str, Any]] = []

        if base_before.get("article") and base_before.get("article") != final_article:
            aid = _ensure_article_alias(
                conn,
                source_a_id,
                base_before.get("article"),
                source_a_id,
                log_id,
            )
            if aid:
                alias_records.append({"id": aid, "article": base_before.get("article")})
        if other_before.get("article"):
            aid = _ensure_article_alias(
                conn,
                source_a_id,
                other_before.get("article"),
                source_b_id,
                log_id,
            )
            if aid:
                alias_records.append({"id": aid, "article": other_before.get("article")})

        for value, src in (
            (base_before.get("name"), source_a_id),
            (other_before.get("name"), source_b_id),
            (base_before.get("local_name"), source_a_id),
            (other_before.get("local_name"), source_b_id),
        ):
            nid = _ensure_name_alias(conn, source_a_id, value, src, log_id)
            if nid:
                name_alias_records.append({"id": nid, "name": value})

        changes["article_aliases"] = alias_records
        changes["name_aliases"] = name_alias_records
        conn.execute(
            "UPDATE product_merge_log SET changes_json=? WHERE id=?",
            (json.dumps(changes, ensure_ascii=False), log_id),
        )

        articles_payload = {
            "source": [base_before.get("article"), other_before.get("article")],
            "result": final_article,
        }
        names_payload = {
            "source": [
                base_before.get("name"),
                other_before.get("name"),
                base_before.get("local_name"),
                other_before.get("local_name"),
            ],
            "result": resolved.get("name"),
            "local_result": resolved.get("local_name"),
        }
        rule_cur = conn.execute(
            """
            INSERT INTO product_merge_rule(
                result_id, field_modes, stock_mode, articles_json, names_json, merge_log_id
            ) VALUES (?,?,?,?,?,?)
            """,
            (
                source_a_id,
                json.dumps(applied_modes, ensure_ascii=False),
                stock_mode,
                json.dumps(articles_payload, ensure_ascii=False),
                json.dumps(names_payload, ensure_ascii=False),
                log_id,
            ),
        )
        rule_id = int(rule_cur.lastrowid)
        changes["rule_id"] = rule_id
        conn.execute(
            "UPDATE product_merge_log SET changes_json=? WHERE id=?",
            (json.dumps(changes, ensure_ascii=False), log_id),
        )

        return {
            "ok": True,
            "result_id": source_a_id,
            "log_id": log_id,
            "summary": summary,
            "field_modes": applied_modes,
            "stock_mode": stock_mode,
            "result_fields": {
                "article": final_article,
                "name": resolved.get("name"),
                "brand_country": resolved.get("brand_country"),
                "local_name": resolved.get("local_name"),
                "photo_file_id": file_id,
                "photo_path": path,
            },
            "final_stocks": final_stocks,
        }


def undo_merge(conn: sqlite3.Connection, merge_id: int) -> Dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM product_merge_log WHERE id=?",
        (merge_id,),
    ).fetchone()
    if not row:
        return {"ok": False, "error": "merge_not_found"}
    if row["reverted_at"]:
        return {"ok": False, "error": "already_reverted"}
    changes = json.loads(row["changes_json"] or "{}")
    base_id = row["result_id"]
    other_id = row["source_b_id"]
    with conn:
        conn.execute("DELETE FROM product_article_alias WHERE merge_log_id=?", (merge_id,))
        conn.execute("DELETE FROM product_name_alias WHERE merge_log_id=?", (merge_id,))
        conn.execute(
            "UPDATE product_merge_rule SET active=0, reverted_at=datetime('now','localtime') WHERE merge_log_id=?",
            (merge_id,),
        )

        base_before = changes.get("base_before") or {}
        other_before = changes.get("other_before") or {}
        conn.execute(
            """
            UPDATE product
            SET article=?, name=?, brand_country=?, local_name=?,
                photo_file_id=?, photo_path=?, archived=?, archived_at=?
            WHERE id=?
            """,
            (
                base_before.get("article"),
                base_before.get("name"),
                base_before.get("brand_country"),
                base_before.get("local_name"),
                base_before.get("photo_file_id"),
                base_before.get("photo_path"),
                int(base_before.get("archived") or 0),
                base_before.get("archived_at"),
                base_id,
            ),
        )
        conn.execute(
            """
            UPDATE product
            SET article=?, name=?, brand_country=?, local_name=?,
                photo_file_id=?, photo_path=?, archived=?, archived_at=?
            WHERE id=?
            """,
            (
                other_before.get("article"),
                other_before.get("name"),
                other_before.get("brand_country"),
                other_before.get("local_name"),
                other_before.get("photo_file_id"),
                other_before.get("photo_path"),
                int(other_before.get("archived") or 0),
                other_before.get("archived_at"),
                other_id,
            ),
        )

        conn.execute("DELETE FROM stock WHERE product_id=?", (base_id,))
        _insert_stocks(
            conn,
            base_id,
            changes.get("base_stocks_before") or [],
            base_before.get("name"),
            base_before.get("local_name"),
        )
        conn.execute("DELETE FROM stock WHERE product_id=?", (other_id,))
        _insert_stocks(
            conn,
            other_id,
            changes.get("other_stocks_before") or [],
            other_before.get("name"),
            other_before.get("local_name"),
        )

        changes["undone_at"] = time.time()
        conn.execute(
            "UPDATE product_merge_log SET reverted_at=datetime('now','localtime'), changes_json=? WHERE id=?",
            (json.dumps(changes, ensure_ascii=False), merge_id),
        )
    return {"ok": True}


def list_history(conn: sqlite3.Connection, limit: int = 20) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT l.id, l.created_at, l.source_a_id, l.source_b_id, l.result_id,
               l.summary, l.field_modes, l.stock_mode, l.reverted_at,
               l.changes_json,
               r.articles_json, r.names_json, r.active
        FROM product_merge_log l
        LEFT JOIN product_merge_rule r ON r.merge_log_id = l.id
        ORDER BY l.id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    history: List[Dict[str, Any]] = []
    for row in rows:
        entry: Dict[str, Any] = {
            "id": row["id"],
            "created_at": row["created_at"],
            "summary": row["summary"],
            "field_modes": json.loads(row["field_modes"] or "{}"),
            "stock_mode": row["stock_mode"],
            "reverted": bool(row["reverted_at"]),
            "reverted_at": row["reverted_at"],
        }
        changes = json.loads(row["changes_json"] or "{}")
        if changes.get("requested_field_modes"):
            entry["requested_field_modes"] = changes.get("requested_field_modes")
        if row["articles_json"] or row["names_json"]:
            entry["rule"] = {
                "articles": json.loads(row["articles_json"] or "{}"),
                "names": json.loads(row["names_json"] or "{}"),
                "active": bool(row["active"]),
            }
        history.append(entry)
    return history
