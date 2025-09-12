from __future__ import annotations

import sqlite3
from typing import Tuple


def total_stock(conn: sqlite3.Connection, pid: int) -> float:
    row = conn.execute(
        "SELECT IFNULL(SUM(qty_pack),0) AS t FROM stock WHERE product_id=?",
        (pid,),
    ).fetchone()
    return float(row["t"] or 0)


def move_specific(conn: sqlite3.Connection, pid: int, src: str, dst: str, qty: int) -> Tuple[bool, str]:
    if qty <= 0:
        return False, "Количество должно быть > 0"
    have_row = conn.execute(
        "SELECT qty_pack FROM stock WHERE product_id=? AND location_code=?",
        (pid, src),
    ).fetchone()
    have = float(have_row["qty_pack"]) if have_row else 0.0
    if have < qty:
        return False, f"Недостаточно на {src}: есть {int(have)}, нужно {qty}"
    with conn:
        conn.execute(
            "UPDATE stock SET qty_pack=qty_pack-? WHERE product_id=? AND location_code=?",
            (float(qty), pid, src),
        )
        conn.execute(
            "DELETE FROM stock WHERE product_id=? AND location_code=? AND qty_pack<=0",
            (pid, src),
        )
        if dst != "HALL":
            prow = conn.execute(
                "SELECT name, local_name FROM product WHERE id=?",
                (pid,),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO stock(product_id,location_code,qty_pack,name,local_name)
                VALUES (?,?,?,?,?)
                ON CONFLICT(product_id,location_code)
                DO UPDATE SET
                    qty_pack=qty_pack+excluded.qty_pack,
                    name=excluded.name,
                    local_name=excluded.local_name
                """,
                (pid, dst, float(qty), prow["name"], prow["local_name"]),
            )
    return True, "OK"


def adjust_location_qty(
    conn: sqlite3.Connection, pid: int, loc: str, delta: int
) -> Tuple[bool, str]:
    if delta == 0:
        return True, "OK"
    if delta > 0:
        prow = conn.execute("SELECT name, local_name FROM product WHERE id=?", (pid,)).fetchone()
        name = prow["name"] if prow else None
        local_name = prow["local_name"] if prow else None
        with conn:
            conn.execute(
                """
                INSERT INTO stock(product_id, location_code, qty_pack, name, local_name)
                VALUES (?,?,?,?,?)
                ON CONFLICT(product_id, location_code)
                DO UPDATE SET
                    qty_pack = stock.qty_pack + excluded.qty_pack,
                    name = excluded.name,
                    local_name = excluded.local_name
                """,
                (pid, loc, float(delta), name, local_name),
            )
            # Positive delta increases total stock: treat as restock and unarchive if needed
            try:
                from app.services.archival import mark_restock
                mark_restock(conn, pid)
            except Exception:
                pass
        return True, "OK"
    else:
        dec = abs(float(delta))
        with conn:
            cur = conn.execute(
                "UPDATE stock SET qty_pack=qty_pack-? WHERE product_id=? AND location_code=? AND qty_pack>=?",
                (dec, pid, loc, dec),
            )
            if cur.rowcount == 0:
                row2 = conn.execute(
                    "SELECT qty_pack FROM stock WHERE product_id=? AND location_code=?",
                    (pid, loc),
                ).fetchone()
                have = float(row2["qty_pack"]) if row2 and row2["qty_pack"] is not None else 0.0
                disp_have = int(have) if float(have).is_integer() else have
                return False, (
                    f"Нельзя уйти в минус на {loc}. Есть {disp_have}, убытие {int(dec) if dec.is_integer() else dec}."
                )
            conn.execute(
                "DELETE FROM stock WHERE product_id=? AND location_code=? AND qty_pack<=0",
                (pid, loc),
            )
        return True, "OK"
