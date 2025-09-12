from __future__ import annotations

import datetime as dt
import sqlite3

from app.db import db


def mark_restock(conn: sqlite3.Connection, pid: int):
    """Update product.last_restock_at and unarchive if needed.

    Must be called inside a transaction using provided connection when total stock increases (real restock),
    e.g., supply import or positive inventory adjustment. Do NOT call for internal moves.
    """
    now = dt.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        """
        UPDATE product
        SET last_restock_at=?, archived=CASE WHEN archived=1 THEN 0 ELSE archived END,
            archived_at=CASE WHEN archived=1 THEN NULL ELSE archived_at END
        WHERE id=?
        """,
        (now, pid),
    )


def _last_restock_for(conn: sqlite3.Connection, pid: int) -> str | None:
    # Prefer explicit last_restock_at if present; otherwise fall back to last 'to_skl' event; else created_at
    row = conn.execute("SELECT last_restock_at, created_at FROM product WHERE id=?", (pid,)).fetchone()
    if not row:
        return None
    if row["last_restock_at"]:
        return str(row["last_restock_at"])  # already ISO-like
    ev = conn.execute(
        "SELECT MAX(ts) AS ts FROM event_log WHERE product_id=? AND type='to_skl' AND (delta IS NULL OR delta>0)",
        (pid,),
    ).fetchone()
    if ev and ev["ts"]:
        return str(ev["ts"])
    return str(row["created_at"]) if row["created_at"] else None


def run_archive_sweep(days_without_restock: int = 30) -> int:
    """Mark products as archived when they have zero total stock and no restock within N days.

    Returns number of products archived in this sweep.
    """
    conn = db()
    try:
        # Candidates: not archived and total stock == 0
        pids = [
            int(r["id"]) for r in conn.execute(
                """
                SELECT p.id
                FROM product p
                LEFT JOIN (
                    SELECT product_id, SUM(qty_pack) AS total
                    FROM stock GROUP BY product_id
                ) t ON t.product_id=p.id
                WHERE p.archived=0 AND IFNULL(t.total,0)=0
                """
            ).fetchall()
        ]
        if not pids:
            return 0
        now = dt.datetime.now(dt.timezone.utc)
        cutoff = now - dt.timedelta(days=days_without_restock)
        cutoff_str = cutoff.astimezone().strftime("%Y-%m-%d %H:%M:%S")
        archived_count = 0
        with conn:
            for pid in pids:
                last = _last_restock_for(conn, pid)
                if not last:
                    continue
                # SQLite stores localtime in event_log.ts; treat strings lexicographically OK for ISO-like
                # Normalize by parsing safely
                try:
                    last_dt = dt.datetime.fromisoformat(str(last))
                except Exception:
                    # try parse without timezone
                    try:
                        last_dt = dt.datetime.strptime(str(last), "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        continue
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=dt.timezone.utc)
                if last_dt <= cutoff:
                    conn.execute(
                        "UPDATE product SET archived=1, archived_at=datetime('now','localtime') WHERE id=?",
                        (pid,),
                    )
                    archived_count += 1
        return archived_count
    finally:
        conn.close()

