from __future__ import annotations

import asyncio
import sqlite3
from typing import Dict, List, Tuple

from aiogram import Bot

from app.db import db


def get_notify_mode(user_id: int, notif_type: str) -> str:
    conn = db()
    try:
        row = conn.execute(
            "SELECT mode FROM user_notify WHERE user_id=? AND notif_type=?",
            (user_id, notif_type),
        ).fetchone()
        return row["mode"] if row else "off"
    finally:
        conn.close()


def set_notify_mode(user_id: int, notif_type: str, mode: str) -> None:
    conn = db()
    try:
        with conn:
            conn.execute(
                "INSERT INTO user_notify(user_id, notif_type, mode) VALUES (?,?,?)\n                 ON CONFLICT(user_id, notif_type) DO UPDATE SET mode=excluded.mode",
                (user_id, notif_type, mode),
            )
    finally:
        conn.close()


def log_event_to_skl(conn: sqlite3.Connection, pid: int, loc: str, delta: float):
    if loc.startswith("SKL") and delta > 0:
        with conn:
            conn.execute(
                "INSERT INTO event_log(type, product_id, location_code, delta) VALUES ('to_skl',?,?,?)",
                (pid, loc, float(delta)),
            )


def _admins_for_mode(notif_type: str, mode: str) -> list[int]:
    conn = db()
    try:
        rows = conn.execute(
            "SELECT ur.tg_id FROM user_role ur JOIN user_notify n ON n.user_id=ur.tg_id\n             WHERE ur.role='admin' AND n.notif_type=? AND n.mode=? AND ur.tg_id IS NOT NULL",
            (notif_type, mode),
        ).fetchall()
        return [int(r["tg_id"]) for r in rows]
    finally:
        conn.close()


def _admin_daily_prefs() -> Dict[int, set[str]]:
    conn = db()
    try:
        rows = conn.execute(
            """
            SELECT ur.tg_id AS uid, n.notif_type AS t
            FROM user_notify n
            JOIN user_role ur ON ur.tg_id = n.user_id AND ur.role='admin'
            WHERE n.mode='daily' AND ur.tg_id IS NOT NULL
            """
        ).fetchall()
        out: Dict[int, set[str]] = {}
        for r in rows:
            uid = int(r["uid"]) if r["uid"] is not None else None
            t = r["t"]
            if uid is None:
                continue
            out.setdefault(uid, set()).add(t)
        return out
    finally:
        conn.close()


def _pid_to_display(conn: sqlite3.Connection, pids: List[int]) -> Dict[int, Tuple[str, str]]:
    if not pids:
        return {}
    placeholders = ",".join(["?"] * len(pids))
    rows = conn.execute(
        f"SELECT id, article, COALESCE(local_name,name) AS disp FROM product WHERE id IN ({placeholders})",
        tuple(pids),
    ).fetchall()
    return {int(r["id"]): (r["disp"], r["article"]) for r in rows}


async def send_daily_digests(bot: Bot):
    conn = db()
    try:
        zero_rows = conn.execute(
            "SELECT DISTINCT product_id FROM event_log WHERE type='zero' AND date(ts)=date('now','localtime')"
        ).fetchall()
        last_rows = conn.execute(
            "SELECT DISTINCT product_id FROM event_log WHERE type='last' AND date(ts)=date('now','localtime')"
        ).fetchall()
        skl_rows = conn.execute(
            "SELECT product_id, SUM(delta) AS tot FROM event_log WHERE type='to_skl' AND date(ts)=date('now','localtime') GROUP BY product_id"
        ).fetchall()
        zero_pids = [int(r["product_id"]) for r in zero_rows]
        last_pids = [int(r["product_id"]) for r in last_rows]
        skl_map = {int(r["product_id"]): float(r["tot"] or 0) for r in skl_rows}
        pids = list(set(zero_pids) | set(last_pids) | set(skl_map.keys()))
        pid_disp = _pid_to_display(conn, pids)
    finally:
        conn.close()

    prefs = _admin_daily_prefs()
    for uid, types in prefs.items():
        sections = []
        if 'zero' in types and zero_pids:
            lines = []
            for pid in zero_pids:
                disp, art = pid_disp.get(pid, (f"id={pid}", "?"))
                lines.append(f"• {disp} ({art})")
            sections.append("<b>Закончились</b>:\n" + "\n".join(lines))
        if 'last' in types and last_pids:
            lines = []
            for pid in last_pids:
                disp, art = pid_disp.get(pid, (f"id={pid}", "?"))
                lines.append(f"• {disp} ({art})")
            sections.append("<b>Последняя пачка</b>:\n" + "\n".join(lines))
        if 'to_skl' in types and skl_map:
            lines = []
            for pid, tot in skl_map.items():
                disp, art = pid_disp.get(pid, (f"id={pid}", "?"))
                qty = int(tot) if float(tot).is_integer() else tot
                lines.append(f"• +{qty} → склад: {disp} ({art})")
            sections.append("<b>Поступило на склад</b>:\n" + "\n".join(lines))
        if not sections:
            continue
        text = "<b>Сводка за день (21:10)</b>\n\n" + "\n\n".join(sections)
        try:
            await bot.send_message(uid, text)
        except Exception:
            pass


def _log_event_threshold(conn: sqlite3.Connection, pid: int, event_type: str):
    if event_type not in ("zero", "last"):
        return
    with conn:
        conn.execute(
            "INSERT INTO event_log(type, product_id, location_code, delta) VALUES (?,?,NULL,NULL)",
            (event_type, pid),
        )


async def notify_instant_thresholds(bot: Bot, pid: int, before: float, after: float):
    # zero
    if before > 0 and after == 0:
        uids = _admins_for_mode('zero', 'instant')
        if uids:
            conn_info = db(); r = conn_info.execute("SELECT article, COALESCE(local_name,name) AS disp FROM product WHERE id=?", (pid,)).fetchone(); conn_info.close()
            text = f"Закончился: {r['disp']} ({r['article']})"
            for uid in uids:
                asyncio.create_task(bot.send_message(uid, text))
        conn_log = db()
        try:
            _log_event_threshold(conn_log, pid, 'zero')
        finally:
            conn_log.close()
    # last pack
    if before > 1 and after == 1:
        uids = _admins_for_mode('last', 'instant')
        if uids:
            conn_info = db(); r = conn_info.execute("SELECT article, COALESCE(local_name,name) AS disp FROM product WHERE id=?", (pid,)).fetchone(); conn_info.close()
            text = f"Осталась последняя пачка: {r['disp']} ({r['article']})"
            for uid in uids:
                asyncio.create_task(bot.send_message(uid, text))
        conn_log = db()
        try:
            _log_event_threshold(conn_log, pid, 'last')
        finally:
            conn_log.close()


async def notify_instant_to_skl(bot: Bot, pid: int, loc: str, delta: float):
    if not loc.startswith('SKL') or delta <= 0:
        return
    uids = _admins_for_mode('to_skl', 'instant')
    if not uids:
        return
    conn = db(); r = conn.execute("SELECT article, COALESCE(local_name,name) AS disp FROM product WHERE id=?", (pid,)).fetchone(); conn.close()
    text = f"На склад {loc} поступило {int(delta) if float(delta).is_integer() else delta}: {r['disp']} ({r['article']})"
    for uid in uids:
        asyncio.create_task(bot.send_message(uid, text))

