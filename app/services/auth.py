from __future__ import annotations

from typing import Optional

from aiogram.types import CallbackQuery

from app.db import db
from app import config as app_config


def _norm_username(u: Optional[str]) -> Optional[str]:
    if not u:
        return None
    u = u.strip()
    if not u:
        return None
    if not u.startswith("@"):
        u = "@" + u
    return u.lower()


def is_super_admin(uid: int, username: Optional[str]) -> bool:
    if uid == app_config.SUPER_ADMIN_ID:
        return True
    un = _norm_username(username)
    return un == app_config.SUPER_ADMIN_USERNAME.lower()


def is_admin(uid: int, username: Optional[str]) -> bool:
    if is_super_admin(uid, username):
        return True
    conn = db()
    un = _norm_username(username)
    row = conn.execute(
        "SELECT 1 FROM user_role WHERE role='admin' AND (tg_id=? OR (username IS NOT NULL AND LOWER(username)=?)) LIMIT 1",
        (uid, un),
    ).fetchone()
    conn.close()
    return bool(row)


def is_seller(uid: int, username: Optional[str]) -> bool:
    conn = db()
    un = _norm_username(username)
    row = conn.execute(
        "SELECT 1 FROM user_role WHERE role='seller' AND (tg_id=? OR (username IS NOT NULL AND LOWER(username)=?)) LIMIT 1",
        (uid, un),
    ).fetchone()
    conn.close()
    return bool(row)


def is_allowed(uid: int, username: Optional[str]) -> bool:
    return is_admin(uid, username) or is_seller(uid, username)


async def require_admin(cb: CallbackQuery) -> bool:
    if is_admin(cb.from_user.id, cb.from_user.username):
        return True
    try:
        await cb.answer("Нет доступа (только для админа)", show_alert=True)
    except Exception:
        pass
    return False
