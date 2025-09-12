from __future__ import annotations

from typing import Dict, Tuple, Optional


move_ctx: Dict[Tuple[int, int], Dict[str, Optional[str]]] = {}


def get_ctx(uid: int, pid: int) -> Dict[str, Optional[str]]:
    c = move_ctx.get((uid, pid))
    if not c:
        c = {"src": None, "dst": None, "qty": 1}
        move_ctx[(uid, pid)] = c
    return c


def ctx_badge(ctx: Dict[str, Optional[str]]) -> str:
    return f"(из: {ctx.get('src') or '—'} → в: {ctx.get('dst') or '—'}, кол-во: {ctx.get('qty') or 1})"


def pop_ctx(uid: int, pid: int) -> None:
    move_ctx.pop((uid, pid), None)
