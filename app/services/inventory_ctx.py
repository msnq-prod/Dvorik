from __future__ import annotations

from typing import Dict, Tuple, Optional


inv_loc_ctx: Dict[int, Dict[str, str]] = {}
inv_qty_ctx: Dict[Tuple[int, int], Dict[str, Optional[str]]] = {}


def _inv_loc_set(uid: int, code: str) -> None:
    inv_loc_ctx[uid] = {"loc": code}


def _inv_loc_get(uid: int) -> Optional[str]:
    d = inv_loc_ctx.get(uid)
    return d.get("loc") if d else None


def _inv_ctx(uid: int, pid: int) -> Dict[str, Optional[str]]:
    c = inv_qty_ctx.get((uid, pid))
    if not c:
        c = {"loc": _inv_loc_get(uid), "qty": 1}
        inv_qty_ctx[(uid, pid)] = c
    return c
