from __future__ import annotations

import logging

from aiogram import Router
from aiogram.types import InlineQuery, InlineQueryResultArticle, InputTextMessageContent

router = Router()


logger = logging.getLogger(__name__)


@router.inline_query()
async def inline_query(iq: InlineQuery):
    import app.bot as botmod
    q = (iq.query or "").strip()
    if not botmod.is_allowed(iq.from_user.id, iq.from_user.username):
        await iq.answer(results=[], cache_time=1, is_personal=True)
        return
    admin_mode = False
    if q.upper().startswith("ADM "):
        admin_mode = True
        q = q[4:].strip()

    conn = botmod.db()

    try:
        rows = []
        include_empty = 1 if admin_mode else 0
        try:
            if q:
                rows = conn.execute(
                    """
                    SELECT p.id, p.article, p.name, p.local_name
                    FROM product_fts f
                    JOIN product p ON p.id=f.rowid
                    LEFT JOIN (
                        SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id
                    ) t ON t.product_id=p.id
                    WHERE product_fts MATCH ? AND p.archived=0
                      AND (COALESCE(t.total,0) > 0 OR ?=1)
                    ORDER BY p.id DESC LIMIT 50
                    """,
                    (q.replace(" ", "* ") + "*", include_empty),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT p.id, p.article, p.name, p.local_name
                    FROM product p
                    LEFT JOIN (
                        SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id
                    ) t ON t.product_id=p.id
                    WHERE p.archived=0 AND (COALESCE(t.total,0) > 0 OR ?=1)
                    ORDER BY p.id DESC LIMIT 50
                    """,
                    (include_empty,),
                ).fetchall()
        except Exception:
            like = f"%{q}%"
            rows = conn.execute(
                """
                SELECT p.id, p.article, p.name, p.local_name
                FROM product p
                LEFT JOIN (
                    SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id
                ) t ON t.product_id=p.id
                WHERE (p.article LIKE ? OR p.name LIKE ? OR COALESCE(p.local_name,'') LIKE ?)
                  AND p.archived=0 AND (COALESCE(t.total,0) > 0 OR ?=1)
                ORDER BY p.id DESC LIMIT 50
                """,
                (like, like, like, include_empty),
            ).fetchall()
            if q:
                sq = botmod._simplify_query(q)
                like_simpl = f"%{sq}%"
                extra = conn.execute(
                    """
                    SELECT p.id, p.article, p.name, p.local_name
                    FROM product p
                    LEFT JOIN (
                        SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id
                    ) t ON t.product_id=p.id
                    WHERE p.archived=0 AND (
                        REPLACE(LOWER(p.name),'ё','е') LIKE ?
                        OR REPLACE(LOWER(COALESCE(p.local_name,'')),'ё','е') LIKE ?
                        OR REPLACE(LOWER(p.article),'ё','е') LIKE ?
                    ) AND (COALESCE(t.total,0) > 0 OR ?=1)
                    ORDER BY p.id DESC LIMIT 50
                    """,
                    (like_simpl, like_simpl, like_simpl, include_empty),
                ).fetchall()
                by_id = {r["id"]: r for r in rows}
                for r in extra:
                    by_id.setdefault(r["id"], r)
                rows = list(by_id.values())[:50]

        results = []
        seen: set[int] = set()
        for r in rows:
            pid = int(r["id"])
            if pid in seen:
                continue
            seen.add(pid)
            disp_name = (r["local_name"] or r["name"] or "").strip()
            if not disp_name:
                disp_name = r["article"] or f"#{pid}"
            stock = botmod.stocks_summary(conn, pid)
            cmd = f"/admin_{pid}" if admin_mode else f"/open_{pid}"
            results.append(
                InlineQueryResultArticle(
                    id=f"{pid}{'-admin' if admin_mode else ''}",
                    title=disp_name,
                    input_message_content=InputTextMessageContent(message_text=cmd),
                    description=("Админ действия — " if admin_mode else "") + f"Остатки: {stock}",
                )
            )
        await iq.answer(results=results, cache_time=1, is_personal=True)
    except Exception:
        logger.exception("Unhandled error while processing inline query: %s", q)
        raise
    finally:
        conn.close()
