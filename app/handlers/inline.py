from __future__ import annotations

from aiogram import Router
from aiogram.types import InlineQuery, InlineQueryResultArticle, InputTextMessageContent

router = Router()


@router.inline_query()
async def inline_query(iq: InlineQuery):
    import app.bot as botmod
    q = (iq.query or "").strip()
    if not botmod.is_allowed(iq.from_user.id, iq.from_user.username):
        await iq.answer(results=[], cache_time=1, is_personal=True)
        return
    conn = botmod.db()

    inv_mode = False
    if q.upper().startswith("INV "):
        inv_mode = True
        q = q[4:].strip()

    only_new = False
    if q.upper().startswith("NEW "):
        only_new = True
        q = q[4:].strip()

    only_incomplete = False
    if q.upper().startswith("INC "):
        only_incomplete = True
        q = q[4:].strip()

    admin_mode = False
    if q.upper().startswith("ADM "):
        admin_mode = True
        q = q[4:].strip()

    rows = []
    try:
        if q:
            if only_new:
                rows = conn.execute(
                    """
                    SELECT p.id, p.article, p.name, p.local_name
                    FROM product_fts f
                    JOIN product p ON p.id=f.rowid
                    LEFT JOIN (
                        SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id
                    ) t ON t.product_id=p.id
                    WHERE product_fts MATCH ? AND p.is_new=1 AND p.archived=0
                      AND (COALESCE(t.total,0) > 0 OR ?=1)
                    ORDER BY p.id DESC LIMIT 50
                    """,
                    (q.replace(" ", "* ") + "*", 1 if admin_mode else 0),
                ).fetchall()
            elif only_incomplete:
                rows = conn.execute(
                    """
                    SELECT p.id, p.article, p.name, p.local_name
                    FROM product_fts f
                    JOIN product p ON p.id=f.rowid
                    LEFT JOIN (
                        SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id
                    ) t ON t.product_id=p.id
                    WHERE product_fts MATCH ? AND p.archived=0
                      AND (p.local_name IS NULL OR (p.photo_file_id IS NULL AND COALESCE(p.photo_path,'')=''))
                      AND (COALESCE(t.total,0) > 0 OR ?=1)
                    ORDER BY p.id DESC LIMIT 50
                    """,
                    (q.replace(" ", "* ") + "*", 1 if admin_mode else 0),
                ).fetchall()
            else:
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
                    (q.replace(" ", "* ") + "*", 1 if admin_mode else 0),
                ).fetchall()
        else:
            if only_new:
                rows = conn.execute(
                    """
                    SELECT p.id, p.article, p.name, p.local_name
                    FROM product p
                    LEFT JOIN (
                        SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id
                    ) t ON t.product_id=p.id
                    WHERE p.is_new=1 AND p.archived=0
                      AND (COALESCE(t.total,0) > 0 OR ?=1)
                    ORDER BY p.id DESC LIMIT 50
                    """,
                    (1 if admin_mode else 0,),
                ).fetchall()
            elif only_incomplete:
                rows = conn.execute(
                    """
                    SELECT p.id, p.article, p.name, p.local_name
                    FROM product p
                    LEFT JOIN (
                        SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id
                    ) t ON t.product_id=p.id
                    WHERE p.archived=0
                      AND (p.local_name IS NULL OR (p.photo_file_id IS NULL AND COALESCE(p.photo_path,'')=''))
                      AND (COALESCE(t.total,0) > 0 OR ?=1)
                    ORDER BY p.id DESC LIMIT 50
                    """,
                    (1 if admin_mode else 0,),
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
                    (1 if admin_mode else 0,),
                ).fetchall()
    except Exception:
        # Fallback to LIKE queries when FTS not available
        like = f"%{q}%"
        if only_new:
            rows = conn.execute(
                """
                SELECT p.id, p.article, p.name, p.local_name
                FROM product p
                LEFT JOIN (
                    SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id
                ) t ON t.product_id=p.id
                WHERE p.is_new=1
                  AND (p.article LIKE ? OR p.name LIKE ? OR COALESCE(p.local_name,'') LIKE ?)
                  AND (COALESCE(t.total,0) > 0 OR ?=1)
                ORDER BY p.id DESC LIMIT 50
                """,
                (like, like, like, 1 if admin_mode else 0),
            ).fetchall()
        elif only_incomplete:
            rows = conn.execute(
                """
                SELECT p.id, p.article, p.name, p.local_name
                FROM product p
                LEFT JOIN (
                    SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id
                ) t ON t.product_id=p.id
                WHERE (p.local_name IS NULL OR (p.photo_file_id IS NULL AND COALESCE(p.photo_path,'')=''))
                  AND (p.article LIKE ? OR p.name LIKE ? OR COALESCE(p.local_name,'') LIKE ?)
                  AND (COALESCE(t.total,0) > 0 OR ?=1)
                ORDER BY p.id DESC LIMIT 50
                """,
                (like, like, like, 1 if admin_mode else 0),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT p.id, p.article, p.name, p.local_name
                FROM product p
                LEFT JOIN (
                    SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id
                ) t ON t.product_id=p.id
                WHERE (p.article LIKE ? OR p.name LIKE ? OR COALESCE(p.local_name,'') LIKE ?)
                  AND (COALESCE(t.total,0) > 0 OR ?=1)
                ORDER BY p.id DESC LIMIT 50
                """,
                (like, like, like, 1 if admin_mode else 0),
            ).fetchall()

    # Extra LIKE catch with simplified query (ё->е)
    if q:
        like_raw = f"%{q}%"
        sq = botmod._simplify_query(q)
        like_simpl = f"%{sq}%"
        cond_total = "COALESCE(t.total,0) > 0 OR ?=1"
        extra = []
        if only_new:
            extra = conn.execute(
                f"""
                SELECT p.id, p.article, p.name, p.local_name
                FROM product p
                LEFT JOIN (SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id) t ON t.product_id=p.id
                WHERE p.archived=0 AND p.is_new=1 AND (
                    p.article LIKE ? OR p.name LIKE ? OR COALESCE(p.local_name,'') LIKE ?
                    OR REPLACE(LOWER(p.name),'ё','е') LIKE ?
                    OR REPLACE(LOWER(COALESCE(p.local_name,'')),'ё','е') LIKE ?
                ) AND ({cond_total})
                ORDER BY p.id DESC LIMIT 50
                """,
                (like_raw, like_raw, like_raw, like_simpl, like_simpl, 1 if admin_mode else 0),
            ).fetchall()
        elif only_incomplete:
            extra = conn.execute(
                f"""
                SELECT p.id, p.article, p.name, p.local_name
                FROM product p
                LEFT JOIN (SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id) t ON t.product_id=p.id
                WHERE p.archived=0 AND (p.local_name IS NULL OR (p.photo_file_id IS NULL AND COALESCE(p.photo_path,'')='')) AND (
                    p.article LIKE ? OR p.name LIKE ? OR COALESCE(p.local_name,'') LIKE ?
                    OR REPLACE(LOWER(p.name),'ё','е') LIKE ?
                    OR REPLACE(LOWER(COALESCE(p.local_name,'')),'ё','е') LIKE ?
                ) AND ({cond_total})
                ORDER BY p.id DESC LIMIT 50
                """,
                (like_raw, like_raw, like_raw, like_simpl, like_simpl, 1 if admin_mode else 0),
            ).fetchall()
        else:
            extra = conn.execute(
                f"""
                SELECT p.id, p.article, p.name, p.local_name
                FROM product p
                LEFT JOIN (SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id) t ON t.product_id=p.id
                WHERE p.archived=0 AND (
                    p.article LIKE ? OR p.name LIKE ? OR COALESCE(p.local_name,'') LIKE ?
                    OR REPLACE(LOWER(p.name),'ё','е') LIKE ?
                    OR REPLACE(LOWER(COALESCE(p.local_name,'')),'ё','е') LIKE ?
                ) AND ({cond_total})
                ORDER BY p.id DESC LIMIT 50
                """,
                (like_raw, like_raw, like_raw, like_simpl, like_simpl, 1 if admin_mode else 0),
            ).fetchall()
        by_id = {r["id"]: r for r in rows}
        for r in extra:
            if r["id"] not in by_id:
                by_id[r["id"]] = r
        rows = list(by_id.values())[:50]

    results = []
    # Спец-элемент: админ может создать новый товар прямо из поиска
    if admin_mode and q:
        results.append(
            InlineQueryResultArticle(
                id=f"new:{q}",
                title=f"➕ Создать товар: {q}",
                input_message_content=InputTextMessageContent(message_text=f"/admin_new {q}"),
                description="Создать новый товар и добавить на локацию",
            )
        )
    for r in rows:
        pid = r["id"]
        disp_name = r["local_name"] or r["name"]
        stock = botmod.stocks_summary(conn, pid)
        cmd = f"/admin_{pid}" if admin_mode else (f"/inv_{pid}" if inv_mode else f"/open_{pid}")
        results.append(
            InlineQueryResultArticle(
                id=str(pid),
                title=f"{disp_name}",
                input_message_content=InputTextMessageContent(message_text=cmd),
                description=("Админ действия — " if admin_mode else ("")) + f"Остатки: {stock}",
            )
        )
    await iq.answer(results=results, cache_time=1, is_personal=True)
    conn.close()
