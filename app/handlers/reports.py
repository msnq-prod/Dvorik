from __future__ import annotations

from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.exceptions import TelegramBadRequest

router = Router()


@router.callback_query(F.data == "reports")
async def cb_reports(cb: CallbackQuery):
    import app.bot as botmod
    if not await botmod.require_admin(cb):
        return
    await botmod._safe_cb_answer(cb)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Заканчиваются (<2)", callback_data="rpt_low"),
         InlineKeyboardButton(text="Нулевой остаток", callback_data="rpt_zero")],
        [InlineKeyboardButton(text="В достатке 3–5", callback_data="rpt_mid"),
         InlineKeyboardButton(text="Весь товар", callback_data="rpt_all")],
        [InlineKeyboardButton(text="🗄️ Архив", callback_data="rpt_arch")],
        [InlineKeyboardButton(text="← Назад", callback_data="admin")]
    ])
    try:
        await cb.message.edit_text("Отчёты:", reply_markup=kb)
    except TelegramBadRequest:
        await cb.message.answer("Отчёты:", reply_markup=kb)


@router.callback_query(F.data == "rpt_low")
async def rpt_low(cb: CallbackQuery):
    import app.bot as botmod
    conn = botmod.db()
    rows = conn.execute("""
        SELECT p.article, COALESCE(p.local_name,p.name) AS disp_name, IFNULL(SUM(s.qty_pack),0) AS total
        FROM product p LEFT JOIN stock s ON s.product_id=p.id
        WHERE p.archived=0
        GROUP BY p.id HAVING total>0 AND total<2
        ORDER BY total ASC, p.id DESC LIMIT 1000
    """).fetchall()
    conn.close()
    if not rows:
        await cb.answer("Нет заканчивающихся.", show_alert=True); return
    import time, csv
    ts = time.strftime('%Y%m%d-%H%M%S')
    path = botmod.REPORTS_DIR / f"low_{ts}.csv"
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(["name", "article", "total"])
        for r in rows:
            tot = float(r['total'])
            disp_tot = int(tot) if tot.is_integer() else tot
            w.writerow([r['disp_name'], r['article'], disp_tot])
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="reports")]])
    await cb.message.answer_document(FSInputFile(path), caption="Заканчиваются (&lt;2)", reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data == "rpt_zero")
async def rpt_zero(cb: CallbackQuery):
    import app.bot as botmod
    conn = botmod.db()
    rows = conn.execute("""
        SELECT p.article, COALESCE(p.local_name,p.name) AS disp_name
        FROM product p LEFT JOIN stock s ON s.product_id=p.id
        WHERE p.archived=0
        GROUP BY p.id HAVING IFNULL(SUM(s.qty_pack),0)=0
        ORDER BY p.id DESC LIMIT 5000
    """).fetchall()
    conn.close()
    if not rows:
        await cb.answer("Нулевых нет.", show_alert=True); return
    import time, csv
    ts = time.strftime('%Y%m%d-%H%M%S')
    path = botmod.REPORTS_DIR / f"zero_{ts}.csv"
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(["name", "article"])
        for r in rows:
            w.writerow([r['disp_name'], r['article']])
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="reports")]])
    await cb.message.answer_document(FSInputFile(path), caption="Нулевой остаток", reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data == "rpt_mid")
async def rpt_mid(cb: CallbackQuery):
    import app.bot as botmod
    conn = botmod.db()
    rows = conn.execute("""
        SELECT p.article, COALESCE(p.local_name,p.name) AS disp_name, IFNULL(SUM(s.qty_pack),0) AS total
        FROM product p LEFT JOIN stock s ON s.product_id=p.id
        WHERE p.archived=0
        GROUP BY p.id HAVING total>=3 AND total<=5
        ORDER BY total DESC, disp_name ASC
    """).fetchall()
    conn.close()
    if not rows:
        await cb.answer("Таких позиций нет.", show_alert=True); return
    import time, csv
    ts = time.strftime('%Y%m%d-%H%M%S')
    path = botmod.REPORTS_DIR / f"mid_{ts}.csv"
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(["name", "article", "total"])
        for r in rows:
            tot = float(r['total'])
            disp_tot = int(tot) if tot.is_integer() else tot
            w.writerow([r['disp_name'], r['article'], disp_tot])
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="reports")]])
    await cb.message.answer_document(FSInputFile(path), caption="В достатке 3–5", reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data == "rpt_all")
async def rpt_all(cb: CallbackQuery):
    import app.bot as botmod
    conn = botmod.db()
    rows = conn.execute("""
        SELECT p.article, COALESCE(p.local_name,p.name) AS disp_name, IFNULL(SUM(s.qty_pack),0) AS total
        FROM product p LEFT JOIN stock s ON s.product_id=p.id
        WHERE p.archived=0
        GROUP BY p.id
        ORDER BY disp_name ASC
    """).fetchall()
    conn.close()
    if not rows:
        await cb.answer("Пока нечего показать.", show_alert=True); return
    import time, csv
    ts = time.strftime('%Y%m%d-%H%M%S')
    path = botmod.REPORTS_DIR / f"all_{ts}.csv"
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(["name", "article", "total"])
        for r in rows:
            tot = float(r['total'])
            disp_tot = int(tot) if tot.is_integer() else tot
            w.writerow([r['disp_name'], r['article'], disp_tot])
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="reports")]])
    await cb.message.answer_document(FSInputFile(path), caption="Все товары", reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data == "rpt_arch")
async def rpt_arch(cb: CallbackQuery):
    import app.bot as botmod
    conn = botmod.db()
    rows = conn.execute(
        """
        SELECT p.article,
               COALESCE(p.local_name, p.name) AS disp_name,
               p.archived_at,
               p.last_restock_at
        FROM product p
        WHERE p.archived=1
        ORDER BY (p.archived_at IS NULL) ASC, p.archived_at DESC, disp_name ASC
        """
    ).fetchall()
    conn.close()
    if not rows:
        await cb.answer("Архив пуст.", show_alert=True); return
    import time, csv
    ts = time.strftime('%Y%m%d-%H%M%S')
    path = botmod.REPORTS_DIR / f"archive_{ts}.csv"
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(["name", "article", "archived_at", "last_restock_at"])
        for r in rows:
            w.writerow([r['disp_name'], r['article'], r['archived_at'] or '', r['last_restock_at'] or ''])
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="reports")]])
    await cb.message.answer_document(FSInputFile(path), caption="Архив — архивные товары", reply_markup=kb)
    await cb.answer()
