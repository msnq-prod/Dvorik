from __future__ import annotations

from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Message
from aiogram.fsm.context import FSMContext

router = Router()


@router.callback_query(F.data == "supply")
async def cb_supply(cb: CallbackQuery):
    import app.bot as botmod
    if not await botmod.require_admin(cb):
        return
    await botmod._safe_cb_answer(cb)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 Загрузить CSV", callback_data="supply_upload")],
        [InlineKeyboardButton(text="🗂️ Список новых", callback_data="supply_list|1")],
        [InlineKeyboardButton(text="← Назад", callback_data="admin")]
    ])
    await cb.message.edit_text("Поставка: загрузите счёт и распределяйте новые позиции.", reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data == "supply_upload")
async def cb_supply_upload(cb: CallbackQuery, state: FSMContext):
    import app.bot as botmod
    await botmod._safe_cb_answer(cb)
    await state.update_data(expect_excel=True)
    await cb.message.edit_text(
        (
            "Принимаем только CSV.\n"
            "Пожалуйста, перед загрузкой конвертируйте файл в CSV: https://convertio.co/ru/xls-csv/\n\n"
            "Затем отправьте получившийся .csv — мы его импортируем."
        ),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Назад", callback_data="supply")],
        ]),
    )
    await cb.answer()


@router.message(F.document)
async def on_document(m: Message, state: FSMContext):
    import asyncio
    import app.bot as botmod

    data = await state.get_data()
    if not data.get("expect_excel"):
        return
    file = m.document
    lower = file.file_name.lower()
    if not lower.endswith(".csv"):
        await m.answer(
            (
                "Принимаем только CSV.\n"
                "Пожалуйста, конвертируйте исходный файл в CSV: https://convertio.co/ru/xls-csv/"
            )
        )
        return
    dest = botmod.UPLOAD_DIR / file.file_name
    await m.bot.download(file, destination=dest)

    norm_csv = None
    conv_info = ""
    norm_csv, norm_stats = await asyncio.to_thread(botmod.csv_to_normalized_csv, str(dest))
    conv_info = f"Нормализованные строки: {norm_stats.get('found',0)}\n"
    if norm_stats.get('errors'):
        conv_info += "Ошибки нормализации:\n" + "\n".join("• "+e for e in norm_stats['errors']) + "\n"

    if not norm_csv:
        text = (
            "Не удалось найти раздел товаров или распознать колонки.\n"
            + conv_info
            + "Пожалуйста, пришлите CSV (см. инструкцию выше)."
        )
        await m.answer(
            text,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="← Назад", callback_data="supply")],
                ]
            ),
        )
        await state.clear()
        return

    stats = await asyncio.to_thread(botmod.import_supply_from_normalized_csv, norm_csv)
    text = (
        f"Импорт завершён.\n"
        f"Всего строк: {stats['imported']}\n"
        f"Создано: {stats['created']}\n"
        f"Обновлено: {stats['updated']}\n"
    )
    text += conv_info
    if norm_csv:
        text += f"Файл CSV: {norm_csv}\n"
    if stats["errors"]:
        text += "Ошибки:\n" + "\n".join("• " + e for e in stats["errors"])
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗂️ Список новых", callback_data="supply_list|1")],
            [InlineKeyboardButton(text="← Назад", callback_data="supply")],
        ]
    )
    await m.answer(text, reply_markup=kb)
    try:
        to_skl_map = stats.get("to_skl", {}) or {}
        for pid, tot in to_skl_map.items():
            if tot and float(tot) > 0:
                await botmod._notify_instant_to_skl(m.bot, int(pid), "SKL-0", float(tot))
    except Exception:
        pass
    await state.clear()


@router.callback_query(F.data.startswith("supply_list|"))
async def supply_list(cb: CallbackQuery):
    import app.bot as botmod
    _, page_s = cb.data.split("|", 1)
    page = max(1, int(page_s))
    conn = botmod.db()
    kb = botmod.kb_supply_page(conn, page)
    conn.close()
    await cb.message.edit_text("Новые позиции:", reply_markup=kb)
    await botmod._safe_cb_answer(cb)
