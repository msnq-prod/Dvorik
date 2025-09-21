from __future__ import annotations

import re
from pathlib import Path

from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Message, FSInputFile
from aiogram.fsm.context import FSMContext

router = Router()


_SAFE_NAME_RX = re.compile(r"[^A-Za-z0-9А-Яа-я_.\-]+")


def _sanitize_filename(name: str) -> str:
    basename = Path(name).name
    cleaned = _SAFE_NAME_RX.sub("_", basename)
    cleaned = cleaned.strip("._")
    return cleaned or "upload"


@router.callback_query(F.data == "supply")
async def cb_supply(cb: CallbackQuery):
    import app.bot as botmod
    if not await botmod.require_admin(cb):
        return
    await botmod._safe_cb_answer(cb)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 Загрузить счёт (CSV/XLS)", callback_data="supply_upload")],
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
            "Принимаем CSV и Excel (.xls/.xlsx).\n"
            "Можно прислать исходный счёт — нормализуем его автоматически перед импортом.\n"
            "Если распознавание не удалось, воспользуйтесь конвертером: https://convertio.co/ru/xls-csv/"
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
    original_name = file.file_name
    if not original_name:
        await m.answer(
            (
                "Не удалось определить имя файла.\n"
                "Пожалуйста, перешлите документ CSV или Excel (.xls/.xlsx) с расширением."
            )
        )
        return
    lower = original_name.lower()
    suffix = Path(lower).suffix
    excel_exts = {".xls", ".xlsx", ".xlsm", ".xltx", ".xltm"}
    if suffix not in {".csv"} and suffix not in excel_exts:
        await m.answer(
            (
                "Принимаем только CSV или Excel (.xls/.xlsx).\n"
                "Пожалуйста, конвертируйте исходный файл в CSV: https://convertio.co/ru/xls-csv/"
            )
        )
        return

    safe_name = _sanitize_filename(original_name or file.file_unique_id)
    dest = botmod.UPLOAD_DIR / safe_name
    await m.bot.download(file, destination=dest)

    source_hash = await asyncio.to_thread(botmod.compute_sha256, str(dest))
    existing = await asyncio.to_thread(botmod.check_import_duplicate, source_hash)
    if existing:
        when = existing.get("created_at")
        supplier = existing.get("supplier")
        invoice = existing.get("invoice")
        parts = ["Этот файл уже импортирован." ]
        if when:
            parts.append(f"Дата импорта: {when}.")
        if supplier:
            parts.append(f"Поставщик: {supplier}.")
        if invoice:
            parts.append(f"Счёт: {invoice}.")
        parts.append("Во избежание дублирования импорт остановлен.")
        await m.answer(
            "\n".join(parts),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="supply")]]
            ),
        )
        await state.clear()
        return

    norm_csv = None
    normalized_stats = {}
    generated_csv_path = None
    normalized_hash_value = None

    if suffix == ".csv":
        norm_csv, normalized_stats = await asyncio.to_thread(botmod.csv_to_normalized_csv, str(dest))
        if not norm_csv:
            conv_info = f"Нормализованные строки: {normalized_stats.get('found',0)}\n"
            if normalized_stats.get('errors'):
                conv_info += "Ошибки нормализации:\n" + "\n".join("• "+e for e in normalized_stats['errors']) + "\n"
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
        if stats.get("imported"):
            items = normalized_stats.get("items", [])
            normalized_hash = await asyncio.to_thread(botmod.compute_sha256, norm_csv)
            normalized_hash_value = normalized_hash
            await asyncio.to_thread(
                botmod.record_import_log,
                original_name=original_name,
                stored_path=str(dest),
                import_type="csv",
                source_hash=source_hash,
                items=items,
                normalized_csv=norm_csv,
                normalized_hash=normalized_hash,
                supplier=None,
                invoice=None,
            )
    else:
        stats = await asyncio.to_thread(botmod.import_supply_from_excel, str(dest))
        if isinstance(stats, dict):
            normalized_stats = stats.pop("normalized_stats", {})
            generated_csv_path = stats.get("normalized_csv")
        else:
            normalized_stats = {}
            generated_csv_path = None
        if (not stats.get("imported")) and normalized_stats.get("found", 0) == 0:
            conv_info = "Нормализованные строки: 0\n"
            if normalized_stats.get("errors"):
                conv_info += "Ошибки нормализации:\n" + "\n".join("• "+e for e in normalized_stats['errors']) + "\n"
            if stats.get("errors"):
                conv_info += "Ошибки импорта:\n" + "\n".join("• "+e for e in stats['errors']) + "\n"
            await m.answer(
                "Не удалось распознать таблицу в Excel файле. Попробуйте прислать CSV.\n" + conv_info,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="← Назад", callback_data="supply")],
                    ]
                ),
            )
            await state.clear()
            return
        if stats.get("imported"):
            items = stats.get("items", []) or normalized_stats.get("items", [])
            supplier = stats.get("supplier") or normalized_stats.get("supplier")
            invoice = stats.get("invoice") or normalized_stats.get("invoice")
            normalized_hash = None
            if generated_csv_path:
                normalized_hash = await asyncio.to_thread(botmod.compute_sha256, generated_csv_path)
                normalized_hash_value = normalized_hash
            await asyncio.to_thread(
                botmod.record_import_log,
                original_name=original_name,
                stored_path=str(dest),
                import_type="excel",
                source_hash=source_hash,
                items=items,
                normalized_csv=generated_csv_path,
                normalized_hash=normalized_hash,
                supplier=supplier,
                invoice=invoice,
            )

    conv_info = f"Нормализованные строки: {normalized_stats.get('found',0)}\n"
    if normalized_stats.get('errors'):
        conv_info += "Ошибки нормализации:\n" + "\n".join("• "+e for e in normalized_stats['errors']) + "\n"

    text = (
        f"Импорт завершён.\n"
        f"Всего строк: {stats['imported']}\n"
        f"Создано: {stats['created']}\n"
        f"Обновлено: {stats['updated']}\n"
    )
    text += conv_info
    if norm_csv:
        text += f"Файл CSV: {norm_csv}\n"
    elif generated_csv_path:
        text += f"Файл CSV: {generated_csv_path}\n"
    text += f"Хэш источника: {source_hash[:16]}…\n"
    if normalized_hash_value:
        text += f"Хэш нормализованного CSV: {normalized_hash_value[:16]}…\n"
    if stats["errors"]:
        text += "Ошибки:\n" + "\n".join("• " + e for e in stats["errors"])
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗂️ Список новых", callback_data="supply_list|1")],
            [InlineKeyboardButton(text="← Назад", callback_data="supply")],
        ]
    )
    await m.answer(text, reply_markup=kb)
    if suffix != ".csv" and generated_csv_path:
        try:
            await m.answer_document(
                FSInputFile(generated_csv_path),
                caption="Нормализованный CSV из Excel",
            )
        except Exception:
            pass
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
