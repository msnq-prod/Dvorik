from __future__ import annotations

import datetime as dt
import json
import secrets
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from flask import Blueprint, current_app, jsonify, render_template, request

from app import config as app_config
from app import constants as const
from app import db as adb
from app import utils_files
from app import utils_number
from app.services import imports as import_svc
from app.services import supply_session as session_svc


bp = Blueprint("supply", __name__)

SESSION_TTL_SECONDS = 60 * 30

# TODO[2024-09-30]: drop compatibility aliases after staged migration.
_sanitize_filename = utils_files.sanitize_filename
_parse_qty = utils_number.to_float_qty
_write_normalized_csv = import_svc.write_normalized_csv


def _purge_sessions(conn) -> None:
    for session in session_svc.purge_expired(conn, SESSION_TTL_SECONDS):
        _cleanup_session_files(session)


def _cleanup_session_files(session: Dict[str, Any], keep_preview: bool = False) -> None:
    for key in ("stored_path", "preview_normalized_path"):
        if keep_preview and key == "preview_normalized_path":
            continue
        path = session.get(key)
        if not path:
            continue
        try:
            Path(path).unlink()
        except FileNotFoundError:
            continue
        except Exception:
            continue


def _discard_session(conn, token: str, *, keep_files: bool = False) -> None:
    session = session_svc.get(conn, token)
    if not session:
        return
    session_svc.delete(conn, token)
    if keep_files:
        return
    _cleanup_session_files(session)


def _build_normalized_rows(rows: Sequence[Tuple[str, str, float]]) -> List[Dict[str, Any]]:
    normalized_rows: List[Dict[str, Any]] = []
    for art, name, qty in rows:
        qty_val = _parse_qty(qty)
        normalized_rows.append(
            {
                "article": str(art or "").strip(),
                "name": str(name or "").strip(),
                "qty": qty_val if qty_val is not None else qty,
            }
        )
    return normalized_rows


def _supply_error(message: str, status: int = 400, **extra):
    payload: Dict[str, Any] = {"success": False, "message": message}
    if extra:
        payload.update(extra)
    return jsonify(payload), status


def _get_signature_exceptions() -> List[Dict[str, Any]]:
    with adb.db() as conn:
        rows = conn.execute(
            "SELECT id, phrase FROM display_name_exception ORDER BY lower(phrase)"
        ).fetchall()
        return [{"id": row["id"], "phrase": row["phrase"]} for row in rows]


@bp.route("/supply", endpoint="supply_page")
def supply_page():
    max_size = current_app.config.get("MAX_CONTENT_LENGTH")
    last_import = None
    with adb.db() as conn:
        row = conn.execute(
            """
            SELECT id, original_name, supplier, invoice, created_at, items_count, items_json
            FROM import_log
            WHERE reverted_at IS NULL
            ORDER BY datetime(created_at) DESC
            LIMIT 1
            """
        ).fetchone()
        if row:
            try:
                items_data = json.loads(row["items_json"]) if row["items_json"] else []
            except Exception:
                items_data = []
            last_import = {
                "id": row["id"],
                "original_name": row["original_name"],
                "supplier": row["supplier"],
                "invoice": row["invoice"],
                "created_at": row["created_at"],
                "items_count": row["items_count"],
                "items": items_data,
            }
    return render_template(
        "supply.html",
        last_import=last_import,
        max_upload_size=max_size,
        allowed_exts=sorted(const.SUPPLY_ALLOWED_EXTS),
        signature_exceptions=_get_signature_exceptions(),
    )


@bp.route("/supply/signature-exceptions", methods=["POST"], endpoint="supply_add_signature_exception")
def supply_add_signature_exception():
    payload = request.get_json(silent=True) or {}
    phrase = (payload.get("phrase") or "").strip()
    if not phrase:
        return _supply_error("Введите исключение для отображения")

    created = True
    try:
        with adb.db() as conn:
            with conn:
                conn.execute(
                    "INSERT INTO display_name_exception(phrase) VALUES (?)",
                    (phrase,),
                )
    except sqlite3.IntegrityError:
        created = False
    except Exception as exc:
        return _supply_error(f"Не удалось сохранить исключение: {exc}")

    exceptions = _get_signature_exceptions()
    return jsonify({"success": True, "created": created, "exceptions": exceptions})


@bp.route("/supply/preview", methods=["POST"], endpoint="supply_preview")
def supply_preview():
    file = request.files.get("file")
    if file is None or not file.filename:
        return _supply_error("Выберите файл поставки")

    original_name = file.filename
    suffix = Path(original_name).suffix.lower()
    if suffix not in const.SUPPLY_ALLOWED_EXTS:
        return _supply_error("Поддерживаются файлы CSV и Excel (.xls/.xlsx)")

    safe_name = _sanitize_filename(original_name)
    base_name = Path(safe_name).stem or "upload"
    unique_suffix = secrets.token_hex(4)
    stored_filename = f"{base_name}_{unique_suffix}{suffix}"
    dest_path = app_config.UPLOAD_DIR / stored_filename

    try:
        app_config.UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        file.save(dest_path)
    except Exception as exc:
        return _supply_error(f"Не удалось сохранить файл: {exc}")

    try:
        source_hash = import_svc.compute_sha256(str(dest_path))
        duplicate = import_svc.check_import_duplicate(source_hash)
        if duplicate:
            try:
                Path(dest_path).unlink()
            except FileNotFoundError:
                pass
            details: List[str] = ["Этот файл уже импортирован."]
            if duplicate.get("created_at"):
                details.append(f"Дата импорта: {duplicate['created_at']}.")
            if duplicate.get("supplier"):
                details.append(f"Поставщик: {duplicate['supplier']}.")
            if duplicate.get("invoice"):
                details.append(f"Счёт: {duplicate['invoice']}.")
            details.append("Во избежание дублирования импорт остановлен.")
            return _supply_error(" ".join(details), status=409, duplicate=True)

        if suffix == ".csv":
            norm_csv_path, stats = import_svc.csv_to_preview(str(dest_path))
            rows = stats.get("rows", []) if isinstance(stats, dict) else []
            pointer = None
            needs_mapping = False
        else:
            rows, stats = import_svc.excel_preview(str(dest_path))
            pointer = stats.get("sheet_pointer") if isinstance(stats, dict) else None
            needs_mapping = bool(stats.get("needs_mapping")) if isinstance(stats, dict) else False
            norm_csv_path = stats.get("preview_csv") if isinstance(stats, dict) else None

    except Exception as exc:
        try:
            Path(dest_path).unlink()
        except FileNotFoundError:
            pass
        return _supply_error(f"Ошибка обработки файла: {exc}")

    errors = stats.get("errors") if isinstance(stats, dict) else []
    warnings_list = stats.get("warnings") if isinstance(stats, dict) else []
    rows = list(rows or [])
    if not rows:
        message = "Не удалось получить строки из файла"
        if errors:
            message += ": " + "; ".join(errors)
        return _supply_error(message)

    preview_payload = stats.get("preview") if isinstance(stats, dict) else None
    if not preview_payload:
        preview_payload = {
            "headers": ["Артикул", "Название", "Количество"],
            "rows": [[a, n, str(q)] for a, n, q in rows[:20]],
            "total_rows": len(rows),
            "total_cols": 3,
        }

    normalized_rows = _build_normalized_rows(rows)

    token = secrets.token_urlsafe(16)

    with adb.db() as conn:
        _purge_sessions(conn)
        session_svc.create(
            conn,
            token,
            {
                "created_at": _now_ts(),
                "original_name": original_name,
                "stored_path": str(dest_path),
                "source_hash": source_hash,
                "import_type": "excel" if suffix != ".csv" else "csv",
                "preview_normalized_path": norm_csv_path,
                "base_name": base_name,
                "initial_rows": normalized_rows,
                "sheet_pointer": pointer,
                "needs_mapping": needs_mapping,
                "supplier": stats.get("supplier"),
                "invoice": stats.get("invoice"),
            },
        )

    response_payload = {
        "success": True,
        "token": token,
        "original": preview_payload,
        "normalized": normalized_rows,
        "found": int(stats.get("found", len(rows))) if isinstance(stats, dict) else len(rows),
        "supplier": stats.get("supplier"),
        "invoice": stats.get("invoice"),
        "warnings": warnings_list,
        "source_hash": source_hash,
        "original_name": original_name,
        "needs_mapping": needs_mapping,
    }
    return jsonify(response_payload)


@bp.route("/supply/preview/mapping", methods=["POST"], endpoint="supply_preview_mapping")
def supply_preview_mapping():
    payload = request.get_json(silent=True) or {}
    token = payload.get("token")
    columns_payload = payload.get("columns") or payload.get("mapping")
    if not token or not isinstance(columns_payload, dict):
        return _supply_error("Передайте токен сессии и выбранные колонки")

    with adb.db() as conn:
        _purge_sessions(conn)
        session = session_svc.get(conn, token)
        if not session:
            return _supply_error("Сессия поставки не найдена или устарела", status=410)
        pointer = session.get("sheet_pointer")
        if not isinstance(pointer, dict):
            return _supply_error("Для этой сессии недоступно ручное сопоставление колонок")

        stored_path = session.get("stored_path")
        if not stored_path or not Path(stored_path).exists():
            _discard_session(conn, token)
            return _supply_error("Исходный файл поставки недоступен", status=410)

        try:
            rows, stats = import_svc.excel_preview_with_mapping(
                str(stored_path),
                column_mapping=dict(columns_payload),
                sheet_path=dict(pointer),
            )
        except Exception as exc:
            return _supply_error(f"Ошибка обработки файла: {exc}")

        errors = stats.get("errors") if isinstance(stats, dict) else []
        warnings_list = stats.get("warnings") if isinstance(stats, dict) else []
        rows = list(rows or [])
        if not rows:
            message = "Не удалось нормализовать строки с указанными колонками"
            if errors:
                message += ": " + "; ".join(errors)
            return _supply_error(message)

        preview_payload = stats.get("preview") if isinstance(stats, dict) else None
        if not preview_payload:
            preview_payload = {
                "headers": ["Артикул", "Название", "Количество"],
                "rows": [[a, n, str(q)] for a, n, q in rows[:20]],
                "total_rows": len(rows),
                "total_cols": 3,
            }

        normalized_rows = _build_normalized_rows(rows)

        try:
            preview_csv_path = _write_normalized_csv(
                rows,
                session.get("base_name") or Path(stored_path).stem,
            )
        except Exception as exc:
            return _supply_error(f"Не удалось подготовить предварительный CSV: {exc}")

        old_preview_norm = session.get("preview_normalized_path")
        if old_preview_norm and old_preview_norm != preview_csv_path:
            try:
                Path(old_preview_norm).unlink()
            except FileNotFoundError:
                pass

        new_pointer = stats.get("sheet_pointer") if isinstance(stats, dict) else None
        session_svc.update(
            conn,
            token,
            preview_normalized_path=preview_csv_path,
            initial_rows=normalized_rows,
            sheet_pointer=dict(new_pointer) if isinstance(new_pointer, dict) else pointer,
            needs_mapping=False,
            supplier=stats.get("supplier") or session.get("supplier"),
            invoice=stats.get("invoice") or session.get("invoice"),
        )

    response_payload = {
        "success": True,
        "token": token,
        "original": preview_payload,
        "normalized": normalized_rows,
        "found": int(stats.get("found", len(rows))) if isinstance(stats, dict) else len(rows),
        "supplier": stats.get("supplier") or session.get("supplier"),
        "invoice": stats.get("invoice") or session.get("invoice"),
        "warnings": warnings_list,
        "source_hash": session.get("source_hash"),
        "original_name": session.get("original_name"),
        "needs_mapping": False,
    }
    return jsonify(response_payload)


@bp.route("/supply/confirm", methods=["POST"], endpoint="supply_confirm")
def supply_confirm():
    payload = request.get_json(silent=True) or {}
    token = payload.get("token")
    if not token:
        return _supply_error("Нет токена сессии")

    with adb.db() as conn:
        session = session_svc.get(conn, token)
        if not session:
            return _supply_error("Сессия не найдена или истекла", status=410)

        rows = session.get("initial_rows") or []
        if not rows:
            return _supply_error("Нет данных для импорта")

        normalized_rows: List[Tuple[str, str, float]] = []
        rows_map: Dict[str, Dict[str, Any]] = {}
        order: List[str] = []
        for item in rows:
            art = str(item.get("article", "")).strip()
            name = str(item.get("name", "")).strip()
            qty_val = _parse_qty(item.get("qty"))
            if not art or qty_val is None:
                continue
            key = art
            if key not in rows_map:
                rows_map[key] = {"article": art, "name": name, "qty": 0.0, "articles": set([art])}
                order.append(key)
            row = rows_map[key]
            row["articles"].add(art)
            if name and (not row["name"] or len(name) > len(row["name"] or "")):
                row["name"] = name
            row["qty"] += qty_val

        final_rows: List[Tuple[str, str, float]] = []
        for key in order:
            row = rows_map[key]
            article = (row.get("article") or "").strip()
            if not article:
                continue
            final_rows.append((article, row.get("name", ""), row.get("qty", 0.0)))

        if not final_rows:
            return _supply_error("Нет строк для импорта")

        duplicate = import_svc.check_import_duplicate(session["source_hash"])
        if duplicate:
            _discard_session(conn, token)
            details = ["Этот файл уже импортирован другим пользователем."]
            if duplicate.get("created_at"):
                details.append(f"Дата: {duplicate['created_at']}.")
            return _supply_error(" ".join(details), status=409, duplicate=True)

        stats = import_svc.import_supply_rows(final_rows)
        if stats.get("errors"):
            return _supply_error("Ошибки при импорте: " + "; ".join(stats["errors"]))

        preview_norm = session.get("preview_normalized_path")
        supplier = payload.get("supplier") or session.get("supplier")
        invoice = payload.get("invoice") or session.get("invoice")

        base_name = session.get("base_name") or Path(session["stored_path"]).stem
        unique_base = f"{base_name}_{dt.datetime.now().strftime('%Y%m%d%H%M%S')}_{token[:6]}"
        normalized_csv_path = _write_normalized_csv(final_rows, unique_base)
        normalized_hash = import_svc.compute_sha256(normalized_csv_path)

        import_svc.record_import_log(
            original_name=session["original_name"],
            stored_path=session["stored_path"],
            import_type=session["import_type"],
            source_hash=session["source_hash"],
            items=final_rows,
            normalized_csv=normalized_csv_path,
            normalized_hash=normalized_hash,
            supplier=supplier,
            invoice=invoice,
        )

        if preview_norm and Path(preview_norm) != Path(normalized_csv_path):
            try:
                Path(preview_norm).unlink()
            except FileNotFoundError:
                pass

        session_svc.update(conn, token, committed=True)
        _cleanup_session_files(session, keep_preview=True)
        session_svc.delete(conn, token)

    response_payload = {
        "success": True,
        "stats": {
            "imported": stats.get("imported", 0),
            "created": stats.get("created", 0),
            "updated": stats.get("updated", 0),
        },
        "supplier": supplier,
        "invoice": invoice,
        "normalized_csv": normalized_csv_path,
    }
    return jsonify(response_payload)


@bp.route("/supply/cancel", methods=["POST"], endpoint="supply_cancel")
def supply_cancel():
    payload = request.get_json(silent=True) or {}
    token = payload.get("token")
    if not token:
        return jsonify({"success": True})
    with adb.db() as conn:
        _discard_session(conn, token)
    return jsonify({"success": True})


@bp.route("/supply/revert", methods=["POST"], endpoint="supply_revert")
def supply_revert():
    with adb.db() as conn:
        row = conn.execute(
            """
            SELECT id, items_json
            FROM import_log
            WHERE reverted_at IS NULL
            ORDER BY datetime(created_at) DESC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return _supply_error("Нет поставок для отмены", status=404)
        try:
            items = json.loads(row["items_json"]) if row["items_json"] else []
        except Exception:
            items = []

        insufficient: List[Dict[str, Any]] = []
        adjustments: List[Tuple[int, float]] = []
        for item in items:
            art = str(item.get("article", "")).strip()
            qty_val = _parse_qty(item.get("qty"))
            if not art or qty_val is None or qty_val <= 0:
                continue
            prow = conn.execute(
                "SELECT id, COALESCE(local_name, name) AS disp_name FROM product WHERE article=?",
                (art,),
            ).fetchone()
            if not prow:
                insufficient.append({"article": art, "reason": "товар удалён"})
                continue
            stock_row = conn.execute(
                "SELECT qty_pack FROM stock WHERE product_id=? AND location_code=?",
                (prow["id"], const.HUB_LOCATION_CODE),
            ).fetchone()
            current_qty = stock_row["qty_pack"] if stock_row else 0.0
            if current_qty is None:
                current_qty = 0.0
            if current_qty < qty_val:
                insufficient.append(
                    {
                        "article": art,
                        "reason": f"на складе {current_qty}, нужно {qty_val}",
                        "product": prow["disp_name"],
                    }
                )
                continue
            adjustments.append((int(prow["id"]), qty_val))

        if insufficient:
            return _supply_error(
                "Товар уже разложен, верните его в склад - 0 и повторите попытку",
                status=409,
                details=insufficient,
            )

        with conn:
            for pid, qty_val in adjustments:
                conn.execute(
                    "UPDATE stock SET qty_pack = qty_pack - ? WHERE product_id=? AND location_code=?",
                    (float(qty_val), pid, const.HUB_LOCATION_CODE),
                )
                conn.execute(
                    "DELETE FROM stock WHERE product_id=? AND location_code=? AND qty_pack<=0.000001",
                    (pid, const.HUB_LOCATION_CODE),
                )
            conn.execute(
                "UPDATE import_log SET reverted_at=datetime('now','localtime') WHERE id=?",
                (row["id"],),
            )

    return jsonify({"success": True})


def _now_ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())


__all__ = [
    "bp",
    "supply_page",
    "supply_preview",
    "supply_preview_mapping",
    "supply_confirm",
    "supply_cancel",
    "supply_revert",
    "supply_add_signature_exception",
]
