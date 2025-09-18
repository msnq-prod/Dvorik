from __future__ import annotations

import csv
import math
import re
from pathlib import Path
from typing import List, Optional, Tuple, Sequence

import hashlib
import json
import sys

import pandas as pd

try:
    import xlrd  # type: ignore
except ImportError:  # pragma: no cover - optional dependency for .xls support
    xlrd = None

try:  # pragma: no cover - optional dependency
    import xlrd2  # type: ignore
except ImportError:
    xlrd2 = None
else:
    if xlrd is None or getattr(xlrd, "__version__", "").startswith("2"):
        sys.modules.setdefault("xlrd", xlrd2)
        xlrd = xlrd2

from app import config as app_config
from app.db import db
from app.services.notify import log_event_to_skl
from app.services.archival import mark_restock


_ART_RX = re.compile(r'^\s*([A-Za-zА-Яа-я0-9\-\._/]+)\s+(.+)$')
_PACK_RX = re.compile(r'(\d+\s*(?:кг|гр|г)\s*[*xх]\s*\d+)', re.IGNORECASE)
COL_ART = {
    "артикул",
    "код",
    "артикул/код",
    "код товара",
    "sku",
    "код/артикул",
    "артикул товара",
}
COL_NAME = {
    "наименование",
    "товар",
    "название",
    "описание",
    "product",
    "item",
    "наименование товара",
    "товары (работы, услуги)",
    "товары (работы,услуги)",
    "товары(работы,услуги)",
    "товары(работы, услуги)",
    "товар/услуга",
}
COL_QTY = {
    "кол-во",
    "количество",
    "кол-во пачек",
    "кол-во мест",
    "мест",
    "количество, шт",
    "количество (шт)",
    "qty",
    "quantity",
    "qty, pcs",
}
COL_PRICE = {"цена", "price", "стоимость", "цен", "amount"}

_SERVICE_KEYWORDS = (
    "транспорт",
    "услуг",
    "логист",
    "достав",
    "экспед",
    "погруз",
    "разгруз",
    "комисс",
    "банк",
    "перевоз",
)


def _accumulate_row(
    rows_map: dict[str, list], order: list[str], article: str, name: str, qty: float
) -> None:
    if article not in rows_map:
        order.append(article)
        rows_map[article] = [article, name, 0.0]
    rows_map[article][2] += qty


_PREVIEW_MAX_ROWS = 200
_PREVIEW_MAX_COLS = 12


def _preview_cell(val) -> str:
    if val is None:
        return ""
    if isinstance(val, float):
        if math.isnan(val):  # type: ignore[arg-type]
            return ""
        if float(val).is_integer():
            return str(int(val))
        return ("%.6f" % float(val)).rstrip("0").rstrip(".")
    return str(val).strip()


def _build_preview_payload(
    df: pd.DataFrame,
    header: Optional[Sequence] = None,
    *,
    max_rows: int = _PREVIEW_MAX_ROWS,
    max_cols: int = _PREVIEW_MAX_COLS,
) -> dict:
    if header is None:
        header_seq = df.columns.tolist()
    else:
        header_seq = list(header)
    headers = [_preview_cell(v) for v in header_seq][:max_cols]
    if not headers:
        headers = [f"Колонка {i+1}" for i in range(min(max_cols, df.shape[1]))]
    rows: List[List[str]] = []
    limit = min(max_rows, len(df))
    for i in range(limit):
        row_vals = df.iloc[i].tolist()
        rows.append([_preview_cell(v) for v in row_vals][:max_cols])
    return {
        "headers": headers,
        "rows": rows,
        "total_rows": int(len(df)),
        "total_cols": int(df.shape[1]),
    }


def _extract_sheet_meta(df_raw: pd.DataFrame) -> dict:
    meta: dict[str, str] = {}
    limit = min(120, len(df_raw))
    for i in range(limit):
        row_vals = df_raw.iloc[i].tolist()
        normalized = [_norm_cell(v) for v in row_vals]
        lowered = [val.lower() for val in normalized]
        for text, low in zip(normalized, lowered):
            if not text:
                continue
            if "счет на оплату" in low and "№" in text and "invoice" not in meta:
                meta["invoice"] = text
        if "supplier" in meta:
            continue
        if any("поставщик" in low for low in lowered):
            idx = next((k for k, low in enumerate(lowered) if "поставщик" in low), None)
            supplier_value = None
            if idx is not None:
                for val in normalized[idx + 1:]:
                    if val and "поставщик" not in val.lower():
                        supplier_value = val
                        break
            if not supplier_value and i + 1 < limit:
                next_row = [_norm_cell(v) for v in df_raw.iloc[i + 1].tolist()]
                supplier_value = next((val for val in next_row if val), None)
            if supplier_value:
                meta["supplier"] = supplier_value
    return meta


def _norm_header(s: str) -> str:
    return re.sub(r'\s+', ' ', (s or "").strip().lower())


def _clean_name(raw_name: str) -> tuple[str, Optional[str]]:
    s = (raw_name or "").strip()
    brand = None
    if "/" in s:
        left, right = s.split("/", 1)
        s = left.strip()
        brand = right.strip() or None
    s = _PACK_RX.sub("", s)
    s = re.sub(r'\s{2,}', ' ', s).strip(" -–—\t")
    return s, brand


def _emptyish(val: Optional[str]) -> bool:
    if val is None:
        return True
    s = str(val).strip().lower()
    return s == "" or s in {"nan", "none", "null"}


def _detect_columns(df: pd.DataFrame):
    headers = {c: _norm_header(c) for c in df.columns}
    inv = {v: k for k, v in headers.items()}
    col_article = None
    for h in headers.values():
        if h in COL_ART:
            col_article = inv[h]
            break
    col_name = None
    for h in headers.values():
        if h in COL_NAME:
            col_name = inv[h]
            break
    if not col_name:
        col_name = df.columns[0]
    col_qty = None
    for h in headers.values():
        if h in COL_QTY:
            col_qty = inv[h]
            break
    if not col_qty:
        num_cols = [c for c in df.columns if str(df[c].dtype).startswith(("int", "float"))]
        col_qty = num_cols[0] if num_cols else None
    is_gordeeva = col_article is not None
    return col_article, col_name, col_qty, is_gordeeva


def _iter_excel_sheets_raw(path: str):
    ext = Path(path).suffix.lower()
    engines: List[Optional[str]]
    if ext in (".xlsx", ".xlsm", ".xltx", ".xltm"):
        engines = ["openpyxl"]
    elif ext == ".xls":
        if xlrd2 is not None:
            try:
                book = xlrd2.open_workbook(path)  # type: ignore[attr-defined]
                for sheet in book.sheets():
                    data = [sheet.row_values(i) for i in range(sheet.nrows)]
                    df = pd.DataFrame(data)
                    yield sheet.name, df
                return
            except Exception as e:
                last_err = e
                # fallback to pandas engines below
        engines = ["xlrd"]
    else:
        engines = [None, "openpyxl", "xlrd"]
    last_err = None
    for eng in engines:
        try:
            xls = pd.ExcelFile(path, engine=eng)
            for name in xls.sheet_names:
                df = xls.parse(name, header=None, dtype=object)
                yield name, df
            return
        except Exception as e:
            last_err = e
    if ext == ".xls":
        raise RuntimeError(
            "Для чтения .xls установите пакеты xlrd>=2.0.1 и xlrd2 (совместимый движок)"
        ) from last_err
    raise last_err


def _norm_cell(v) -> str:
    s = str(v if v is not None else "")
    s = s.replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _find_header_triplet(cells: List[str]) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    art_idx = name_idx = qty_idx = None
    for j, v in enumerate(cells):
        h = _norm_header(v)
        if h == "артикул" and art_idx is None:
            art_idx = j
        if (h in COL_NAME or "товар" in h) and name_idx is None:
            name_idx = j
        if (h in COL_QTY or h.startswith("кол-") or "кол" in h or "шт" in h) and qty_idx is None:
            qty_idx = j
    return art_idx, name_idx, qty_idx


def _looks_like_article(token: str) -> bool:
    t = _norm_cell(token)
    if not t:
        return False
    if not re.search(r"\d", t):
        return False
    if re.fullmatch(r"[A-Za-zА-Яа-я0-9\-_/\. ]{2,}", t):
        return True
    return False


def _locate_goods_section(df_raw: pd.DataFrame) -> tuple[Optional[int], Optional[int]]:
    tgt = "товарыработыуслуги"
    sec_row = None
    nrows = len(df_raw)
    for i in range(min(2000, nrows)):
        vals = df_raw.iloc[i].tolist()
        for v in vals:
            f = re.sub(r"[^a-zа-я0-9]+", "", (_norm_cell(v) or "").lower().replace("ё", "е"))
            if tgt in f:
                sec_row = i
                break
        if sec_row is not None:
            break
    if sec_row is None:
        header_guess = None
        for idx in range(min(2000, nrows)):
            vals_raw = df_raw.iloc[idx].tolist()
            normalized = [_norm_header(_norm_cell(v)) for v in vals_raw]
            has_name = any(
                any(key in cell for key in ("наимен", "товар", "product", "item"))
                for cell in normalized
            )
            has_qty = any(
                any(key in cell for key in ("кол", "qty", "quantity")) for cell in normalized
            )
            has_price = any(any(key in cell for key in COL_PRICE) for cell in normalized if cell)
            has_index = any(cell.startswith("№") or cell in {"no", "#", "n"} for cell in normalized if cell)
            if (has_name and has_qty) and (has_price or has_index):
                header_guess = idx
                break
        if header_guess is None:
            return None, None
        sec_row = header_guess - 1 if header_guess > 0 else header_guess
        hdr_row = header_guess
        return sec_row, hdr_row

    def row_has_headers(idx: int) -> bool:
        vals = [_norm_header(_norm_cell(v)) for v in df_raw.iloc[idx].tolist()]
        has_name = any(v in COL_NAME or ("товар" in v or "наимен" in v) for v in vals if v)
        has_qty = any(v in COL_QTY or v.startswith("кол-") or "кол" in v or "мест" in v or "шт" in v for v in vals if v)
        return has_name and has_qty
    for h in range(sec_row, min(sec_row + 6, nrows)):
        if row_has_headers(h):
            return sec_row, h
    return sec_row, None


def _unique_headers(cells: list[str]) -> list[str]:
    seen = {}
    out = []
    for idx, v in enumerate(cells):
        name = _norm_cell(v) or f"col{idx}"
        base = name
        k = 1
        while name in seen:
            k += 1
            name = f"{base}_{k}"
        seen[name] = True
        out.append(name)
    return out


def _infer_cols_no_header(df_block: pd.DataFrame) -> tuple[Optional[int], Optional[int], Optional[int]]:
    if df_block.empty:
        return None, None, None
    sample = min(50, len(df_block))
    ncols = df_block.shape[1]
    best = (-1, -1, -1)
    for j in range(0, max(0, ncols - 1)):
        qty_col = df_block.columns[j + 1]
        cnt_num = 0
        for i in range(sample):
            val = df_block.iloc[i][qty_col]
            if _to_float_qty(val) is not None:
                cnt_num += 1
        score = cnt_num
        if score > best[0]:
            best = (score, j, j + 1)
    if best[0] <= 0:
        return None, None, None
    name_idx, qty_idx = best[1], best[2]
    art_idx = None
    if name_idx - 1 >= 0:
        art_col = df_block.columns[name_idx - 1]
        cnt_art = 0
        cnt_nonempty = 0
        for i in range(sample):
            raw = _norm_cell(df_block.iloc[i][art_col])
            if raw:
                cnt_nonempty += 1
                if _looks_like_article(raw):
                    cnt_art += 1
        if cnt_nonempty > 0 and cnt_art / max(1, cnt_nonempty) >= 0.5:
            art_idx = name_idx - 1
    return name_idx, qty_idx, art_idx


def _write_normalized_csv(rows: List[Tuple[str, str, float]], base_name: str) -> str:
    safe_base = re.sub(r"[^A-Za-zА-Яа-я0-9_.\-]+", "_", base_name)
    out_path = app_config.NORMALIZED_DIR / f"{safe_base}.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["article", "name", "qty"])
        w.writeheader()
        for art, name, qty in rows:
            w.writerow({"article": art, "name": name, "qty": qty})
    return str(out_path)


def _empty_import_stats() -> dict:
    return {"imported": 0, "created": 0, "updated": 0, "errors": [], "to_skl": {}}


def _extract_excel_rows(path: str) -> Tuple[List[Tuple[str, str, float]], dict]:
    stats = {"found": 0, "errors": []}
    meta: dict[str, str] = {}
    rows_map: dict[str, list] = {}
    order: list[str] = []
    try:
        selected = None
        sel_hdr = None
        sel_hdr_vals = None
        preview_payload = None
        for sheet_name, raw in _iter_excel_sheets_raw(path):
            meta_candidate = _extract_sheet_meta(raw)
            for key, value in meta_candidate.items():
                meta.setdefault(key, value)
            sec_row, hdr_row = _locate_goods_section(raw)
            if sec_row is None:
                continue
            start_data = (hdr_row + 1) if hdr_row is not None else (sec_row + 1)
            block = raw.iloc[start_data:].copy()
            block = block.dropna(how="all")
            if block.empty:
                continue
            selected = block
            sel_hdr = hdr_row
            sel_hdr_vals = raw.iloc[hdr_row].tolist() if hdr_row is not None else None
            try:
                preview_source = raw.iloc[start_data : start_data + _PREVIEW_MAX_ROWS].copy()
            except Exception:
                preview_source = block.iloc[:_PREVIEW_MAX_ROWS].copy()
            if preview_source.empty:
                preview_source = block.iloc[:_PREVIEW_MAX_ROWS].copy()
            preview_payload = _build_preview_payload(
                preview_source.fillna(""),
                header=sel_hdr_vals,
            )
            if preview_payload is not None:
                preview_payload["sheet"] = sheet_name
                preview_payload["header_row"] = int(sel_hdr) if sel_hdr is not None else None
                preview_payload["start_row"] = int(start_data)
            break
        if selected is None:
            return [], stats

        if sel_hdr is not None:
            hdr_cells = [str(v) if v is not None else "" for v in (sel_hdr_vals or selected.iloc[0].tolist())]
            a_idx, n_idx, q_idx = _find_header_triplet(hdr_cells)
            if n_idx is not None and q_idx is not None:
                df = selected.reset_index(drop=True)
                name_col = df.columns[n_idx]
                qty_col = df.columns[q_idx]
                art_col = df.columns[a_idx] if a_idx is not None else None
            else:
                headers = _unique_headers([_norm_cell(v) for v in hdr_cells])
                df = selected.iloc[:, :len(headers)].copy()
                df.columns = headers
                col_art, col_name, col_qty, _ = _detect_columns(df)
                if not (col_name and col_qty):
                    df = selected.reset_index(drop=True)
                    name_idx, qty_idx, art_idx = _infer_cols_no_header(df)
                    if name_idx is None or qty_idx is None:
                        return [], stats
                    name_col = df.columns[name_idx]
                    qty_col = df.columns[qty_idx]
                    art_col = df.columns[art_idx] if art_idx is not None else None
                else:
                    name_col = col_name
                    qty_col = col_qty
                    art_col = col_art
        else:
            df = selected.reset_index(drop=True)
            name_idx, qty_idx, art_idx = _infer_cols_no_header(df)
            if name_idx is None or qty_idx is None:
                return [], stats
            name_col = df.columns[name_idx]
            qty_col = df.columns[qty_idx]
            art_col = df.columns[art_idx] if art_idx is not None else None

        empty_streak = 0
        for _, row in df.iterrows():
            raw_name = _norm_cell(row.get(name_col, ""))
            if not raw_name:
                empty_streak += 1
                if empty_streak >= 10:
                    break
                continue
            empty_streak = 0
            low = raw_name.lower()
            if any(x in low for x in ("итог", "всего", "итого")):
                break
            if any(kw in low for kw in _SERVICE_KEYWORDS):
                continue
            qty = _to_float_qty(row.get(qty_col))
            if qty is None or qty <= 0:
                continue
            art = None
            if art_col is not None:
                art = _norm_cell(row.get(art_col, "")) or None
            if not art or not _looks_like_article(art):
                m = _ART_RX.match(raw_name)
                if m:
                    art, raw_name = m.group(1), m.group(2)
            name, brand = _clean_name(raw_name)
            if _emptyish(art) or not _looks_like_article(art):
                continue
            if _emptyish(name) or not re.search(r"[A-Za-zА-Яа-я]", name):
                continue
            _accumulate_row(rows_map, order, art, name, float(qty))
        rows_out = [(rows_map[k][0], rows_map[k][1], rows_map[k][2]) for k in order]
        stats["found"] = len(rows_out)
        if meta.get("supplier"):
            stats["supplier"] = meta["supplier"]
        if meta.get("invoice"):
            stats["invoice"] = meta["invoice"]
        stats["items"] = rows_out
        if preview_payload is not None:
            stats["preview"] = preview_payload
        return rows_out, stats
    except Exception as e:
        stats["errors"].append(str(e))
        return [], stats


def excel_to_normalized_csv(path: str) -> Tuple[Optional[str], dict]:
    rows, stats = _extract_excel_rows(path)
    if not rows:
        return None, stats
    base_name = Path(path).stem
    out_csv = _write_normalized_csv(rows, base_name)
    return out_csv, stats


def csv_to_normalized_csv(path: str) -> Tuple[Optional[str], dict]:
    stats = {"found": 0, "errors": []}
    rows_map: dict[str, list] = {}
    order: list[str] = []
    base_name = Path(path).stem
    preview_df: Optional[pd.DataFrame] = None
    preview_header: Optional[Sequence] = None
    try:
        try:
            df = pd.read_csv(path)
            preview_df = df.head(_PREVIEW_MAX_ROWS).copy()
            preview_header = df.columns.tolist()
        except Exception:
            df = pd.read_csv(path, sep=';')
            preview_df = df.head(_PREVIEW_MAX_ROWS).copy()
            preview_header = df.columns.tolist()
        # эвристика на случай отсутствия шапки
        if df.shape[1] < 2 or any(str(c).strip() == '' for c in df.columns):
            # обработка как без шапки
            raw = pd.read_csv(path, header=None)
            df = raw
            preview_df = raw.head(_PREVIEW_MAX_ROWS).copy()
            preview_header = None
            name_idx, qty_idx, art_idx = _infer_cols_no_header(df)
            if name_idx is None or qty_idx is None:
                return None, stats
            name_col = df.columns[name_idx]
            qty_col = df.columns[qty_idx]
            art_col = df.columns[art_idx] if art_idx is not None else None
            empty_streak = 0
            for _, row in df.iterrows():
                raw_name = _norm_cell(row.get(name_col, ""))
                if not raw_name:
                    empty_streak += 1
                    if empty_streak >= 10:
                        break
                    continue
                empty_streak = 0
                low = raw_name.lower()
                if any(kw in low for kw in _SERVICE_KEYWORDS):
                    continue
                qty = _to_float_qty(row.get(qty_col))
                if qty is None or qty <= 0:
                    continue
                art = None
                if art_col is not None:
                    art = _norm_cell(row.get(art_col, "")) or None
                if not art or not _looks_like_article(art):
                    m = _ART_RX.match(raw_name)
                    if m:
                        art, raw_name = m.group(1), m.group(2)
                name, brand = _clean_name(raw_name)
                if _emptyish(art) or not _looks_like_article(art) or _emptyish(name) or not re.search(r"[A-Za-zА-Яа-я]", name):
                    continue
                _accumulate_row(rows_map, order, art, name, float(qty))
        else:
            headers = [str(c) for c in df.columns]
            col_art, col_name, col_qty, _ = _detect_columns(df)
            if col_name and col_qty:
                name_col = col_name
                qty_col = col_qty
                art_col = col_art
                empty_streak = 0
                for _, row in df.iterrows():
                    raw_name = _norm_cell(row.get(name_col, ""))
                    if not raw_name:
                        empty_streak += 1
                        if empty_streak >= 10:
                            break
                        continue
                    empty_streak = 0
                    low = raw_name.lower()
                    if any(kw in low for kw in _SERVICE_KEYWORDS):
                        continue
                    qty = _to_float_qty(row.get(qty_col))
                    if qty is None or qty <= 0:
                        continue
                    art = None
                    if art_col is not None:
                        art = _norm_cell(row.get(art_col, "")) or None
                    if not art or not _looks_like_article(art):
                        m = _ART_RX.match(raw_name)
                        if m:
                            art, raw_name = m.group(1), m.group(2)
                    name, brand = _clean_name(raw_name)
                    if _emptyish(art) or not _looks_like_article(art) or _emptyish(name) or not re.search(r"[A-Za-zА-Яа-я]", name):
                        continue
                    _accumulate_row(rows_map, order, art, name, float(qty))
            else:
                raw = pd.read_csv(path, header=None)
                df = raw
                name_idx, qty_idx, art_idx = _infer_cols_no_header(df)
                if name_idx is None or qty_idx is None:
                    return None, stats
                name_col = df.columns[name_idx]
                qty_col = df.columns[qty_idx]
                art_col = df.columns[art_idx] if art_idx is not None else None
                empty_streak = 0
                for _, row in df.iterrows():
                    raw_name = _norm_cell(row.get(name_col, ""))
                    if not raw_name:
                        empty_streak += 1
                        if empty_streak >= 10:
                            break
                        continue
                    empty_streak = 0
                    low = raw_name.lower()
                    if any(kw in low for kw in _SERVICE_KEYWORDS):
                        continue
                    qty = _to_float_qty(row.get(qty_col))
                    if qty is None or qty <= 0:
                        continue
                    art = None
                    if art_col is not None:
                        art = _norm_cell(row.get(art_col, "")) or None
                    if _emptyish(art):
                        m = _ART_RX.match(raw_name)
                        if m:
                            art, raw_name = m.group(1), m.group(2)
                    name, brand = _clean_name(raw_name)
                    if _emptyish(art) or not _looks_like_article(art) or _emptyish(name) or not re.search(r"[A-Za-zА-Яа-я]", name):
                        continue
                    _accumulate_row(rows_map, order, art, name, float(qty))

        rows_out = [(rows_map[k][0], rows_map[k][1], rows_map[k][2]) for k in order]
        stats["found"] = len(rows_out)
        stats["items"] = rows_out
        if preview_df is None:
            preview_df = df.head(_PREVIEW_MAX_ROWS).copy()
            preview_header = df.columns.tolist()
        stats["preview"] = _build_preview_payload(
            preview_df.fillna(""),
            header=preview_header,
        )
        if not rows_out:
            return None, stats
        out_csv = _write_normalized_csv(rows_out, base_name)
        return out_csv, stats
    except Exception as e:
        stats["errors"].append(str(e))
        return None, stats


def _import_article_rows(rows: List[Tuple[str, str, float]], *, err_prefix: str, start_index: int) -> dict:
    stats = _empty_import_stats()
    conn = db()
    row_idx = start_index - 1
    try:
        for art_raw, name_raw, qty_raw in rows:
            row_idx += 1
            try:
                art = (art_raw or "").strip()
                name = (name_raw or "").strip()
                qty = _to_float_qty(qty_raw)
                if _emptyish(art) or _emptyish(name) or qty is None or qty <= 0:
                    continue
                if not _looks_like_article(art):
                    continue
                with conn:
                    cur = conn.execute(
                        "INSERT OR IGNORE INTO product(article, name, is_new) VALUES (?,?,1)",
                        (art, name),
                    )
                    pid = conn.execute("SELECT id FROM product WHERE article=?", (art,)).fetchone()["id"]
                    if (cur.rowcount or 0) > 0:
                        stats["created"] += 1
                    else:
                        conn.execute(
                            "UPDATE product SET name = COALESCE(NULLIF(name,''), ?) WHERE id=?",
                            (name, pid),
                        )
                        stats["updated"] += 1
                    prow = conn.execute(
                        "SELECT name, local_name FROM product WHERE id=?",
                        (pid,),
                    ).fetchone()
                    conn.execute(
                        """
                        INSERT INTO stock(product_id, location_code, qty_pack, name, local_name)
                        VALUES (?,?,?,?,?)
                        ON CONFLICT(product_id, location_code)
                        DO UPDATE SET
                            qty_pack = qty_pack + excluded.qty_pack,
                            name = excluded.name,
                            local_name = excluded.local_name
                        """,
                        (pid, "SKL-0", float(qty), prow["name"], prow["local_name"]),
                    )
                    log_event_to_skl(conn, pid, "SKL-0", float(qty))
                    mark_restock(conn, pid)
                    stats["to_skl"][pid] = stats["to_skl"].get(pid, 0) + float(qty)
                stats["imported"] += 1
            except Exception as e_row:
                stats["errors"].append(f"{err_prefix} {row_idx}: {e_row}")
                continue
        return stats
    finally:
        conn.close()


def import_supply_from_normalized_csv(path: str) -> dict:
    try:
        with open(path, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            if r.fieldnames is None or set(map(str.lower, r.fieldnames)) != {"article", "name", "qty"}:
                stats = _empty_import_stats()
                stats["errors"].append("CSV должен иметь колонки article,name,qty (utf-8)")
                return stats
            rows: List[Tuple[str, str, float]] = []
            for row in r:
                rows.append((row.get("article", ""), row.get("name", ""), row.get("qty", "")))
    except Exception as e:
        stats = _empty_import_stats()
        stats["errors"].append(str(e))
        return stats
    return _import_article_rows(rows, err_prefix="CSV строка", start_index=2)


def import_supply_from_excel(path: str) -> dict:
    rows, normalize_stats = _extract_excel_rows(path)
    if not rows:
        stats = _empty_import_stats()
        if normalize_stats["errors"]:
            stats["errors"].extend(normalize_stats["errors"])
        else:
            stats["errors"].append("Не удалось найти товары в Excel файле")
        stats["normalized_stats"] = normalize_stats
        return stats
    normalized_csv = _write_normalized_csv(rows, Path(path).stem)
    stats = _import_article_rows(rows, err_prefix="Excel позиция", start_index=1)
    stats["normalized_stats"] = normalize_stats
    stats["normalized_csv"] = normalized_csv
    stats["supplier"] = normalize_stats.get("supplier")
    stats["invoice"] = normalize_stats.get("invoice")
    stats["items"] = rows
    return stats


def import_supply_rows(rows: Sequence[Tuple[str, str, float]]) -> dict:
    """Import already-normalized rows into stock.

    This is a thin facade used by the admin UI where rows are already validated
    on the client. It mirrors the behaviour of importing from a normalized CSV,
    including auto-creating products and logging restocks.
    """

    return _import_article_rows(list(rows), err_prefix="Row", start_index=1)


def _to_float_qty(val) -> Optional[float]:
    if val is None:
        return None
    s = str(val).strip()
    if s == "":
        return None
    s = s.replace(" ", "")
    s = s.replace(",", ".")
    try:
        f = float(s)
    except Exception:
        return None
    if not math.isfinite(f):
        return None
    return f


def compute_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def check_import_duplicate(source_hash: str) -> Optional[dict]:
    conn = db()
    row = conn.execute(
        "SELECT id, original_name, stored_path, created_at, supplier, invoice FROM import_log WHERE source_hash=?",
        (source_hash,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def record_import_log(
    *,
    original_name: str,
    stored_path: str,
    import_type: str,
    source_hash: str,
    items: List[Tuple[str, str, float]],
    normalized_csv: Optional[str],
    normalized_hash: Optional[str],
    supplier: Optional[str],
    invoice: Optional[str],
) -> None:
    payload = json.dumps(
        [
            {"article": art, "name": name, "qty": qty}
            for art, name, qty in items
        ],
        ensure_ascii=False,
    )
    conn = db()
    with conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO import_log(
                original_name,
                stored_path,
                import_type,
                source_hash,
                normalized_csv,
                normalized_hash,
                supplier,
                invoice,
                items_count,
                items_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                original_name,
                stored_path,
                import_type,
                source_hash,
                normalized_csv,
                normalized_hash,
                supplier,
                invoice,
                len(items),
                payload,
            ),
        )
    conn.close()
