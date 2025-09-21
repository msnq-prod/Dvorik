import importlib
import sys
from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture()
def imports_module(tmp_path, monkeypatch):
    cfg = tmp_path / "config.json"
    cfg.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("CONFIG_PATH", str(cfg))

    repo_root = Path(__file__).resolve().parents[1]
    monkeypatch.syspath_prepend(str(repo_root))

    for mod in [m for m in list(sys.modules) if m == "app" or m.startswith("app.")]:
        sys.modules.pop(mod, None)

    return importlib.import_module("app.services.imports")


def _sample_path(name: str) -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    return repo_root / "data" / "uploads" / name


def test_extract_excel_rows_gordeeva(imports_module):
    sample = _sample_path("Счет на оплату № 16791 от 26.06.2025.xls")
    if not sample.exists():
        pytest.skip("Sample invoice file is not available in the repository")

    rows, stats = imports_module._extract_excel_rows(str(sample))

    assert stats["errors"] == []
    assert stats.get("warnings") == []
    assert stats["found"] == len(rows) == len({art for art, _, _ in rows}) == 71
    first_art, first_name, first_qty = rows[0]
    assert first_art == "AG-B/3127"
    assert "суфле" in first_name.lower()
    assert first_qty == 2.0
    assert all("услуг" not in name.lower() for _, name, _ in rows)
    assert rows[-1] == ("111000521", 'Мармелад жев."Осьминоги"', 2.0)


def test_extract_excel_rows_marmeladland(imports_module):
    sample = _sample_path("Счет на оплату (1).xls")
    if not sample.exists():
        pytest.skip("Sample invoice file is not available in the repository")

    rows, stats = imports_module._extract_excel_rows(str(sample))

    assert stats["errors"] == []
    assert stats.get("warnings") == []
    assert stats["found"] == len(rows) == 13
    assert rows[0] == ("1013208", "Мармелад Анаконда 1 кг (12)", 10.0)
    assert rows[-1] == ("1150019", "Мармелад Джелли бинс 1 кг (12)", 4.0)


def test_extract_excel_rows_manual_mapping(imports_module, tmp_path):
    sample_path = tmp_path / "manual_sample.xlsx"
    data = [
        ["№", "Артикул", "Наименование товара", "Кол-во"],
        [1, "SKU-001", "Маршмеллоу Клубника", 5],
        [2, "SKU-002", "Маршмеллоу Ваниль", 3],
    ]
    df = pd.DataFrame(data)
    df.to_excel(sample_path, header=False, index=False)

    auto_rows, auto_stats = imports_module._extract_excel_rows(str(sample_path))

    pointer = auto_stats.get("sheet_pointer")
    assert pointer is not None
    headers = pointer.get("header_values")
    assert headers, "header values should be provided for manual mapping"

    def find_index(keyword: str) -> int:
        keyword = keyword.lower()
        for idx, value in enumerate(headers):
            if keyword in str(value).lower():
                return idx
        raise AssertionError(f"keyword {keyword!r} not found in headers {headers}")

    art_idx = find_index("артик")
    name_idx = find_index("наимен")
    qty_idx = find_index("кол")

    manual_mapping = {
        "article": art_idx,
        "name": headers[name_idx],
        "qty": qty_idx,
    }

    manual_rows, manual_stats = imports_module._extract_excel_rows(
        str(sample_path),
        column_mapping=manual_mapping,
        sheet_path=pointer,
    )

    assert manual_rows == auto_rows
    assert manual_stats.get("preview", {}).get("sheet") == pointer.get("sheet")
    assert manual_stats.get("preview", {}).get("start_row") == pointer.get("start_row")
    assert manual_stats.get("sheet_pointer", {}).get("header_values")[: len(headers)] == list(headers)

    rows, stats = imports_module._extract_excel_rows(str(sample_path), sheet_path=pointer)

    assert stats["errors"] == []
    assert stats["found"] == 2
    assert rows == [
        ("SKU-001", "Маршмеллоу Клубника", 5.0),
        ("SKU-002", "Маршмеллоу Ваниль", 3.0),
    ]


def test_extract_excel_rows_with_code_header(imports_module, tmp_path):
    sample_path = tmp_path / "code_header.xlsx"
    data = [
        ["№", "Код", "Наименование", "Кол-во"],
        [1, "SKU-001", "Маршмеллоу Клубника", 5],
        [2, "SKU-002", "Маршмеллоу Ваниль", 3],
    ]
    df = pd.DataFrame(data)
    df.to_excel(sample_path, header=False, index=False)

    rows, stats = imports_module._extract_excel_rows(str(sample_path))

    assert stats["errors"] == []
    assert stats["found"] == 2
    assert rows == [
        ("SKU-001", "Маршмеллоу Клубника", 5.0),
        ("SKU-002", "Маршмеллоу Ваниль", 3.0),
    ]


def test_accumulate_rows_uses_name_key(imports_module):
    rows_map = {}
    order = []

    imports_module._accumulate_row(rows_map, order, "AG-B/100", "Суфле Персик", 1.0)
    imports_module._accumulate_row(rows_map, order, "AG-B/200", "Суфле Персик", 2.5)

    key = imports_module._name_key("Суфле Персик")
    assert order == [key]
    row = rows_map[key]
    assert row["qty"] == pytest.approx(3.5)
    assert row["article"] == "AG-B/100"
    assert row["articles"] == {"AG-B/100", "AG-B/200"}

    imports_module._accumulate_row(rows_map, order, "AG-B/100", "Суфле Яблоко", 1.0)
    second_key = imports_module._name_key("Суфле Яблоко")
    assert second_key in rows_map
    assert order == [key, second_key]
