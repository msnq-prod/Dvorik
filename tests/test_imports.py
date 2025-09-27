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


@pytest.fixture()
def imports_with_db(tmp_path, monkeypatch):
    cfg = tmp_path / "config.json"
    cfg.write_text("{}", encoding="utf-8")
    db_path = tmp_path / "imports.sqlite3"
    monkeypatch.setenv("CONFIG_PATH", str(cfg))
    monkeypatch.setenv("DB_PATH", str(db_path))

    repo_root = Path(__file__).resolve().parents[1]
    monkeypatch.syspath_prepend(str(repo_root))

    for mod in [m for m in list(sys.modules) if m == "app" or m.startswith("app.")]:
        sys.modules.pop(mod, None)

    db_module = importlib.import_module("app.db")
    db_module.init_db()
    imports_module = importlib.import_module("app.services.imports")
    return imports_module, db_module


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


def test_extract_excel_rows_quantity_with_text(imports_module, tmp_path):
    sample_path = tmp_path / "quantity_text.xlsx"
    data = [
        ["№", "Артикул", "Наименование товара", "Кол-во"],
        [1, "SKU-001", "Маршмеллоу Клубника", "1 пакет (1 кг)"],
        [2, "SKU-002", "Маршмеллоу Ваниль", "2 коробки"],
    ]
    df = pd.DataFrame(data)
    df.to_excel(sample_path, header=False, index=False)

    rows, stats = imports_module._extract_excel_rows(str(sample_path))

    assert stats["errors"] == []
    assert stats["found"] == 2
    assert rows == [
        ("SKU-001", "Маршмеллоу Клубника", 1.0),
        ("SKU-002", "Маршмеллоу Ваниль", 2.0),
    ]


def test_extract_excel_rows_quantity_with_units(imports_module, tmp_path):
    sample_path = tmp_path / "quantity_units.xlsx"
    data = [
        ["№", "Артикул", "Наименование товара", "Кол-во"],
        [1, "SKU-010", "Маршмеллоу Персик", "пакет 1 кг"],
        [2, "SKU-011", "Маршмеллоу Апельсин", "масса ~ 0,75 кг"],
        [3, "SKU-012", "Маршмеллоу Лимон", "вес нетто: 1 250,5 г"],
    ]
    df = pd.DataFrame(data)
    df.to_excel(sample_path, header=False, index=False)

    rows, stats = imports_module._extract_excel_rows(str(sample_path))

    assert stats["errors"] == []
    assert stats["found"] == 3
    assert rows == [
        ("SKU-010", "Маршмеллоу Персик", 1.0),
        ("SKU-011", "Маршмеллоу Апельсин", 0.75),
        ("SKU-012", "Маршмеллоу Лимон", 1250.5),
    ]


def test_import_article_rows_creates_supplier_mapping(imports_with_db):
    imports_module, db_module = imports_with_db
    conn = db_module.db()
    try:
        stats = imports_module._import_article_rows(
            [("SKU-100", "Маршмеллоу", 5)],
            err_prefix="Row",
            start_index=1,
            supplier_name="Поставщик A",
        )
        assert stats["created"] == 1
        supplier_row = conn.execute(
            "SELECT id FROM supplier WHERE name=?",
            ("Поставщик A",),
        ).fetchone()
        assert supplier_row is not None
        supplier_id = int(supplier_row["id"])
        sku_rows = conn.execute(
            "SELECT code FROM supplier_sku WHERE supplier_id=?",
            (supplier_id,),
        ).fetchall()
        assert [r["code"] for r in sku_rows] == ["SKU-100"]
    finally:
        conn.close()


def test_import_article_rows_new_supplier_creates_new_product(imports_with_db):
    imports_module, db_module = imports_with_db
    conn = db_module.db()
    try:
        imports_module._import_article_rows(
            [("DUP-1", "Маршмеллоу", 4)],
            err_prefix="Row",
            start_index=1,
            supplier_name="Поставщик A",
        )
        imports_module._import_article_rows(
            [("DUP-1", "Маршмеллоу", 2)],
            err_prefix="Row",
            start_index=1,
            supplier_name="Поставщик B",
        )
        product_ids = conn.execute(
            "SELECT id FROM product WHERE article=? ORDER BY id",
            ("DUP-1",),
        ).fetchall()
        assert len(product_ids) == 2
        assert product_ids[0]["id"] != product_ids[1]["id"]
        supplier_counts = conn.execute(
            "SELECT supplier_id, COUNT(*) AS c FROM supplier_sku WHERE code=? GROUP BY supplier_id",
            ("DUP-1",),
        ).fetchall()
        supplier_map = {int(r["supplier_id"]): int(r["c"]) for r in supplier_counts}
        assert len(supplier_map) == 2
        assert all(count == 1 for count in supplier_map.values())
    finally:
        conn.close()


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


def _repeated_block_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            ["Прайс-лист ООО \"Тест\"", "", "", ""],
            ["Товары (работы, услуги)", "", "", ""],
            ["", "", "", ""],
            ["10", "SKU-001", 'Мармелад "Апельсин"', "№1"],
            ["15", "SKU-002", "Маршмеллоу/Ваниль", "№2"],
            ["5", "SKU-003", "Печенье. Домашнее", "№3"],
            ["7", "SKU-004", "Карамель Особая", "№4"],
        ]
    )


def _expected_repeated_rows():
    return [
        ("SKU-001", 'Мармелад "Апельсин"', 10.0),
        ("SKU-002", "Маршмеллоу", 15.0),
        ("SKU-003", "Печенье. Домашнее", 5.0),
        ("SKU-004", "Карамель Особая", 7.0),
    ]


def test_extract_excel_rows_without_header_repeated_block(imports_module, tmp_path):
    sample_path = tmp_path / "no_header_block.xlsx"
    df = _repeated_block_rows()
    df.to_excel(sample_path, header=False, index=False)

    rows, stats = imports_module._extract_excel_rows(str(sample_path))

    assert stats["errors"] == []
    assert stats["found"] == len(rows) == 4
    assert rows == _expected_repeated_rows()


def test_csv_to_normalized_csv_without_header_repeated_block(imports_module, tmp_path):
    sample_path = tmp_path / "no_header_block.csv"
    df = _repeated_block_rows()
    df.to_csv(sample_path, header=False, index=False, encoding="utf-8")

    out_csv, stats = imports_module.csv_to_normalized_csv(str(sample_path))

    assert stats["errors"] == []
    assert stats["found"] == 4
    assert stats.get("items") == _expected_repeated_rows()
    assert out_csv is not None and Path(out_csv).exists()
