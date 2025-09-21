import csv
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
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    for mod in [m for m in list(sys.modules) if m == "app" or m.startswith("app.")]:
        sys.modules.pop(mod, None)

    return importlib.import_module("app.services.imports")


def _sample_path(name: str) -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    return repo_root / "data" / "uploads" / name


def test_extract_excel_rows_gordeeva(imports_module):
    if getattr(imports_module, "xlrd2", None) is None:
        pytest.skip("xlrd2 is not available in the test environment")
    rows, stats = imports_module._extract_excel_rows(
        str(_sample_path("Счет на оплату № 16791 от 26.06.2025.xls"))
    )

    if stats["errors"] and any(".xls" in err for err in stats["errors"]):
        pytest.skip("xlrd-compatible engine is unavailable for .xls files")

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
    if getattr(imports_module, "xlrd2", None) is None:
        pytest.skip("xlrd2 is not available in the test environment")
    rows, stats = imports_module._extract_excel_rows(
        str(_sample_path("Счет на оплату (1).xls"))
    )

    if stats["errors"] and any(".xls" in err for err in stats["errors"]):
        pytest.skip("xlrd-compatible engine is unavailable for .xls files")

    assert stats["errors"] == []
    assert stats.get("warnings") == []
    assert stats["found"] == len(rows) == 13
    assert rows[0] == ("1013208", "Мармелад Анаконда 1 кг (12)", 10.0)
    assert rows[-1] == ("1150019", "Мармелад Джелли бинс 1 кг (12)", 4.0)


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


def test_extract_excel_rows_without_header_repeated_block(imports_module, tmp_path):
    rows_data = [["Товары (работы, услуги)", None, None, None]]
    for i in range(1, 53):
        rows_data.append([f"Примечание {i}", None, None, None])
    rows_data.extend(
        [
            [1, "AA-001", 'Желе "Вишня" / 0.5 кг.', 5],
            [2, "BB-002", 'Печенье "Ореховое" / 0.45 кг.', 3],
            [3, "CC-003", 'Конфеты "Микс" / 2.5 кг.', 7],
        ]
    )
    df = pd.DataFrame(rows_data)
    excel_path = tmp_path / "no_header.xlsx"
    df.to_excel(excel_path, header=False, index=False)

    rows, stats = imports_module._extract_excel_rows(str(excel_path))

    assert stats["errors"] == []
    assert stats["found"] == 3
    expected = [
        ("AA-001", 'Желе "Вишня"', 5.0),
        ("BB-002", 'Печенье "Ореховое"', 3.0),
        ("CC-003", 'Конфеты "Микс"', 7.0),
    ]
    assert rows == expected
    assert all(not art.isdigit() for art, _, _ in rows)


def test_csv_without_header_repeated_block(imports_module, tmp_path):
    rows_data = [[" ", " ", " ", " "]]
    rows_data.append(["Товары (работы, услуги)", "", "", ""])
    for i in range(1, 53):
        rows_data.append([f"Комментарий {i}", "", "", ""])
    rows_data.extend(
        [
            [1, "AA-001", 'Желе "Вишня" / 0.5 кг.', 5],
            [2, "BB-002", 'Печенье "Ореховое" / 0.45 кг.', 3],
            [3, "CC-003", 'Конфеты "Микс" / 2.5 кг.', 7],
        ]
    )
    csv_path = tmp_path / "no_header.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(rows_data)

    norm_path, stats = imports_module.csv_to_normalized_csv(str(csv_path))

    assert stats["errors"] == []
    assert stats["found"] == 3
    expected = [
        ("AA-001", 'Желе "Вишня"', 5.0),
        ("BB-002", 'Печенье "Ореховое"', 3.0),
        ("CC-003", 'Конфеты "Микс"', 7.0),
    ]
    assert stats["items"] == expected
    assert norm_path is not None
