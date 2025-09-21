import importlib
import sys
from pathlib import Path

import pytest
import pandas as pd


@pytest.fixture()
def imports_module(tmp_path, monkeypatch):
    cfg = tmp_path / "config.json"
    cfg.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("CONFIG_PATH", str(cfg))

    try:
        import xlrd2  # type: ignore
    except ImportError:
        pass
    else:
        sys.modules.pop("xlrd", None)
        monkeypatch.setitem(sys.modules, "xlrd", xlrd2)

    for mod in [m for m in list(sys.modules) if m == "app" or m.startswith("app.")]:
        sys.modules.pop(mod, None)

    return importlib.import_module("app.services.imports")


def _sample_path(name: str) -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    path = repo_root / "data" / "uploads" / name
    if not path.exists():
        pytest.skip(f"Sample file {name} is not available")
    return path


def test_extract_excel_rows_gordeeva(imports_module):
    rows, stats = imports_module._extract_excel_rows(
        str(_sample_path("Счет на оплату № 16791 от 26.06.2025.xls"))
    )

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
    rows, stats = imports_module._extract_excel_rows(
        str(_sample_path("Счет на оплату (1).xls"))
    )

    assert stats["errors"] == []
    assert stats.get("warnings") == []
    assert stats["found"] == len(rows) == 13
    assert rows[0] == ("1013208", "Мармелад Анаконда 1 кг (12)", 10.0)
    assert rows[-1] == ("1150019", "Мармелад Джелли бинс 1 кг (12)", 4.0)


def test_extract_excel_rows_with_code_header(imports_module, tmp_path):
    df = pd.DataFrame(
        [
            {"Код": "A001", "Наименование": "Зефир клубничный", "Кол-во": 5, "Цена": 120},
            {"Код": "B002", "Наименование": "Зефир ванильный", "Кол-во": 3, "Цена": 95},
        ]
    )
    file_path = tmp_path / "invoice.xlsx"
    df.to_excel(file_path, index=False)

    rows, stats = imports_module._extract_excel_rows(str(file_path))

    assert stats["errors"] == []
    assert stats.get("warnings") == []
    assert stats["found"] == len(rows) == 2
    assert rows == [
        ("A001", "Зефир клубничный", 5.0),
        ("B002", "Зефир ванильный", 3.0),
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
