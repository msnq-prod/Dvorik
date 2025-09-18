import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture()
def imports_module(tmp_path, monkeypatch):
    cfg = tmp_path / "config.json"
    cfg.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("CONFIG_PATH", str(cfg))

    for mod in [m for m in list(sys.modules) if m == "app" or m.startswith("app.")]:
        sys.modules.pop(mod, None)

    return importlib.import_module("app.services.imports")


def _sample_path(name: str) -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    return repo_root / "data" / "uploads" / name


def test_extract_excel_rows_gordeeva(imports_module):
    rows, stats = imports_module._extract_excel_rows(
        str(_sample_path("Счет на оплату № 16791 от 26.06.2025.xls"))
    )

    assert stats["errors"] == []
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
    assert stats["found"] == len(rows) == 13
    assert rows[0] == ("1013208", "Мармелад Анаконда 1 кг (12)", 10.0)
    assert rows[-1] == ("1150019", "Мармелад Джелли бинс 1 кг (12)", 4.0)
