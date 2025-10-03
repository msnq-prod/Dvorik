from __future__ import annotations

from io import BytesIO

import pytest
from openpyxl import Workbook

from dvorik.services.imports.strategies import column_detect, csv_parse, sheet_parse


def test_parse_csv_detects_delimiter_and_trims_headers():
    csv_content = "Артикул ; Кол-во ; Цена\nA1 ; 10 ; 100\n"
    rows = csv_parse.parse_csv(csv_content)

    assert list(rows[0].keys()) == ["Артикул", "Кол-во", "Цена"]
    assert rows[0]["Артикул"].strip() == "A1"
    assert rows[0]["Кол-во"].strip() == "10"
    assert rows[0]["Цена"].strip() == "100"


def test_parse_csv_accepts_file_like_objects():
    csv_bytes = b"sku,qty\nX1,5\n"
    rows = csv_parse.parse_csv(BytesIO(csv_bytes))

    assert rows == [{"sku": "X1", "qty": "5"}]


def test_parse_sheet_reads_active_sheet_and_skips_empty_rows():
    workbook = Workbook()
    sheet = workbook.active
    sheet["A1"] = "Артикул"
    sheet["B1"] = "Кол-во"
    sheet.append(["A-1", 3])
    sheet.append([None, None])
    sheet.append(["A-2", 0])

    stream = BytesIO()
    workbook.save(stream)
    data = stream.getvalue()

    rows = sheet_parse.parse_sheet(data)

    assert rows == [
        {"Артикул": "A-1", "Кол-во": 3},
        {"Артикул": "A-2", "Кол-во": 0},
    ]


def test_detect_columns_uses_canonical_aliases():
    headers = ["Артикул", "Наименование", "Кол-во", "Дополнительно"]
    mapping = column_detect.detect_columns(headers)

    assert mapping.article == "Артикул"
    assert mapping.name == "Наименование"
    assert mapping.qty == "Кол-во"
    assert mapping.price is None

    projected = mapping.apply({"Артикул": "SKU-1", "Наименование": "Widget", "Кол-во": 4})
    assert projected == {"article": "SKU-1", "name": "Widget", "qty": 4}


def test_detect_columns_from_rows_sequence():
    rows = [
        {"sku": "A"},
        {"sku": "B"},
    ]
    mapping = column_detect.detect_columns(rows)

    assert mapping.article == "sku"
