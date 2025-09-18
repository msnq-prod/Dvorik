#!/usr/bin/env python3
"""Utility to convert legacy Excel .xls files into raw CSV dumps."""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Iterable, Optional

try:
    import xlrd  # type: ignore
except ImportError:  # pragma: no cover - handled at runtime when conversion starts
    xlrd = None

try:  # pragma: no cover - optional dependency for legacy .xls support
    import xlrd2  # type: ignore
except ImportError:
    xlrd2 = None
else:
    if xlrd is None or getattr(xlrd, "__version__", "").startswith("2"):
        sys.modules.setdefault("xlrd", xlrd2)
        xlrd = xlrd2

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert one or more .xls spreadsheets into CSV files",
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        help="Paths to .xls files or directories containing them",
    )
    parser.add_argument(
        "--sheet",
        help="Sheet name or index to export (default: first sheet)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Directory for CSV output (default: alongside source file)",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8-sig",
        help="Target CSV encoding (default: utf-8-sig)",
    )
    parser.add_argument(
        "--delimiter",
        default=",",
        help="CSV delimiter (default: comma)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate inputs without writing files",
    )
    return parser.parse_args()


def collect_input_files(paths: Iterable[str]) -> list[Path]:
    files: list[Path] = []
    for raw in paths:
        path = Path(raw)
        if path.is_dir():
            files.extend(sorted(path.rglob("*.xls")))
        elif path.is_file() and path.suffix.lower() == ".xls":
            files.append(path)
        else:
            raise FileNotFoundError(f"Unsupported path: {path}")
    if not files:
        raise FileNotFoundError("No .xls files found in the provided paths")
    return files


def parse_sheet(sheet_arg: Optional[str]):
    if sheet_arg is None:
        return 0
    if sheet_arg.isdigit():
        return int(sheet_arg)
    return sheet_arg


def convert_file(src: Path, out_dir: Optional[Path], sheet, encoding: str, delimiter: str, dry_run: bool) -> Path:
    if not src.exists():
        raise FileNotFoundError(src)

    target = (out_dir / src.with_suffix(".csv").name) if out_dir else src.with_suffix(".csv")
    if dry_run:
        return target

    target.parent.mkdir(parents=True, exist_ok=True)

    if xlrd is None:
        raise ModuleNotFoundError(
            "xlrd is required to read .xls files. Install it with 'pip install xlrd>=2.0.1'."
        )

    df = pd.read_excel(
        src,
        sheet_name=sheet,
        header=None,
        dtype=str,
        engine="xlrd",
    )

    df = df.where(df.notna(), "")  # keep empty cells empty

    df.to_csv(
        target,
        index=False,
        header=False,
        encoding=encoding,
        sep=delimiter,
        quoting=csv.QUOTE_ALL,
        na_rep="",
        lineterminator="\n",
    )
    return target


def main() -> None:
    args = parse_args()
    files = collect_input_files(args.inputs)
    sheet = parse_sheet(args.sheet)

    outputs = []
    for src in files:
        out_path = convert_file(src, args.output_dir, sheet, args.encoding, args.delimiter, args.dry_run)
        outputs.append(out_path)

    for path in outputs:
        print(path)


if __name__ == "__main__":
    main()
