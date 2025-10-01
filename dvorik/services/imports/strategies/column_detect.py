from __future__ import annotations

import dataclasses
import unicodedata
from collections.abc import Iterable, Sequence
from typing import Mapping, MutableMapping

_CANONICAL_ALIASES: Mapping[str, tuple[str, ...]] = {
    "article": ("article", "sku", "артикул", "код", "vendorcode", "vendor_code"),
    "name": ("name", "product", "наименование", "товар", "title"),
    "qty": ("qty", "quantity", "кол-во", "amount", "count", "количество"),
    "price": ("price", "cost", "цена", "стоимость", "unitprice"),
    "barcode": ("barcode", "штрихкод", "ean", "ean13"),
}


def _normalise(text: str) -> str:
    value = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in value.lower() if ch.isalnum())


@dataclasses.dataclass(slots=True, frozen=True)
class ColumnMapping:
    """Mapping between canonical column names and the actual source headers."""

    article: str | None = None
    name: str | None = None
    qty: str | None = None
    price: str | None = None
    barcode: str | None = None

    def as_dict(self) -> Mapping[str, str]:
        data = dataclasses.asdict(self)
        return {key: value for key, value in data.items() if value}

    def apply(self, row: Mapping[str, object]) -> MutableMapping[str, object]:
        """Project ``row`` according to the mapping returning canonical keys."""

        result: MutableMapping[str, object] = {}
        for canonical, column in self.as_dict().items():
            result[canonical] = row.get(column)
        return result


def detect_columns(
    rows_or_headers: Sequence[Mapping[str, object]] | Iterable[str],
) -> ColumnMapping:
    """Detect column mapping either from headers or the first row of data."""

    headers: Iterable[str]
    if isinstance(rows_or_headers, Sequence) and rows_or_headers:
        first_item = rows_or_headers[0]
        if isinstance(first_item, Mapping):
            headers = first_item.keys()
        else:
            headers = (str(value) for value in rows_or_headers)  # type: ignore[assignment]
    else:
        headers = (str(value) for value in rows_or_headers)

    normalised = {_normalise(header): header for header in headers}
    mapping: dict[str, str] = {}

    for canonical, aliases in _CANONICAL_ALIASES.items():
        for alias in aliases:
            header = normalised.get(alias)
            if header:
                mapping[canonical] = header
                break

    return ColumnMapping(**mapping)


__all__ = ["ColumnMapping", "detect_columns"]
