from __future__ import annotations

import html
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Iterable

__all__ = [
    "NBSP",
    "escape",
    "bold",
    "italic",
    "underline",
    "code",
    "pre",
    "join_non_empty",
    "bullet_list",
    "label_value",
    "format_money",
    "format_quantity",
    "format_bool",
    "format_datetime",
]

NBSP = "\u00a0"


def escape(text: object | None) -> str:
    """Return HTML-escaped representation of *text* for Telegram messages."""

    if text is None:
        return ""
    return html.escape(str(text), quote=False)


def bold(text: object | None) -> str:
    """Wrap *text* with ``<b>`` tags after escaping it."""

    return f"<b>{escape(text)}</b>"


def italic(text: object | None) -> str:
    """Wrap *text* with ``<i>`` tags after escaping it."""

    return f"<i>{escape(text)}</i>"


def underline(text: object | None) -> str:
    """Wrap *text* with ``<u>`` tags after escaping it."""

    return f"<u>{escape(text)}</u>"


def code(text: object | None) -> str:
    """Wrap *text* with ``<code>`` tags after escaping it."""

    return f"<code>{escape(text)}</code>"


def pre(text: object | None) -> str:
    """Wrap *text* with ``<pre>`` tags after escaping it."""

    return f"<pre>{escape(text)}</pre>"


def join_non_empty(parts: Iterable[str | None], *, separator: str = "\n") -> str:
    """Join non-empty *parts* using *separator*."""

    return separator.join(part for part in parts if part)


def bullet_list(items: Iterable[str | None], *, bullet: str = "•") -> str:
    """Return ``\n``-joined bullet list preserving existing markup."""

    escaped_bullet = escape(bullet) if bullet else ""
    lines = []
    for item in items:
        if not item:
            continue
        if escaped_bullet:
            lines.append(f"{escaped_bullet}{NBSP}{item}")
        else:
            lines.append(item)
    return "\n".join(lines)


def label_value(label: str, value: str | None) -> str | None:
    """Return ``"<b>Label:</b> value"`` style string when *value* is provided."""

    if not value:
        return None
    label_part = bold(f"{label}:")
    return f"{label_part}{NBSP}{value}"


def _to_decimal(value: float | int | Decimal) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:  # pragma: no cover - defensive
        raise ValueError("value cannot be converted to Decimal") from exc


def _format_decimal(value: Decimal, *, precision: int) -> str:
    quantizer = Decimal("1").scaleb(-precision)
    quantized = value.quantize(quantizer) if precision >= 0 else value
    if precision > 0:
        formatted = f"{quantized:,.{precision}f}"
        if "." in formatted:
            formatted = formatted.rstrip("0").rstrip(".")
    elif precision == 0:
        formatted = f"{quantized:,.0f}"
    else:  # pragma: no cover - precision < 0 is not expected currently
        formatted = f"{quantized}"
    return formatted.replace(",", NBSP)


def format_money(
    amount: float | int | Decimal | None,
    *,
    currency: str = "₽",
    precision: int = 2,
    none_text: str = "—",
) -> str:
    """Format monetary *amount* with currency sign suitable for HTML output."""

    if amount is None:
        return escape(none_text)

    value = _to_decimal(amount)
    formatted = _format_decimal(value, precision=precision)
    currency_part = escape(currency)
    return join_non_empty([escape(formatted), currency_part], separator=NBSP)


def format_quantity(
    value: float | int | Decimal | None,
    *,
    unit: str | None = None,
    precision: int = 2,
    none_text: str = "—",
) -> str:
    """Format numeric quantity optionally appending *unit* label."""

    if value is None:
        return escape(none_text)

    decimal_value = _to_decimal(value)
    formatted = _format_decimal(decimal_value, precision=precision)
    unit_part = escape(unit) if unit else ""
    return join_non_empty([escape(formatted), unit_part], separator=NBSP)


def format_bool(
    value: bool | None,
    *,
    true_text: str = "Да",
    false_text: str = "Нет",
    none_text: str = "—",
) -> str:
    """Return human-readable representation for boolean flags."""

    if value is None:
        return escape(none_text)
    return escape(true_text if value else false_text)


def format_datetime(
    value: datetime | date | str | None,
    *,
    datetime_format: str = "%d.%m.%Y %H:%M",
    date_format: str = "%d.%m.%Y",
    none_text: str = "—",
) -> str:
    """Format ``datetime``/``date`` objects or plain strings for display."""

    if value is None:
        return escape(none_text)
    if isinstance(value, datetime):
        return escape(value.strftime(datetime_format))
    if isinstance(value, date):
        return escape(value.strftime(date_format))
    return escape(value)
