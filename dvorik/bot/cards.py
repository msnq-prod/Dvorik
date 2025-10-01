from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

from dvorik.domain.models import Product, StockSnapshot

from . import texts

__all__ = [
    "CardSection",
    "Card",
    "RenderedCard",
    "product_card",
]


@dataclass(slots=True)
class CardSection:
    """Logical section inside a card."""

    title: str | None = None
    lines: Sequence[str] = field(default_factory=tuple)

    def render(self) -> str:
        """Render section into text block."""

        content = [line for line in self.lines if line]
        if not content:
            return ""
        if self.title:
            return texts.join_non_empty(
                (texts.bold(self.title), *content),
                separator="\n",
            )
        return "\n".join(content)


@dataclass(slots=True)
class RenderedCard:
    """Rendered card ready to be sent via Telegram."""

    text: str
    parse_mode: str = "HTML"
    photo_file_id: str | None = None
    photo_path: str | None = None

    def as_message_kwargs(self) -> dict[str, object]:
        """Return kwargs usable with ``bot.send_message`` or ``bot.send_photo``."""

        payload: dict[str, object] = {"text": self.text, "parse_mode": self.parse_mode}
        if self.photo_file_id:
            payload["photo"] = self.photo_file_id
        elif self.photo_path:
            payload["photo"] = self.photo_path
        return payload


@dataclass(slots=True)
class Card:
    """High level description of a message card."""

    title: str | None = None
    sections: Sequence[CardSection] = field(default_factory=tuple)
    footer: str | None = None
    photo_file_id: str | None = None
    photo_path: str | None = None

    def render(self) -> RenderedCard:
        """Render card into ``RenderedCard`` object."""

        blocks = []
        if self.title:
            blocks.append(texts.bold(self.title))
        blocks.extend(filter(None, (section.render() for section in self.sections)))
        if self.footer:
            blocks.append(self.footer)

        text = "\n\n".join(blocks)
        return RenderedCard(
            text=text,
            photo_file_id=self.photo_file_id,
            photo_path=self.photo_path,
        )


def _build_meta_lines(product: Product) -> list[str]:
    lines: list[str] = []
    lines.append(texts.label_value("Артикул", texts.code(product.article)) if product.article else None)
    lines.append(texts.label_value("Штрихкод", texts.code(product.barcode)) if product.barcode else None)
    lines.append(texts.label_value("Локальное название", texts.escape(product.local_name)) if product.local_name else None)
    lines.append(texts.label_value("Единица", texts.escape(product.unit)) if product.unit else None)
    if product.price is not None:
        price_value = texts.format_money(product.price)
        if product.unit:
            price_value = f"{price_value}{texts.NBSP}/{texts.NBSP}{texts.escape(product.unit)}"
        lines.append(texts.label_value("Цена", price_value))
    if product.vat_rate is not None:
        vat_value = f"{texts.escape(product.vat_rate)}%"
        lines.append(texts.label_value("Ставка НДС", vat_value))
    if product.is_new:
        lines.append(texts.italic("Новинка"))
    if product.archived:
        lines.append(texts.label_value("Статус", texts.bold("В архиве")))
    if product.last_restock_at:
        lines.append(
            texts.label_value("Последнее пополнение", texts.format_datetime(product.last_restock_at))
        )
    if product.updated_at:
        lines.append(texts.label_value("Обновлено", texts.format_datetime(product.updated_at)))
    return [line for line in lines if line]


def _build_stock_lines(product: Product, stock: Sequence[StockSnapshot] | None) -> list[str]:
    if not stock:
        return []

    lines: list[str] = []
    for snapshot in stock:
        location_name = snapshot.location.title or snapshot.location.code
        qty_text = texts.format_quantity(snapshot.qty_pack, unit=product.unit)
        if snapshot.reserved_pack:
            reserve = texts.format_quantity(snapshot.reserved_pack, unit=product.unit)
            qty_text = texts.join_non_empty([qty_text, texts.italic(f"(резерв {reserve})")], separator=" ")
        lines.append(texts.label_value(location_name, qty_text))
    return [line for line in lines if line]


def product_card(
    product: Product,
    /,
    *,
    stock: Sequence[StockSnapshot] | None = None,
    show_description: bool = True,
) -> RenderedCard:
    """Build card representation for :class:`~dvorik.domain.models.Product`."""

    sections: list[CardSection] = []

    meta_lines = _build_meta_lines(product)
    if meta_lines:
        sections.append(CardSection(title="Характеристики", lines=meta_lines))

    stock_lines = _build_stock_lines(product, stock)
    if stock_lines:
        sections.append(CardSection(title="Остатки", lines=stock_lines))

    footer: str | None = None
    if show_description and product.description and product.description.strip():
        footer = texts.escape(product.description.strip())

    card = Card(
        title=product.name or "Товар",
        sections=sections,
        footer=footer,
        photo_file_id=product.photo_file_id,
        photo_path=product.photo_path,
    )
    return card.render()
