"""Dataclasses representing core domain entities."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Tuple


@dataclass(slots=True)
class Manufacturer:
    """Manufacturer of a product."""

    name: str
    id: int | None = None
    country: str | None = None
    created_at: str | None = None


@dataclass(slots=True)
class Supplier:
    """Supplier that provides products for imports."""

    name: str
    id: int | None = None
    contact: str | None = None
    created_at: str | None = None


@dataclass(slots=True)
class Product:
    """Product available in the catalog."""

    name: str
    id: int | None = None
    article: str | None = None
    barcode: str | None = None
    local_name: str | None = None
    description: str | None = None
    unit: str | None = None
    manufacturer_id: int | None = None
    price: float | None = None
    vat_rate: float | None = None
    is_new: bool = False
    archived: bool = False
    archived_at: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    last_restock_at: str | None = None
    photo_file_id: str | None = None
    photo_path: str | None = None


@dataclass(slots=True)
class SupplierSku:
    """SKU mapping for a supplier."""

    product_id: int
    supplier_id: int
    code: str
    id: int | None = None
    barcode: str | None = None
    pack_qty: float | None = None
    active: bool = True
    created_at: str | None = None
    updated_at: str | None = None


@dataclass(slots=True)
class Location:
    """Physical or logical storage location."""

    code: str
    kind: str
    title: str
    created_at: str | None = None


@dataclass(slots=True)
class StockItem:
    """Quantity of a product at a specific location."""

    product_id: int
    location_code: str
    qty_pack: float
    reserved_pack: float = 0.0
    updated_at: str | None = None


@dataclass(slots=True)
class StockSnapshot:
    """Aggregated stock data combining product and location details."""

    product: Product
    location: Location
    qty_pack: float
    reserved_pack: float = 0.0


@dataclass(slots=True)
class LowStockRecord:
    """Projection of stock that is near depletion."""

    product: Product
    location: Location
    qty_pack: float
    threshold: float | None = None


@dataclass(slots=True)
class UserRole:
    """Telegram user role assignment."""

    role: str
    id: int | None = None
    tg_id: int | None = None
    username: str | None = None
    display_name: str | None = None
    created_at: str | None = None


@dataclass(slots=True)
class UserNotifySetting:
    """Notification preferences for a user."""

    user_id: int
    notif_type: str
    mode: str
    updated_at: str | None = None


@dataclass(slots=True)
class EventLogEntry:
    """Recorded domain event emitted by the system."""

    event_type: str
    id: int | None = None
    ts: str | None = None
    product_id: int | None = None
    location_code: str | None = None
    user_id: int | None = None
    delta: float | None = None
    payload_json: str | None = None


@dataclass(slots=True)
class ImportLogEntry:
    """Metadata about a processed import file."""

    original_name: str
    stored_path: str
    import_type: str
    source_hash: str
    id: int | None = None
    normalized_csv: str | None = None
    normalized_hash: str | None = None
    supplier: str | None = None
    invoice: str | None = None
    items_count: int = 0
    items_json: str | None = None
    reverted_at: str | None = None
    created_at: str | None = None


@dataclass(slots=True)
class ScheduleDay:
    """Represents opening hours for a day."""

    date: str
    is_open: bool = True
    notes: str | None = None


@dataclass(slots=True)
class ScheduleAssignment:
    """Assignment of a user to a schedule day."""

    date: str
    tg_id: int
    source: str
    created_at: str | None = None


@dataclass(slots=True)
class ScheduleTransferRequest:
    """Request to swap a scheduled assignment."""

    date: str
    from_tg_id: int
    to_tg_id: int
    status: str
    id: int | None = None
    created_at: str | None = None
    expires_at: str | None = None


@dataclass(slots=True)
class ScheduleAnchor:
    """Anchor denoting the start date of a generated schedule."""

    start_date: str
    id: int | None = None


@dataclass(slots=True)
class RegistrationRequest:
    """User registration request awaiting approval."""

    tg_id: int
    id: int | None = None
    username: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    requested_role: str = "admin"
    status: str = "pending"
    created_at: str | None = None


@dataclass(slots=True)
class DisplayNameException:
    """Record marking display names that should stay untouched."""

    phrase: str
    id: int | None = None
    created_at: str | None = None


@dataclass(slots=True)
class QueryRegistryEntry:
    """Stored SQL snippet overriding default repository queries."""

    key: str
    sql: str
    description: str | None = None
    updated_at: str | None = None


@dataclass(slots=True)
class UiWidget:
    """Registered widget available for admin layouts."""

    module: str
    name: str
    title: str
    id: int | None = None
    description: str | None = None
    entrypoint: str | None = None
    config_schema: str | None = None


@dataclass(slots=True)
class UiWidgetInstance:
    """Widget instance placed on a specific zone."""

    widget_id: int
    zone: str
    position: int = 0
    id: int | None = None
    config_json: str | None = None
    enabled: bool = True


@dataclass(slots=True)
class UiMenuEntry:
    """Menu entry describing navigation for the admin UI."""

    slug: str
    title: str
    id: int | None = None
    url: str | None = None
    icon: str | None = None
    parent_id: int | None = None
    position: int = 0
    target: str | None = None
    visible: bool = True


@dataclass(slots=True)
class ScheduledJob:
    """Registered background job."""

    name: str
    schedule_type: str
    id: int | None = None
    schedule_expression: str | None = None
    next_run_at: str | None = None
    last_run_at: str | None = None
    task_module: str | None = None
    task_name: str | None = None
    config_json: str | None = None
    enabled: bool = True


@dataclass(slots=True)
class AuditLogEntry:
    """Entry describing an administrative action."""

    action: str
    id: int | None = None
    created_at: str | None = None
    actor_id: int | None = None
    actor_username: str | None = None
    entity: str | None = None
    entity_id: str | None = None
    payload_json: str | None = None


@dataclass(slots=True)
class ProductDetail:
    """Composite view of a product including related data."""

    product: Product
    manufacturer: Manufacturer | None = None
    supplier_skus: Tuple[SupplierSku, ...] = field(default_factory=tuple)
    stock_items: Tuple[StockItem, ...] = field(default_factory=tuple)


__all__ = [
    "Manufacturer",
    "Supplier",
    "Product",
    "SupplierSku",
    "Location",
    "StockItem",
    "StockSnapshot",
    "LowStockRecord",
    "UserRole",
    "UserNotifySetting",
    "EventLogEntry",
    "ImportLogEntry",
    "ScheduleDay",
    "ScheduleAssignment",
    "ScheduleTransferRequest",
    "ScheduleAnchor",
    "RegistrationRequest",
    "DisplayNameException",
    "QueryRegistryEntry",
    "UiWidget",
    "UiWidgetInstance",
    "UiMenuEntry",
    "ScheduledJob",
    "AuditLogEntry",
    "ProductDetail",
]
