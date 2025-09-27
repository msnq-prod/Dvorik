from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Set, Tuple

# Location / stock related constants
HUB_LOCATION_CODE = "SKL-0"

LOCATION_KIND_LABEL: Dict[str, str] = {
    "SKL": "Склад",
    "DOMIK": "Домик",
    "HALL": "Зал",
    "COUNTER": "за стойкой",
}

BOOL_COLS: Set[str] = {"is_new", "archived", "is_open"}

ENUM_TRANSLATIONS: Dict[Tuple[str, str], Dict[str, str]] = {
    ("user_notify", "notif_type"): {
        "zero": "нулевые",
        "last": "заканчиваются",
        "to_skl": "в склад",
    },
    ("user_notify", "mode"): {
        "off": "выкл",
        "daily": "ежедневно",
        "instant": "мгновенно",
    },
    ("schedule_transfer_request", "status"): {
        "pending": "ожидает",
        "accepted": "принято",
        "declined": "отклонено",
        "cancelled": "отменено",
        "expired": "истекло",
    },
    ("user_role", "role"): {
        "admin": "админ",
        "seller": "продавец",
    },
}

TABLE_LABELS: Dict[str, str] = {
    "product": "Товары",
    "location": "Локации",
    "stock": "Наличие",
    "user_role": "Пользователи и роли",
    "user_notify": "Настройки уведомлений",
    "event_log": "Журнал событий",
    "schedule_day": "График: календарь",
    "schedule_assignment": "График: назначения",
    "schedule_transfer_request": "График: переносы",
    "schedule_anchor": "График: якорь",
    "registration_request": "Заявки на регистрацию",
    "manufacturer": "Производители",
}

COLUMN_LABELS: Dict[str, Dict[str, str]] = {
    "product": {
        "id": "ID",
        "article": "Артикул",
        "name": "Название",
        "brand_country": "Бренд/Страна",
        "local_name": "Локальное имя",
        "photo_file_id": "Фото (file_id)",
        "photo_path": "Фото (путь)",
        "is_new": "Новинка",
        "archived": "Архив",
        "archived_at": "Дата архивации",
        "last_restock_at": "Последнее поступление",
        "created_at": "Создано",
        "manufacturer_id": "Производитель (ID)",
        "manufacturer_name": "Производитель",
        "manufacturer_country": "Страна",
    },
    "manufacturer": {
        "id": "ID",
        "name": "Производитель",
        "country": "Страна",
    },
    "location": {
        "code": "Код",
        "kind": "Тип",
        "title": "Название",
    },
    "stock": {
        "product_id": "Товар (ID)",
        "location_code": "Локация",
        "qty_pack": "Количество (уп.)",
        "name": "Название",
        "local_name": "Локальное имя",
    },
    "user_role": {
        "id": "ID",
        "tg_id": "Telegram ID",
        "username": "Логин",
        "display_name": "Имя",
        "role": "Роль",
        "created_at": "Создано",
    },
    "user_notify": {
        "user_id": "Пользователь (ID)",
        "notif_type": "Тип уведомления",
        "mode": "Режим",
    },
    "event_log": {
        "id": "ID",
        "ts": "Время",
        "type": "Тип",
        "product_id": "Товар (ID)",
        "location_code": "Локация",
        "delta": "Изменение",
    },
    "schedule_day": {
        "date": "Дата",
        "is_open": "Открыто",
        "notes": "Заметки",
    },
    "schedule_assignment": {
        "date": "Дата",
        "tg_id": "Telegram ID",
        "source": "Источник",
        "created_at": "Создано",
    },
    "schedule_transfer_request": {
        "id": "ID",
        "date": "Дата",
        "from_tg_id": "От (TG)",
        "to_tg_id": "К (TG)",
        "status": "Статус",
        "created_at": "Создано",
        "expires_at": "Истекает",
    },
    "schedule_anchor": {
        "id": "ID",
        "start_date": "Начало",
    },
    "registration_request": {
        "id": "ID",
        "tg_id": "Telegram ID",
        "username": "Логин",
        "first_name": "Имя",
        "last_name": "Фамилия",
        "requested_role": "Запрошенная роль",
        "status": "Статус",
        "created_at": "Создано",
    },
}

BROWSE_HIDDEN_COLUMNS: Dict[str, Set[str]] = {
    "product": {
        "brand_country",
        "photo_file_id",
        "photo_path",
        "is_new",
        "archived",
        "archived_at",
        "created_at",
    }
}

RU_MONTHS: Dict[int, str] = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь",
}

PRIMARY_TABLES: Set[str] = {"product", "manufacturer", "user_role"}
HIDDEN_TABLES: Set[str] = {"stock"}

SUPPLY_ALLOWED_EXTS: Set[str] = {".csv", ".xls", ".xlsx", ".xlsm", ".xltx", ".xltm"}

__all__ = [
    "HUB_LOCATION_CODE",
    "LOCATION_KIND_LABEL",
    "BOOL_COLS",
    "ENUM_TRANSLATIONS",
    "TABLE_LABELS",
    "COLUMN_LABELS",
    "BROWSE_HIDDEN_COLUMNS",
    "RU_MONTHS",
    "PRIMARY_TABLES",
    "HIDDEN_TABLES",
    "SUPPLY_ALLOWED_EXTS",
]
