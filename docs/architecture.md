# Архитектура Dvorik

## Слои приложения
- **Telegram-бот (`app/`)** - сценарии aiogram, обёртки над SQLite и бизнес-логика, которая работает как из бота, так и из фоновых задач.
- **Веб-админка (`admin_ui/`)** - Flask-приложение с блюпринтами и шаблонами, использующее те же сервисы и базу данных.
- **Служебные скрипты (`scripts/`, `stress_test.py`)** - миграции, конвертация XLS→CSV, нагрузочные проверки.

## Сервисы и ответственность
- `app/services/imports.py` - загрузка поставок, нормализация CSV/Excel, построение диффов и вызовы в `supply_session`.
- `app/services/supply_session.py` - долговечные сессии предпросмотра поставок: CRUD по таблице `import_session`, TTL-очистка.
- `app/services/stock.py` - движение остатков, проверки «не уйти в минус», запись событий в `event_log`.
- `app/services/reports.py` - выборки для отчётов (low/zero/mid/all/arch), шарятся между ботом и админкой.
- `app/services/search.py` - поиск карточек, построение списков похожих товаров и групп.
- `app/services/products_display.py` - нормализация имён товаров, подготовка отображаемых названий.
- `app/services/notify.py` - отправка мгновенных и ежедневных уведомлений с использованием настроек `user_notify`.
- `app/services/schedule*.py` - генерация расписания, отчёты и вспомогательные функции для календаря продавцов.

## База данных
- Конфигурация хранится в `app/db.py`. Инициализация включает создание основных таблиц, FTS5-структуры и миграции через `try/except sqlite3.OperationalError`.
- Таблица `import_session` отвечает за устойчивость поставок: в ней сохраняются пути к файлам, отпечатки, исходные строки и статус коммита.
- `get_default_supplier_id()` гарантирует наличие поставщика `__default__`, к которому привязываются SKU без явного источника.

## Поиск связей и аудит изменений
Полезные команды `rg` для быстрой навигации по коду:
- `rg -n "\\bSKL-0\\b"`
- `rg -n "def _sanitize_filename\\("`
- `rg -n "_write_normalized_csv\\("`
- `rg -n "_parse_qty\\("`
- `rg -n "_primary_product_name\\(|_strip_display_exceptions\\("`
- `rg -n "_cards_search\\(|_find_similar_cards\\(|_find_similar_groups\\("`
- `rg -n "RU_MONTHS|TABLE_LABELS|COLUMN_LABELS|BROWSE_HIDDEN_COLUMNS|ENUM_TRANSLATIONS|LOCATION_KIND_LABEL|BOOL_COLS|SUPPLY_ALLOWED_EXTS"`
- `rg -n "int\\(.*\\) if .*\\.is_integer\\(\\) else"`
