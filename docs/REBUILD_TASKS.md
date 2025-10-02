# Dvorik Rebuild Backlog

This document describes the rebuild backlog for the Dvorik project. The goal is to replace the legacy codebase with a modular architecture rooted at `dvorik/app.py`. Tasks are organized by phases to enable parallel workstreams without merge conflicts.

## Principles

- Remove all legacy packages (`app/`, `admin_ui/`, related scripts) and rebuild from scratch under the `dvorik/` namespace.
- Keep runtime data directories (`data/`, `media/`, `reports/`) intact.
- Stabilize public APIs (events, registries, data access, layouts) before migrating features.
- Use Python 3.12, Flask, aiogram, and SQLite (WAL).
- Provide centralized registries and namespaced callbacks to avoid collisions.

## Phase 0 — Reset & Skeleton

### Ticket 0.1 — Remove legacy code and reset ignores
- Remove `app/`, `admin_ui/`, `scripts/xls_to_csv.py`, and existing tests (store temporarily if needed).
- Create empty `dvorik/__init__.py`.
- Keep `.gitignore`; postpone `.env.example` update.
- **DoD:** project imports as the `dvorik` package; no runtime functionality yet.

### Ticket 0.2 — Skeleton module layout
- Create `dvorik/app.py` (composition root placeholder).
- Create `dvorik/__init__.py` with metadata stub.
- Create empty package initializers: `dvorik/core`, `dvorik/db`, `dvorik/domain`, `dvorik/repo`, `dvorik/services`, `dvorik/admin`, `dvorik/bot`, `dvorik/plugins`.
- Stub admin server (`dvorik/admin/server.py`, templates/static skeleton) and bot entry (`dvorik/bot/main.py`).
- Add example plugin module (`dvorik/plugins/example/__init__.py`).
- **DoD:** `from dvorik.app import create_system` imports; admin and bot modules import without errors.

## Phase 1 — Core (Events, Registries, Scheduler, Config)

### Ticket 1.1 — Event bus
- Implement `dvorik/core/events.py` with `subscribe`, `unsubscribe`, `publish` supporting sync/async subscribers; log exceptions.
- **DoD:** smoke test: two subscribers (one async) receive payload.

### Ticket 1.2 — Registries
- Implement singleton registries in `dvorik/core/registry.py`: `MenuRegistry`, `WidgetRegistry`, `BotRouterRegistry`, `JobRegistry`, `QueryRegistry` (proxy to DB layer).
- **DoD:** registering and retrieving entries works (`WidgetRegistry.register/get`).

### Ticket 1.3 — Scheduler
- Create `dvorik/core/scheduler.py` with `register_daily`, `register_cron`, `run_forever(loop)` handling graceful errors.
- **DoD:** jobs register and trigger in test event loop.

### Ticket 1.4 — Configuration
- Implement `dvorik/core/config.py` loading from `.env`, `config.json`, environment variables (env takes precedence).
- Manage configuration fields: `BOT_TOKEN`, `SUPER_ADMIN_ID`, `SUPER_ADMIN_USERNAME`, `ADMIN_PORT`, `DB_PATH`, directories (`data/`, `media/`, `reports/` etc.), `PAGE_SIZE`, `CARDS_PAGE_SIZE`, `STOCK_PAGE_SIZE`, `PHOTO_QUALITY`, plugin options (`PLUGIN_PATHS`, `PLUGIN_DISABLED`).
- Auto-create directories if missing.
- **DoD:** import yields valid paths and creates directories.

### Ticket 1.5 — Plugin loader
- Implement `dvorik/core/plugins.py` with `load_plugins(dir="dvorik/plugins")`, registry helpers, auto-import loop.
- **DoD:** logs “Loaded N plugins …”; example plugin registers itself.

## Phase 2 — Database & Query Layer

### Ticket 2.1 — SQLite connection helpers
- Create `dvorik/db/conn.py` with `db()` returning SQLite connection configured for WAL, `busy_timeout`, `row_factory`.
- **DoD:** connection returns with PRAGMA applied.

### Ticket 2.2 — Migration runner
- Create `dvorik/db/migrations.py` with `init_db()` to create new schema (see Ticket 2.3); handle `sqlite3.OperationalError` gracefully.
- **DoD:** repeated `init_db()` runs succeed.

### Ticket 2.3 — Schema definition
- In `init_db()` create tables: `product`, `manufacturer`, `location`, `stock`, `user_role`, `user_notify`, `event_log`, `import_log`, schedule tables (`schedule_day`, `schedule_assignment`, `schedule_transfer_request`, `schedule_anchor`), `registration_request`, `product_fts` with triggers, `supplier`, `supplier_sku`, `display_name_exception`.
- Add modular tables: `query_registry`, `ui_widget`, `ui_widget_instance`, `ui_menu`, `scheduled_job`, `audit_log` with appropriate columns.
- **DoD:** schema creation idempotent.

### Ticket 2.4 — Query registry API
- Implement `dvorik/db/query_registry.py` with `get_query(conn, key, default_sql)` and `set_query(...)` to override stored SQL.
- **DoD:** missing key returns default SQL; stored entries override.

## Phase 3 — Domain, Repositories, Services

### Ticket 3.1 — Domain contracts
- Create `dvorik/domain/models.py` dataclasses (`Product`, `Location`, `StockItem`, `UserRole`, etc.).
- Create `dvorik/domain/ports.py` defining repository interfaces (`ProductRepo`, `StockRepo`, `ScheduleRepo`, `ImportLogRepo`).
- **DoD:** models and interfaces import cleanly.

### Ticket 3.2 — Repository implementations
- Implement repositories (`product_repo.py`, `stock_repo.py`, `schedule_repo.py`, `import_repo.py`) that accept a `conn` argument and fetch SQL via query registry.
- Provide methods such as `low_stock(limit)`, `product_detail(pid)`, `search_fts`, `stock_by_location()`, `schedule_assignments(month)`.
- **DoD:** methods work on empty DB (returning defaults).

### Ticket 3.3 — Services (use-cases)
- Implement business services:
  - `dvorik/services/stock.py` (`set_location_qty`, `move_specific`, `adjust_with_hub`).
  - `dvorik/services/notify.py` (`notify_instant_thresholds`, `notify_instant_to_skl`, `send_daily_digests`) subscribing to events.
  - `dvorik/services/imports/__init__.py` facade coordinating strategies.
  - `dvorik/services/imports/strategies/` (`column_detect`, `sheet_parse`, `csv_parse`).
  - `dvorik/services/schedule.py` for schedule generation and retrieval.
- **DoD:** services import and strategies wired correctly.

## Phase 4 — Admin UI 2.0

### Ticket 4.1 — Flask app factory
- Implement `dvorik/admin/server.py` `create_app()` to initialize DB, register blueprints (home, superadmin, api tables, supply), load plugins, register widgets.
- Provide `/health` endpoint; ensure `python -m dvorik.admin.server` runs.
- **DoD:** server starts and `/health` returns response.

### Ticket 4.2 — Widget API
- Implement `dvorik/admin/widgets/api.py` defining `Widget` base class with `render()` method returning markup.
- Create `dvorik/admin/widgets/builtin.py` with widgets (`LowStockWidget`, `ScheduleMiniWidget`, `StockByLocationWidget`), register via registry.
- Seed three widget instances into `ui_widget_instance` for `home.main` zone when configuration empty.
- **DoD:** default widgets available on home page.

### Ticket 4.3 — Home layout rendering
- Implement `dvorik/admin/blueprints/home.py` to load widget instances and render via template.
- Update templates (`base.html`, `home.html`) to support zones.
- **DoD:** home page renders widget outputs.

### Ticket 4.4 — Menu management
- Implement `dvorik/admin/blueprints/menus.py` providing context processor for menu entries (`ui_menu`, fallback static list).
- Update base template to render offcanvas from DB entries.
- **DoD:** dynamic menu works; static fallback retained.

### Ticket 4.5 — Superadmin console
- Implement `dvorik/admin/blueprints/superadmin.py` CRUD for widgets, menu entries, query registry, scheduled jobs; log to `audit_log`.
- Create templates under `dvorik/admin/templates/superadmin/`.
- **DoD:** admins can manage widgets, SQL, menu, jobs.

### Ticket 4.6 — Generic table browser
- Implement `dvorik/admin/blueprints/tables.py` for CRUD operations on tables.
- Provide templates `table.html`, `form.html`.
- **DoD:** tables (non-virtual) manageable via UI.

### Ticket 4.7 — Supply management UI
- Implement `dvorik/admin/blueprints/supply.py` for preview/import/revert flows using import services; create template `supply.html`.
- **DoD:** preview → confirm → revert flow works and logs import events.

## Phase 5 — Bot 2.0

### Ticket 5.1 — Bot entry & dispatcher
- Implement `dvorik/bot/main.py`: initialize DB, bot, dispatcher, load plugins, register built-in routers, start scheduler background loop.
- **DoD:** bot launches (no handlers yet).

### Ticket 5.2 — Router registry
- Create built-in routers (`core.py`, `admin.py`, `stock.py`, `supply.py`) registering themselves via `BotRouterRegistry.register("builtin.core", router)` etc.
- **DoD:** dispatch routes available.

### Ticket 5.3 — Callback namespace helpers
- Implement `dvorik/bot/callbacks.py` wrapping core callback utilities (`build`, `parse`).
- Implement `dvorik/bot/keyboards.py` generating buttons with namespaced callbacks; update routers accordingly.
- **DoD:** callback parsing avoids string collisions.

### Ticket 5.4 — Cards & texts
- Implement `dvorik/bot/cards.py` defining card presentation components.
- Implement `dvorik/bot/texts.py` for text formatting helpers.
- **DoD:** product card renders via card component.

### Ticket 5.5 — Event integration
- Update services to publish events (`stock.adjusted`, `import.completed`) via event bus; `notify` service subscribes for instant/daily notifications.
- **DoD:** events trigger notifications through bus.

## Phase 6 — Plugins

### Ticket 6.1 — Example plugin
- Update `dvorik/plugins/example/__init__.py` to register a widget (“Top SKUs”), add admin menu entry, and bot router under namespace `example`.
- **DoD:** plugin contributions appear in admin UI and bot.

## Phase 7 — Security

### Ticket 7.1 — Admin authentication
- Implement `dvorik/admin/auth.py` with session-based login using secrets from env and `@require_superadmin` guard.
- Protect superadmin and supply endpoints with decorator; respond 403 for anonymous.
- **DoD:** access control enforced.

### Ticket 7.2 — Audit logging
- Ensure superadmin CRUD writes to `audit_log`; expose logs in UI browser.
- **DoD:** audit entries visible.

## Phase 8 — Composition Root

### Ticket 8.1 — `dvorik/app.py`
- Implement `create_system()` to run `init_db()`, load plugins, register built-in widgets/routers/jobs, and return factories to run bot/admin apps.
- Provide module entry points (`python -m dvorik.bot`, `python -m dvorik.admin`).
- **DoD:** both applications start via new entry points.

## Phase 9 — Developer Experience & CI

### Ticket 9.1 — Requirements & scripts
- Update `requirements.txt` (retain aiogram, Flask, pandas, openpyxl, xlrd/xlrd2, Pillow) and add optional dev dependencies.
- Create `run_admin.command`, `run_bot.command` invoking new entry points.
- **DoD:** helper scripts run new architecture.

### Ticket 9.2 — Tests
- Create new tests (`tests/test_events.py`, `test_callbacks.py`, `test_query_registry.py`, `test_import_strategies.py`, `test_stock_service.py`).
- **DoD:** `pytest` passes.

### Ticket 9.3 — Documentation
- Create `docs/ARCHITECTURE.md` describing layers and contracts.
- Update `README.md` with new commands, plugin info, superadmin overview.
- **DoD:** documentation reflects architecture.

## Additional Cross-Cutting Tasks

### CI/CD & Tooling
- Add lint/type/test stages to CI (e.g., `flake8`, `mypy`, `pytest`) with cached dependencies.
- Provide configuration files: `.flake8`, `mypy.ini`, `.editorconfig`, optional `pyproject.toml` for formatter (Black/Ruff).
- Set up pre-commit hooks for formatting and quick checks.
- Deliver Docker/Compose definitions for admin and bot services with shared SQLite volume; add Makefile targets (`make dev`, `make test`, `make lint`, `make db-backup`).

### Observability & Operations
- Standardize structured logging (JSON) with correlation fields (request/job ID, user ID).
- Expose health endpoints (`/health`, `/ready`) and bot ping checks.
- Surface scheduler status (job list, last run) in admin UI.

### Security & Access Control
- Extend `ui_menu` schema with `required_role` and filter menus by user role.
- Implement CSRF protection for superadmin forms.
- Document secret management: BOT token and admin secrets sourced only from environment; update `.env.example`.

### Plugin Lifecycle
- Version plugin API via `dvorik/core/version.py`; validate `plugin.api_version` during load.
- Support optional plugin migrations (`plugin.migrate(conn)` after base `init_db()`).
- Allow configuration to disable auto-loading or whitelist plugin directories.

### Data Management & Performance
- Cache query registry entries with in-memory TTL and invalidation on `updated_at` change.
- Schedule periodic SQLite maintenance (VACUUM, ANALYZE, integrity checks) via scheduler jobs.
- Provide backup script (`scripts/db_backup.py`) and scheduled job to run it.

### Testing Enhancements
- Add contract tests for widgets (rendering with stub repositories).
- Replace `stress_test.py` with new integration scenario covering import → stock movement → notifications → reporting.
- Add property-based tests for callback namespace utilities and event bus (exception resilience).

### UX / i18n / Frontend
- Prepare internationalization hooks (gettext or lightweight dictionary) for templates and bot texts.
- Manage static assets with cache-busting (versioned query strings).

### Documentation & Process
- Maintain ADR/RFC folder capturing key architectural decisions (plugin API, registries, RBAC).
- Introduce `CODEOWNERS` to define ownership for core/db/ui/bot components.
- Document release process (tags, changelog, migration steps, DB backup prerequisites).

## Parallel Workstreams

- **Core & Database (Phases 1–2):** 2 engineers.
- **Admin UI (Phase 4):** 2 engineers (widgets, superadmin, tables).
- **Bot (Phase 5):** 1–2 engineers.
- **Services (Phase 3 & Phase 5 integrations):** 1 engineer.
- **Plugins & Security (Phases 6–7):** 1 engineer.
- **Docs & Tests (Phase 9 & cross-cutting):** 1 engineer.

This plan enables parallel development while avoiding merge conflicts by establishing the new namespace and shared registries before feature implementation.
