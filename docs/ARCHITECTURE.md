# Dvorik Architecture

This document provides an overview of the rebuilt Dvorik codebase. The new
package lives under the `dvorik/` namespace and replaces the former legacy
modules. The rebuild emphasises clear layering, explicit contracts, central
registries and plugin-friendly composition.

## Layered view

The project is organised around the following layers:

```
+---------------------+---------------------------------------------------+
| User interfaces     | Telegram bot (aiogram) + Flask admin UI           |
+---------------------+---------------------------------------------------+
| Application services| Stock, imports, schedule, notifications           |
+---------------------+---------------------------------------------------+
| Data access         | Repository implementations backed by SQLite       |
+---------------------+---------------------------------------------------+
| Domain              | Dataclasses & repository protocols                |
+---------------------+---------------------------------------------------+
| Core                | Config, events, scheduler, plugin loader, registries|
+---------------------+---------------------------------------------------+
| Infrastructure      | SQLite connection helpers & migrations            |
+---------------------+---------------------------------------------------+
```

The `dvorik.app.create_system()` factory acts as the composition root. It
initialises shared services (configuration, database, plugin discovery), wires
built-in widgets/routers/jobs and returns callables to run the admin UI or bot
runtimes.

## Core layer

The `dvorik/core` package provides cross-cutting building blocks:

- **Configuration loader (`config.py`)** — loads `.env`, `config.json` and
  environment variables with deterministic precedence, instantiates the `Config`
  dataclass and ensures runtime directories exist.
- **Event bus (`events.py`)** — asynchronous publish/subscribe registry used to
  decouple services. Subscribers can be sync or async functions and errors are
  logged without interrupting other callbacks.
- **Registries (`registry.py`)** — singleton registries for menus, widgets, bot
  routers, jobs and query overrides. (See file for details.)
- **Scheduler (`scheduler.py`)** — helpers to register cron/daily jobs and run
  them in an asyncio loop, integrating with the job registry.
- **Plugin loader (`plugins.py`)** — discovers modules in `dvorik/plugins`,
  validates metadata and exposes helpers (e.g. `register_widget`,
  `register_bot_router`) used by plugins and built-in components.

These utilities are imported by both the admin UI and the bot to guarantee the
same set of widgets, menus and routers is available regardless of which process
registers them first.

## Database & query infrastructure

The `dvorik/db` package owns SQLite access:

- `conn.py` exposes `db()` that opens connections configured for WAL,
  `busy_timeout`, foreign keys and row factories returning `sqlite3.Row`
  instances.
- `migrations.py` implements `init_db()` which creates the schema covering the
  product catalogue, stock, scheduling, notifications, audit tables and UI
  registries. The function is idempotent and safe to call on every start.
- `query_registry.py` provides a dynamic SQL override store. Callers fetch SQL by
  key with a fallback string, allowing the admin superuser to tweak queries at
  runtime without redeploying code.

`init_db()` is invoked by both the composition root and the Flask factory to
ensure the schema exists before serving requests.

## Domain contracts

Domain models and interfaces live under `dvorik/domain`:

- `models.py` defines dataclasses for catalogue entities (`Product`,
  `Manufacturer`, `SupplierSku`), logistics (`Location`, `StockItem`), schedule
  (`ScheduleDay`, `ScheduleAssignment`), notifications and import logs. These
  types represent the data exchanged between layers.
- `ports.py` declares repository protocols (`ProductRepo`, `StockRepo`,
  `ScheduleRepo`, `ImportLogRepo`). Services depend on these runtime-checkable
  interfaces instead of concrete implementations.

## Repository layer

The `dvorik/repo` package contains SQLite-backed implementations of the domain
ports:

- `product_repo.py` exposes `SQLiteProductRepo` with methods like
  `search_fts()` and `product_detail()` that source SQL via the query registry.
- `stock_repo.py`, `schedule_repo.py` and `import_repo.py` follow the same
  pattern, issuing parameterised SQL retrieved through `get_query()` so that the
  stored SQL can be overridden without altering code.

Repositories are instantiated by services or handlers by passing an open SQLite
connection, keeping transaction boundaries explicit.

## Application services

Business workflows reside under `dvorik/services`:

- `stock.py` handles inventory adjustments, location moves and hub
  reconciliations while emitting events for downstream subscribers.
- `notify.py` subscribes to stock/import events and dispatches instant or daily
  Telegram notifications via the event bus.
- `imports/` hosts parsing strategies (column detection, sheet parsing, CSV
  normalisation) orchestrated by `imports/__init__.py`.
- `schedule.py` aggregates calendar data from repositories and prepares views for
  the bot/admin UI.

Services publish domain events through `dvorik.core.events` so that other
components (e.g. notifications, audit log) can react without tight coupling.

## User interfaces

### Flask admin UI

The admin application is built by `dvorik/admin/server.py` which initialises the
DB, loads plugins and registers blueprints. Widgets are registered via
`dvorik/admin/widgets`, using the widget registry to keep layout definitions in
SQLite (`ui_widget`, `ui_widget_instance`).

Operational visibility is provided through two HTTP endpoints: `/health` offers
a lightweight liveness probe while `/ready` performs deeper checks (database
connectivity plus plugin registration) used by deployment health checks.

Blueprints cover the home dashboard, menu management, supply workflows and
superadmin tools. They consume services and repositories, using the query
registry to render dynamic tables.

### Telegram bot

`dvorik/bot/main.py` builds the aiogram application: it loads configuration,
initialises the database, registers built-in routers and kicks off the polling
loop. Routers are declared in `dvorik/bot/routers/` and registered through the
bot router registry to avoid callback collisions. Callback helpers in
`callbacks.py` and `keyboards.py` enforce namespaced prefixes for inline buttons.
A `/ping` command is exposed via the core router, reporting the scheduler
heartbeat so operators can quickly confirm background jobs are still advancing.

The bot and admin share the same scheduler/job registry; `create_system()` wires
an hourly/daily tick that publishes `scheduler.daily`, enabling background jobs
for notifications or maintenance.

## Plugins

Plugins live under `dvorik/plugins`. The loader imports each package, validates
its metadata and lets plugins register contributions via helper functions. The
example plugin demonstrates how to:

- register a dashboard widget (`TopSkusWidget`), ensuring it appears in the
  widget catalogue and default layout;
- expose a superadmin menu entry linking to the widget configuration;
- add a bot router with custom commands.

This keeps extension points declarative and consistent between bot and admin
processes.

## Composition and entry points

`python -m dvorik.app` is not meant to run directly. Instead use:

- `python -m dvorik.admin` — calls `create_system()` and starts the Flask server
  on `Config.admin_port`.
- `python -m dvorik.bot` — initialises the same system and runs the Telegram bot
  with shared configuration.
- `python -m dvorik.admin.server` — lower-level factory useful for embedding or
  tests.

Both entry points rely on `dvorik.app.DvorikSystem` to ensure plugins, registries
and scheduler jobs are aligned across runtimes.

## Runtime data & directories

Configuration fields control path layout for runtime data (`data/`, `media/`,
`reports/`, uploads, normalised uploads, photos). The loader creates missing
folders on startup, meaning the bot/admin can run in clean environments without
manual setup.

## Extensibility checklist

To build a feature on top of the new architecture:

1. Model new data in `dvorik/domain/models.py` and expose repository contracts in
   `ports.py` if additional persistence is required.
2. Implement repository methods retrieving SQL via `query_registry.get_query()`
   so administrators can override queries from the UI.
3. Write services that orchestrate repositories and publish events for cross-cut
   features (notifications, audit log).
4. Register UI widgets or bot routers through the registries to keep discovery
   centralised and plugin-friendly.
5. Add background jobs via `dvorik.core.scheduler.register_daily`/`register_cron`
   and register them in `JobRegistry`.

Following these steps keeps modules composable and consistent with the rebuild
principles.
