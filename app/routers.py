from __future__ import annotations

from aiogram import Dispatcher


def register(dp: Dispatcher) -> None:
    # Import routers locally to avoid circular imports at module import time.
    from app.handlers import core, reports, supply, inline, stock, admin, notify_ui, product, product_admin, admin_create, schedule, registration

    # Базовые команды (/start, home)
    dp.include_router(core.router)
    dp.include_router(reports.router)
    dp.include_router(supply.router)
    dp.include_router(inline.router)
    dp.include_router(stock.router)
    dp.include_router(admin.router)
    dp.include_router(notify_ui.router)
    dp.include_router(product.router)
    dp.include_router(product_admin.router)
    dp.include_router(admin_create.router)
    dp.include_router(schedule.router)
    dp.include_router(registration.router)
