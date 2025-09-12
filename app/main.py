from __future__ import annotations

import asyncio
import datetime as dt

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode

from app import config as app_config
from app import db as app_db
from app.routers import register as register_routers
from app.services.notify import send_daily_digests
from app.services.archival import run_archive_sweep


async def main() -> None:
    # Ensure DB schema
    app_db.init_db()

    # Validate token early for clearer error in double-click run
    if not app_config.BOT_TOKEN:
        print("Ошибка: не задан BOT_TOKEN. Укажите его в config.json или переменной окружения BOT_TOKEN.")
        raise SystemExit(2)

    # Configure Bot + Dispatcher
    session = AiohttpSession(timeout=40)
    bot = Bot(app_config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML), session=session)
    dp = Dispatcher()

    # Register all routers
    register_routers(dp)

    # Background daily tasks at 21:10 local time
    async def _daily_scheduler():
        while True:
            now = dt.datetime.now()
            run_time = now.replace(hour=21, minute=10, second=0, microsecond=0)
            if run_time <= now:
                run_time = run_time + dt.timedelta(days=1)
            await asyncio.sleep((run_time - now).total_seconds())
            try:
                # Run archive sweep before sending digests
                try:
                    archived = run_archive_sweep(30)
                    if archived:
                        print(f"Archived {archived} products by sweep")
                except Exception as e:
                    try:
                        print(f"Archive sweep error: {e}")
                    except Exception:
                        pass
                await send_daily_digests(bot)
            except Exception as e:
                try:
                    print(f"Daily digest error: {e}")
                except Exception:
                    pass
            await asyncio.sleep(5)

    asyncio.create_task(_daily_scheduler())

    print("Бот запущен. Ctrl+C для остановки.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
