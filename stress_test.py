import asyncio
import os
import random
import string
import time
from pathlib import Path

import sys
from types import ModuleType, SimpleNamespace


def _stub_aiogram_and_deps():
    # Простые заглушки модулей aiogram/aiohttp/pandas для импорта app.bot без зависимостей
    aiogram = ModuleType('aiogram')
    aiogram.exceptions = ModuleType('aiogram.exceptions')
    aiogram.exceptions.TelegramBadRequest = type('TelegramBadRequest', (Exception,), {})

    class _Router:
        def message(self, *a, **kw):
            return lambda f: f
        def callback_query(self, *a, **kw):
            return lambda f: f
        def inline_query(self, *a, **kw):
            return lambda f: f

    class _Dispatcher:
        def include_router(self, *a, **kw):
            pass

    class _Bot:
        def __init__(self, *a, **kw):
            pass

    aiogram.Bot = _Bot
    aiogram.Dispatcher = _Dispatcher
    aiogram.Router = _Router
    class _F:
        def __getattr__(self, name):
            return self
        def __call__(self, *a, **kw):
            return self
        def __eq__(self, other):
            return self
        def startswith(self, *a, **kw):
            return self
        def contains(self, *a, **kw):
            return self
        def len(self):
            return self
        def __gt__(self, other):
            return self
        def __lt__(self, other):
            return self
    aiogram.F = _F()

    aiogram.client = ModuleType('aiogram.client')
    aiogram.client.session = ModuleType('aiogram.client.session')
    aiogram.client.session.aiohttp = ModuleType('aiogram.client.session.aiohttp')
    class _Sess:
        def __init__(self, *a, **kw):
            pass
    aiogram.client.session.aiohttp.AiohttpSession = _Sess

    aiogram.client.default = ModuleType('aiogram.client.default')
    aiogram.client.default.DefaultBotProperties = lambda parse_mode=None: SimpleNamespace(parse_mode=parse_mode)

    aiogram.enums = ModuleType('aiogram.enums')
    aiogram.enums.ParseMode = SimpleNamespace(HTML='HTML')

    aiogram.filters = ModuleType('aiogram.filters')
    aiogram.filters.CommandStart = lambda *a, **kw: object()

    aiogram.fsm = ModuleType('aiogram.fsm')
    aiogram.fsm.context = ModuleType('aiogram.fsm.context')
    aiogram.fsm.context.FSMContext = object
    aiogram.fsm.state = ModuleType('aiogram.fsm.state')
    aiogram.fsm.state.StatesGroup = object
    aiogram.fsm.state.State = object

    aiogram.types = ModuleType('aiogram.types')
    aiogram.types.Message = object
    aiogram.types.CallbackQuery = object
    aiogram.types.InlineKeyboardMarkup = object
    aiogram.types.InlineKeyboardButton = object
    aiogram.types.InlineQuery = object
    aiogram.types.InlineQueryResultArticle = object
    aiogram.types.InputTextMessageContent = object
    aiogram.types.FSInputFile = object

    aiogram.utils = ModuleType('aiogram.utils')
    aiogram.utils.keyboard = ModuleType('aiogram.utils.keyboard')
    class _IKB:
        def __init__(self):
            pass
        def button(self, *a, **kw):
            pass
        def adjust(self, *a, **kw):
            pass
        def row(self, *a, **kw):
            pass
        def as_markup(self):
            return None
    aiogram.utils.keyboard.InlineKeyboardBuilder = _IKB

    sys.modules['aiogram'] = aiogram
    sys.modules['aiogram.exceptions'] = aiogram.exceptions
    sys.modules['aiogram.client'] = aiogram.client
    sys.modules['aiogram.client.session'] = aiogram.client.session
    sys.modules['aiogram.client.session.aiohttp'] = aiogram.client.session.aiohttp
    sys.modules['aiogram.client.default'] = aiogram.client.default
    sys.modules['aiogram.enums'] = aiogram.enums
    sys.modules['aiogram.filters'] = aiogram.filters
    sys.modules['aiogram.fsm'] = aiogram.fsm
    sys.modules['aiogram.fsm.context'] = aiogram.fsm.context
    sys.modules['aiogram.fsm.state'] = aiogram.fsm.state
    sys.modules['aiogram.types'] = aiogram.types
    sys.modules['aiogram.utils'] = aiogram.utils
    sys.modules['aiogram.utils.keyboard'] = aiogram.utils.keyboard

    # aiohttp заглушка
    aiohttp = ModuleType('aiohttp')
    sys.modules['aiohttp'] = aiohttp

    # pandas не трогаем, если установлен — используем реальный для CSV/XLSX тестов


_stub_aiogram_and_deps()

import app.bot as botmod


def _rand_article(n: int = 8) -> str:
    s = ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))
    return f"ART-{s}"


def _gen_csv(path: Path, rows: list[tuple[str, str, float]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        f.write('article,name,qty\n')
        for a, name, q in rows:
            f.write(f"{a},{name},{q}\n")


async def prepare_db(db_path: str):
    # Переключаемся на отдельную тестовую БД
    botmod.DB_PATH = db_path
    # Чистим старые файлы WAL/SHM
    for suf in ('', '-wal', '-shm'):
        p = Path(db_path + suf)
        if p.exists():
            try:
                p.unlink()
            except Exception:
                pass
    botmod.init_db()


async def import_data(csv_a: Path, csv_b: Path):
    # Параллельный импорт двух наборов (B пересекается с A)
    stats_a, stats_b1, stats_b2 = await asyncio.gather(
        asyncio.to_thread(botmod.import_supply_from_normalized_csv, str(csv_a)),
        asyncio.to_thread(botmod.import_supply_from_normalized_csv, str(csv_b)),
        asyncio.to_thread(botmod.import_supply_from_normalized_csv, str(csv_b)),
    )
    return stats_a, stats_b1, stats_b2


async def random_moves(num_workers: int, ops_per_worker: int):
    conn = botmod.db()
    pids = [int(r['id']) for r in conn.execute("SELECT id FROM product").fetchall()]
    locs = [r['code'] for r in conn.execute("SELECT code FROM location").fetchall()]
    conn.close()

    async def worker(idx: int):
        rnd = random.Random(idx * 1234567 + int(time.time()))
        for _ in range(ops_per_worker):
            pid = rnd.choice(pids)
            conn = botmod.db()
            try:
                # 50% — перемещение, 50% — инвентаризация (+/−) в SKL-0
                if rnd.random() < 0.5:
                    # выбираем источник с остатком, при отсутствии — добавим +1 на SKL-0
                    rows = conn.execute(
                        "SELECT location_code, qty_pack FROM stock WHERE product_id=? AND qty_pack>0",
                        (pid,),
                    ).fetchall()
                    if not rows:
                        botmod.adjust_location_qty(conn, pid, 'SKL-0', 1)
                        rows = conn.execute(
                            "SELECT location_code, qty_pack FROM stock WHERE product_id=? AND qty_pack>0",
                            (pid,),
                        ).fetchall()
                    if not rows:
                        continue
                    src = rnd.choice(rows)['location_code']
                    qty = 1 if rnd.random() < 0.8 else 2
                    dst = rnd.choice([c for c in locs if c != src and c != 'HALL'])
                    ok, _ = botmod.move_specific(conn, pid, src, dst, qty)
                    # Лог для суточной сводки, если на склад
                    if ok and dst.startswith('SKL'):
                        botmod._log_event_to_skl(conn, pid, dst, qty)
                else:
                    delta = rnd.choice([-1, 1, 2])
                    # инвентаризация в SKL-0
                    ok, _ = botmod.adjust_location_qty(conn, pid, 'SKL-0', delta)
                    if not ok:
                        # если ушли бы в минус — просто пропустим
                        pass
            finally:
                conn.close()

    await asyncio.gather(*(worker(i) for i in range(num_workers)))


async def concurrent_adjustments_stability(iters: int = 2000, concurrency: int = 50) -> bool:
    """Пытается поймать потерю инкрементов при adjust_location_qty (гонка).

    Инициируем qty=0 на SKL-0 для произвольного товара, затем параллельно делаем +1.
    Ожидаем ровно iters инкрементов.
    """
    conn = botmod.db()
    pid = int(conn.execute("SELECT id FROM product ORDER BY id LIMIT 1").fetchone()['id'])
    with conn:
        conn.execute("DELETE FROM stock WHERE product_id=? AND location_code='SKL-0'", (pid,))
    conn.close()

    async def worker(n):
        for _ in range(n):
            c = botmod.db()
            try:
                botmod.adjust_location_qty(c, pid, 'SKL-0', 1)
            finally:
                c.close()

    per = iters // concurrency
    await asyncio.gather(*(worker(per) for _ in range(concurrency)))
    conn = botmod.db()
    try:
        row = conn.execute("SELECT qty_pack FROM stock WHERE product_id=? AND location_code='SKL-0'", (pid,)).fetchone()
        got = int(row['qty_pack']) if row and row['qty_pack'] is not None else 0
        print('Concurrent adjust expected', iters, 'got', got)
        return got == iters
    finally:
        conn.close()


async def forced_lost_update_demo(concurrency: int = 50) -> int:
    """Форсируем гонку: все читают старое значение и одновременно пишут +1.

    Возвращает полученное значение (ожидали бы == concurrency при корректной атомарности).
    """
    conn = botmod.db()
    pid = int(conn.execute("SELECT id FROM product ORDER BY id LIMIT 1").fetchone()['id'])
    with conn:
        conn.execute("DELETE FROM stock WHERE product_id=? AND location_code='SKL-0'", (pid,))
        conn.execute("INSERT INTO stock(product_id, location_code, qty_pack) VALUES (?,?,?)", (pid, 'SKL-0', 0.0))
    conn.close()

    # Синхронизатор, чтобы все сделали SELECT до первого UPDATE
    start = asyncio.Event()
    reads = {}

    async def actor(i):
        c = botmod.db()
        try:
            row = c.execute("SELECT qty_pack FROM stock WHERE product_id=? AND location_code='SKL-0'", (pid,)).fetchone()
            have = float(row['qty_pack']) if row and row['qty_pack'] is not None else 0.0
            reads[i] = have
            await start.wait()
            # Используем именно adjust_location_qty (атомарный инкремент)
            botmod.adjust_location_qty(c, pid, 'SKL-0', 1)
        finally:
            c.close()

    tasks = [asyncio.create_task(actor(i)) for i in range(concurrency)]
    # ждём пока все прочитают
    while len(reads) < concurrency:
        await asyncio.sleep(0.001)
    start.set()
    await asyncio.gather(*tasks)

    conn = botmod.db()
    try:
        got = int(conn.execute("SELECT qty_pack FROM stock WHERE product_id=? AND location_code='SKL-0'", (pid,)).fetchone()['qty_pack'])
        print('Forced lost-update demo: expected', concurrency, 'got', got)
        return got
    finally:
        conn.close()


def validate_invariants() -> dict:
    conn = botmod.db()
    try:
        issues = []
        # Проверка на отрицательные остатки
        neg = conn.execute("SELECT * FROM stock WHERE qty_pack < 0").fetchall()
        if neg:
            issues.append(f"Negative stock rows: {len(neg)}")
        # Проверка сумм
        pids = [int(r['id']) for r in conn.execute("SELECT id FROM product").fetchall()]
        for pid in pids:
            row = conn.execute("SELECT IFNULL(SUM(qty_pack),0) AS t FROM stock WHERE product_id=?", (pid,)).fetchone()
            s = float(row['t'] or 0)
            if s < 0:
                issues.append(f"Total negative for pid={pid}")
        # Быстрый поиск FTS (если доступен)
        have_fts = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='product_fts'"
        ).fetchone()
        fts_ok = None
        if have_fts:
            try:
                conn.execute(
                    "SELECT p.id FROM product_fts f JOIN product p ON p.id=f.rowid WHERE product_fts MATCH ? LIMIT 5",
                    ("ART*",),
                ).fetchall()
                fts_ok = True
            except Exception:
                fts_ok = False
        # Кол-во товаров/строк stock
        pcount = conn.execute("SELECT COUNT(*) AS c FROM product").fetchone()['c']
        scount = conn.execute("SELECT COUNT(*) AS c FROM stock").fetchone()['c']
        return {
            'issues': issues,
            'fts_ok': fts_ok,
            'products': pcount,
            'stock_rows': scount,
        }
    finally:
        conn.close()


class FakeBot:
    def __init__(self):
        self.messages = []

    async def send_message(self, uid, text):
        self.messages.append((uid, text))


async def daily_digest_check() -> int:
    # Создадим тест-админа и настройки «daily» для всех типов
    conn = botmod.db()
    with conn:
        conn.execute("INSERT OR IGNORE INTO user_role(tg_id, username, role) VALUES (?,?, 'admin')", (999999, '@stress_admin'))
        for t in ('zero', 'last', 'to_skl'):
            conn.execute(
                "INSERT OR REPLACE INTO user_notify(user_id, notif_type, mode) VALUES (?,?, 'daily')",
                (999999, t),
            )
    conn.close()
    fake = FakeBot()
    await botmod.send_daily_digests(fake)
    return len(fake.messages)


async def compress_photo_check() -> bool:
    from PIL import Image

    src = Path('media/photos/_stress_src.jpg')
    dst = Path('media/photos/_stress_out.jpg')
    src.parent.mkdir(parents=True, exist_ok=True)
    # Генерируем тестовую картинку
    img = Image.new('RGB', (640, 480), color=(120, 180, 220))
    img.save(src, format='JPEG', quality=92)
    await asyncio.to_thread(botmod._compress_image_to_jpeg, src, dst, botmod.PHOTO_QUALITY)
    return dst.exists()


async def main():
    random.seed(42)
    test_db = 'data/stress.sqlite3'
    await prepare_db(test_db)

    # Готовим данные для импорта
    N = 1000
    rows = [(f"ART-{i:05d}", f"Product {i}", 5.0) for i in range(N)]
    rows_b = [(f"ART-{i:05d}", f"Product {i}", 3.0) for i in range(500)]
    csv_a = Path('data/uploads/normalized/_stress_A.csv')
    csv_b = Path('data/uploads/normalized/_stress_B.csv')
    _gen_csv(csv_a, rows)
    _gen_csv(csv_b, rows_b)

    # Импортируем параллельно
    s_a, s_b1, s_b2 = await import_data(csv_a, csv_b)
    print('Import A:', s_a['imported'], 'created:', s_a['created'], 'updated:', s_a['updated'])
    print('Import B1:', s_b1['imported'], 'created:', s_b1['created'], 'updated:', s_b1['updated'])
    print('Import B2:', s_b2['imported'], 'created:', s_b2['created'], 'updated:', s_b2['updated'])

    # Параллельные операции
    await random_moves(num_workers=16, ops_per_worker=400)

    # Валидации
    res = validate_invariants()
    print('Validate:', res)

    # Сводка «в конце дня»
    sent = await daily_digest_check()
    print('Daily digest messages sent:', sent)

    # Сжатие фото
    photo_ok = await compress_photo_check()
    print('Photo compression OK:', photo_ok)

    # Конкурентные инкременты через adjust_location_qty
    adj_ok = await concurrent_adjustments_stability(iters=5000, concurrency=100)
    print('Concurrent adjustments OK:', adj_ok)

    # Демонстрация возможной гонки (если бы была) для adjust_location_qty
    lost = await forced_lost_update_demo(concurrency=100)

    # Простой Excel-тест (шапка Артикул/Наименование/Кол-во)
    try:
        import pandas as pd
        import numpy as np
        xls_path = Path('data/uploads/_stress_simple.xlsx')
        df = pd.DataFrame({
            'Артикул': [f'X{i:04d}' for i in range(50)],
            'Наименование': [f'Item {i}' for i in range(50)],
            'Кол-во': np.ones(50)
        })
        df.to_excel(xls_path, index=False)
        norm_xls, stats_xls = botmod.excel_to_normalized_csv(str(xls_path))
        print('Excel normalize:', bool(norm_xls), stats_xls.get('found'), stats_xls.get('errors'))
        if norm_xls:
            s = botmod.import_supply_from_normalized_csv(norm_xls)
            print('Excel import:', s['imported'])
        excel_ok = True
    except Exception as e:
        print('Excel test failed:', e)
        excel_ok = False

    # Негативные кейсы CSV (плохой заголовок, пустые/мусорные строки)
    bad_csv = Path('data/uploads/_bad.csv')
    with open(bad_csv, 'w', encoding='utf-8') as f:
        f.write('foo,bar,baz\n1,2,3\n, , \n')
    norm_bad, bad_stats = botmod.csv_to_normalized_csv(str(bad_csv))
    print('Bad CSV normalize:', bool(norm_bad), bad_stats)

    ok = not res['issues'] and (sent >= 0) and photo_ok and adj_ok and (lost == 100) and excel_ok
    print('OVERALL:', 'OK' if ok else 'HAS ISSUES')


if __name__ == '__main__':
    asyncio.run(main())
