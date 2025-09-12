import html
import sqlite3


def product_caption(conn: sqlite3.Connection, r: sqlite3.Row) -> str:
    pid = r["id"]
    name_html = html.escape(r["name"] or "")
    article_html = html.escape(r["article"] or "")
    brand_val = r["brand_country"]
    brand = f"\nПроизводитель: {html.escape(brand_val)}" if brand_val else ""
    local_val = r["local_name"]
    local = f"\nЛокальное имя: <i>{html.escape(local_val)}</i>" if local_val else "\nЛокальное имя: —"
    rows = conn.execute(
        """
        SELECT location_code, qty_pack FROM stock
        WHERE product_id=? AND qty_pack>0 ORDER BY location_code
    """,
        (pid,),
    ).fetchall()
    total = 0.0
    stock_lines = []
    for s in rows:
        q = float(s["qty_pack"])
        total += q
        disp = int(q) if float(q).is_integer() else q
        stock_lines.append(f"• {html.escape(s['location_code'])}: {disp}")
    stock_text = "\n".join(stock_lines) if stock_lines else "нет"
    cap = (
        f"<b>{name_html}</b>\n"
        f"{article_html}{brand}{local}\n\n"
        f"Остатки:\n{stock_text}\n"
        f"— — — — —\n"
        f"Итого: {int(total) if total.is_integer() else total}"
    )
    return cap


def stocks_summary(conn: sqlite3.Connection, pid: int) -> str:
    rows = conn.execute(
        "SELECT location_code, qty_pack FROM stock WHERE product_id=? AND qty_pack>0 ORDER BY location_code",
        (pid,),
    ).fetchall()
    parts = []
    for s in rows:
        q = float(s["qty_pack"]) if s["qty_pack"] is not None else 0.0
        disp = int(q) if float(q).is_integer() else q
        parts.append(f"{s['location_code']}: {disp}")
    return "; ".join(parts) if parts else "нет"


def notify_text() -> str:
    return (
        "<b>Уведомления</b>\n\n"
        "Выберите режим для каждого пункта. Текущий вариант отмечен ✅.\n\n"
        "1) Товар закончился — частота уведомлений\n"
        "2) Осталась последняя пачка — частота уведомлений\n"
        "3) Товар ушёл на склад — частота уведомлений"
    )
