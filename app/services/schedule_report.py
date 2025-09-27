from __future__ import annotations

import datetime as dt
import html as _html
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from PIL import Image, ImageDraw, ImageFont

from app.services import schedule as sched


def _week_blocks(month_start: dt.date) -> List[List[dt.date]]:
    first = dt.date(month_start.year, month_start.month, 1)
    start = first - dt.timedelta(days=first.weekday())  # Monday=0
    blocks = []
    for w in range(5):
        week = [start + dt.timedelta(days=w*7 + i) for i in range(7)]
        blocks.append(week)
    return blocks


def _name_for(s) -> str:
    dn = (getattr(s, 'display_name', None) or '').strip()
    nm = (getattr(s, 'username', None) or '').strip()
    return dn or nm or str(getattr(s, 'tg_id', ''))


def render_two_month_png(m1: dt.date, m2: dt.date, out_png: Path, conn=None) -> Path:
    own = False
    if conn is None:
        conn = sched._conn(); own = True
    try:
        sellers = sched.list_sellers(conn)[:5]
        names = [_name_for(s) for s in sellers]
        # Geometry
        cell_w, cell_h = 40, 26
        name_w = 140
        padding = 16
        week_gap = 10
        header_h = 20
        month_gap = 24
        # One month height: sum of 5 blocks, each has header+5 rows
        block_h = header_h + 5*cell_h
        month_h = 5*block_h + 4*week_gap + padding*2 + 24  # +month title
        width = padding*2 + name_w + 7*cell_w
        height = month_h*2 + month_gap
        img = Image.new('RGB', (width, height), 'white')
        draw = ImageDraw.Draw(img)
        try:
            font = ImageFont.truetype("Arial.ttf", 14)
            font_small = ImageFont.truetype("Arial.ttf", 12)
        except Exception:
            font = ImageFont.load_default()
            font_small = ImageFont.load_default()

        def draw_month(month_start: dt.date, top: int):
            # Title
            title = month_start.strftime("%B %Y").capitalize()
            draw.text((padding, top), title, fill='black', font=font)
            y = top + 24
            # For each week block
            blocks = _week_blocks(month_start)
            for bi, week in enumerate(blocks):
                # header with day numbers
                x = padding + name_w
                for d in week:
                    txt = f"{d.day}" if d.month == month_start.month else ""
                    tw, th = draw.textsize(txt, font=font_small)
                    draw.text((x + (cell_w-tw)//2, y), txt, fill='black', font=font_small)
                    x += cell_w
                y += header_h
                # rows: up to 5 sellers
                for i in range(5):
                    # name
                    nm = names[i] if i < len(names) else "—"
                    draw.text((padding, y + 4), nm[:18], fill='black', font=font_small)
                    # cells
                    x = padding + name_w
                    for d in week:
                        rect = (x, y, x+cell_w, y+cell_h)
                        # border
                        draw.rectangle(rect, outline='#DDDDDD')
                        if d.month == month_start.month:
                            if not sched.is_open(d, conn):
                                # non-working day
                                draw.text((x + cell_w//2 - 4, y + 6), '✖', fill='red', font=font_small)
                            else:
                                ass = sched.get_assignments(d, conn)
                                if i < len(sellers) and sellers[i].tg_id in ass:
                                    draw.text((x + cell_w//2 - 4, y + 6), 'О', fill='black', font=font_small)
                                else:
                                    draw.text((x + cell_w//2 - 4, y + 6), '–', fill='#666666', font=font_small)
                        x += cell_w
                    y += cell_h
                if bi < len(blocks) - 1:
                    y += week_gap

        draw_month(m1, padding)
        draw_month(m2, padding + month_h + month_gap)
        out_png.parent.mkdir(parents=True, exist_ok=True)
        img.save(out_png)
        return out_png
    finally:
        if own:
            conn.close()


def png_to_pdf(png_path: Path, pdf_path: Path) -> Path:
    img = Image.open(png_path).convert('RGB')
    img.save(pdf_path, "PDF", resolution=150.0)
    return pdf_path


_MONTH_NAMES_RU = {
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


def _month_title_ru(d: dt.date) -> str:
    return f"{_MONTH_NAMES_RU.get(d.month, d.strftime('%B')).capitalize()} {d.year}"


def _day_meta(week: List[dt.date], month_start: dt.date, conn) -> List[Optional[Dict[str, Set[int]]]]:
    info: List[Optional[Dict[str, Set[int]]]] = []
    for day in week:
        if day.month != month_start.month:
            info.append(None)
            continue
        assigned = set(sched.get_assignments(day, conn))
        closed = not sched.is_open(day, conn)
        info.append({"assigned": assigned, "closed": closed})
    return info


def render_two_month_html(m1: dt.date, m2: dt.date, out_html: Path, conn=None) -> Path:
    own = False
    if conn is None:
        conn = sched._conn(); own = True
    try:
        sellers = sched.list_sellers(conn)
        weekdays = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
        now = dt.datetime.now().strftime('%d.%m.%Y %H:%M')

        def render_month(month_start: dt.date) -> str:
            weeks = _week_blocks(month_start)
            rows: List[str] = []
            rows.append(f"<section class=\"month\"><h2>{_html.escape(_month_title_ru(month_start))}</h2>")
            rows.append("<table class=\"schedule-table\">")
            header_cells = ''.join(f"<th>{_html.escape(w)}</th>" for w in weekdays)
            rows.append(f"<thead><tr><th>Сотрудник</th>{header_cells}</tr></thead>")
            rows.append("<tbody>")
            for week in weeks:
                meta = _day_meta(week, month_start, conn)
                day_cells = ''.join(
                    "<td class=\"day-num\">{}</td>".format(d.day if d.month == month_start.month else "")
                    for d in week
                )
                rows.append(f"<tr class=\"week-days\"><td></td>{day_cells}</tr>")
                for seller in sellers:
                    name = _html.escape(_name_for(seller) or "")
                    cell_html: List[str] = []
                    for info, day in zip(meta, week):
                        if info is None:
                            cell_html.append("<td class=\"na\"></td>")
                            continue
                        if info["closed"]:
                            cell_html.append("<td class=\"closed\">✖</td>")
                        elif seller.tg_id in info["assigned"]:
                            cell_html.append("<td class=\"on\">О</td>")
                        else:
                            cell_html.append("<td class=\"off\">–</td>")
                    rows.append(f"<tr><td class=\"name\">{name}</td>{''.join(cell_html)}</tr>")
            rows.append("</tbody></table></section>")
            return ''.join(rows)

        html_parts = [
            "<!DOCTYPE html>",
            "<html lang=\"ru\">",
            "<head>",
            "  <meta charset=\"utf-8\">",
            "  <title>График работы</title>",
            "  <style>",
            "    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 24px; color: #222; background: #fff; }",
            "    h1 { font-size: 24px; margin-bottom: 16px; }",
            "    .month { margin-bottom: 40px; }",
            "    .month h2 { margin: 0 0 12px; font-size: 20px; }",
            "    .schedule-table { border-collapse: collapse; width: 100%; max-width: 960px; }",
            "    .schedule-table th, .schedule-table td { border: 1px solid #d0d0d0; padding: 6px 8px; text-align: center; font-size: 14px; }",
            "    .schedule-table th:first-child, .schedule-table td:first-child { text-align: left; font-weight: 600; min-width: 180px; }",
            "    .schedule-table td.day-num { background: #f3f3f3; font-weight: 600; }",
            "    .schedule-table tr.week-days td { border-top: 2px solid #bdbdbd; }",
            "    .schedule-table td.off { color: #666; }",
            "    .schedule-table td.on { color: #0a7a00; font-weight: 600; }",
            "    .schedule-table td.closed { color: #c21807; font-weight: 600; }",
            "    .legend { margin: 16px 0 32px; font-size: 14px; }",
            "    .legend span { display: inline-block; min-width: 24px; text-align: center; padding: 2px 6px; margin-right: 8px; border: 1px solid #d0d0d0; border-radius: 4px; }",
            "    .legend .on { border-color: #0a7a00; color: #0a7a00; }",
            "    .legend .off { color: #666; }",
            "    .legend .closed { border-color: #c21807; color: #c21807; }",
            "    .generated { font-size: 12px; color: #666; margin-top: 24px; }",
            "  </style>",
            "</head>",
            "<body>",
            "  <h1>График работы продавцов</h1>",
            "  <div class=\"legend\">",
            "    <span class=\"on\">О</span> — в смене,",
            "    <span class=\"off\">–</span> — выходной,",
            "    <span class=\"closed\">✖</span> — нерабочий день",
            "  </div>",
            render_month(m1),
            render_month(m2),
            f"  <div class=\"generated\">Файл создан: {now}</div>",
            "</body>",
            "</html>",
        ]
        out_html.parent.mkdir(parents=True, exist_ok=True)
        out_html.write_text('\n'.join(html_parts), encoding='utf-8')
        return out_html
    finally:
        if own:
            conn.close()
