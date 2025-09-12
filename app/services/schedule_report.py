from __future__ import annotations

import datetime as dt
from typing import List, Tuple
from pathlib import Path

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
