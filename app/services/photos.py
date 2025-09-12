from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Optional

import aiohttp
from PIL import Image
from aiogram import Bot

from app import config as app_config


def compress_image_to_jpeg(src_path: Path, dest_path: Path, quality: int) -> None:
    with Image.open(src_path) as im:
        im = im.convert('RGB')
        w, h = im.size
        new_size = (max(1, w // 2), max(1, h // 2))
        im2 = im.resize(new_size)
        im2.save(dest_path, format='JPEG', quality=quality, optimize=True)


async def download_and_compress_photo(bot: Bot, file_id: str, pid: int) -> Optional[str]:
    """Download photo by file_id, compress, and store in media/photos.
    Returns relative path or None on error.
    """
    try:
        f = await bot.get_file(file_id)
        file_path = f.file_path
        url = f"https://api.telegram.org/file/bot{app_config.BOT_TOKEN}/{file_path}"
        app_config.PHOTOS_DIR.mkdir(parents=True, exist_ok=True)
        ts = int(time.time())
        tmp_path = app_config.PHOTOS_DIR / f"tmp_{pid}_{ts}"
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url) as resp:
                resp.raise_for_status()
                with open(tmp_path, 'wb') as out:
                    while True:
                        chunk = await resp.content.read(65536)
                        if not chunk:
                            break
                        out.write(chunk)
        dest = app_config.PHOTOS_DIR / f"p_{pid}_{ts}.jpg"
        from asyncio import to_thread
        await to_thread(compress_image_to_jpeg, tmp_path, dest, app_config.PHOTO_QUALITY)
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        return str(dest)
    except Exception:
        return None


async def ensure_local_photo(bot: Bot, pid: int, photo_id: Optional[str]) -> Optional[str]:
    """If DB has only Telegram file_id, download/store local photo and update path.
    Returns path or None.
    """
    if not photo_id:
        return None
    from app.db import db
    conn = db()
    try:
        row = conn.execute("SELECT photo_path FROM product WHERE id=?", (pid,)).fetchone()
        have = (row and (row["photo_path"] or "").strip())
        if have and os.path.isfile(have):
            return have
        rel = await download_and_compress_photo(bot, photo_id, pid)
        if rel:
            with conn:
                conn.execute("UPDATE product SET photo_path=? WHERE id=?", (rel, pid))
        return rel
    finally:
        conn.close()

