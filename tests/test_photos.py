from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
from PIL import Image

from app import config as app_config
from app.services import photos


class _DummyBot:
    def __init__(self, source_path: Path):
        self._source_path = Path(source_path)
        self.calls = []

    async def get_file(self, file_id: str):
        self.calls.append(("get_file", file_id))
        return SimpleNamespace(file_path="telegram/source/path")

    async def download_file(self, file_path: str, destination: Path):
        self.calls.append(("download_file", file_path, Path(destination)))
        dest_path = Path(destination)
        dest_path.write_bytes(self._source_path.read_bytes())


class _FailingBot(_DummyBot):
    async def download_file(self, file_path: str, destination: Path):
        raise RuntimeError("download failure")


def _make_sample_image(path: Path) -> None:
    image = Image.new("RGB", (10, 10), color=(255, 0, 0))
    image.save(path, format="PNG")


def test_download_and_compress_photo_success(monkeypatch, tmp_path):
    photo_dir = tmp_path / "photos"
    monkeypatch.setattr(app_config, "PHOTOS_DIR", photo_dir)
    monkeypatch.setattr(photos.time, "time", lambda: 1_700_000_000)

    source = tmp_path / "source.png"
    _make_sample_image(source)

    bot = _DummyBot(source)
    result = asyncio.run(photos.download_and_compress_photo(bot, "file42", pid=17))

    assert result is not None
    dest_path = Path(result)
    assert dest_path.exists()
    assert dest_path.parent == photo_dir
    assert dest_path.name == "p_17_1700000000.jpg"

    # Temporary files should be cleaned up
    assert not list(photo_dir.glob("tmp_*"))

    with Image.open(dest_path) as img:
        assert img.format == "JPEG"


def test_download_and_compress_photo_failure(monkeypatch, tmp_path, caplog):
    photo_dir = tmp_path / "photos"
    monkeypatch.setattr(app_config, "PHOTOS_DIR", photo_dir)
    monkeypatch.setattr(photos.time, "time", lambda: 1_700_000_123)

    source = tmp_path / "source.png"
    _make_sample_image(source)

    bot = _FailingBot(source)
    with caplog.at_level("ERROR"):
        result = asyncio.run(photos.download_and_compress_photo(bot, "file42", pid=23))

    assert result is None
    assert "Failed to download or compress photo" in caplog.text

    # No destination file or temp leftovers should remain
    assert not list(photo_dir.glob("p_23_*.jpg"))
    assert not list(photo_dir.glob("tmp_*"))
