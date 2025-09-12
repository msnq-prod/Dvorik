import os
import json
from pathlib import Path

# Paths
CONFIG_PATH = Path("config.json")
DATA_DIR = Path("data")
UPLOAD_DIR = DATA_DIR / "uploads"
NORMALIZED_DIR = UPLOAD_DIR / "normalized"
REPORTS_DIR = Path("reports")
PHOTOS_DIR = Path("media/photos")

def _load_config() -> dict:
    if CONFIG_PATH.exists():
        with CONFIG_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    return {}

_cfg = _load_config()

# Public settings (kept compatible with marm_bot expectations)
BOT_TOKEN = _cfg.get("BOT_TOKEN") or os.getenv("BOT_TOKEN")

try:
    SUPER_ADMIN_ID = int(_cfg.get("SUPER_ADMIN_ID") or os.getenv("SUPER_ADMIN_ID") or 0)
except ValueError:
    raise RuntimeError("SUPER_ADMIN_ID должен быть целым числом")

SUPER_ADMIN_USERNAME = (
    _cfg.get("SUPER_ADMIN_USERNAME")
    or os.getenv("SUPER_ADMIN_USERNAME")
    or "@msnq_nikita"
)

# Database path (mutable via env/config and marm_bot facade)
_db_path_env = os.getenv("DB_PATH")
_db_path_cfg = _cfg.get("DB_PATH")
if _db_path_env:
    DB_PATH = _db_path_env
elif _db_path_cfg:
    DB_PATH = _db_path_cfg
else:
    # Fallbacks: prefer sqlite3 file; if absent but legacy .db exists, use it
    candidate_sqlite = DATA_DIR / "marm.sqlite3"
    candidate_legacy = DATA_DIR / "marm.db"
    if candidate_sqlite.exists():
        DB_PATH = str(candidate_sqlite)
    elif candidate_legacy.exists():
        DB_PATH = str(candidate_legacy)
    else:
        DB_PATH = str(candidate_sqlite)

# Images
PHOTO_QUALITY = 85

# Pagination/constants
PAGE_SIZE = 10
CARDS_PAGE_SIZE = 20
STOCK_PAGE_SIZE = 30

# Ensure folders exist
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
NORMALIZED_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
PHOTOS_DIR.mkdir(parents=True, exist_ok=True)
