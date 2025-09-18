from __future__ import annotations

from aiogram.enums import ParseMode

from app import config as app_config
from app import db as app_db
from app.ui.states import CardFill, AdminStates, AdminEdit
from app.ui.keyboards import (
    grid_buttons,
    locations_2col_keyboard,
    month_calendar_kb,
    admin_day_actions_kb,
    kb_main,
    kb_qty,
    kb_pick_src,
    kb_pick_dst,
    kb_route_src,
    kb_route_dst,
    kb_supply_page,
    kb_cards_page,
    kb_notify,
)
from app.ui.texts import product_caption, stocks_summary, notify_text
from app.ui.cards import kb_card, build_card_for_user, build_admin_item_card
from app.services.auth import (
    _norm_username,
    is_super_admin,
    is_admin,
    is_seller,
    is_allowed,
    require_admin,
)
from app.services.notify import (
    send_daily_digests,
    notify_instant_thresholds as _notify_instant_thresholds,
    notify_instant_to_skl as _notify_instant_to_skl,
    log_event_to_skl as _log_event_to_skl,
    get_notify_mode as _get_notify_mode,
    set_notify_mode as _set_notify_mode,
)
from app.services.stock import (
    move_specific,
    adjust_location_qty,
    total_stock,
)
from app.services.photos import (
    compress_image_to_jpeg as _compress_image_to_jpeg,
    download_and_compress_photo as _download_and_compress_photo,
    ensure_local_photo as _ensure_local_photo,
)
from app.services.imports import (
    excel_to_normalized_csv,
    csv_to_normalized_csv,
    import_supply_from_normalized_csv,
    import_supply_from_excel,
    compute_sha256,
    check_import_duplicate,
    record_import_log,
)
from app.services.move_ctx import get_ctx as _ctx, ctx_badge as _ctx_badge, move_ctx, pop_ctx
from app.services.inventory_ctx import _inv_loc_set, _inv_loc_get, _inv_ctx
from app.utils import safe_cb_answer as _safe_cb_answer, extract_pid_from_cbdata as _extract_pid_from_cbdata
from app.services.products import has_incomplete as _has_incomplete


# Re-export config values
CONFIG_PATH = app_config.CONFIG_PATH
BOT_TOKEN = app_config.BOT_TOKEN
SUPER_ADMIN_ID = app_config.SUPER_ADMIN_ID
SUPER_ADMIN_USERNAME = app_config.SUPER_ADMIN_USERNAME
DB_PATH = app_config.DB_PATH
UPLOAD_DIR = app_config.UPLOAD_DIR
NORMALIZED_DIR = app_config.NORMALIZED_DIR
REPORTS_DIR = app_config.REPORTS_DIR
PHOTOS_DIR = app_config.PHOTOS_DIR
PHOTO_QUALITY = app_config.PHOTO_QUALITY
PAGE_SIZE = app_config.PAGE_SIZE
CARDS_PAGE_SIZE = app_config.CARDS_PAGE_SIZE
STOCK_PAGE_SIZE = app_config.STOCK_PAGE_SIZE

# DB helpers

def db():
    return app_db.db()


def init_db():
    return app_db.init_db()
