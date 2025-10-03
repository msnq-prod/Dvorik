from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.json"

CONFIG_KEYS = {
    "BOT_TOKEN",
    "SUPER_ADMIN_ID",
    "SUPER_ADMIN_USERNAME",
    "ADMIN_PORT",
    "DB_PATH",
    "DATA_DIR",
    "MEDIA_DIR",
    "REPORTS_DIR",
    "UPLOAD_DIR",
    "NORMALIZED_DIR",
    "PHOTOS_DIR",
    "PAGE_SIZE",
    "CARDS_PAGE_SIZE",
    "STOCK_PAGE_SIZE",
    "PHOTO_QUALITY",
    "PLUGIN_PATHS",
    "PLUGIN_DISABLED",
}


@dataclass(slots=True)
class Config:
    """In-memory representation of runtime configuration values."""

    bot_token: str
    super_admin_id: int | None
    super_admin_username: str
    admin_port: int
    db_path: Path
    data_dir: Path
    media_dir: Path
    reports_dir: Path
    uploads_dir: Path
    normalized_uploads_dir: Path
    photos_dir: Path
    page_size: int
    cards_page_size: int
    stock_page_size: int
    photo_quality: int
    plugin_paths: tuple[str, ...]
    plugin_disabled: bool

    def as_dict(self) -> Dict[str, Any]:
        """Return a serialisable mapping of the configuration values."""

        return {
            "BOT_TOKEN": self.bot_token,
            "SUPER_ADMIN_ID": self.super_admin_id,
            "SUPER_ADMIN_USERNAME": self.super_admin_username,
            "ADMIN_PORT": self.admin_port,
            "DB_PATH": str(self.db_path),
            "DATA_DIR": str(self.data_dir),
            "MEDIA_DIR": str(self.media_dir),
            "REPORTS_DIR": str(self.reports_dir),
            "UPLOAD_DIR": str(self.uploads_dir),
            "NORMALIZED_DIR": str(self.normalized_uploads_dir),
            "PHOTOS_DIR": str(self.photos_dir),
            "PAGE_SIZE": self.page_size,
            "CARDS_PAGE_SIZE": self.cards_page_size,
            "STOCK_PAGE_SIZE": self.stock_page_size,
            "PHOTO_QUALITY": self.photo_quality,
            "PLUGIN_PATHS": list(self.plugin_paths),
            "PLUGIN_DISABLED": self.plugin_disabled,
        }


DEFAULTS: Dict[str, Any] = {
    "BOT_TOKEN": "",
    "SUPER_ADMIN_ID": None,
    "SUPER_ADMIN_USERNAME": "@superadmin",
    "ADMIN_PORT": 8000,
    "DATA_DIR": PROJECT_ROOT / "data",
    "MEDIA_DIR": PROJECT_ROOT / "media",
    "REPORTS_DIR": PROJECT_ROOT / "reports",
    "UPLOAD_DIR": PROJECT_ROOT / "data" / "uploads",
    "NORMALIZED_DIR": PROJECT_ROOT / "data" / "uploads" / "normalized",
    "PHOTOS_DIR": PROJECT_ROOT / "media" / "photos",
    "DB_PATH": PROJECT_ROOT / "data" / "marm.sqlite3",
    "PAGE_SIZE": 10,
    "CARDS_PAGE_SIZE": 20,
    "STOCK_PAGE_SIZE": 30,
    "PHOTO_QUALITY": 85,
    "PLUGIN_PATHS": ("dvorik/plugins",),
    "PLUGIN_DISABLED": False,
}

_CONFIG: Config | None = None


def load_config(
    *,
    env_path: str | Path | None = None,
    json_path: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> Config:
    """Load configuration from JSON file, ``.env`` file and environment."""

    env_path = Path(env_path) if env_path is not None else DEFAULT_ENV_PATH
    json_path = Path(json_path) if json_path is not None else DEFAULT_CONFIG_PATH
    env_mapping = dict(environ or os.environ)

    values: Dict[str, Any] = dict(DEFAULTS)
    values.update(_read_json(json_path))
    values.update(_read_env_file(env_path))

    for key in CONFIG_KEYS:
        if key in env_mapping:
            values[key] = env_mapping[key]

    bot_token = str(values.get("BOT_TOKEN") or "")
    super_admin_id = _parse_optional_int(values.get("SUPER_ADMIN_ID"))
    super_admin_username = str(values.get("SUPER_ADMIN_USERNAME") or "")
    admin_port = _parse_int(values.get("ADMIN_PORT"), field="ADMIN_PORT", default=8000)

    data_dir = _resolve_path(values.get("DATA_DIR"), PROJECT_ROOT / "data")
    media_dir = _resolve_path(values.get("MEDIA_DIR"), PROJECT_ROOT / "media")
    reports_dir = _resolve_path(values.get("REPORTS_DIR"), PROJECT_ROOT / "reports")

    uploads_default = data_dir / "uploads"
    uploads_dir = _resolve_path(values.get("UPLOAD_DIR"), uploads_default)

    normalized_default = uploads_dir / "normalized"
    normalized_dir = _resolve_path(values.get("NORMALIZED_DIR"), normalized_default)

    photos_default = media_dir / "photos"
    photos_dir = _resolve_path(values.get("PHOTOS_DIR"), photos_default)

    db_default = data_dir / "marm.sqlite3"
    db_path = _resolve_path(values.get("DB_PATH"), db_default)

    page_size = _parse_int(values.get("PAGE_SIZE"), field="PAGE_SIZE", default=10)
    cards_page_size = _parse_int(
        values.get("CARDS_PAGE_SIZE"), field="CARDS_PAGE_SIZE", default=20
    )
    stock_page_size = _parse_int(
        values.get("STOCK_PAGE_SIZE"), field="STOCK_PAGE_SIZE", default=30
    )
    photo_quality = _parse_int(
        values.get("PHOTO_QUALITY"), field="PHOTO_QUALITY", default=85
    )
    plugin_paths = _parse_plugin_paths(values.get("PLUGIN_PATHS"), DEFAULTS["PLUGIN_PATHS"])
    plugin_disabled = _parse_bool(
        values.get("PLUGIN_DISABLED"), field="PLUGIN_DISABLED", default=False
    )

    config = Config(
        bot_token=bot_token,
        super_admin_id=super_admin_id,
        super_admin_username=super_admin_username,
        admin_port=admin_port,
        db_path=db_path,
        data_dir=data_dir,
        media_dir=media_dir,
        reports_dir=reports_dir,
        uploads_dir=uploads_dir,
        normalized_uploads_dir=normalized_dir,
        photos_dir=photos_dir,
        page_size=page_size,
        cards_page_size=cards_page_size,
        stock_page_size=stock_page_size,
        photo_quality=photo_quality,
        plugin_paths=plugin_paths,
        plugin_disabled=plugin_disabled,
    )

    _ensure_directories(config)
    return config


def get_config(*, refresh: bool = False) -> Config:
    """Return cached configuration, reloading when ``refresh`` is ``True``."""

    global _CONFIG
    if _CONFIG is None or refresh:
        _CONFIG = load_config()
    return _CONFIG


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except json.JSONDecodeError as exc:  # pragma: no cover - fatal configuration error
        raise RuntimeError(f"Invalid JSON in configuration file: {path}") from exc
    if not isinstance(data, dict):  # pragma: no cover - fatal configuration error
        raise RuntimeError(f"Configuration file must contain an object: {path}")
    return {key: value for key, value in data.items() if key in CONFIG_KEYS}


def _read_env_file(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}

    result: Dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if value and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        result[key] = value
    return {key: value for key, value in result.items() if key in CONFIG_KEYS}


def _parse_int(value: Any, *, field: str, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return default
        try:
            return int(stripped)
        except ValueError as exc:
            raise RuntimeError(f"{field} must be an integer") from exc
    raise RuntimeError(f"{field} must be an integer")


def _parse_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(stripped)
        except ValueError as exc:
            raise RuntimeError("SUPER_ADMIN_ID must be an integer") from exc
    raise RuntimeError("SUPER_ADMIN_ID must be an integer")


def _resolve_path(value: Any, default: Path) -> Path:
    path_value = default
    if value not in (None, ""):
        if isinstance(value, Path):
            path_value = value
        else:
            path_value = Path(str(value))
    path_value = path_value.expanduser()
    if not path_value.is_absolute():
        path_value = PROJECT_ROOT / path_value
    return path_value.resolve()


def _parse_plugin_paths(value: Any, default: Sequence[Any]) -> tuple[str, ...]:
    if not default:
        default_values: Sequence[Any] = ()
    else:
        default_values = default

    if value in (None, ""):
        candidates: Iterable[Any] = default_values
    elif isinstance(value, (list, tuple, set)):
        candidates = value
    elif isinstance(value, Path):
        candidates = (value,)
    else:
        text = str(value).strip()
        if not text:
            candidates = default_values
        else:
            normalised = text
            for separator in ("\n", ",", ";"):
                normalised = normalised.replace(separator, os.pathsep)
            candidates = normalised.split(os.pathsep)

    resolved: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate in (None, ""):
            continue
        text = str(candidate).strip()
        if not text or text.lower() in seen:
            continue
        seen.add(text.lower())
        resolved.append(text)

    if resolved:
        return tuple(resolved)

    fallback: list[str] = []
    fallback_seen: set[str] = set()
    for candidate in default_values:
        if candidate in (None, ""):
            continue
        text = str(candidate).strip()
        if not text or text.lower() in fallback_seen:
            continue
        fallback.append(text)
        fallback_seen.add(text.lower())
    return tuple(fallback)


def _parse_bool(value: Any, *, field: str, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if not text:
            return default
        if text in {"1", "true", "yes", "on", "enable", "enabled"}:
            return True
        if text in {"0", "false", "no", "off", "disable", "disabled"}:
            return False
    raise RuntimeError(f"{field} must be a boolean")


def _ensure_directories(config: Config) -> None:
    for directory in {
        config.data_dir,
        config.media_dir,
        config.reports_dir,
        config.uploads_dir,
        config.normalized_uploads_dir,
        config.photos_dir,
        config.db_path.parent,
    }:
        directory.mkdir(parents=True, exist_ok=True)


config = get_config()


__all__ = ["Config", "load_config", "get_config", "config"]

