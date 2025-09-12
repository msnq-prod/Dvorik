#!/usr/bin/env bash
set -euo pipefail

# Абсолютный путь к каталогу со скриптом
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Поиск Python (как в run_bot.command)
if [ -x "/opt/homebrew/opt/python@3.12/bin/python3.12" ]; then
  PY="/opt/homebrew/opt/python@3.12/bin/python3.12"
elif [ -x "/usr/local/opt/python@3.12/bin/python3.12" ]; then
  PY="/usr/local/opt/python@3.12/bin/python3.12"
elif command -v python3.12 >/dev/null 2>&1; then
  PY="$(command -v python3.12)"
elif command -v python3 >/dev/null 2>&1; then
  PY="$(command -v python3)"
else
  echo "Не найден Python 3.12 / python3. Установите: brew install python@3.12" >&2
  exit 1
fi
echo "Использую интерпретатор: $PY"

# Проверяем/создаём venv (та же логика)
NEED_RECREATE=0
if [ -d "venv" ]; then
  if [ ! -f "venv/bin/activate" ] || [ ! -x "venv/bin/python" ]; then
    echo "Обнаружен повреждённый venv — пересоздаю..."
    rm -rf venv
    NEED_RECREATE=1
  fi
fi
if [ -x "venv/bin/python" ]; then
  VENV_VER="$(venv/bin/python - <<'PY'
import sys
print(f"{sys.version_info[0]}.{sys.version_info[1]}")
PY
)"
  if [ "$VENV_VER" != "3.12" ]; then
    echo "Найден venv на Python ${VENV_VER}, но требуется 3.12 — пересоздаю..."
    rm -rf venv
    NEED_RECREATE=1
  fi
else
  NEED_RECREATE=1
fi

create_venv() {
  echo "Создаю виртуальное окружение в ./venv"
  rm -rf venv 2>/dev/null || true
  if "$PY" -m venv venv --without-pip && [ -f "venv/bin/activate" ]; then
    return 0
  fi
  if "$PY" -m venv venv && [ -f "venv/bin/activate" ]; then
    return 0
  fi
  echo "Пробую установить virtualenv и создать окружение..."
  "$PY" -m pip install --user --upgrade pip || true
  "$PY" -m pip install --user virtualenv || true
  if "$PY" -m virtualenv venv && [ -f "venv/bin/activate" ]; then
    return 0
  fi
  return 1
}

if [ $NEED_RECREATE -eq 1 ]; then
  if ! create_venv; then
    echo "Не удалось корректно создать виртуальное окружение в каталоге ./venv." >&2
    exit 1
  fi
fi

if [ -f "venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  source "venv/bin/activate"
else
  echo "Файл venv/bin/activate не найден после создания. Прерываю." >&2
  exit 1
fi

# Устанавливаем/обновляем зависимости
python -m ensurepip --upgrade || true
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

export PYTHONUNBUFFERED=1
export PYTHONPATH="$SCRIPT_DIR:${PYTHONPATH:-}"

# Настройки хоста/порта админки: слушаем на всех интерфейсах, если не задано явно
export ADMIN_HOST="${ADMIN_HOST:-0.0.0.0}"
export ADMIN_PORT="${ADMIN_PORT:-8000}"

# Определим локальный IP для подсказки URL (best-effort)
LAN_IP="$({
  "$PY" - <<'PY'
import socket
ip = None
try:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
except Exception:
    pass
if not ip:
    try:
        ip = socket.gethostbyname(socket.gethostname())
    except Exception:
        ip = "127.0.0.1"
print(ip)
PY
} 2>/dev/null | tr -d '\n')"

echo "Админка будет доступна:"
echo "  - на этом компьютере:   http://127.0.0.1:${ADMIN_PORT}"
if [ -n "${LAN_IP}" ] && [ "${LAN_IP}" != "127.0.0.1" ]; then
  echo "  - из локальной сети:    http://${LAN_IP}:${ADMIN_PORT}"
fi
echo "Слушаю на ${ADMIN_HOST}:${ADMIN_PORT} (если фаерволл открыт)"

python -m admin_ui
