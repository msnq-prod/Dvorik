#!/usr/bin/env bash
set -euo pipefail

# Абсолютный путь к каталогу со скриптом
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# 1) Ищем Python 3.12 (ARM/Mac, Intel/Mac, системный)
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

# 2) Проверим существующий venv: если он не на 3.12 или битый — удалим и пересоздадим
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
  # Попытка 1: venv без pip (главное получить activate)
  if "$PY" -m venv venv --without-pip && [ -f "venv/bin/activate" ]; then
    return 0
  fi
  # Попытка 2: стандартный модуль venv
  if "$PY" -m venv venv && [ -f "venv/bin/activate" ]; then
    return 0
  fi
  # Попытка 3: через virtualenv
  echo "Пробую установить virtualenv и создать окружение..."
  "$PY" -m pip install --user --upgrade pip || true
  "$PY" -m pip install --user virtualenv || true
  if "$PY" -m virtualenv venv && [ -f "venv/bin/activate" ]; then
    return 0
  fi
  return 1
}

# 3) Создаём venv на 3.12 при необходимости
if [ $NEED_RECREATE -eq 1 ]; then
  if ! create_venv; then
    echo "Не удалось корректно создать виртуальное окружение в каталоге ./venv." >&2
    echo "Советы:" >&2
    echo "  1) Обновите Python 3.12: brew reinstall python@3.12" >&2
    echo "  2) Переустановите pip: $PY -m ensurepip --upgrade" >&2
    echo "  3) Установите virtualenv: $PY -m pip install --user virtualenv" >&2
    echo "  4) Попробуйте снова: ./run_bot.command" >&2
    exit 1
  fi
fi

# 4) Активируем окружение (bash/zsh)
if [ -f "venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  source "venv/bin/activate"
else
  echo "Файл venv/bin/activate не найден после создания. Прерываю." >&2
  exit 1
fi

# 5) Устанавливаем pip (если нужно), обновляем и ставим зависимости в venv
python -m ensurepip --upgrade || true
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

# 6) Запуск бота (через модуль app.main)

# Проверим, что есть пакет app
if [ ! -d "$SCRIPT_DIR/app" ]; then
  echo "Каталог $SCRIPT_DIR/app не найден — проверьте структуру проекта." >&2
  ls -la "$SCRIPT_DIR" >&2
  exit 1
fi

export PYTHONUNBUFFERED=1
export PYTHONPATH="$SCRIPT_DIR:${PYTHONPATH:-}"

# Небольшая валидация config.json (если есть)
if [ -f "$SCRIPT_DIR/config.json" ]; then
  python - <<'PY'
import json,sys
try:
    with open('config.json','r',encoding='utf-8') as f:
        cfg=json.load(f)
    bt=(cfg or {}).get('BOT_TOKEN')
    sid=(cfg or {}).get('SUPER_ADMIN_ID')
    if not bt or not str(bt).strip():
        print('ВНИМАНИЕ: BOT_TOKEN пуст в config.json — бот не сможет стартовать.', file=sys.stderr)
    if not sid:
        print('ВНИМАНИЕ: SUPER_ADMIN_ID пуст в config.json.', file=sys.stderr)
except Exception as e:
    print('Предупреждение: config.json не прочитан:', e, file=sys.stderr)
PY
fi

START_TS=$(date +%s)
python -m app.main
CODE=$?
END_TS=$(date +%s)
DUR=$((END_TS-START_TS))

if [ $CODE -ne 0 ] || [ $DUR -lt 3 ]; then
  echo
  echo "Бот завершился (код $CODE) за ${DUR}с. См. сообщения выше."
  # Удерживаем окно для двойного клика из Finder
  if [ -t 0 ]; then
    read -n 1 -s -r -p "Нажмите любую клавишу для выхода..."
    echo
  fi
fi
