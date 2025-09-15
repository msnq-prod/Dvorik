# MARM Bot — Telegram‑бот складского учёта

Телеграм‑бот для учёта товаров на складах. Написан на Python 3.12 и aiogram 3.7, данные хранятся в SQLite (режим WAL, FTS5 при наличии). Проект включает локальную веб‑админку и может запускаться в Docker.

## Возможности
- учёт остатков по локациям, поставки и инвентаризации;
- поиск по названию и артикулу (inline);
- уведомления о критичных остатках;
- экспорт отчётов в CSV;
- загрузка и сжатие фотографий товара;
- веб‑интерфейс для просмотра и редактирования базы.

## Быстрый старт
macOS:
```
./run_bot.command
```
Другие системы:
```
python3.12 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -U pip
pip install -r requirements.txt
python -m app.main
```
Напишите боту `/start` с аккаунта главного администратора.

### Веб‑админка
```
./run_admin.command
```
По умолчанию UI доступен на `http://127.0.0.1:8000`. Для запуска вручную: `python -m admin_ui` (переменные `ADMIN_HOST`, `ADMIN_PORT`).

## Конфигурация
Файл `config.json`:
```json
{
  "BOT_TOKEN": "<токен>",
  "SUPER_ADMIN_ID": 123456789,
  "SUPER_ADMIN_USERNAME": "@your_username"
}
```
Параметры можно переопределить через переменные окружения. База по умолчанию: `data/marm.sqlite3`.

## Роли
- Главный админ — из `config.json`, управляет ролями.
- Админ — полный доступ.
- Продавец — просмотр и базовые операции.

## Повседневные сценарии
1. **Импорт поставки** – «Поставка → Загрузить CSV», файлы сохраняются в `data/uploads/normalized`. Новые товары идут в `SKL‑0`.
2. **Карточка товара** – остатки по локациям, быстрое перемещение, добавление имени и фото.
3. **Инвентаризация** – «Инвентаризация → локация → позиция», корректировки недопустимы в минус.

## Поиск
В чате начните вводить название или артикул. Префиксы:
- `INV ` – инвентаризация;
- `NEW ` – только новые;
- `INC ` – незаполнённые карточки;
- `ADM ` – админ‑действия.

## Отчёты
«Отчёты» формируют CSV в каталоге `reports/`: заканчиваются, нулевые остатки, в достатке, все товары, архив.

## Уведомления
Типы: закончился, последняя пачка, поступило. Режимы `off`/`daily`/`instant`. Ежедневная сводка отправляется в 21:10. Настройка в админке.

## Фото
Фото сжимаются до JPEG (качество `PHOTO_QUALITY`) и сохраняются в `media/photos`. Повторная отправка заменяет снимок.

## База и производительность
SQLite в WAL с `busy_timeout` и `synchronous=NORMAL`. Таблицы: `product`, `location`, `stock`, `user_role`, `user_notify`, `event_log`, виртуальная `product_fts`. Миграции выполняются при старте.

## Стресс‑проверка
```
python stress_test.py
```
Создаёт тестовую БД `data/stress.sqlite3`, выполняет массовые операции и проверяет инварианты. Ожидаемый итог: `OVERALL: OK`.

## Docker
`docker-compose.yml` разворачивает весь стек: контейнеры с ботом (`bot`) и веб‑админкой (`admin`), которые делят каталог с данными.

1. Скопируйте `.env.example` в `.env` и заполните `BOT_TOKEN`, `SUPER_ADMIN_ID`, `SUPER_ADMIN_USERNAME`, `ADMIN_PORT`.
2. Запуск обоих контейнеров: `docker compose up -d --build`.
3. Админка доступна на `http://<IP>:<ADMIN_PORT>`.
4. Остановка: `docker compose down`.

В томах `data`, `media`, `reports` сохраняются база и файлы; логи доступны через `docker compose logs -f bot` и `docker compose logs -f admin`.

## CI/CD

GitHub Actions workflow `.github/workflows/ci.yml` запускает линтер, тесты и сборку Docker-образа.

1. **lint_test** — установка зависимостей, запуск `flake8` и `pytest`.
2. **build_push** — после успешных проверок собирает и пушит образ с тегами `latest` и `${{ github.sha }}` в Docker Hub.
3. **deploy** (опционально) — по SSH выполняет `docker-compose pull && docker-compose up -d`.

Перед запуском настроите секреты репозитория:
- `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN` — учётные данные Docker Hub;
- `SSH_HOST`, `SSH_USER`, `SSH_KEY` — данные для деплоя (если нужен).

Workflow выполняется при `push` и `pull request` в ветку `main`.

## Файловая карта
- Вход: `app/main.py`
- Конфиг: `app/config.py`
- БД: `app/db.py`
- Импорт: `app/services/imports.py`
- Перемещения и инвентаризация: `app/services/stock.py`
- Уведомления: `app/services/notify.py`
- Фото: `app/services/photos.py`
- UI и клавиатуры: `app/ui/*`
- Хендлеры: `app/handlers/*`
