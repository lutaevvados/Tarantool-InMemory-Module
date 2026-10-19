"""
Пул подключений к Citus coordinator для подсистемы диалогов.

Patroni-кластер (users/posts/friends) и Citus (messages) — разные СУБД.
Приложение ходит в Citus только через coordinator: он маршрутизирует
запросы на нужный shard/worker автоматически.

Архитектура Citus в этом проекте:
  coordinator → worker_1, worker_2  (изначально)
             → worker_1, worker_2, worker_3  (после решардинга)

Шардирование таблицы messages:
  Ключ шардирования: dialog_id (UUID пары пользователей)
  Количество шардов: 16 (задаётся через CITUS_SHARD_COUNT)
  Размещение: shard_count шардов равномерно раскладывается по workers

Почему dialog_id, а не from_user_id — см. dialogs.py.
"""

import os
import time
from contextlib import contextmanager

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

# ─── Параметры подключения к Citus coordinator ────────────────────────────
CITUS_HOST = os.getenv("CITUS_HOST", "localhost")
CITUS_PORT = os.getenv("CITUS_PORT", "5444")
CITUS_DB = os.getenv("CITUS_DB", "dialogs")
CITUS_USER = os.getenv("CITUS_USER", "postgres")
CITUS_PASSWORD = os.getenv("CITUS_PASSWORD", "postgres")

# Количество шардов: при 2 workers по 8 шардов на каждый.
# При добавлении третьего worker rebalancer перенесёт часть шардов.
CITUS_SHARD_COUNT = int(os.getenv("CITUS_SHARD_COUNT", "16"))

CITUS_CONN_INFO = (
    f"host={CITUS_HOST} port={CITUS_PORT} dbname={CITUS_DB} "
    f"user={CITUS_USER} password={CITUS_PASSWORD}"
)

# Пул создаётся с open=False — открывается явно в init_citus_pool()
citus_pool = ConnectionPool(
    conninfo=CITUS_CONN_INFO,
    min_size=1,
    max_size=10,
    open=False,
    kwargs={"row_factory": dict_row},
)

# ─── SQL-схема (fallback, если файл citus/schema.sql не найден) ────────────
MESSAGES_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS messages (
    id          UUID        NOT NULL DEFAULT gen_random_uuid(),
    dialog_id   UUID        NOT NULL,           -- ключ шардирования
    from_user_id UUID       NOT NULL,
    to_user_id  UUID        NOT NULL,
    text        TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dialog_id, id)                  -- составной PK включает ключ шардирования
);

-- Основной индекс: получить диалог по dialog_id, сортировка по времени
CREATE INDEX IF NOT EXISTS idx_messages_dialog_created
    ON messages (dialog_id, created_at ASC, id ASC);

-- Дополнительный: найти все входящие/исходящие сообщения пользователя
CREATE INDEX IF NOT EXISTS idx_messages_from_user
    ON messages (from_user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_messages_to_user
    ON messages (to_user_id, created_at DESC);
"""


# ─── Управление пулом ──────────────────────────────────────────────────────

def init_citus_pool():
    """
    Открывает пул подключений к coordinator с повторными попытками.
    Вызывается при старте FastAPI-приложения (lifespan).
    """
    for attempt in range(20):
        try:
            citus_pool.open(wait=True)
            print("Подключение к Citus coordinator успешно.")
            return
        except Exception as e:
            print(f"Citus попытка {attempt + 1}: {e}")
            time.sleep(2)
    raise RuntimeError("Не удалось подключиться к Citus coordinator.")


def close_citus_pool():
    """Закрывает пул при остановке приложения."""
    citus_pool.close()


@contextmanager
def get_citus_connection():
    """Контекстный менеджер: берёт соединение из пула и возвращает обратно."""
    with citus_pool.connection() as conn:
        yield conn


# ─── Инициализация схемы ───────────────────────────────────────────────────

def _table_is_distributed(cur) -> bool:
    """Проверяет, распределена ли уже таблица messages в Citus."""
    cur.execute(
        """
        SELECT 1
        FROM pg_dist_partition
        WHERE logicalrelid = 'messages'::regclass;
        """
    )
    return cur.fetchone() is not None


def init_dialog_schema():
    """
    Идемпотентно создаёт таблицу messages и делает её distributed по dialog_id.

    Шаги:
      1. Выполняет CREATE TABLE IF NOT EXISTS (из файла или встроенного SQL).
      2. Если таблица ещё не distributed — вызывает create_distributed_table().
      3. При повторном запуске приложения — ничего не меняет.
    """
    # Предпочитаем файл citus/schema.sql — там может быть актуальная версия
    schema_path = os.path.join(os.path.dirname(__file__), "citus", "schema.sql")
    if os.path.exists(schema_path):
        with open(schema_path, "r", encoding="utf-8") as f:
            schema_sql = f.read()
    else:
        schema_sql = MESSAGES_SCHEMA_SQL

    with get_citus_connection() as conn:
        with conn.cursor() as cur:
            # 1. Создаём таблицу и индексы (идемпотентно)
            cur.execute(schema_sql)

            # 2. Делаем таблицу distributed только если ещё не сделана
            if not _table_is_distributed(cur):
                cur.execute(f"SET citus.shard_count = {int(CITUS_SHARD_COUNT)};")
                cur.execute(
                    """
                    SELECT create_distributed_table(
                        'messages',
                        'dialog_id',
                        colocate_with => 'none'
                    );
                    """
                )
                print(
                    f"Таблица messages распределена по dialog_id, "
                    f"shard_count={CITUS_SHARD_COUNT}."
                )
            else:
                print("Таблица messages уже распределена в Citus.")
