import os
import time
from contextvars import ContextVar
import psycopg
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row
from psycopg.errors import OperationalError

# Контекстная переменная для отслеживания типа транзакции
_is_read_only: ContextVar[bool] = ContextVar('is_read_only', default=False)

# Настройки для master
DB_MASTER_HOST = os.getenv("POSTGRES_MASTER_HOST", "localhost")
DB_MASTER_PORT = os.getenv("POSTGRES_MASTER_PORT", "5432")

# Настройки для slave
DB_SLAVE_HOST = os.getenv("POSTGRES_SLAVE_HOST", "localhost")
DB_SLAVE_PORT = os.getenv("POSTGRES_SLAVE_PORT", "5432")

DB_NAME = os.getenv("POSTGRES_DB", "social_network")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# Строки подключения
MASTER_CONN_INFO = f"host={DB_MASTER_HOST} port={DB_MASTER_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"
SLAVE_CONN_INFO = f"host={DB_SLAVE_HOST} port={DB_SLAVE_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"

# Пулы подключений
master_pool = ConnectionPool(
    conninfo=MASTER_CONN_INFO,
    min_size=1,
    max_size=10,
    open=False,
    kwargs={"row_factory": dict_row}
)

slave_pool = ConnectionPool(
    conninfo=SLAVE_CONN_INFO,
    min_size=1,
    max_size=10,
    open=False,
    kwargs={"row_factory": dict_row}
)

def ensure_database_exists():
    """Проверяет существование целевой БД и создает ее, если она еще не создана."""
    admin_conn_info = f"host={DB_MASTER_HOST} port={DB_MASTER_PORT} dbname=postgres user={DB_USER} password={DB_PASSWORD}"
    for attempt in range(15):
        try:
            with psycopg.connect(admin_conn_info, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
                    if not cur.fetchone():
                        cur.execute(f'CREATE DATABASE "{DB_NAME}"')
                        print(f"✓ База данных '{DB_NAME}' создана на Master.")
            return
        except Exception as e:
            print(f"Ожидание запуска Master для создания БД (попытка {attempt + 1}): {e}")
            time.sleep(2)

def init_db_pool():
    """Инициализирует оба пула подключений."""
    ensure_database_exists()
    for attempt in range(15):
        try:
            master_pool.open(wait=True)
            print("✓ Подключение к Master пулу успешно!")
            break
        except Exception as e:
            print(f"Master попытка {attempt + 1}: {e}")
            time.sleep(2)
    else:
        raise RuntimeError("Не удалось подключиться к Master базе данных.")
    
    for attempt in range(15):
        try:
            slave_pool.open(wait=True)
            print("✓ Подключение к Slave пулу успешно!")
            break
        except Exception as e:
            print(f"Slave попытка {attempt + 1}: {e}")
            time.sleep(2)
    else:
        raise RuntimeError("Не удалось подключиться к Slave базе данных.")

def close_db_pool():
    """Закрывает оба пула подключений."""
    master_pool.close()
    slave_pool.close()

def set_read_only(read_only: bool):
    """Устанавливает флаг read-only для текущей операции."""
    _is_read_only.set(read_only)

def is_read_only() -> bool:
    """Проверяет, является ли текущая операция read-only."""
    return _is_read_only.get()

class DbConnection:
    """Контекстный менеджер для получения соединения из пула с retry-логикой.

    При получении соединения выполняет валидацию (SELECT 1).
    Если соединение "мёртвое" (после failover Patroni или таймаута HAProxy),
    инвалидирует его и берёт новое из пула.
    """

    def __init__(self, max_retries: int = 3):
        self.max_retries = max_retries
        self.conn = None
        self.pool = None

    def __enter__(self):
        self.pool = slave_pool if is_read_only() else master_pool
        last_error = None

        for attempt in range(self.max_retries):
            try:
                self.conn = self.pool.getconn()
                # Валидируем, что соединение живое
                with self.conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return self.conn
            except OperationalError as e:
                last_error = e
                if self.conn is not None:
                    try:
                        self.conn.close()
                    except Exception:
                        pass
                    self.conn = None
                time.sleep(1)

        raise RuntimeError(
            f"Не удалось получить рабочее соединение после {self.max_retries} попыток: {last_error}"
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn is not None:
            if exc_type is not None:
                # Исключение внутри блока — откатываем и закрываем
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                try:
                    self.conn.close()
                except Exception:
                    pass
            else:
                # Нормальный выход — фиксируем изменения и возвращаем в пул
                try:
                    self.conn.commit()
                except Exception:
                    pass
                try:
                    self.pool.putconn(self.conn)
                except Exception:
                    try:
                        self.conn.close()
                    except Exception:
                        pass
            self.conn = None
        return False
