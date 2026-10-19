#!/usr/bin/env bash
# ============================================================
# bootstrap.sh — инициализация Citus-кластера
#
# Запускается один раз через сервис citus_bootstrap в docker-compose.
# После успешного выполнения сервис завершается (restart: "no").
#
# Что делает этот скрипт:
#   1. Ждёт, пока coordinator и workers полностью готовы.
#   2. Создаёт БД dialogs на coordinator.
#   3. Включает расширение citus на coordinator.
#   4. Создаёт БД dialogs на каждом worker.
#   5. Включает расширение citus на каждом worker.
#   6. Регистрирует worker_1 и worker_2 в coordinator.
#      (worker_3 НЕ регистрируется — он резерв для решардинга)
#
# После выполнения coordinator готов принимать запросы.
# Приложение (FastAPI) само создаст таблицу messages и распределит её.
# ============================================================

set -e

COORDINATOR_HOST="${CITUS_COORDINATOR_HOST:-citus_coordinator}"
COORDINATOR_PORT="${CITUS_COORDINATOR_PORT:-5432}"
DB_NAME="${CITUS_DB:-dialogs}"
PG_USER="${POSTGRES_USER:-postgres}"

WORKERS=("citus_worker_1" "citus_worker_2")

# Функция: ждать готовности PostgreSQL-сервера
wait_for_postgres() {
    local host="$1"
    local port="${2:-5432}"
    echo "Ожидание готовности PostgreSQL на ${host}:${port}..."
    for i in $(seq 1 30); do
        if pg_isready -h "$host" -p "$port" -U "$PG_USER" > /dev/null 2>&1; then
            echo "  ${host}:${port} готов."
            return 0
        fi
        echo "  Попытка $i/30: ${host} ещё не готов..."
        sleep 3
    done
    echo "ОШИБКА: ${host}:${port} не ответил за 90 секунд."
    exit 1
}

# Функция: выполнить SQL на указанном хосте
exec_sql() {
    local host="$1"
    local port="${2:-5432}"
    local dbname="${3:-postgres}"
    local sql="$4"
    PGPASSWORD="$POSTGRES_PASSWORD" psql \
        -h "$host" -p "$port" -U "$PG_USER" -d "$dbname" \
        -c "$sql"
}

# ── Шаг 1: Ждём coordinator ──────────────────────────────────────────────
wait_for_postgres "$COORDINATOR_HOST" "$COORDINATOR_PORT"

# ── Шаг 2: Создаём БД на coordinator ────────────────────────────────────
echo "Создание базы данных '${DB_NAME}' на coordinator..."
exec_sql "$COORDINATOR_HOST" "$COORDINATOR_PORT" "postgres" \
    "SELECT pg_catalog.pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='${DB_NAME}' AND pid <> pg_backend_pid();"
exec_sql "$COORDINATOR_HOST" "$COORDINATOR_PORT" "postgres" \
    "CREATE DATABASE \"${DB_NAME}\";" 2>/dev/null || echo "  База '${DB_NAME}' уже существует."

# ── Шаг 3: Включаем расширение citus на coordinator ─────────────────────
echo "Включение расширения citus на coordinator..."
exec_sql "$COORDINATOR_HOST" "$COORDINATOR_PORT" "$DB_NAME" \
    "CREATE EXTENSION IF NOT EXISTS citus;"

# ── Шаг 4-5: Настраиваем workers ─────────────────────────────────────────
for worker in "${WORKERS[@]}"; do
    wait_for_postgres "$worker" "5432"

    echo "Создание базы данных '${DB_NAME}' на ${worker}..."
    exec_sql "$worker" "5432" "postgres" \
        "CREATE DATABASE \"${DB_NAME}\";" 2>/dev/null || echo "  База уже существует на ${worker}."

    echo "Включение расширения citus на ${worker}..."
    exec_sql "$worker" "5432" "$DB_NAME" \
        "CREATE EXTENSION IF NOT EXISTS citus;"
done

# ── Шаг 6: Регистрируем workers в coordinator ─────────────────────────────
echo "Регистрация workers в coordinator..."
for worker in "${WORKERS[@]}"; do
    exec_sql "$COORDINATOR_HOST" "$COORDINATOR_PORT" "$DB_NAME" \
        "SELECT citus_add_node('${worker}', 5432);" 2>/dev/null \
        || echo "  Worker ${worker} уже зарегистрирован."
done

# ── Финальная проверка ────────────────────────────────────────────────────
echo ""
echo "Активные workers:"
exec_sql "$COORDINATOR_HOST" "$COORDINATOR_PORT" "$DB_NAME" \
    "SELECT nodename, nodeport, isactive FROM pg_dist_node WHERE noderole = 'primary';"

echo ""
echo "Citus bootstrap завершён успешно."
echo "worker_3 НЕ зарегистрирован — используется для демонстрации решардинга."
echo "Для добавления worker_3: POST /admin/dialogs/worker/add"
echo "Для перебалансировки:   POST /admin/dialogs/rebalance"
