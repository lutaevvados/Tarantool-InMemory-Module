"""
Наполнение БД тестовыми данными: пользователи, дружба, посты.

Запуск (после поднятия docker-compose):
  python seed_data.py

Переменные окружения:
  POSTGRES_* / REDIS_* — как у приложения.
  SEED_USERS — количество пользователей (по умолчанию 10).
  SEED_POSTS_PER_USER — количество постов на пользователя (по умолчанию 20).
  SEED_PASSWORD — пароль тестовых пользователей (по умолчанию password123).
"""

from __future__ import annotations

import os
import random
import sys
from datetime import date, timedelta

import cache
from auth import hash_password
from cache import FEED_MAX_SIZE, init_redis, replace_feed
from db import close_db_pool, DbConnection, init_db_pool, set_read_only

SAMPLE_POSTS = [
    "Сегодня отличный день для изучения highload-архитектуры!",
    "Кэширование ленты в Redis — must have для соцсетей.",
    "Patroni + etcd + HAProxy = отказоустойчивый PostgreSQL.",
    "Fan-out on write ускоряет чтение ленты друзей.",
    "Инвалидация кэша важнее, чем сам кэш.",
    "Последние 1000 постов друзей храним в Sorted Set.",
    "Перестройка ленты из СУБД спасает после сбоя Redis.",
    "JWT + bcrypt — базовая безопасность API.",
    "Реплики разгружают master от SELECT-запросов.",
    "Тестовые данные помогают проверить /post/feed.",
    "Добавил друга — лента перестроилась автоматически.",
    "Удалил пост — кэши подписчиков обновились.",
    "Offset и limit позволяют листать ленту порциями.",
    "Без очереди fan-out тоже работает, но синхронно.",
    "OTUS Highload Architect — практика важнее теории.",
]

LINE = "=" * 60
PASSWORD = os.getenv("SEED_PASSWORD", "password123")
CITIES = ["Москва", "Санкт-Петербург", "Казань", "Новосибирск"]

_REDIS_CLIENT = None
_REDIS_CLIENT_RESOLVED = False


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default

    try:
        value = int(raw)
    except ValueError:
        print(
            f"Warning: {name}={raw!r} is not int, using default={default}",
            file=sys.stderr,
        )
        return default

    return max(minimum, value)


def _short_id(user_id: str) -> str:
    s = str(user_id)
    return f"{s[:8]}..." if len(s) > 8 else s


def _commit(conn) -> None:
    """
    Аккуратный commit, если соединение его поддерживает.
    Если commit делается контекстным менеджером get_db_connection(),
    то это не должно мешать.
    """
    commit = getattr(conn, "commit", None)
    if callable(commit):
        try:
            commit()
        except Exception:
            pass


def _feed_key(user_id: str) -> str:
    """
    Пытаемся узнать реальный формат Redis-ключа ленты из cache.py.
    Поддерживаются варианты:
      - cache.feed_key(user_id)
      - cache.FEED_KEY_TEMPLATE = "feed:{user_id}"
      - cache.FEED_KEY_TEMPLATE = "feed:{}"
      - cache.FEED_KEY_TEMPLATE = "feed:%s"
      - cache.FEED_KEY_PREFIX = "feed:"
    Если ничего нет — используем feed:<user_id>.
    """
    fn = getattr(cache, "feed_key", None)
    if callable(fn):
        try:
            return str(fn(user_id))
        except Exception:
            pass

    template = getattr(cache, "FEED_KEY_TEMPLATE", None)
    if isinstance(template, str):
        try:
            return template.format(user_id=user_id)
        except Exception:
            pass

        try:
            return template.format(user_id)
        except Exception:
            pass

        try:
            return template % user_id
        except Exception:
            pass

    prefix = getattr(cache, "FEED_KEY_PREFIX", None)
    if isinstance(prefix, str):
        return f"{prefix}{user_id}"

    return f"feed:{user_id}"


def _feed_key_pattern() -> str:
    """
    Паттерн для подсчёта всех feed:* ключей в Redis.
    """
    try:
        sample = _feed_key("__seed_probe__")
        if "__seed_probe__" in sample:
            return sample.replace("__seed_probe__", "*")
    except Exception:
        pass

    pattern = getattr(cache, "FEED_KEY_PATTERN", None)
    if isinstance(pattern, str):
        return pattern

    return "feed:*"


def _get_redis_client():
    """
    Достаём Redis-клиент из cache.py максимально аккуратно.
    Если в cache.py есть get_redis()/redis_client/_redis — используем.
    """
    global _REDIS_CLIENT, _REDIS_CLIENT_RESOLVED

    if _REDIS_CLIENT_RESOLVED:
        return _REDIS_CLIENT

    client = None

    for name in ("get_redis", "get_client", "redis_client", "client", "_redis"):
        obj = getattr(cache, name, None)
        if obj is None:
            continue

        if callable(obj):
            try:
                obj = obj()
            except Exception:
                continue

        if obj is not None:
            client = obj
            break

    _REDIS_CLIENT = client
    _REDIS_CLIENT_RESOLVED = True
    return client


def _redis_feed_count(user_id: str):
    """
    Количество элементов в ленте пользователя в Redis.

    Предполагаем Sorted Set, но на всякий случай пробуем также list/set.
    """
    client = _get_redis_client()
    if client is None:
        return None

    key = _feed_key(user_id)

    for method in ("zcard", "llen", "scard"):
        if hasattr(client, method):
            try:
                return int(getattr(client, method)(key))
            except Exception:
                continue

    return None


def _redis_feed_keys_count():
    """
    Сколько всего feed:* ключей сейчас в Redis.
    """
    client = _get_redis_client()
    if client is None:
        return None

    pattern = _feed_key_pattern()

    try:
        if hasattr(client, "scan_iter"):
            return sum(1 for _ in client.scan_iter(pattern))

        if hasattr(client, "keys"):
            return len(client.keys(pattern))
    except Exception:
        return None

    return None


def _close_redis() -> None:
    """
    Если в cache.py есть функция закрытия Redis-подключения — вызываем её.
    """
    for name in ("close_redis", "close_cache"):
        fn = getattr(cache, name, None)
        if callable(fn):
            try:
                fn()
                return
            except Exception:
                continue


def _create_user(cur, first_name: str, second_name: str, city: str, password: str) -> str:
    cur.execute(
        """
        INSERT INTO users (first_name, second_name, birthdate, gender, biography, city, password_hash)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """,
        (
            first_name,
            second_name,
            date(1990, 1, 1) + timedelta(days=random.randint(0, 5000)),
            random.choice(["Мужской", "Женский"]),
            f"Тестовый профиль {first_name}",
            city,
            hash_password(password),
        ),
    )

    row = cur.fetchone()
    if row is None:
        raise RuntimeError("INSERT INTO users ... RETURNING id вернул пустую строку")

    try:
        uid = row["id"]
    except (TypeError, KeyError):
        uid = row[0]

    return str(uid)


def _build_friend_links(user_ids: list[str]) -> dict[str, list[str]]:
    """
    Кольцевая схема:
    каждый пользователь читает следующего и через одного.

    Для 10 пользователей:
      User0 -> User1, User2
      User1 -> User2, User3
      ...
      User9 -> User0, User1
    """
    links: dict[str, list[str]] = {}
    n = len(user_ids)

    if n == 0:
        return links

    for i, uid in enumerate(user_ids):
        targets: list[str] = []

        for offset in (1, 2):
            friend_id = user_ids[(i + offset) % n]

            if friend_id != uid and friend_id not in targets:
                targets.append(friend_id)

        links[uid] = targets

    return links


def _print_users_table(users: list[dict]) -> None:
    print(LINE)
    print("ТЕСТОВЫЕ ПОЛЬЗОВАТЕЛИ")
    print(LINE)
    print()

    for user in users:
        print(f"[{user['index']}] {user['first_name']} {user['second_name']}")
        print(f"    ID: {user['id']}")
        print(f"    City: {user['city']}")
        print()


def _print_friends(users: list[dict], links: dict[str, list[str]]) -> None:
    users_by_id = {user["id"]: user for user in users}

    print(LINE)
    print("ДРУЖЕСКИЕ СВЯЗИ")
    print(LINE)
    print()

    for user in users:
        print(f"{user['first_name']} ({_short_id(user['id'])})")
        print("  читает:")

        targets = links.get(user["id"], [])
        if not targets:
            print("    -")
        else:
            for friend_id in targets:
                friend = users_by_id.get(friend_id)
                friend_name = friend["first_name"] if friend else "Unknown"
                print(f"    {friend_name} ({_short_id(friend_id)})")

        print()


def seed(users_count: int = 10, posts_per_user: int = 20) -> None:
    if users_count <= 0:
        raise ValueError("users_count должен быть больше 0")

    if posts_per_user < 0:
        raise ValueError("posts_per_user не может быть отрицательным")

    init_db_pool()

    try:
        init_redis()

        users: list[dict] = []
        user_ids: list[str] = []

        # ------------------------------------------------------------
        # 1. Создание пользователей
        # ------------------------------------------------------------
        print("Создание пользователей...")

        with DbConnection() as conn:
            with conn.cursor() as cur:
                for i in range(users_count):
                    first_name = f"User{i}"
                    second_name = f"Testov{i}"
                    city = random.choice(CITIES)

                    uid = _create_user(
                        cur,
                        first_name=first_name,
                        second_name=second_name,
                        city=city,
                        password=PASSWORD,
                    )

                    user = {
                        "index": i,
                        "id": uid,
                        "first_name": first_name,
                        "second_name": second_name,
                        "city": city,
                    }

                    users.append(user)
                    user_ids.append(uid)

                    print(f"User {i}")
                    print(f"ID: {uid}")
                    print(f"Login password: {PASSWORD}")
                    print()

            _commit(conn)

        print(f"[OK] Создано пользователей: {len(users)}")
        print()
        _print_users_table(users)

        users_by_id = {user["id"]: user for user in users}
        links = _build_friend_links(user_ids)
        total_links = 0

        # ------------------------------------------------------------
        # 2. Создание дружеских связей
        # ------------------------------------------------------------
        print(LINE)
        print("СОЗДАНИЕ ДРУЖЕСКИХ СВЯЗЕЙ")
        print(LINE)
        print()

        with DbConnection() as conn:
            with conn.cursor() as cur:
                for uid in user_ids:
                    for friend_id in links.get(uid, []):
                        cur.execute(
                            """
                            INSERT INTO friends (user_id, friend_id)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING;
                            """,
                            (uid, friend_id),
                        )

                        # Для INSERT ... ON CONFLICT DO NOTHING:
                        # rowcount == 1, если связь вставилась.
                        try:
                            if cur.rowcount is not None and cur.rowcount >= 0:
                                total_links += cur.rowcount
                            else:
                                total_links += 1
                        except Exception:
                            total_links += 1

            _commit(conn)

        print(f"[OK] Создано связей: {total_links}")
        print()
        _print_friends(users, links)

        # ------------------------------------------------------------
        # 3. Создание постов
        # ------------------------------------------------------------
        print(LINE)
        print("СОЗДАНИЕ ПОСТОВ")
        print(LINE)
        print()

        total_posts = 0

        with DbConnection() as conn:
            with conn.cursor() as cur:
                for user in users:
                    uid = user["id"]

                    for n in range(posts_per_user):
                        text = (
                            SAMPLE_POSTS[n % len(SAMPLE_POSTS)]
                            + f" #{n} от {uid[:8]}"
                        )

                        cur.execute(
                            """
                            INSERT INTO posts (author_user_id, text)
                            VALUES (%s, %s);
                            """,
                            (uid, text),
                        )

                    total_posts += posts_per_user

                    print(f"{user['first_name']}:")
                    print(f"  posts created: {posts_per_user}")

            _commit(conn)

        print()
        print(f"Всего постов: {total_posts}")
        print()

        # ------------------------------------------------------------
        # 4. Перестройка Redis-лент из PostgreSQL
        # ------------------------------------------------------------
        print(LINE)
        print("ПЕРЕСТРОЙКА КЭШЕЙ ЛЕНТ ИЗ СУБД")
        print(LINE)
        print()

        try:
            feed_limit = int(FEED_MAX_SIZE)
        except (TypeError, ValueError):
            feed_limit = 1000

        if feed_limit < 0:
            feed_limit = 0

        results: list[dict] = []

        set_read_only(True)
        try:
            for user in users:
                uid = user["id"]

                with DbConnection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT p.id, p.text, p.author_user_id, p.created_at
                            FROM posts p
                            INNER JOIN friends f ON f.friend_id = p.author_user_id
                            WHERE f.user_id = %s
                            ORDER BY p.created_at DESC
                            LIMIT %s;
                            """,
                            (uid, feed_limit),
                        )
                        posts = cur.fetchall()

                replace_result = replace_feed(uid, posts)

                pg_count = len(posts)
                friend_ids = links.get(uid, [])
                expected = min(feed_limit, len(friend_ids) * posts_per_user)
                redis_count = _redis_feed_count(uid)

                # Вдруг replace_feed сам возвращает размер кэша.
                if redis_count is None and isinstance(replace_result, int):
                    redis_count = replace_result

                if pg_count != expected:
                    status = "WARN"
                elif redis_count is None:
                    status = "OK (Redis check skipped)"
                elif redis_count != expected:
                    status = "WARN"
                else:
                    status = "OK"

                results.append(
                    {
                        "user_id": uid,
                        "expected": expected,
                        "pg": pg_count,
                        "redis": redis_count,
                        "status": status,
                    }
                )

                friend_names = ", ".join(
                    users_by_id[friend_id]["first_name"]
                    for friend_id in friend_ids
                    if friend_id in users_by_id
                ) or "-"

                print(user["first_name"])
                print(f"ID: {uid}")
                print(f"Redis key: {_feed_key(uid)}")
                print(f"Friends: {len(friend_ids)} ({friend_names})")
                print(f"Expected posts: {expected}")
                print(f"PostgreSQL: {pg_count}")
                print(f"Redis: {'N/A' if redis_count is None else redis_count}")
                print(f"Status: {status}")
                print()

        finally:
            set_read_only(False)

        ok_count = sum(1 for item in results if item["status"].startswith("OK"))
        warn_count = len(results) - ok_count
        redis_keys_count = _redis_feed_keys_count()

        if warn_count == 0:
            print("[OK] Redis cache rebuilt")
        else:
            print(f"[WARN] Redis cache rebuilt with {warn_count} warning(s)")

        print()

        # ------------------------------------------------------------
        # 5. Итоговая сводка
        # ------------------------------------------------------------
        print(LINE)
        print("ГОТОВО")
        print(LINE)
        print()

        print(f"Всего пользователей: {len(users)}")
        print(f"Всего постов: {total_posts}")
        print(f"Всего дружеских связей: {total_links}")
        print(f"Redis feed-ключей для созданных пользователей: {len(results)}")

        if redis_keys_count is not None:
            print(f"Всего feed:* ключей в Redis: {redis_keys_count}")

        print(f"Rebuild OK: {ok_count}/{len(results)}")

        if warn_count:
            print(f"Rebuild WARN: {warn_count}/{len(results)}")

        print(f"Пароль всех тестовых пользователей: {PASSWORD}")
        print()

        print("Для тестирования:")
        print()

        sample_users = users[:10]

        for user in sample_users:
            uid = user["id"]

            print(f"USER {user['index']}:")
            print(f"  Name: {user['first_name']} {user['second_name']}")
            print(f"  ID: {uid}")
            print(f"  Redis key: {_feed_key(uid)}")
            print(f'  Login: POST /login {{"id": "{uid}", "password": "{PASSWORD}"}}')
            print(f"  Feed: GET /post/feed")
            print(f"  Redis CLI: redis-cli ZCARD {_feed_key(uid)}")
            print()

        if len(users) > len(sample_users):
            print(f"... и еще {len(users) - len(sample_users)} пользователей")
            print()

    finally:
        try:
            close_db_pool()
        except Exception:
            pass

        _close_redis()


if __name__ == "__main__":
    users = _env_int("SEED_USERS", 10, minimum=0)
    posts = _env_int("SEED_POSTS_PER_USER", 20, minimum=0)

    try:
        seed(users_count=users, posts_per_user=posts)
    except Exception as exc:
        print(f"Ошибка seed: {exc}", file=sys.stderr)
        sys.exit(1)