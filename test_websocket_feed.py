"""
Скрипт проверки асинхронного онлайн-обновления ленты постов через WebSocket и RabbitMQ.

Сценарий:
1. Регистрация двух пользователей (Пользователь A — Автор, Пользователь B — Подписчик).
2. Авторизация обоих пользователей.
3. Пользователь B добавляет Пользователя A в друзья.
4. Пользователь B подключается к WebSocket каналу /post/feed/posted.
5. Пользователь A создает пост через REST API (POST /post/create).
6. Пользователь B в реальном времени получает событие о посте через WebSocket.
7. Пользователь B запрашивает ленту (GET /post/feed) и уверяется, что пост в ленте.
"""

import json
import sys
import time
import urllib.request
import urllib.parse
import websockets
import asyncio

BASE_URL = "http://localhost:8000"
WS_URL = "ws://localhost:8000/post/feed/posted"


def api_request(path, method="GET", data=None, token=None):
    url = f"{BASE_URL}{path}"
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    body_bytes = json.dumps(data).encode("utf-8") if data else None
    req = urllib.request.Request(url, data=body_bytes, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        try:
            err = json.loads(body)
        except Exception:
            err = body
        return e.code, err


def register_and_login(first_name, second_name):
    # Регистрация
    user_data = {
        "first_name": first_name,
        "second_name": second_name,
        "birthdate": "1995-05-05",
        "gender": "male",
        "biography": "Тест вебсокетов",
        "city": "Москва",
        "password": "password123",
    }
    status, res = api_request("/user/register", method="POST", data=user_data)
    if status != 201:
        print(f"[ОШИБКА] Регистрация {first_name}: {status} {res}")
        sys.exit(1)

    user_id = res["user_id"]

    # Логин
    status, res = api_request("/login", method="POST", data={"id": user_id, "password": "password123"})
    if status != 200:
        print(f"[ОШИБКА] Логин {first_name}: {status} {res}")
        sys.exit(1)

    token = res["token"]
    print(f"✓ Пользователь {first_name} создан (ID: {user_id})")
    return user_id, token


async def main():
    print("=== ТЕСТ ОНЛАЙН-ОБНОВЛЕНИЯ ЛЕНТЫ ЧЕРЕЗ WEBSOCKET И RABBITMQ ===")

    # 1. Создаем Автора и Подписчика
    author_id, author_token = register_and_login("Автор_WS", "Постов")
    sub_id, sub_token = register_and_login("Подписчик_WS", "Ленты")

    # 2. Подписчик добавляет Автора в друзья
    status, res = api_request(f"/friend/set/{author_id}", method="PUT", token=sub_token)
    if status != 200:
        print(f"[ОШИБКА] Добавление в друзья: {status} {res}")
        sys.exit(1)
    print("✓ Подписчик добавил Автора в друзья")

    # 3. Подключаемся по WebSocket от имени Подписчика
    ws_uri = f"{WS_URL}?token={sub_token}"
    print(f"-> Подключение к WebSocket: {ws_uri}")

    async with websockets.connect(ws_uri) as ws:
        print("✓ WebSocket соединение установлено.")

        # 4. Автор создает новый пост в отдельной таске через REST API
        post_text = f"Тестовый пост в реальном времени! timestamp={time.time()}"

        def create_post_task():
            time.sleep(0.5)  # Небольшая пауза для гарантии готовности сокета
            st, post_res = api_request(
                "/post/create",
                method="POST",
                data={"text": post_text},
                token=author_token,
            )
            print(f"✓ Автор создал пост REST API (ID: {post_res})")
            return st, post_res

        loop = asyncio.get_running_loop()
        create_task = loop.run_in_executor(None, create_post_task)

        # 5. Подписчик ожидает событие из WebSocket
        print("-> Ожидание события из WebSocket...")
        try:
            msg_text = await asyncio.wait_for(ws.recv(), timeout=5.0)
            msg_data = json.loads(msg_text)
            print("★ [ПОЛУЧЕНО СОБЫТИЕ ПО WEBSOCKET]:")
            print(json.dumps(msg_data, indent=2, ensure_ascii=False))

            assert msg_data["text"] == post_text
            assert msg_data["author_user_id"] == author_id
            print("✓ Проверка данных события прошла успешно!")

        except asyncio.TimeoutError:
            print("[ОШИБКА] Превышено время ожидания сообщения по WebSocket!")
            sys.exit(1)

        await create_task

    # 6. Проверяем получение поста через GET /post/feed
    status, feed_res = api_request("/post/feed", method="GET", token=sub_token)
    if status == 200 and len(feed_res) > 0 and feed_res[0]["text"] == post_text:
        print("✓ Пост успешно материализован и получен из ленты /post/feed")
    else:
        print(f"[ОШИБКА] Проверка /post/feed не пройдена: status={status}, feed={feed_res}")
        sys.exit(1)

    print("\n=== ВСЕ ТЕСТЫ WEBSOCKET И RABBITMQ УСПЕШНО ПРОЙДЕНЫ! ===")


if __name__ == "__main__":
    asyncio.run(main())
