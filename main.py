import os
import asyncio
from contextlib import asynccontextmanager
from functools import wraps
from uuid import UUID
from fastapi import FastAPI, HTTPException, Depends, status, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
import psycopg

from db import init_db_pool, close_db_pool, DbConnection, set_read_only
from auth import hash_password, verify_password, create_access_token, verify_token, verify_token_str
from schemas import (
    UserRegisterRequest,
    UserRegisterResponse,
    LoginRequest,
    LoginResponse,
    UserGetResponse,
    PostCreateRequest,
    PostUpdateRequest,
    PostResponse,
    DialogSendRequest,
    DialogMessageResponse,
)
from cache import (
    FEED_MAX_SIZE,
    init_redis,
    close_redis,
    get_feed,
    replace_feed,
    invalidate_feed,
    push_post_to_feeds,
    update_post_in_feeds,
    remove_post_from_feeds,
    remove_author_posts_from_feed,
)
from dialog_db import (
    init_citus_pool,
    close_citus_pool,
    init_dialog_schema,
    get_citus_connection,
)
from tarantool_db import (
    init_tarantool,
    close_tarantool,
    tarantool_dialog_send,
    tarantool_dialog_list,
    tarantool_dialog_count,
    tarantool_dialog_clear,
)
from dialogs import make_dialog_id
from ws_manager import ws_manager
from mq import (
    publish_post_created_event,
    start_feed_materialization_consumer,
    start_ws_notification_consumer,
    close_mq,
)
from typing import List
from datetime import datetime, timezone
import uuid

# Активный бэкенд подсистемы диалогов ("tarantool" или "citus")
DIALOG_BACKEND = os.getenv("DIALOG_BACKEND", "tarantool").lower()


def read_only(func):
    """Декоратор для маркирования read-only операций."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        set_read_only(True)
        try:
            return func(*args, **kwargs)
        finally:
            set_read_only(False)
    return wrapper


def init_db_schema():
    """
    Выполняет инициализацию таблиц БД, если они еще не созданы.
    Использует schema.sql в качестве первоначального скрипта.
    """
    with DbConnection() as conn:
        with conn.cursor() as cur:
            schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
            if os.path.exists(schema_path):
                with open(schema_path, "r", encoding="utf-8") as f:
                    schema_sql = f.read()
                cur.execute(schema_sql)
                print("Схема базы данных проверена/создана.")
            else:
                print("Внимание: Файл schema.sql не найден!")


def _validate_uuid(value: str, field_name: str = "ID") -> str:
    try:
        UUID(value)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Неверный формат {field_name}. Ожидается UUID."
        )
    return value


def _user_exists(user_id: str) -> bool:
    with DbConnection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM users WHERE id = %s;", (user_id,))
            return cur.fetchone() is not None


def _get_friend_subscriber_ids(author_user_id: str) -> list[str]:
    """Пользователи, у которых author_user_id в друзьях (получатели ленты)."""
    with DbConnection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT user_id FROM friends WHERE friend_id = %s;",
                (author_user_id,),
            )
            rows = cur.fetchall()
    return [str(row["user_id"]) for row in rows]


def _load_feed_posts_from_db(user_id: str) -> list[dict]:
    """Последние FEED_MAX_SIZE постов друзей пользователя из СУБД."""
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
                (user_id, FEED_MAX_SIZE),
            )
            return cur.fetchall()


def rebuild_feed_cache(user_id: str) -> list[dict]:
    """Перестройка кэша ленты пользователя из СУБД. Возвращает посты."""
    set_read_only(True)
    try:
        posts = _load_feed_posts_from_db(user_id)
    finally:
        set_read_only(False)

    replace_feed(user_id, posts)
    return posts


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db_pool()
    try:
        init_db_schema()
    except Exception as e:
        print(f"Ошибка инициализации БД на старте: {e}")
    try:
        init_redis()
    except Exception as e:
        print(f"Ошибка подключения к Redis на старте: {e}")
    try:
        init_citus_pool()
        init_dialog_schema()
    except Exception as e:
        print(f"Ошибка подключения к Citus на старте: {e}")
    try:
        init_tarantool()
    except Exception as e:
        print(f"Ошибка подключения к Tarantool на старте: {e}")

    # Фоновое создание потребителей RabbitMQ
    mat_task = None
    ws_task = None
    try:
        mat_task = asyncio.create_task(
            start_feed_materialization_consumer(
                get_subscribers_fn=_get_friend_subscriber_ids,
                push_cache_fn=push_post_to_feeds,
            )
        )
        ws_task = asyncio.create_task(
            start_ws_notification_consumer(
                on_user_message_fn=ws_manager.send_to_user,
            )
        )
    except Exception as e:
        print(f"Ошибка инициализации RabbitMQ воркеров на старте: {e}")

    yield

    if mat_task:
        mat_task.cancel()
    if ws_task:
        ws_task.cancel()

    try:
        await close_mq()
    except Exception:
        pass

    close_redis()
    close_citus_pool()
    close_tarantool()
    close_db_pool()


app = FastAPI(
    lifespan=lifespan,
    title="Социальная сеть OTUS HA",
    description=(
        "Социальная сеть для курса OTUS Highload Architect: "
        "Patroni HA, кэш ленты в Redis, диалоги в Citus, "
        "отложенная материализация ленты через RabbitMQ и WebSocket."
    ),
    version="1.2.0"
)


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": f"Внутренняя ошибка сервера: {str(exc)}"}
    )


@app.post("/user/register", response_model=UserRegisterResponse, status_code=status.HTTP_201_CREATED)
def register_user(request: UserRegisterRequest):
    """
    Регистрация нового пользователя.
    Принимает персональные данные и пароль. Пароль хэшируется.
    Возвращает сгенерированный UUID.
    Запрос идёт на WRITE MASTER.
    """
    hashed_pwd = hash_password(request.password)

    try:
        with DbConnection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO users (first_name, second_name, birthdate, gender, biography, city, password_hash)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id;
                    """,
                    (
                        request.first_name,
                        request.second_name,
                        request.birthdate,
                        request.gender,
                        request.biography,
                        request.city,
                        hashed_pwd
                    )
                )
                result = cur.fetchone()
                if not result or "id" not in result:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Не удалось получить ID созданного пользователя"
                    )
                user_id = str(result["id"])
                return UserRegisterResponse(user_id=user_id)
    except Exception as e:
        print(f"Ошибка при регистрации: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка сервера при регистрации пользователя: {str(e)}"
        )


@app.post("/login", response_model=LoginResponse)
def login(request: LoginRequest):
    """
    Авторизация пользователя.
    Принимает UUID пользователя и пароль. В случае успеха возвращает JWT токен.
    Запрос идёт на WRITE MASTER.
    """
    try:
        UUID(request.id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Неверный формат ID пользователя. Ожидается UUID."
        )

    with DbConnection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, password_hash FROM users WHERE id = %s;",
                (request.id,)
            )
            user = cur.fetchone()

    if not user or not verify_password(request.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверный ID пользователя или пароль"
        )

    token = create_access_token(user["id"])
    return LoginResponse(token=token)


@app.get("/user/get/{id}", response_model=UserGetResponse)
@read_only
def get_user_profile(id: str, current_user_id: str = Depends(verify_token)):
    """
    Получение анкеты пользователя по ID.
    Требуется авторизация (Bearer JWT в заголовке Authorization).
    Запрос идёт на READ SLAVE.
    """
    try:
        UUID(id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Неверный формат ID пользователя. Ожидается UUID."
        )

    with DbConnection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, first_name, second_name, birthdate, gender, biography, city
                FROM users
                WHERE id = %s;
                """,
                (id,)
            )
            user = cur.fetchone()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Пользователь с ID {id} не найден"
        )

    return UserGetResponse(**user)


@app.get("/user/search", response_model=List[UserGetResponse])
@read_only
def search_users(
    first_name: str,
    second_name: str,
    current_user_id: str = Depends(verify_token),
):
    """
    Поиск пользователей по имени и фамилии.
    Требуется авторизация (Bearer JWT в заголовке Authorization).
    Запрос идёт на READ SLAVE.
    """
    with DbConnection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    first_name,
                    second_name,
                    birthdate,
                    gender,
                    biography,
                    city
                FROM users
                WHERE first_name LIKE %s
                  AND second_name LIKE %s
                ORDER BY id;
                """,
                (
                    first_name + "%",
                    second_name + "%"
                )
            )

            users = cur.fetchall()

    return [UserGetResponse(**user) for user in users]


# ========================= FRIENDS =========================

@app.put("/friend/set/{user_id}")
def friend_set(user_id: str, current_user_id: str = Depends(verify_token)):
    """Добавить пользователя в друзья. Лента текущего пользователя перестраивается."""
    friend_id = _validate_uuid(user_id, "ID друга")
    if friend_id == current_user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Нельзя добавить себя в друзья"
        )
    if not _user_exists(friend_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Пользователь не найден"
        )

    with DbConnection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO friends (user_id, friend_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING;
                """,
                (current_user_id, friend_id),
            )

    # Инвалидация + перестройка: в ленте появляются посты нового друга
    rebuild_feed_cache(current_user_id)
    return {"message": "Друг успешно добавлен"}


@app.put("/friend/delete/{user_id}")
def friend_delete(user_id: str, current_user_id: str = Depends(verify_token)):
    """Удалить пользователя из друзей. Посты автора убираются из кэша ленты."""
    friend_id = _validate_uuid(user_id, "ID друга")

    with DbConnection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM friends WHERE user_id = %s AND friend_id = %s;",
                (current_user_id, friend_id),
            )

    remove_author_posts_from_feed(current_user_id, friend_id)
    return {"message": "Пользователь удалён из друзей"}


# ========================= POSTS & WEBSOCKET =========================

@app.post("/post/create", response_model=str)
async def post_create(request: PostCreateRequest, current_user_id: str = Depends(verify_token)):
    """
    Создать пост.
    Записывает пост в СУБД и публикует событие в RabbitMQ для отложенной
    материализации ленты и отправки уведомлений друзьям по WebSocket.
    По спецификации OpenAPI возвращает PostId (string).
    """
    with DbConnection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO posts (author_user_id, text)
                VALUES (%s, %s)
                RETURNING id, text, author_user_id, created_at;
                """,
                (current_user_id, request.text),
            )
            post = cur.fetchone()

    post_id = str(post["id"])

    # Публикация события в RabbitMQ (асинхронный Fan-out и WebSocket доставка)
    await publish_post_created_event(post)

    return post_id


@app.websocket("/post/feed/{action}")
async def websocket_feed_posted(websocket: WebSocket, action: str):
    """
    Асинхронное WebSocket API согласно спецификации asyncapi.json:
    Канал /post/feed/posted.

    При добавлении нового поста друга подписчику WebSocket'а приходит
    событие о новом посте в реальном времени.

    Авторизация: передача JWT-токена в query-параметре `?token=<token>`.
    """
    query_string = websocket.scope.get("query_string", b"").decode()
    token = None
    for part in query_string.split("&"):
        if part.startswith("token="):
            token = part.split("=", 1)[1]
            break

    if not token:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    try:
        current_user_id = verify_token_str(token)
    except Exception:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await ws_manager.connect(current_user_id, websocket)
    try:
        while True:
            # Читаем клиентские ping/pong сообщения, если они поступают
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(current_user_id, websocket)
    except Exception:
        ws_manager.disconnect(current_user_id, websocket)


@app.put("/post/update")
def post_update(request: PostUpdateRequest, current_user_id: str = Depends(verify_token)):
    """Обновить свой пост. Инвалидация/обновление кэшей лент подписчиков."""
    post_id = _validate_uuid(request.id, "ID поста")

    with DbConnection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, text, author_user_id, created_at FROM posts WHERE id = %s;",
                (post_id,),
            )
            old_post = cur.fetchone()
            if not old_post:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Пост не найден"
                )
            if str(old_post["author_user_id"]) != current_user_id:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Можно изменять только свои посты"
                )

            cur.execute(
                """
                UPDATE posts
                SET text = %s, updated_at = NOW()
                WHERE id = %s
                RETURNING id, text, author_user_id, created_at;
                """,
                (request.text, post_id),
            )
            new_post = cur.fetchone()

    subscribers = _get_friend_subscriber_ids(current_user_id)
    update_post_in_feeds(subscribers, old_post, new_post)
    return {"message": "Пост успешно изменён"}


@app.put("/post/delete/{id}")
def post_delete(id: str, current_user_id: str = Depends(verify_token)):
    """Удалить свой пост. Удаление из кэшей лент подписчиков."""
    post_id = _validate_uuid(id, "ID поста")

    with DbConnection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, text, author_user_id, created_at FROM posts WHERE id = %s;",
                (post_id,),
            )
            post = cur.fetchone()
            if not post:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Пост не найден"
                )
            if str(post["author_user_id"]) != current_user_id:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Можно удалять только свои посты"
                )

            cur.execute("DELETE FROM posts WHERE id = %s;", (post_id,))

    subscribers = _get_friend_subscriber_ids(current_user_id)
    remove_post_from_feeds(subscribers, post)
    return {"message": "Пост успешно удалён"}


@app.get("/post/get/{id}", response_model=PostResponse)
@read_only
def post_get(id: str):
    """Получить пост по ID."""
    post_id = _validate_uuid(id, "ID поста")

    with DbConnection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, text, author_user_id FROM posts WHERE id = %s;",
                (post_id,),
            )
            post = cur.fetchone()

    if not post:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Пост не найден"
        )

    return PostResponse(
        id=str(post["id"]),
        text=post["text"],
        author_user_id=str(post["author_user_id"]),
    )


@app.get("/post/feed", response_model=List[PostResponse])
def post_feed(
    offset: int = Query(0, ge=0),
    limit: int = Query(10, ge=1),
    current_user_id: str = Depends(verify_token),
):
    """
    Лента постов друзей.
    Читается из Redis-кэша. При промахе — перестройка из СУБД.
    """
    cached = get_feed(current_user_id, offset=offset, limit=limit)
    if cached is not None:
        return [PostResponse(**item) for item in cached]

    posts = rebuild_feed_cache(current_user_id)
    page = posts[offset: offset + limit]
    return [
        PostResponse(
            id=str(p["id"]),
            text=p["text"],
            author_user_id=str(p["author_user_id"]),
        )
        for p in page
    ]


@app.post("/post/feed/rebuild")
def post_feed_rebuild(current_user_id: str = Depends(verify_token)):
    """Принудительная перестройка кэша ленты текущего пользователя из СУБД."""
    posts = rebuild_feed_cache(current_user_id)
    return {
        "message": "Кэш ленты перестроен из СУБД",
        "posts_count": len(posts),
    }


@app.post("/post/feed/rebuild/{user_id}")
def post_feed_rebuild_user(user_id: str, current_user_id: str = Depends(verify_token)):
    """
    Перестройка кэша ленты указанного пользователя из СУБД.
    Нужна для демонстрации критерия «перестройка кешей из СУБД».
    """
    target_id = _validate_uuid(user_id, "ID пользователя")
    if not _user_exists(target_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Пользователь не найден"
        )
    posts = rebuild_feed_cache(target_id)
    return {
        "message": f"Кэш ленты пользователя {target_id} перестроен из СУБД",
        "posts_count": len(posts),
    }


@app.delete("/post/feed/cache")
def post_feed_invalidate(current_user_id: str = Depends(verify_token)):
    """Инвалидация (удаление) кэша ленты текущего пользователя."""
    invalidate_feed(current_user_id)
    return {"message": "Кэш ленты инвалидирован"}


# ========================= DIALOGS (Citus) =========================

def _ensure_extra_worker_db(host: str, port: int, db_name: str, user: str, password: str):
    """
    Вспомогательная функция: создаёт БД и включает расширение citus
    на новом worker-узле перед его регистрацией в coordinator.
    Вызывается при добавлении worker_3 для решардинга.
    """
    # Подключаемся к системной БД postgres для создания новой
    admin_dsn = f"host={host} port={port} dbname=postgres user={user} password={password}"
    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,))
            if cur.fetchone() is None:
                cur.execute(f'CREATE DATABASE "{db_name}";')

    # Включаем расширение citus в созданной БД
    db_dsn = f"host={host} port={port} dbname={db_name} user={user} password={password}"
    with psycopg.connect(db_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS citus;")


@app.post("/dialog/{user_id}/send")
def dialog_send(
    user_id: str,
    request: DialogSendRequest,
    current_user_id: str = Depends(verify_token),
):
    """
    Отправить сообщение пользователю user_id.

    Механизм шардинга:
      1. Вычисляем dialog_id = UUID5(sorted([from_id, to_id])).
      2. INSERT в Citus → coordinator по dialog_id определяет нужный shard.
      3. Запись уходит на конкретный worker (single-shard write).

    Требует авторизации (Bearer JWT).
    """
    to_user_id = _validate_uuid(user_id, "ID получателя")
    if to_user_id == current_user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Нельзя отправить сообщение самому себе",
        )
    if not _user_exists(to_user_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Пользователь не найден",
        )

    dialog_id = make_dialog_id(current_user_id, to_user_id)

    if DIALOG_BACKEND == "tarantool":
        msg_id = str(uuid.uuid4())
        created_at = datetime.now(timezone.utc).isoformat()
        res = tarantool_dialog_send(
            msg_id=msg_id,
            dialog_id=dialog_id,
            from_user_id=current_user_id,
            to_user_id=to_user_id,
            text=request.text,
            created_at=created_at,
        )
        return {
            "message": "Сообщение отправлено",
            "id": res["id"],
            "dialog_id": res["dialog_id"],
            "backend": "tarantool_udf",
        }

    with get_citus_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO messages (dialog_id, from_user_id, to_user_id, text)
                VALUES (%s, %s, %s, %s)
                RETURNING id, dialog_id, created_at;
                """,
                (dialog_id, current_user_id, to_user_id, request.text),
            )
            row = cur.fetchone()

    return {
        "message": "Сообщение отправлено",
        "id": str(row["id"]),
        "dialog_id": str(row["dialog_id"]),
        "backend": "citus_sql",
    }


@app.get("/dialog/{user_id}/list", response_model=List[DialogMessageResponse])
def dialog_list(user_id: str, current_user_id: str = Depends(verify_token)):
    """
    Получить историю диалога текущего пользователя с user_id.

    Выбор бэкенда выполняется автоматически по DIALOG_BACKEND ("tarantool" или "citus").
    При tarantool вызов выполняется через Lua UDF хранимую процедуру.
    """
    other_user_id = _validate_uuid(user_id, "ID собеседника")
    dialog_id = make_dialog_id(current_user_id, other_user_id)

    if DIALOG_BACKEND == "tarantool":
        messages = tarantool_dialog_list(dialog_id)
        return [
            DialogMessageResponse(
                **{
                    "from": msg["from"],
                    "to": msg["to"],
                    "text": msg["text"],
                }
            )
            for msg in messages
        ]

    with get_citus_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT from_user_id, to_user_id, text
                FROM messages
                WHERE dialog_id = %s
                ORDER BY created_at ASC, id ASC;
                """,
                (dialog_id,),
            )
            rows = cur.fetchall()

    return [
        DialogMessageResponse(
            **{
                "from": str(row["from_user_id"]),
                "to": str(row["to_user_id"]),
                "text": row["text"],
            }
        )
        for row in rows
    ]


@app.get("/admin/dialogs/backend")
def get_dialog_backend(current_user_id: str = Depends(verify_token)):
    """Получение текущего активного бэкенда диалогов и количества сообщений."""
    tarantool_cnt = 0
    try:
        tarantool_cnt = tarantool_dialog_count()
    except Exception:
        pass

    citus_cnt = 0
    try:
        with get_citus_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS cnt FROM messages;")
                citus_cnt = int(cur.fetchone()["cnt"])
    except Exception:
        pass

    return {
        "active_backend": DIALOG_BACKEND,
        "tarantool_messages_count": tarantool_cnt,
        "citus_messages_count": citus_cnt,
        "hint": "Для переключения используйте POST /admin/dialogs/backend/set?backend=tarantool|citus",
    }


@app.post("/admin/dialogs/backend/set")
def set_dialog_backend(backend: str, current_user_id: str = Depends(verify_token)):
    """Динамическое переключение бэкенда подсистемы диалогов (tarantool или citus)."""
    global DIALOG_BACKEND
    backend_val = backend.lower().strip()
    if backend_val not in ("tarantool", "citus"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Недопустимый бэкенд. Укажите 'tarantool' или 'citus'.",
        )
    DIALOG_BACKEND = backend_val
    return {
        "message": f"Бэкенд подсистемы диалогов успешно изменён на '{DIALOG_BACKEND}'",
        "active_backend": DIALOG_BACKEND,
    }


# ========================= ADMIN: управление шардами =========================

@app.get("/admin/dialogs/shards")
def dialog_shards(current_user_id: str = Depends(verify_token)):
    """
    Диагностика: размещение шардов messages по worker-узлам.

    Показывает:
      - список активных workers
      - общее количество шардов
      - сколько шардов на каждом worker
      - общее количество сообщений
    """
    with get_citus_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM citus_get_active_worker_nodes();")
            workers = cur.fetchall()
            cur.execute(
                """
                SELECT
                    s.shardid,
                    n.nodename,
                    n.nodeport,
                    p.shardstate
                FROM pg_dist_shard s
                JOIN pg_dist_placement p ON s.shardid = p.shardid
                JOIN pg_dist_node n ON p.groupid = n.groupid
                WHERE s.logicalrelid = 'messages'::regclass
                ORDER BY s.shardid, n.nodename;
                """
            )
            shards = cur.fetchall()
            cur.execute("SELECT COUNT(*) AS cnt FROM messages;")
            total = cur.fetchone()["cnt"]

    by_node: dict[str, int] = {}
    for item in shards:
        name = item["nodename"]
        by_node[name] = by_node.get(name, 0) + 1

    return {
        "workers": workers,
        "shard_count": len({item["shardid"] for item in shards}),
        "shards_per_node": by_node,
        "messages_total": int(total),
        "placements": shards,
    }


@app.get("/admin/dialogs/placement/{user_id}")
def dialog_placement(user_id: str, current_user_id: str = Depends(verify_token)):
    """
    Диагностика: на каком shard/worker хранится диалог текущий пользователь ↔ user_id.

    Полезно для проверки что «Эффект Леди Гаги» решён:
    разные диалоги одной звезды попадают на разные workers.
    """
    other_user_id = _validate_uuid(user_id, "ID собеседника")
    dialog_id = make_dialog_id(current_user_id, other_user_id)

    with get_citus_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT get_shard_id_for_distribution_column('messages', %s::uuid)
                    AS shardid;
                """,
                (dialog_id,),
            )
            shard_row = cur.fetchone()
            shard_id = shard_row["shardid"]
            cur.execute(
                """
                SELECT n.nodename, n.nodeport
                FROM pg_dist_placement p
                JOIN pg_dist_node n ON p.groupid = n.groupid
                WHERE p.shardid = %s;
                """,
                (shard_id,),
            )
            placement = cur.fetchall()
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM messages WHERE dialog_id = %s;",
                (dialog_id,),
            )
            msg_count = cur.fetchone()["cnt"]

    return {
        "dialog_id": dialog_id,
        "shard_id": shard_id,
        "placement": placement,
        "messages_in_dialog": int(msg_count),
        "note": (
            "Ключ шардирования — dialog_id пары, а не from_user_id. "
            "Диалоги звезды с разными людьми лежат на разных шардах."
        ),
    }


@app.post("/admin/dialogs/worker/add")
def dialog_add_worker(current_user_id: str = Depends(verify_token)):
    """
    Шаг 1 онлайн-решардинга: подключить citus_worker_3 в кластер.

    Процесс решардинга без даунтайма:
      1. Вызвать POST /admin/dialogs/worker/add  ← этот эндпоинт
         (добавляет узел, шарды ещё не переносятся)
      2. Вызвать POST /admin/dialogs/rebalance
         (Citus переносит шарды через logical replication,
          таблица messages доступна на чтение и запись всё время)

    worker_3 должен быть запущен (он есть в docker-compose).
    """
    host = os.getenv("CITUS_EXTRA_WORKER_HOST", "citus_worker_3")
    port = int(os.getenv("CITUS_EXTRA_WORKER_PORT", "5432"))
    db_name = os.getenv("CITUS_DB", "dialogs")
    user = os.getenv("CITUS_USER", "postgres")
    password = os.getenv("CITUS_PASSWORD", "postgres")

    # Подготавливаем worker_3: создаём БД и включаем расширение citus
    try:
        _ensure_extra_worker_db(host, port, db_name, user, password)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Не удалось подготовить worker {host}: {e}",
        )

    with get_citus_connection() as conn:
        with conn.cursor() as cur:
            # Проверяем, не зарегистрирован ли уже
            cur.execute(
                "SELECT nodename, nodeport FROM pg_dist_node WHERE nodename = %s;",
                (host,),
            )
            existing = cur.fetchone()
            if existing:
                return {
                    "message": "Worker уже есть в метаданных Citus",
                    "node": existing,
                }
            # Регистрируем worker_3 в coordinator
            cur.execute("SELECT citus_add_node(%s, %s) AS nodeid;", (host, port))
            nodeid = cur.fetchone()["nodeid"]
            cur.execute("SELECT * FROM citus_get_active_worker_nodes();")
            workers = cur.fetchall()

    return {
        "message": "Worker добавлен. Шарды ещё на старых узлах. Запустите rebalance.",
        "nodeid": nodeid,
        "workers": workers,
    }


@app.post("/admin/dialogs/rebalance")
def dialog_rebalance(current_user_id: str = Depends(verify_token)):
    """
    Шаг 2 онлайн-решардинга: запустить перебалансировку шардов.

    Citus использует logical replication для переноса данных:
      - Таблица messages остаётся доступной для чтения И записи.
      - Новые сообщения продолжают вставляться в процессе миграции.
      - После завершения шарды равномерно распределены по всем workers.

    Команда возвращается немедленно, ребалансировка идёт в фоне.
    Статус можно проверить через GET /admin/dialogs/shards.
    """
    with get_citus_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT citus_rebalance_start() AS job_id;")
            job = cur.fetchone()

    return {
        "message": "Ребалансировка запущена. Чтение и запись продолжаются.",
        "job_id": str(job["job_id"]) if job else None,
        "hint": "Проверьте прогресс через GET /admin/dialogs/shards",
    }
