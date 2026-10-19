"""
Кэш ленты постов друзей в Redis.

Ключ: feed:{user_id} — Sorted Set
  score  = unix timestamp создания поста
  member = JSON {"id","text","author_user_id"}

В ленте хранятся последние FEED_MAX_SIZE (1000) обновлений.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import redis

FEED_MAX_SIZE = 1000
FEED_KEY_PREFIX = "feed:"

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

_redis_client: redis.Redis | None = None


def get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True,
            socket_connect_timeout=5,
        )
    return _redis_client


def init_redis() -> None:
    client = get_redis()
    client.ping()
    print(f"✓ Подключение к Redis успешно ({REDIS_HOST}:{REDIS_PORT})")


def close_redis() -> None:
    global _redis_client
    if _redis_client is not None:
        _redis_client.close()
        _redis_client = None


def _feed_key(user_id: str) -> str:
    return f"{FEED_KEY_PREFIX}{user_id}"


def _post_member(post: dict[str, Any]) -> str:
    return json.dumps(
        {
            "id": str(post["id"]),
            "text": post["text"],
            "author_user_id": str(post["author_user_id"]),
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _score_from_created_at(created_at: datetime | float | int | str | None) -> float:
    if created_at is None:
        return datetime.now(timezone.utc).timestamp()
    if isinstance(created_at, (int, float)):
        return float(created_at)
    if isinstance(created_at, str):
        created_at = datetime.fromisoformat(created_at)
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    return created_at.timestamp()


def feed_exists(user_id: str) -> bool:
    return bool(get_redis().exists(_feed_key(user_id)))


def get_feed(user_id: str, offset: int = 0, limit: int = 10) -> list[dict[str, Any]] | None:
    """
    Читает ленту из кэша.
    Возвращает None, если кэша нет (нужна перестройка из БД).
    """
    client = get_redis()
    key = _feed_key(user_id)
    if not client.exists(key):
        return None

    start = max(offset, 0)
    end = start + max(limit, 1) - 1
    members = client.zrevrange(key, start, end)
    return [json.loads(m) for m in members]


def replace_feed(user_id: str, posts: list[dict[str, Any]]) -> None:
    """Полная перестройка ленты пользователя (последние FEED_MAX_SIZE постов)."""
    client = get_redis()
    key = _feed_key(user_id)
    pipe = client.pipeline()
    pipe.delete(key)

    trimmed = posts[:FEED_MAX_SIZE]
    for post in trimmed:
        pipe.zadd(
            key,
            {_post_member(post): _score_from_created_at(post.get("created_at"))},
        )
    pipe.execute()


def invalidate_feed(user_id: str) -> None:
    """Инвалидация кэша ленты пользователя."""
    get_redis().delete(_feed_key(user_id))


def push_post_to_feeds(subscriber_ids: list[str], post: dict[str, Any]) -> None:
    """Fan-out: добавить пост в ленты подписчиков (кто добавил автора в друзья)."""
    if not subscriber_ids:
        return

    client = get_redis()
    member = _post_member(post)
    score = _score_from_created_at(post.get("created_at"))
    pipe = client.pipeline()

    for user_id in subscriber_ids:
        key = _feed_key(user_id)
        # Обновляем только существующие кэши; пустые пересоберутся при чтении
        if client.exists(key):
            pipe.zadd(key, {member: score})
            # Оставляем только последние FEED_MAX_SIZE
            pipe.zremrangebyrank(key, 0, -(FEED_MAX_SIZE + 1))

    pipe.execute()


def update_post_in_feeds(subscriber_ids: list[str], old_post: dict[str, Any], new_post: dict[str, Any]) -> None:
    """Обновить пост во всех лентах подписчиков."""
    if not subscriber_ids:
        return

    client = get_redis()
    old_member = _post_member(old_post)
    new_member = _post_member(new_post)
    score = _score_from_created_at(new_post.get("created_at") or old_post.get("created_at"))
    pipe = client.pipeline()

    for user_id in subscriber_ids:
        key = _feed_key(user_id)
        if client.exists(key):
            pipe.zrem(key, old_member)
            pipe.zadd(key, {new_member: score})

    pipe.execute()


def remove_post_from_feeds(subscriber_ids: list[str], post: dict[str, Any]) -> None:
    """Удалить пост из лент подписчиков."""
    if not subscriber_ids:
        return

    client = get_redis()
    member = _post_member(post)
    pipe = client.pipeline()

    for user_id in subscriber_ids:
        key = _feed_key(user_id)
        if client.exists(key):
            pipe.zrem(key, member)

    pipe.execute()


def remove_author_posts_from_feed(user_id: str, author_user_id: str) -> None:
    """Удалить из ленты все посты конкретного автора (после удаления из друзей)."""
    client = get_redis()
    key = _feed_key(user_id)
    if not client.exists(key):
        return

    members = client.zrange(key, 0, -1)
    to_remove = []
    for member in members:
        try:
            data = json.loads(member)
        except json.JSONDecodeError:
            continue
        if str(data.get("author_user_id")) == str(author_user_id):
            to_remove.append(member)

    if to_remove:
        client.zrem(key, *to_remove)
