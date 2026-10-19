"""
Модуль работы с RabbitMQ через aio-pika.

Обеспечивает:
1. Подключение к RabbitMQ и инициализацию Topic Exchange (`feed_events`).
2. Публикацию события создания поста (`post.created`).
3. Слушатель отложенной материализации ленты (Fan-out и обработка «Эффекта Леди Гаги»).
4. Публикацию точечных уведомлений для WebSocket с Routing Key `feed.user.<user_id>`.
5. Слушатель WebSocket-сообщений для доставки подключенным клиентам.
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Callable, Awaitable

import aio_pika
from aio_pika import ExchangeType, Message, DeliveryMode

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

EXCHANGE_NAME = "feed_events"
POST_CREATED_ROUTING_KEY = "post.created"
USER_WS_ROUTING_PREFIX = "feed.user."
CELEBRITY_THRESHOLD = int(os.getenv("CELEBRITY_THRESHOLD", "500"))

_connection: aio_pika.RobustConnection | None = None
_channel: aio_pika.RobustChannel | None = None
_exchange: aio_pika.RobustExchange | None = None


async def get_mq_channel() -> aio_pika.RobustChannel:
    global _connection, _channel, _exchange
    if _connection is None or _connection.is_closed:
        url = f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/"
        _connection = await aio_pika.connect_robust(url)
        _channel = await _connection.channel()
        _exchange = await _channel.declare_exchange(
            EXCHANGE_NAME,
            type=ExchangeType.TOPIC,
            durable=True,
        )
    return _channel


async def get_mq_exchange() -> aio_pika.RobustExchange:
    await get_mq_channel()
    assert _exchange is not None
    return _exchange


async def close_mq() -> None:
    global _connection, _channel, _exchange
    if _connection is not None and not _connection.is_closed:
        await _connection.close()
        _connection = None
        _channel = None
        _exchange = None


async def publish_post_created_event(post: dict[str, Any]) -> None:
    """
    Публикация события создания поста в RabbitMQ (в фоновой очереди).
    Отделяет время ответа API создания поста от фанаута по друзьям.
    """
    try:
        exchange = await get_mq_exchange()
        body = json.dumps(
            {
                "id": str(post["id"]),
                "text": post["text"],
                "author_user_id": str(post["author_user_id"]),
                "created_at": post.get("created_at").isoformat() if hasattr(post.get("created_at"), "isoformat") else str(post.get("created_at")),
            },
            ensure_ascii=False,
        ).encode("utf-8")

        message = Message(
            body=body,
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
        )

        await exchange.publish(message, routing_key=POST_CREATED_ROUTING_KEY)
    except Exception as e:
        print(f"[RabbitMQ] Ошибка публикации поста {post.get('id')}: {e}")


async def publish_user_ws_notification(user_id: str, post_data: dict[str, Any]) -> None:
    """
    Отправка события поста целевому пользователю через Routing Key `feed.user.<user_id>`.
    Гарантирует доставку только целевым подписчикам (друзьям).
    """
    try:
        exchange = await get_mq_exchange()
        routing_key = f"{USER_WS_ROUTING_PREFIX}{user_id}"
        body = json.dumps(post_data, ensure_ascii=False).encode("utf-8")

        message = Message(
            body=body,
            content_type="application/json",
            delivery_mode=DeliveryMode.NOT_PERSISTENT,
        )

        await exchange.publish(message, routing_key=routing_key)
    except Exception as e:
        print(f"[RabbitMQ] Ошибка отправки WS-уведомления для user {user_id}: {e}")


async def start_feed_materialization_consumer(
    get_subscribers_fn: Callable[[str], list[str]],
    push_cache_fn: Callable[[list[str], dict[str, Any]], None],
) -> None:
    """
    Фоновый воркер: обрабатывает очередь `post.created`.
    Реализует отложенную материализацию ленты и отправку событий подписчикам.
    С учетом «Эффекта Леди Гаги» (Celebrity handling).
    """
    channel = await get_mq_channel()
    await channel.set_qos(prefetch_count=20)

    exchange = await get_mq_exchange()
    queue = await channel.declare_queue(
        "feed_materialization_queue",
        durable=True,
    )
    await queue.bind(exchange, routing_key=POST_CREATED_ROUTING_KEY)

    print("✓ RabbitMQ Consumer отложенной материализации ленты запущен.")

    async for message in queue:
        async with message.process():
            try:
                data = json.loads(message.body.decode("utf-8"))
                author_id = data["author_user_id"]

                # Получаем подписчиков (друзей автора)
                subscribers = await asyncio.to_thread(get_subscribers_fn, author_id)
                if not subscribers:
                    continue

                is_celebrity = len(subscribers) >= CELEBRITY_THRESHOLD
                print(
                    f"[RabbitMQ] Пост {data['id']} от автора {author_id}. "
                    f"Подписчиков: {len(subscribers)}. "
                    f"Леди Гага: {'ДА' if is_celebrity else 'НЕТ'}"
                )

                if not is_celebrity:
                    # Обычный автор: Fan-out в кэши лент Redis
                    try:
                        await asyncio.wait_for(
                            asyncio.to_thread(push_cache_fn, subscribers, data),
                            timeout=5.0,
                        )
                    except asyncio.TimeoutError:
                        pass
                    except Exception:
                        pass
                else:
                    # «Эффект Леди Гаги»: материализация батчами по 100 пользователей,
                    # чтобы не блокировать поток и не перезаполнять память единоразово.
                    chunk_size = 100
                    for i in range(0, len(subscribers), chunk_size):
                        chunk = subscribers[i : i + chunk_size]
                        await asyncio.to_thread(push_cache_fn, chunk, data)
                        await asyncio.sleep(0.01)  # Делаем паузу между батчами

                # Точечная отправка событий через WebSocket всем онлайн-друзьям
                for sub_id in subscribers:
                    await publish_user_ws_notification(sub_id, data)

            except Exception as e:
                print(f"[RabbitMQ] Ошибка обработки поста в воркере: {e}")


async def start_ws_notification_consumer(
    on_user_message_fn: Callable[[str, dict[str, Any]], Awaitable[None]],
) -> None:
    """
    Фоновый слушатель WS-уведомлений для инстанса приложения:
    Подписывается на все `feed.user.#` и передает сообщения в WebSocket ConnectionManager.
    """
    channel = await get_mq_channel()
    exchange = await get_mq_exchange()

    queue = await channel.declare_queue(
        "",  # Временная эксклюзивная очередь для данного веб-инстанса
        exclusive=True,
        auto_delete=True,
    )
    await queue.bind(exchange, routing_key=f"{USER_WS_ROUTING_PREFIX}#")

    print("✓ RabbitMQ Consumer WebSocket-уведомлений запущен.")

    async for message in queue:
        async with message.process():
            try:
                routing_key = message.routing_key or ""
                if routing_key.startswith(USER_WS_ROUTING_PREFIX):
                    user_id = routing_key[len(USER_WS_ROUTING_PREFIX) :]
                    data = json.loads(message.body.decode("utf-8"))
                    await on_user_message_fn(user_id, data)
            except Exception as e:
                print(f"[RabbitMQ] Ошибка обработки WS сообщения: {e}")
