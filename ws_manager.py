"""
Менеджер WebSocket-соединений.

Управляет сокетами подключенных пользователей (user_id -> Set[WebSocket]).
Обеспечивает реальное время отправки обновлений постов в канал /post/feed/posted.
"""

from __future__ import annotations

import json
from typing import Any
from fastapi import WebSocket


class ConnectionManager:
    def __init__(self) -> None:
        self.active_connections: dict[str, set[WebSocket]] = {}

    async def connect(self, user_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        if user_id not in self.active_connections:
            self.active_connections[user_id] = set()
        self.active_connections[user_id].add(websocket)
        print(f"[WebSocket] Пользователь {user_id} подключился к /post/feed/posted. Активных сокетов: {len(self.active_connections[user_id])}")

    def disconnect(self, user_id: str, websocket: WebSocket) -> None:
        if user_id in self.active_connections:
            self.active_connections[user_id].discard(websocket)
            if not self.active_connections[user_id]:
                del self.active_connections[user_id]
        print(f"[WebSocket] Пользователь {user_id} отключился от /post/feed/posted.")

    async def send_to_user(self, user_id: str, data: dict[str, Any]) -> None:
        """Отправляет событие поста всем активным WebSocket-соединениям конкретного пользователя."""
        if user_id not in self.active_connections:
            return

        connections = list(self.active_connections[user_id])
        payload = json.dumps(data, ensure_ascii=False)

        for websocket in connections:
            try:
                await websocket.send_text(payload)
            except Exception as e:
                print(f"[WebSocket] Ошибка отправки пользователю {user_id}: {e}")
                self.disconnect(user_id, websocket)


ws_manager = ConnectionManager()
