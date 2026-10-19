"""
Модуль подключения к In-Memory СУБД Tarantool для подсистемы диалогов.

ВАЖНЕЙШЕЕ ТРЕБОВАНИЕ ДЗ:
  Вся логика подсистемы диалогов вынесена в UDF (Lua хранимые процедуры в Tarantool).
  Python-код НЕ производит прямых запросов к space'ам через драйвер (без select/insert/update/delete по space).
  Общение с Tarantool происходит СТРОГО через вызовы хранимых процедур (`conn.call`).

Хранимые процедуры в Tarantool (tarantool/init.lua):
  - dialog_send(id, dialog_id, from_user_id, to_user_id, text, created_at)
  - dialog_list(dialog_id)
  - dialog_count()
  - dialog_clear()
"""

import os
import time
import tarantool

TARANTOOL_HOST = os.getenv("TARANTOOL_HOST", "localhost")
TARANTOOL_PORT = int(os.getenv("TARANTOOL_PORT", "3301"))

_tarantool_connection = None


def get_tarantool_connection():
    """
    Возвращает активное подключение к Tarantool или создаёт новое с автоматическим переподключением.
    """
    global _tarantool_connection
    if _tarantool_connection is not None:
        try:
            # Проверка живое ли соединение
            _tarantool_connection.ping()
            return _tarantool_connection
        except Exception:
            _tarantool_connection = None

    try:
        conn = tarantool.connect(TARANTOOL_HOST, TARANTOOL_PORT, connection_timeout=5.0)
        _tarantool_connection = conn
        return conn
    except Exception as e:
        print(f"Ошибка подключения к Tarantool ({TARANTOOL_HOST}:{TARANTOOL_PORT}): {e}")
        raise


def init_tarantool():
    """
    Инициализация подключения к Tarantool на старте приложения (с повторными попытками).
    """
    for attempt in range(20):
        try:
            conn = get_tarantool_connection()
            conn.ping()
            print(f"Подключение к Tarantool In-Memory СУБД успешно ({TARANTOOL_HOST}:{TARANTOOL_PORT}).")
            return
        except Exception as e:
            print(f"Tarantool попытка {attempt + 1}: {e}")
            time.sleep(2)
    raise RuntimeError("Не удалось подключиться к Tarantool In-Memory СУБД.")


def close_tarantool():
    """Закрытие соединения при завершении работы приложения."""
    global _tarantool_connection
    if _tarantool_connection is not None:
        try:
            _tarantool_connection.close()
        except Exception:
            pass
        _tarantool_connection = None


# ─── Вызовы UDF Хранимых Процедур ──────────────────────────────────────────

def tarantool_dialog_send(
    msg_id: str,
    dialog_id: str,
    from_user_id: str,
    to_user_id: str,
    text: str,
    created_at: str,
) -> dict:
    """
    Вызов Lua UDF процедуры dialog_send в Tarantool.
    Прямых операций над space в Python НЕТ.
    """
    conn = get_tarantool_connection()
    # Вызов хранимой процедуры по названию через conn.call
    resp = conn.call(
        "dialog_send",
        (msg_id, dialog_id, from_user_id, to_user_id, text, created_at)
    )
    # resp.data содержит список результатов, возвращаемых процедурой [result_dict]
    if resp and resp.data:
        data = resp.data[0]
        return {
            "id": data.get("id") if isinstance(data, dict) else data[0],
            "dialog_id": data.get("dialog_id") if isinstance(data, dict) else data[1],
            "created_at": data.get("created_at") if isinstance(data, dict) else data[5],
        }
    raise RuntimeError("Tarantool UDF dialog_send не вернул данных.")


def tarantool_dialog_list(dialog_id: str) -> list[dict]:
    """
    Вызов Lua UDF процедуры dialog_list в Tarantool.
    Возвращает сообщения диалога с сортировкой по created_at.
    """
    conn = get_tarantool_connection()
    resp = conn.call("dialog_list", (dialog_id,))
    if resp and resp.data:
        # resp.data[0] — список сообщений, возвращенных Lua функцией
        messages = resp.data[0]
        result = []
        for m in messages:
            if isinstance(m, dict):
                result.append({
                    "from": str(m["from_user_id"]),
                    "to": str(m["to_user_id"]),
                    "text": m["text"],
                })
            elif isinstance(m, (list, tuple)):
                result.append({
                    "from": str(m[2]),
                    "to": str(m[3]),
                    "text": m[4],
                })
        return result
    return []


def tarantool_dialog_count() -> int:
    """Вызов UDF процедуры dialog_count."""
    conn = get_tarantool_connection()
    resp = conn.call("dialog_count", ())
    if resp and resp.data:
        return int(resp.data[0])
    return 0


def tarantool_dialog_clear():
    """Вызов UDF процедуры dialog_clear."""
    conn = get_tarantool_connection()
    return conn.call("dialog_clear", ()).data
