-- ============================================================
-- Схема таблицы messages для Citus (диалоги между пользователями)
-- ============================================================
--
-- Ключ шардирования: dialog_id
--
-- dialog_id — это детерминированный UUID пары пользователей:
--   dialog_id = UUID5(sorted([user_a_id, user_b_id]))
--
-- Почему dialog_id, а не from_user_id:
--   Если шардировать по from_user_id, все сообщения звезды
--   (пользователь с миллионом диалогов) уходят на один shard.
--   С dialog_id каждый диалог звезды попадает на разные shards
--   → нагрузка распределяется равномерно («Эффект Леди Гаги» решён).
--
-- Запрос GET /dialog/{user_id}/list всегда фильтрует по dialog_id
-- → single-shard запрос → максимальная производительность.
-- ============================================================

CREATE TABLE IF NOT EXISTS messages (
    id           UUID         NOT NULL DEFAULT gen_random_uuid(),
    dialog_id    UUID         NOT NULL,           -- ключ шардирования в Citus
    from_user_id UUID         NOT NULL,           -- кто отправил
    to_user_id   UUID         NOT NULL,           -- кому отправлено
    text         TEXT         NOT NULL,           -- текст сообщения
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    -- Составной PRIMARY KEY: dialog_id должен входить в PK distributed-таблицы
    PRIMARY KEY (dialog_id, id)
);

-- Основной индекс: получить все сообщения диалога в хронологическом порядке.
-- Используется в GET /dialog/{user_id}/list.
CREATE INDEX IF NOT EXISTS idx_messages_dialog_created
    ON messages (dialog_id, created_at ASC, id ASC);

-- Вспомогательный индекс: все исходящие сообщения пользователя.
CREATE INDEX IF NOT EXISTS idx_messages_from_user
    ON messages (from_user_id, created_at DESC);

-- Вспомогательный индекс: все входящие сообщения пользователя.
CREATE INDEX IF NOT EXISTS idx_messages_to_user
    ON messages (to_user_id, created_at DESC);
