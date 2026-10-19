-- Конфигурация и инициализация Tarantool In-Memory СУБД для модуля диалогов

box.cfg{
    listen = 3301,
    read_only = false,
    memtx_memory = 256 * 1024 * 1024 -- 256 МБ оперативной памяти для хранения данных
}

-- Предоставляем суперпользовательские права гостевому подключению для удобства работы в dev-среде
box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})

-- Создаем space (аналог таблицы) messages в памяти (движок memtx)
local messages = box.schema.space.create('messages', {
    if_not_exists = true,
    engine = 'memtx'
})

-- Определение формата полей space
messages:format({
    {name = 'id', type = 'string'},
    {name = 'dialog_id', type = 'string'},
    {name = 'from_user_id', type = 'string'},
    {name = 'to_user_id', type = 'string'},
    {name = 'text', type = 'string'},
    {name = 'created_at', type = 'string'}
})

-- Первичный индекс по UUID сообщения (id)
messages:create_index('primary', {
    type = 'tree',
    parts = {'id'},
    if_not_exists = true
})

-- Вторичный составной индекс по (dialog_id, created_at) для быстрых выборка диалога в хронологическом порядке
messages:create_index('dialog_created', {
    type = 'tree',
    parts = {'dialog_id', 'created_at'},
    unique = false,
    if_not_exists = true
})

-- ============================================================================
-- UDF (User-Defined Functions / Lua Хранимые процедуры)
-- Логика модуля диалогов перенесена полностью в Tarantool
-- ============================================================================

--- UDF: Отправка сообщения в диалог
-- @param id UUID сообщения
-- @param dialog_id UUID диалога (пары пользователей)
-- @param from_user_id UUID отправителя
-- @param to_user_id UUID получателя
-- @param text Текст сообщения
-- @param created_at ISO timestamp создания
-- @return Таблица с полями созданного сообщения
function dialog_send(id, dialog_id, from_user_id, to_user_id, text, created_at)
    local tuple = box.space.messages:insert{
        id,
        dialog_id,
        from_user_id,
        to_user_id,
        text,
        created_at
    }
    return {
        id = tuple[1],
        dialog_id = tuple[2],
        from_user_id = tuple[3],
        to_user_id = tuple[4],
        text = tuple[5],
        created_at = tuple[6]
    }
end

--- UDF: Получение списка сообщений диалога
-- @param dialog_id UUID диалога
-- @return Массив сообщений диалога, отсортированный по created_at ASC
function dialog_list(dialog_id)
    local result = {}
    for _, tuple in box.space.messages.index.dialog_created:pairs({dialog_id}, {iterator = 'EQ'}) do
        table.insert(result, {
            id = tuple[1],
            dialog_id = tuple[2],
            from_user_id = tuple[3],
            to_user_id = tuple[4],
            text = tuple[5],
            created_at = tuple[6]
        })
    end
    return result
end

--- UDF: Получение общего количества сообщений в Tarantool
-- @return Number count
function dialog_count()
    return box.space.messages:count()
end

--- UDF: Очистка сообщений (для сброса перед тестами)
function dialog_clear()
    box.space.messages:truncate()
    return {status = "cleared"}
end

print("Tarantool UDF module initialized successfully on port 3301.")
