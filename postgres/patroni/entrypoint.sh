#!/bin/bash
set -e

# Убедимся что директория существует и имеет правильные права
mkdir -p /var/lib/postgresql/pgdata
chmod 700 /var/lib/postgresql/pgdata
chown -R postgres:postgres /var/lib/postgresql

# Генерируем patroni.yml из template с переменными окружения
envsubst < /etc/patroni/patroni-template.yml > /etc/patroni/patroni.yml

chown postgres:postgres /etc/patroni/patroni.yml

# Запускаем Patroni от пользователя postgres
exec su postgres -c "patroni /etc/patroni/patroni.yml"
