FROM postgres:15-alpine

RUN apk add --no-cache \
    python3 \
    py3-pip \
    curl \
    bash \
    gettext \
    gcc \
    musl-dev \
    libffi-dev \
    openssl-dev

RUN pip3 install --break-system-packages --no-cache-dir \
    patroni[etcd] \
    psycopg2-binary \
    pyyaml

COPY patroni-template.yml /etc/patroni/patroni-template.yml
COPY init-db.sql /etc/patroni/init-db.sql
COPY post_init.sh /etc/patroni/post_init.sh
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh /etc/patroni/post_init.sh

EXPOSE 5432 8008

ENTRYPOINT ["/entrypoint.sh"]
