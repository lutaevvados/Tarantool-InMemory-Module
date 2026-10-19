#!/bin/bash
set -e

CONN_URL="$1"
echo "Running Patroni post_init script..."
psql "$CONN_URL" -f /etc/patroni/init-db.sql || true
