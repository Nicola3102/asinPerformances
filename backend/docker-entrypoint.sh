#!/bin/sh
set -e
# Compose 不再使用周期性 healthcheck；仅在容器启动时等待 MySQL TCP 可连后再启动 uvicorn。
HOST="${MYSQL_HOST:-mysql}"
PORT="${MYSQL_PORT:-3306}"
echo "docker-entrypoint: waiting for MySQL ${HOST}:${PORT} ..."
n=0
while [ "$n" -lt 120 ]; do
  if python -c "import socket; s=socket.socket(); s.settimeout(2); s.connect((\"${HOST}\", int(\"${PORT}\"))); s.close()" 2>/dev/null; then
    echo "docker-entrypoint: MySQL is reachable."
    exec "$@"
  fi
  n=$((n + 1))
  sleep 1
done
echo "docker-entrypoint: MySQL not reachable after 120s" >&2
exit 1
