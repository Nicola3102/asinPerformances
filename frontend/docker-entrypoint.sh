#!/bin/sh
set -e
cd /app
# 命名卷 node_modules 可能长期保留旧依赖（例如后来才加入 chart.js）；ci 按 lock 全量装齐
npm ci --no-audit --no-fund || npm install --no-audit --no-fund
exec "$@"
