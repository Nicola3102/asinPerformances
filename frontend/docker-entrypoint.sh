#!/bin/sh
set -e
cd /app
# 命名卷 node_modules 可能残缺（曾装失败、或 lock 升级后未拉齐）；缺 chart 则清空再装
if [ -f package-lock.json ]; then
  if [ ! -d node_modules/chart.js ] || [ ! -d node_modules/react-chartjs-2 ]; then
    echo "docker-entrypoint: 缺少 chart.js / react-chartjs-2，删除 node_modules 后执行 npm ci"
    rm -rf node_modules
  fi
  npm ci --no-audit --no-fund || npm install --no-audit --no-fund
else
  npm install --no-audit --no-fund
fi
# Vite 开发态的优化依赖缓存位于 node_modules/.vite；容器重建、HMR 中断或浏览器旧会话
# 偶发会导致依赖图与 browserHash 短暂失配。每次启动前清理，可显著降低脏缓存复发概率。
if [ -d node_modules/.vite ]; then
  echo "docker-entrypoint: 清理 Vite 预构建缓存 node_modules/.vite"
  rm -rf node_modules/.vite
fi
exec "$@"
