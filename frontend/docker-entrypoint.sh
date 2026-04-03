#!/bin/sh
set -e
cd /app
# 与 package.json / lock 同步（命名卷 node_modules 会遮挡镜像内依赖，需在启动时安装）
npm install
exec "$@"
