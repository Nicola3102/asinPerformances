#!/usr/bin/env python3
"""
执行 ONLINE_SQL：从 .env 中的 online_db_* 连接读取数据，写入本地 MySQL 的 MYSQL_DB_NAME 表（asin_performances）。
用法（在 backend 目录下）:
  python run_online_sync.py
或:
  python -m app.services.online_sync
"""
import sys

# 确保 backend 为当前目录，以便加载 app 和 .env
from app.services.online_sync import sync_from_online_db

if __name__ == "__main__":
    try:
        n = sync_from_online_db()
        print(f"OK: 已从 online 库同步 {n} 条到表 asin_performances (MYSQL_DB_NAME)")
        sys.exit(0)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
