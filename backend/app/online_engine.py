"""
Online 库共享连接池。供 asin_controller、online_sync 等复用，避免频繁创建/销毁连接。
"""
from sqlalchemy import create_engine

from app.config import settings

_online_engine = None


def get_online_engine():
    """
    获取 online 库的共享 engine（懒加载）。
    连接池：pool_size=5, max_overflow=10，长查询超时 30 分钟，空闲连接 30 分钟回收。
    """
    global _online_engine
    if _online_engine is None:
        if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
            raise ValueError("Online DB 未配置：需设置 online_db_host, online_db_user 等")
        _online_engine = create_engine(
            settings.online_database_url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            pool_recycle=1800,
            connect_args={
                "connect_timeout": 30,
                "read_timeout": 1800,
                "write_timeout": 1800,
            },
        )
    return _online_engine


def dispose_online_engine():
    """应用关闭时释放连接池（可选调用）。"""
    global _online_engine
    if _online_engine is not None:
        try:
            _online_engine.dispose()
        except Exception:
            pass
        _online_engine = None
