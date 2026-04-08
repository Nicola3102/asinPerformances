"""
Online 库连接池：与 PolarDB 等线上库通信。

- ``get_online_engine()``：后台同步、定时任务、大批量读写的**主池**（较大 pool、较长 pool_timeout）。
- ``get_online_reporting_engine()``：**报表/用户请求**专用小池（短 pool_timeout，拿不到连接快速失败，避免整页挂 30s）。
"""
from sqlalchemy import create_engine

from app.config import settings

_online_sync_engine = None
_online_reporting_engine = None


def _connect_args() -> dict:
    return {
        "connect_timeout": 30,
        "read_timeout": 1800,
        "write_timeout": 1800,
    }


def get_online_engine():
    """
    后台同步 / 定时任务等使用的 online 连接池（懒加载）。
    """
    global _online_sync_engine
    if _online_sync_engine is None:
        if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
            raise ValueError("Online DB 未配置：需设置 online_db_host, online_db_user 等")
        _online_sync_engine = create_engine(
            settings.online_database_url,
            pool_pre_ping=True,
            pool_size=settings.ONLINE_SYNC_POOL_SIZE,
            max_overflow=settings.ONLINE_SYNC_POOL_OVERFLOW,
            pool_timeout=settings.ONLINE_SYNC_POOL_TIMEOUT,
            pool_recycle=1800,
            connect_args=_connect_args(),
        )
    return _online_sync_engine


def get_online_reporting_engine():
    """
    报表类接口（如 PST New Listing 读 amazon_listing）专用池：小且 pool_timeout 短，避免与后台任务共池时长时间排队。
    """
    global _online_reporting_engine
    if _online_reporting_engine is None:
        if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
            raise ValueError("Online DB 未配置：需设置 online_db_host, online_db_user 等")
        _online_reporting_engine = create_engine(
            settings.online_database_url,
            pool_pre_ping=True,
            pool_size=settings.ONLINE_REPORT_POOL_SIZE,
            max_overflow=settings.ONLINE_REPORT_POOL_OVERFLOW,
            pool_timeout=settings.ONLINE_REPORT_POOL_TIMEOUT,
            pool_recycle=1800,
            connect_args=_connect_args(),
        )
    return _online_reporting_engine


def dispose_online_engine():
    """释放后台同步用 online 连接池。"""
    global _online_sync_engine
    if _online_sync_engine is not None:
        try:
            _online_sync_engine.dispose()
        except Exception:
            pass
        _online_sync_engine = None


def dispose_online_reporting_engine():
    """释放报表专用 online 连接池。"""
    global _online_reporting_engine
    if _online_reporting_engine is not None:
        try:
            _online_reporting_engine.dispose()
        except Exception:
            pass
        _online_reporting_engine = None


def dispose_all_online_engines():
    dispose_online_reporting_engine()
    dispose_online_engine()
