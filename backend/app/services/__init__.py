"""业务服务包。避免在 ``import app.services`` 时立刻加载所有子模块（例如仅运行 ``report_pst`` 时不需要 online_sync）。"""

from __future__ import annotations

__all__ = ["sync_from_online_db"]


def __getattr__(name: str):
    if name == "sync_from_online_db":
        from app.services.online_sync import sync_from_online_db as _sync

        globals()["sync_from_online_db"] = _sync
        return _sync
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
