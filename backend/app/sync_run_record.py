"""
记录最近一次同步执行时间（东八区），用于定时任务：偶数整点若本小时内已执行（含手动）则不再执行。
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

# 东八区 UTC+8
TZ_ASIA_SHANGHAI = timezone(timedelta(hours=8))
FILENAME = ".last_sync_run"


def _get_record_path() -> Path:
    base = Path(__file__).resolve().parent
    log_dir = base / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / FILENAME


def record_sync_run() -> None:
    """记录当前时间为最近一次同步执行时间（UTC 写入文件，便于跨时区）。"""
    path = _get_record_path()
    path.write_text(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"), encoding="utf-8")


def get_last_sync_run_utc() -> datetime | None:
    """读取最近一次同步时间（UTC），若文件不存在或无效则返回 None。"""
    path = _get_record_path()
    if not path.exists():
        return None
    try:
        text = path.read_text(encoding="utf-8").strip()
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def get_last_sync_run_asia() -> datetime | None:
    """最近一次同步时间（东八区）。"""
    utc = get_last_sync_run_utc()
    if utc is None:
        return None
    return utc.astimezone(TZ_ASIA_SHANGHAI)


def now_asia() -> datetime:
    """当前时间（东八区）。"""
    return datetime.now(TZ_ASIA_SHANGHAI)


def is_even_hour(asia_dt: datetime) -> bool:
    """东八区小时是否为偶数（0,2,4,...,22）。"""
    return asia_dt.hour % 2 == 0


def should_run_scheduled_sync() -> bool:
    """
    当前（东八区）是否应执行定时同步：
    - 若当前不是偶数整点所在的小时，返回 True（由 cron 保证只在整点调用，此处仅做“本小时内是否已跑过”的判断）；
    - 若本小时内已有执行记录（含手动），返回 False；
    - 否则返回 True。
    约定：仅在东八区偶数整点（0,2,4,...,22）触发时调用，用于判断该偶数小时内是否已执行过。
    """
    now = now_asia()
    last = get_last_sync_run_asia()
    if last is None:
        return True
    # 同一自然日且同一小时（东八区）内已执行过，则不再执行
    if (last.date(), last.hour) == (now.date(), now.hour):
        return False
    return True
