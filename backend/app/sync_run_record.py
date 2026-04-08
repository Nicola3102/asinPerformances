"""
记录最近一次同步执行时间（东八区），用于定时任务避免同一小时内重复执行。
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

# 东八区 UTC+8
TZ_ASIA_SHANGHAI = timezone(timedelta(hours=8))
SYNC_FILENAME = ".last_sync_run"
MONITOR_FILENAME = ".last_monitor_run"
LISTING_TRACKING_FILENAME = ".last_listing_tracking_run"
DAILY_UPLOAD_DS_FILENAME = ".last_daily_upload_ds_run"


def _get_record_path(filename: str) -> Path:
    base = Path(__file__).resolve().parent
    log_dir = base / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / filename


def record_sync_run() -> None:
    """记录当前时间为最近一次同步执行时间（UTC 写入文件，便于跨时区）。"""
    path = _get_record_path(SYNC_FILENAME)
    path.write_text(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"), encoding="utf-8")


def get_last_sync_run_utc() -> datetime | None:
    """读取最近一次同步时间（UTC），若文件不存在或无效则返回 None。"""
    path = _get_record_path(SYNC_FILENAME)
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


def is_n_hour_slot(asia_dt: datetime, interval_hours: int) -> bool:
    """东八区当前小时是否命中 N 小时窗口。"""
    interval = max(1, int(interval_hours or 1))
    return asia_dt.hour % interval == 0


def should_run_scheduled_sync() -> bool:
    """
    当前（东八区）是否应执行定时同步：
    - 由 cron 保证只在命中的小时窗口整点调用，此处仅做“本小时内是否已跑过”的判断；
    - 若本小时内已有执行记录（含手动），返回 False；
    - 否则返回 True。
    约定：仅在东八区定时窗口整点触发时调用，用于判断该小时内是否已执行过。
    """
    now = now_asia()
    last = get_last_sync_run_asia()
    if last is None:
        return True
    # 同一自然日且同一小时（东八区）内已执行过，则不再执行
    if (last.date(), last.hour) == (now.date(), now.hour):
        return False
    return True


def record_listing_tracking_run() -> None:
    """记录 listing_tracking 最近一次定时回填执行时间（UTC 写入文件，便于跨时区）。"""
    path = _get_record_path(LISTING_TRACKING_FILENAME)
    path.write_text(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"), encoding="utf-8")


def get_last_listing_tracking_run_asia() -> datetime | None:
    """最近一次 listing_tracking 定时回填时间（东八区）。"""
    path = _get_record_path(LISTING_TRACKING_FILENAME)
    if not path.exists():
        return None
    try:
        text = path.read_text(encoding="utf-8").strip()
        utc = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return utc.astimezone(TZ_ASIA_SHANGHAI)
    except Exception:
        return None


def should_run_listing_tracking_sync() -> bool:
    """
    当前（东八区）是否应执行 listing_tracking 定时回填：
    - 若同一自然日同一小时已执行过，则返回 False
    - 否则返回 True
    """
    now = now_asia()
    path = _get_record_path(LISTING_TRACKING_FILENAME)
    if not path.exists():
        return True
    try:
        text = path.read_text(encoding="utf-8").strip()
        last_utc = datetime.fromisoformat(text.replace("Z", "+00:00"))
        last = last_utc.astimezone(TZ_ASIA_SHANGHAI)
    except Exception:
        return True
    if (last.date(), last.hour) == (now.date(), now.hour):
        return False
    return True


def record_daily_upload_ds_run() -> None:
    """记录 daily_upload_asin_data_ds 最近一次定时执行时间（UTC 写入文件）。"""
    path = _get_record_path(DAILY_UPLOAD_DS_FILENAME)
    path.write_text(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"), encoding="utf-8")


def get_last_daily_upload_ds_run_asia() -> datetime | None:
    """最近一次 DS 定时同步时间（东八区）。"""
    path = _get_record_path(DAILY_UPLOAD_DS_FILENAME)
    if not path.exists():
        return None
    try:
        text = path.read_text(encoding="utf-8").strip()
        utc = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return utc.astimezone(TZ_ASIA_SHANGHAI)
    except Exception:
        return None


def should_run_daily_upload_ds_sync() -> bool:
    """
    当前（东八区）是否应执行 DailyUploadDS 定时同步：
    - 若同一自然日同一小时已执行过，则返回 False
    - 否则返回 True
    """
    now = now_asia()
    last = get_last_daily_upload_ds_run_asia()
    if last is None:
        return True
    if (last.date(), last.hour) == (now.date(), now.hour):
        return False
    return True


def record_monitor_run() -> None:
    """记录当前时间为最近一次 monitor 执行时间（UTC 写入文件，便于跨时区）。"""
    path = _get_record_path(MONITOR_FILENAME)
    path.write_text(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"), encoding="utf-8")


def get_last_monitor_run_utc() -> datetime | None:
    """读取最近一次 monitor 执行时间（UTC），若文件不存在或无效则返回 None。"""
    path = _get_record_path(MONITOR_FILENAME)
    if not path.exists():
        return None
    try:
        text = path.read_text(encoding="utf-8").strip()
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def get_last_monitor_run_asia() -> datetime | None:
    """最近一次 monitor 执行时间（东八区）。"""
    utc = get_last_monitor_run_utc()
    if utc is None:
        return None
    return utc.astimezone(TZ_ASIA_SHANGHAI)


def should_run_monitor_sync(interval_hours: int = 6) -> bool:
    """
    当前（东八区）是否应执行 monitor 定时同步：
    - 仅在命中 N 小时窗口时使用；
    - 若本小时内已有执行记录，则返回 False；
    - 否则返回 True。
    """
    now = now_asia()
    last = get_last_monitor_run_asia()
    if last is None:
        return True
    if (last.date(), last.hour) == (now.date(), now.hour):
        return False
    return True
