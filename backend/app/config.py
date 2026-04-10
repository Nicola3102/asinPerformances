from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings

# 固定从 backend 目录加载 .env，避免用 -m 从项目根运行时读不到
_BACKEND_DIR = Path(__file__).resolve().parent.parent
_ENV_FILE = _BACKEND_DIR / ".env"


class Settings(BaseSettings):
    MYSQL_HOST: str = "mysql"
    MYSQL_PORT: int = 3306
    MYSQL_USER: str = "rug_user"
    MYSQL_PASSWORD: str = "rug_password"
    MYSQL_DATABASE: str = "rug"
    MYSQL_DB_NAME: str = Field(default="asin_performances", validation_alias="MYSQL_DB_NAME")

    ONLINE_DB_HOST: str = Field(default="", validation_alias="online_db_host")
    ONLINE_DB_PORT: int = Field(default=3306, validation_alias="online_db_port")
    ONLINE_DB_NAME: str = Field(default="", validation_alias="online_db_name")
    ONLINE_DB_USER: str = Field(default="", validation_alias="online_db_user")
    ONLINE_DB_PWD: str = Field(default="", validation_alias="online_db_pwd")

    # online 库：后台同步/定时任务用较大连接池；报表接口（如 New Listing）用独立小池 + 短 pool_timeout，避免抢池卡 30s
    ONLINE_SYNC_POOL_SIZE: int = Field(default=8, validation_alias="online_sync_pool_size")
    ONLINE_SYNC_POOL_OVERFLOW: int = Field(default=12, validation_alias="online_sync_pool_overflow")
    ONLINE_SYNC_POOL_TIMEOUT: int = Field(default=30, validation_alias="online_sync_pool_timeout")
    # 报表池默认给到中等容量：避免在页面轮询/多标签并发时频繁 pool timeout 导致 KPI 置 0
    ONLINE_REPORT_POOL_SIZE: int = Field(default=4, validation_alias="online_report_pool_size")
    ONLINE_REPORT_POOL_OVERFLOW: int = Field(default=4, validation_alias="online_report_pool_overflow")
    ONLINE_REPORT_POOL_TIMEOUT: int = Field(default=5, validation_alias="online_report_pool_timeout")

    # GET /api/trend/new-listing?format=json 进程内缓存（本地 session 矩阵会变，不宜过长）
    NEW_LISTING_JSON_CACHE_TTL_SEC: int = Field(default=120, validation_alias="new_listing_json_cache_ttl_sec")
    NEW_LISTING_JSON_CACHE_MAX_KEYS: int = Field(default=32, validation_alias="new_listing_json_cache_max_keys")

    # 定时同步：东八区每 N 小时执行一次；后端进程需常驻
    ENABLE_SCHEDULED_SYNC: bool = Field(default=True, validation_alias="enable_scheduled_sync")
    SYNC_INTERVAL_HOURS: int = Field(default=2, validation_alias="sync_interval_hours")
    MONITOR_SYNC_INTERVAL_HOURS: int = Field(default=6, validation_alias="monitor_sync_interval_hours")
    GROUP_A_SYNC_INTERVAL_HOURS: int = Field(default=4, validation_alias="group_a_sync_interval_hours")
    # ListingTracking 定时回填：更新最近 N 周（不含当前周），按本地 listing_tracking 表内已有 pid 推导。
    LISTING_TRACKING_RECENT_WEEKS: int = Field(
        default=2,
        validation_alias="listing_tracking_recent_weeks",
    )
    # ListingTracking 每天执行次数（>=1）；若=1 则每天 00:00 执行；若>1 则在一天内均匀分配小时点。
    LISTING_TRACKING_DAILY_TIMES: int = Field(
        default=1,
        validation_alias="listing_tracking_daily_times",
    )
    LISTING_TRACKING_WRITE_CHUNK_SIZE: int = Field(
        default=500,
        validation_alias="listing_tracking_write_chunk_size",
    )
    LISTING_TRACKING_READER_WORKERS: int = Field(
        default=4,
        validation_alias="listing_tracking_reader_workers",
    )
    IMAGE_MODEL_RAW: str = Field(
        default="qwen-image-edit-2511",
        validation_alias="image_model",
    )
    LISTING_TRACKING_IMAGE_MODEL_PREFIXES_RAW: str = Field(
        default="",
        validation_alias="listing_tracking_image_model_prefixes",
    )
    SYNC_TIMEZONE: str = Field(default="Asia/Shanghai", validation_alias="sync_timezone")
    # daily_upload_asin_data_ds 定时：listing 扫描下界（YYYY-MM-DD）；上界为东八区当日（含）
    # DailyUpload（非 DS）同步 + 导出 report4.html 的定时开关；如只想保留 DailyUploadDS，可设为 false
    ENABLE_DAILY_UPLOAD_SCHEDULE: bool = Field(
        default=True,
        validation_alias="enable_daily_upload_schedule",
    )
    ENABLE_DAILY_UPLOAD_DS_SCHEDULE: bool = Field(
        default=True,
        validation_alias="enable_daily_upload_ds_schedule",
    )
    DAILY_UPLOAD_DS_INTERVAL_HOURS: int = Field(
        default=2,
        validation_alias="daily_upload_ds_interval_hours",
        description="仅当 daily_upload_ds_daily_times=0 时使用：每 N 小时整点执行（旧行为）",
    )
    # daily_upload_ds_daily_times>=1：在 SYNC_TIMEZONE 日历日内从 first_run_hour 起均匀分布 N 次（默认 11、23 点）
    DAILY_UPLOAD_DS_DAILY_TIMES: int = Field(
        default=2,
        ge=0,
        le=24,
        validation_alias="daily_upload_ds_daily_times",
    )
    DAILY_UPLOAD_DS_FIRST_RUN_HOUR: int = Field(
        default=11,
        ge=0,
        le=23,
        validation_alias="daily_upload_ds_first_run_hour",
    )
    DAILY_UPLOAD_DS_START_DATE: str = Field(
        default="2026-02-20",
        validation_alias="daily_upload_ds_start_date",
        description="已不使用：DailyUploadDS 定时与无参 CLI 均为东八区当日往前 35 天起算，保留仅为兼容旧 .env",
    )
    # daily_ad_cost_sales 定时：在 SYNC_TIMEZONE 内从 first_run 起均匀分布 N 次（与 daily_upload_ds 类似，支持首跑分）
    ENABLE_DAILY_AD_COST_SALES_SCHEDULE: bool = Field(
        default=True,
        validation_alias="enable_daily_ad_cost_sales_schedule",
    )
    DAILY_AD_COST_SALES_DAILY_TIMES: int = Field(
        default=2,
        ge=1,
        le=24,
        validation_alias="daily_ad_cost_sales_daily_times",
    )
    DAILY_AD_COST_SALES_FIRST_RUN_HOUR: int = Field(
        default=8,
        ge=0,
        le=23,
        validation_alias="daily_ad_cost_sales_first_run_hour",
    )
    DAILY_AD_COST_SALES_FIRST_RUN_MINUTE: int = Field(
        default=0,
        ge=0,
        le=59,
        validation_alias="daily_ad_cost_sales_first_run_minute",
    )

    def daily_upload_ds_cron_hours(self) -> tuple[list[int], str]:
        """
        与 APScheduler 一致：在 SYNC_TIMEZONE 日历日内整点触发的小时列表。
        - daily_upload_ds_daily_times >= 1：从 daily_upload_ds_first_run_hour 起 24h 内均匀分布 N 次；
        - daily_upload_ds_daily_times == 0：每 daily_upload_ds_interval_hours 小时从 0 点起整点（legacy）。
        """
        n_raw = int(self.DAILY_UPLOAD_DS_DAILY_TIMES or 0)
        if n_raw <= 0:
            ds_interval = max(1, int(self.DAILY_UPLOAD_DS_INTERVAL_HOURS or 2))
            hours = list(range(0, 24, ds_interval))
            return hours, f"interval={ds_interval}h (legacy)"
        n = max(1, min(24, n_raw))
        first = max(0, min(23, int(self.DAILY_UPLOAD_DS_FIRST_RUN_HOUR)))
        if n == 1:
            return [first], f"daily_times=1 first_hour={first}"
        hours_set = {(first + int(round(i * 24 / n))) % 24 for i in range(n)}
        hours_sorted = sorted(hours_set)
        if len(hours_sorted) < n:
            extra = first
            while len(hours_sorted) < n and extra < first + 48:
                cand = extra % 24
                if cand not in hours_sorted:
                    hours_sorted.append(cand)
                extra += 1
            hours_sorted = sorted(hours_sorted)[:n]
        return hours_sorted, f"daily_times={n} first_hour={first} hours={hours_sorted}"

    def daily_ad_cost_sales_cron_slots(self) -> tuple[list[tuple[int, int]], str]:
        """
        APScheduler 在 SYNC_TIMEZONE 下的 (hour, minute) 触发点列表。
        从 first_run_hour:first_run_minute 起在 24h 内均匀分布 daily_times 次；各次共用同一 minute。
        """
        n = max(1, min(24, int(self.DAILY_AD_COST_SALES_DAILY_TIMES or 1)))
        first = max(0, min(23, int(self.DAILY_AD_COST_SALES_FIRST_RUN_HOUR)))
        minute = max(0, min(59, int(self.DAILY_AD_COST_SALES_FIRST_RUN_MINUTE)))
        if n == 1:
            slots = [(first, minute)]
            return slots, f"daily_times=1 first={first:02d}:{minute:02d} slots={slots}"
        hours_set = {(first + int(round(i * 24 / n))) % 24 for i in range(n)}
        hours_sorted = sorted(hours_set)
        if len(hours_sorted) < n:
            extra = first
            while len(hours_sorted) < n and extra < first + 48:
                cand = extra % 24
                if cand not in hours_sorted:
                    hours_sorted.append(cand)
                extra += 1
            hours_sorted = sorted(hours_sorted)[:n]
        slots = [(h, minute) for h in hours_sorted]
        return slots, f"daily_times={n} first={first:02d}:{minute:02d} hours={[h for h, _ in slots]}"

    @property
    def database_url(self) -> str:
        return (
            f"mysql+pymysql://{self.MYSQL_USER}:{self.MYSQL_PASSWORD}"
            f"@{self.MYSQL_HOST}:{self.MYSQL_PORT}/{self.MYSQL_DATABASE}"
        )

    @property
    def online_database_url(self) -> str:
        return (
            f"mysql+pymysql://{self.ONLINE_DB_USER}:{self.ONLINE_DB_PWD}"
            f"@{self.ONLINE_DB_HOST}:{self.ONLINE_DB_PORT}/{self.ONLINE_DB_NAME}"
        )

    @property
    def listing_tracking_image_model_prefixes(self) -> list[str]:
        raw = self.IMAGE_MODEL_RAW or self.LISTING_TRACKING_IMAGE_MODEL_PREFIXES_RAW or ""
        return [item.strip() for item in raw.split(",") if item.strip()]

    class Config:
        env_file = str(_ENV_FILE)
        extra = "ignore"


settings = Settings()
