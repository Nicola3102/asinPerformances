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
    ENABLE_DAILY_UPLOAD_DS_SCHEDULE: bool = Field(
        default=True,
        validation_alias="enable_daily_upload_ds_schedule",
    )
    DAILY_UPLOAD_DS_INTERVAL_HOURS: int = Field(
        default=2,
        validation_alias="daily_upload_ds_interval_hours",
    )
    DAILY_UPLOAD_DS_START_DATE: str = Field(
        default="2026-02-20",
        validation_alias="daily_upload_ds_start_date",
    )

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
