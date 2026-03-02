from pydantic import Field
from pydantic_settings import BaseSettings


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

    # 定时同步：东八区每 N 小时执行一次 online_sync
    ENABLE_SCHEDULED_SYNC: bool = Field(default=True, validation_alias="enable_scheduled_sync")
    SYNC_INTERVAL_HOURS: int = Field(default=2, validation_alias="sync_interval_hours")
    SYNC_TIMEZONE: str = Field(default="Asia/Shanghai", validation_alias="sync_timezone")

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

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
