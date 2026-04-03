"""
本地表 daily_upload_asin_dates：按「上新 listing 行」维度存 (asin, created_at, pid, store_id) 与
按日历日的 session_date、sessions。数据由 app.services.daily_upload_asin_data 从线上
amazon_listing + amazon_sales_and_traffic_daily 同步；仅覆盖命令行指定上新日区间内出现的 ASIN，
并非线上 daily 表的全量镜像。
"""

from sqlalchemy import BigInteger, Column, Date, Integer, String, UniqueConstraint

from app.database import Base


class DailyUploadAsinData(Base):
    __tablename__ = "daily_upload_asin_dates"
    __table_args__ = (
        UniqueConstraint(
            "asin",
            "created_at",
            "pid",
            "store_id",
            "session_date",
            name="uq_daily_upload_asin_created_pid_store_date",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    asin = Column(String(32), nullable=False, index=True)
    pid = Column(BigInteger, nullable=True, index=True)
    paren_asin = Column(String(32), nullable=True, index=True)
    store_id = Column(Integer, nullable=False, index=True)
    status = Column(String(32), nullable=True, index=True)
    created_at = Column(Date, nullable=True, index=True)
    # PST 报表使用：amazon_listing.open_date（按日历日）
    open_date = Column(Date, nullable=True, index=True)
    session_date = Column(Date, nullable=False, index=True)
    sessions = Column(BigInteger, nullable=True)

