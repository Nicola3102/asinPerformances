from sqlalchemy import Boolean, Column, Integer, String, BigInteger, Numeric, DateTime, UniqueConstraint

from app.database import Base


class AsinPerformance(Base):
    __tablename__ = "asin_performances"
    __table_args__ = (
        UniqueConstraint(
            "store_id",
            "parent_asin",
            "child_asin",
            "week_no",
            "search_query",
            name="uq_asin_perf_store_parent_child_week_query",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column(Integer, nullable=True, index=True)
    parent_asin = Column(String(32), nullable=True, index=True)
    child_asin = Column(String(32), nullable=True, index=True)
    parent_asin_create_at = Column(DateTime, nullable=True)
    parent_order_total = Column(Numeric(20, 2), nullable=True)
    order_num = Column(Integer, nullable=True)
    order_id = Column(String(512), nullable=True, index=True)  # 子 asin 的订单 id，多个用逗号分隔
    week_no = Column(Integer, nullable=True)  #使用订单表中的utc时间
    child_impression_count = Column(BigInteger, nullable=True)
    child_session_count = Column(BigInteger, nullable=True)
    search_query = Column(String(512), nullable=True)
    search_query_volume = Column(BigInteger, nullable=True)
    search_query_impression_count = Column(BigInteger, nullable=True)
    search_query_purchase_count = Column(BigInteger, nullable=True)
    search_query_total_impression = Column(BigInteger, nullable=True)
    search_query_click_count = Column(BigInteger, nullable=True)
    search_query_total_click = Column(BigInteger, nullable=True)
    operation_status = Column(Boolean, default=False, nullable=False, server_default="0")
    ad_check = Column(Boolean, default=False, nullable=False, server_default="0")
    ad_created_at = Column(DateTime, nullable=True)
    operated_at = Column(DateTime, nullable=True)
    checked_status = Column(String(32), default="pending", nullable=False, server_default="pending")
    checked_at = Column(DateTime, nullable=True)
