from sqlalchemy import BigInteger, Boolean, Column, DateTime, Integer, String, UniqueConstraint

from app.database import Base


class GroupA(Base):
    __tablename__ = "group_A"
    __table_args__ = (
        UniqueConstraint(
            "store_id",
            "parent_asin",
            "child_asin",
            "week_no",
            "search_query",
            name="uq_group_a_store_parent_child_week_query",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column(Integer, nullable=True, index=True)
    parent_asin = Column(String(32), nullable=True, index=True)
    parent_asin_created_at = Column(DateTime, nullable=True)
    child_asin = Column(String(32), nullable=True, index=True)
    child_impression_count = Column(BigInteger, nullable=True)
    child_cart = Column(BigInteger, nullable=True)
    child_session_count = Column(BigInteger, nullable=True)
    week_no = Column(Integer, nullable=True, index=True)
    search_query = Column(String(512), nullable=True)
    search_query_volume = Column(BigInteger, nullable=True)
    search_query_impression_count = Column(BigInteger, nullable=True)
    search_query_cart_count = Column(BigInteger, nullable=True)
    search_query_total_impression_count = Column(BigInteger, nullable=True)
    search_query_click_count = Column(BigInteger, nullable=True)
    # 兼容：若旧表已存在 search_query_total_click，则会保留；新逻辑写入 *_count 字段
    search_query_total_click_count = Column(BigInteger, nullable=True)
    migrated_to_asin_performances = Column(Boolean, default=False, nullable=False, server_default="0")
    operation_status = Column(Boolean, default=False, nullable=False, server_default="0")
    operated_at = Column(DateTime, nullable=True)

