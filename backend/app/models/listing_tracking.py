from sqlalchemy import BigInteger, Column, DateTime, Integer, String, UniqueConstraint

from app.database import Base


class ListingTracking(Base):
    __tablename__ = "listing_tracking"
    __table_args__ = (
        UniqueConstraint(
            "batch_id",
            "pid",
            "week_no",
            "store_id",
            name="uq_listing_tracking_batch_pid_week_store",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(Integer, nullable=False, index=True)
    pid = Column(BigInteger, nullable=False, index=True)
    pid_asin_count = Column(Integer, nullable=True)
    pid_active_asin_count = Column(Integer, nullable=True)
    parent_asin = Column(String(32), nullable=True, index=True)
    created_at = Column(DateTime, nullable=True, index=True)
    week_no = Column(Integer, nullable=False, index=True)
    total_impression = Column(BigInteger, nullable=True)
    total_session = Column(BigInteger, nullable=True)
    total_click = Column(BigInteger, nullable=True)
    total_order = Column(BigInteger, nullable=True)
    session_asin = Column(String(4096), nullable=True)
    impression_asin = Column(String(4096), nullable=True)
    used_text_model = Column(String(255), nullable=True)
    store_id = Column(Integer, nullable=False, index=True)
    used_image_model = Column(String(255), nullable=True)
