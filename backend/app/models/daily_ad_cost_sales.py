from sqlalchemy import BigInteger, Column, Date, Float, Integer, Numeric, String, UniqueConstraint

from app.database import Base


class DailyAdCostSales(Base):
    __tablename__ = "daily_ad_cost_sales"
    __table_args__ = (
        UniqueConstraint(
            "ad_asin",
            "store_id",
            "pid",
            "purchase_date",
            name="uq_daily_ad_cost_sales_asin_store_pid_date",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    ad_asin = Column(String(32), nullable=True, index=True)
    store_id = Column(Integer, nullable=True, index=True)
    pid = Column(BigInteger, nullable=True, index=True)
    variation_id = Column(BigInteger, nullable=True, index=True)
    purchase_date = Column(Date, nullable=True, index=True)
    clicks = Column(BigInteger, nullable=True)
    impressions = Column(BigInteger, nullable=True)
    purchases = Column(BigInteger, nullable=True)
    ad_cost = Column(Numeric(20, 2), nullable=True)
    sales_1d = Column(Numeric(20, 2), nullable=True)
    ad_sales_1d = Column(Float, nullable=True)
    tad_sales = Column(Float, nullable=True)
    # order_item 同店同 ASIN SUM(total_amount)：与 purchase_date 同日
    tsales = Column(Numeric(20, 2), nullable=True)
