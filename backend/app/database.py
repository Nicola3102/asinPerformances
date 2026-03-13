import time

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import NullPool

from app.config import settings

# 本地库：保持 NullPool，不建立连接池
_engine_kwargs = {
    "pool_pre_ping": True,
    "echo": False,
    "poolclass": NullPool,
}

engine = create_engine(settings.database_url, **_engine_kwargs)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _ensure_order_id_column():
    """若 asin_performances 表缺少 order_id 列则添加（兼容已有表）。"""
    from sqlalchemy import text
    with engine.connect() as conn:
        if engine.dialect.name == "mysql":
            r = conn.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'asin_performances' AND COLUMN_NAME = 'order_id'"
                )
            ).scalar()
            if r == 0:
                conn.execute(text("ALTER TABLE asin_performances ADD COLUMN order_id VARCHAR(512) NULL"))
                conn.execute(text("CREATE INDEX ix_asin_performances_order_id ON asin_performances (order_id)"))
                conn.commit()
        # 其他 dialect 可在此扩展


def _ensure_operation_columns():
    """若 asin_performances 表缺少 operation_status / operated_at / query_* 列则添加（兼容已有表）。"""
    from sqlalchemy import text
    with engine.connect() as conn:
        if engine.dialect.name == "mysql":
            for col, ddl in [
                ("operation_status", "ALTER TABLE asin_performances ADD COLUMN operation_status TINYINT(1) NOT NULL DEFAULT 0"),
                ("operated_at", "ALTER TABLE asin_performances ADD COLUMN operated_at DATETIME NULL"),
                ("checked_status", "ALTER TABLE asin_performances ADD COLUMN checked_status VARCHAR(32) NOT NULL DEFAULT 'pending'"),
                ("checked_at", "ALTER TABLE asin_performances ADD COLUMN checked_at DATETIME NULL"),
            ]:
                r = conn.execute(
                    text(
                        "SELECT COUNT(*) FROM information_schema.COLUMNS "
                        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'asin_performances' AND COLUMN_NAME = :col"
                    ),
                    {"col": col},
                ).scalar()
                if r == 0:
                    conn.execute(text(ddl))
                    conn.commit()


def init_db():
    """Create all tables. Called on startup. Retries if MySQL not ready (e.g. in Docker)."""
    from app.models import asin_performance  # noqa: F401

    max_attempts = 10
    for attempt in range(1, max_attempts + 1):
        try:
            Base.metadata.create_all(bind=engine)
            _ensure_order_id_column()
            _ensure_operation_columns()
            return
        except OperationalError as e:
            if attempt == max_attempts:
                raise
            time.sleep(2)
            continue
