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
    """若 asin_performances 表缺少 operation_status / ad_check / ad_created_at / operated_at / query_* 列则添加（兼容已有表）。"""
    from sqlalchemy import text
    with engine.connect() as conn:
        if engine.dialect.name == "mysql":
            for col, ddl in [
                ("operation_status", "ALTER TABLE asin_performances ADD COLUMN operation_status TINYINT(1) NOT NULL DEFAULT 0"),
                ("ad_check", "ALTER TABLE asin_performances ADD COLUMN ad_check TINYINT(1) NOT NULL DEFAULT 0"),
                ("ad_created_at", "ALTER TABLE asin_performances ADD COLUMN ad_created_at DATETIME NULL"),
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


def _ensure_group_a_operation_columns():
    """若 group_A 表缺少兼容列则添加（兼容已有表）。"""
    from sqlalchemy import text
    with engine.connect() as conn:
        if engine.dialect.name == "mysql":
            old_purchase_col = conn.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'group_A' AND COLUMN_NAME = 'search_query_purchase_count'"
                )
            ).scalar()
            new_cart_col = conn.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'group_A' AND COLUMN_NAME = 'search_query_cart_count'"
                )
            ).scalar()
            if old_purchase_col and not new_cart_col:
                conn.execute(
                    text(
                        "ALTER TABLE group_A CHANGE COLUMN search_query_purchase_count search_query_cart_count BIGINT NULL"
                    )
                )
                conn.commit()
            for col, ddl in [
                ("operation_status", "ALTER TABLE group_A ADD COLUMN operation_status TINYINT(1) NOT NULL DEFAULT 0"),
                ("operated_at", "ALTER TABLE group_A ADD COLUMN operated_at DATETIME NULL"),
                ("child_session_count", "ALTER TABLE group_A ADD COLUMN child_session_count BIGINT NULL"),
                ("search_query_cart_count", "ALTER TABLE group_A ADD COLUMN search_query_cart_count BIGINT NULL"),
                ("search_query_total_click_count", "ALTER TABLE group_A ADD COLUMN search_query_total_click_count BIGINT NULL"),
                ("migrated_to_asin_performances", "ALTER TABLE group_A ADD COLUMN migrated_to_asin_performances TINYINT(1) NOT NULL DEFAULT 0"),
            ]:
                r = conn.execute(
                    text(
                        "SELECT COUNT(*) FROM information_schema.COLUMNS "
                        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'group_A' AND COLUMN_NAME = :col"
                    ),
                    {"col": col},
                ).scalar()
                if r == 0:
                    conn.execute(text(ddl))
                    conn.commit()


def _ensure_listing_tracking_schema():
    """兼容 listing_tracking 旧表结构：修正字段类型/非空约束，并补唯一键。"""
    from sqlalchemy import text
    with engine.connect() as conn:
        if engine.dialect.name != "mysql":
            return
        table_exists = conn.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'listing_tracking'"
            )
        ).scalar()
        if not table_exists:
            return

        batch_id_exists = conn.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'listing_tracking' AND COLUMN_NAME = 'batch_id'"
            )
        ).scalar()
        batch_week_exists = conn.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'listing_tracking' AND COLUMN_NAME = 'batch_week'"
            )
        ).scalar()
        if not batch_id_exists and batch_week_exists:
            conn.execute(
                text(
                    "ALTER TABLE listing_tracking "
                    "CHANGE COLUMN batch_week batch_id INT NOT NULL"
                )
            )
            conn.commit()
        elif not batch_id_exists:
            conn.execute(text("ALTER TABLE listing_tracking ADD COLUMN batch_id INT NOT NULL"))
            conn.commit()

        image_col_exists = conn.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'listing_tracking' AND COLUMN_NAME = 'used_image_model'"
            )
        ).scalar()
        old_image_col_exists = conn.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'listing_tracking' AND COLUMN_NAME = 'user_image_model'"
            )
        ).scalar()
        if not image_col_exists and old_image_col_exists:
            conn.execute(
                text(
                    "ALTER TABLE listing_tracking "
                    "CHANGE COLUMN user_image_model used_image_model VARCHAR(255) NULL"
                )
            )
            conn.commit()
        elif not image_col_exists:
            conn.execute(text("ALTER TABLE listing_tracking ADD COLUMN used_image_model VARCHAR(255) NULL"))
            conn.commit()

        created_at_exists = conn.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'listing_tracking' AND COLUMN_NAME = 'created_at'"
            )
        ).scalar()
        if not created_at_exists:
            conn.execute(text("ALTER TABLE listing_tracking ADD COLUMN created_at DATETIME NULL"))
            conn.commit()

        pid_asin_count_exists = conn.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'listing_tracking' AND COLUMN_NAME = 'pid_asin_count'"
            )
        ).scalar()
        if not pid_asin_count_exists:
            conn.execute(text("ALTER TABLE listing_tracking ADD COLUMN pid_asin_count INT NULL"))
            conn.commit()

        pid_active_asin_count_exists = conn.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'listing_tracking' AND COLUMN_NAME = 'pid_active_asin_count'"
            )
        ).scalar()
        if not pid_active_asin_count_exists:
            conn.execute(text("ALTER TABLE listing_tracking ADD COLUMN pid_active_asin_count INT NULL"))
            conn.commit()

        total_click_exists = conn.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'listing_tracking' AND COLUMN_NAME = 'total_click'"
            )
        ).scalar()
        if not total_click_exists:
            conn.execute(text("ALTER TABLE listing_tracking ADD COLUMN total_click BIGINT NULL"))
            conn.commit()

        session_asin_exists = conn.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'listing_tracking' AND COLUMN_NAME = 'session_asin'"
            )
        ).scalar()
        if not session_asin_exists:
            conn.execute(text("ALTER TABLE listing_tracking ADD COLUMN session_asin VARCHAR(4096) NULL"))
            conn.commit()

        impression_asin_exists = conn.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'listing_tracking' AND COLUMN_NAME = 'impression_asin'"
            )
        ).scalar()
        if not impression_asin_exists:
            conn.execute(text("ALTER TABLE listing_tracking ADD COLUMN impression_asin VARCHAR(4096) NULL"))
            conn.commit()

        for ddl in [
            "ALTER TABLE listing_tracking MODIFY COLUMN batch_id INT NOT NULL",
            "ALTER TABLE listing_tracking MODIFY COLUMN pid BIGINT NOT NULL",
            "ALTER TABLE listing_tracking MODIFY COLUMN pid_asin_count INT NULL",
            "ALTER TABLE listing_tracking MODIFY COLUMN pid_active_asin_count INT NULL",
            "ALTER TABLE listing_tracking MODIFY COLUMN parent_asin VARCHAR(32) NULL",
            "ALTER TABLE listing_tracking MODIFY COLUMN created_at DATETIME NULL",
            "ALTER TABLE listing_tracking MODIFY COLUMN week_no INT NOT NULL",
            "ALTER TABLE listing_tracking MODIFY COLUMN total_impression BIGINT NULL",
            "ALTER TABLE listing_tracking MODIFY COLUMN total_session BIGINT NULL",
            "ALTER TABLE listing_tracking MODIFY COLUMN total_click BIGINT NULL",
            "ALTER TABLE listing_tracking MODIFY COLUMN total_order BIGINT NULL",
            "ALTER TABLE listing_tracking MODIFY COLUMN session_asin VARCHAR(4096) NULL",
            "ALTER TABLE listing_tracking MODIFY COLUMN impression_asin VARCHAR(4096) NULL",
            "ALTER TABLE listing_tracking MODIFY COLUMN used_text_model VARCHAR(255) NULL",
            "ALTER TABLE listing_tracking MODIFY COLUMN store_id INT NOT NULL",
            "ALTER TABLE listing_tracking MODIFY COLUMN used_image_model VARCHAR(255) NULL",
        ]:
            conn.execute(text(ddl))
            conn.commit()

        uq_exists = conn.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'listing_tracking' "
                "AND CONSTRAINT_NAME = 'uq_listing_tracking_batch_pid_week_store'"
            )
        ).scalar()
        if not uq_exists:
            conn.execute(
                text(
                    "ALTER TABLE listing_tracking "
                    "ADD CONSTRAINT uq_listing_tracking_batch_pid_week_store "
                    "UNIQUE KEY (batch_id, pid, week_no, store_id)"
                )
            )
            conn.commit()


def _ensure_listing_tracking_indexes():
    """为 Trend 聚合与过滤补充常用索引（若不存在则创建）。"""
    from sqlalchemy import text

    with engine.connect() as conn:
        if engine.dialect.name != "mysql":
            return
        # information_schema.STATISTICS: 同一索引名在表内唯一
        wanted = [
            ("ix_listing_tracking_week_no", "CREATE INDEX ix_listing_tracking_week_no ON listing_tracking (week_no)"),
            ("ix_listing_tracking_store_week", "CREATE INDEX ix_listing_tracking_store_week ON listing_tracking (store_id, week_no)"),
            ("ix_listing_tracking_batch_week", "CREATE INDEX ix_listing_tracking_batch_week ON listing_tracking (batch_id, week_no)"),
            ("ix_listing_tracking_parent_week", "CREATE INDEX ix_listing_tracking_parent_week ON listing_tracking (parent_asin, week_no)"),
            ("ix_listing_tracking_pid", "CREATE INDEX ix_listing_tracking_pid ON listing_tracking (pid)"),
            ("ix_listing_tracking_created_at", "CREATE INDEX ix_listing_tracking_created_at ON listing_tracking (created_at)"),
            ("ix_listing_tracking_used_text_model", "CREATE INDEX ix_listing_tracking_used_text_model ON listing_tracking (used_text_model)"),
            ("ix_listing_tracking_used_image_model", "CREATE INDEX ix_listing_tracking_used_image_model ON listing_tracking (used_image_model)"),
        ]
        for idx_name, ddl in wanted:
            exists = conn.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.STATISTICS "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'listing_tracking' AND INDEX_NAME = :idx"
                ),
                {"idx": idx_name},
            ).scalar()
            if not exists:
                conn.execute(text(ddl))
                conn.commit()


def init_db():
    """Create all tables. Called on startup. Retries if MySQL not ready (e.g. in Docker)."""
    from app.models import asin_performance, group_a, listing_tracking  # noqa: F401

    max_attempts = 10
    for attempt in range(1, max_attempts + 1):
        try:
            Base.metadata.create_all(bind=engine)
            _ensure_order_id_column()
            _ensure_operation_columns()
            _ensure_group_a_operation_columns()
            _ensure_listing_tracking_schema()
            _ensure_listing_tracking_indexes()
            return
        except OperationalError as e:
            if attempt == max_attempts:
                raise
            time.sleep(2)
            continue
