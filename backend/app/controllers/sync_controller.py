import logging

from fastapi import APIRouter, HTTPException

from app.services.online_sync import sync_from_online_db
from app.sync_run_record import record_sync_run

router = APIRouter(prefix="/api", tags=["sync"])
logger = logging.getLogger(__name__)


@router.post("/sync-from-online")
def trigger_sync_from_online():
    """从 online 数据库执行 SQL，将结果插入到本地 MYSQL_DB_NAME（asin_performances）表，并返回检查结果。"""
    logger.info("Sync from online DB requested")
    try:
        out = sync_from_online_db()
        rows_fetched = out["rows_fetched_from_online"]
        rows_inserted = out["rows_inserted"]
        rows_updated = out.get("rows_updated", 0)
        local_count = out["local_table_count_after"]
        table_name = out["table_name"]
        insert_ok = out["insert_ok"]
        step2_error = out.get("step2_error")

        if rows_fetched == 0:
            msg = "同步完成，但 online 库返回 0 条（请检查 SQL 日期范围或 store_id 是否有数据）。"
        elif step2_error:
            msg = (
                f"Step 1 已完成：已写入 {rows_inserted + rows_updated} 条到表 {table_name}，当前表内共 {local_count} 条。"
                f" Step 2 未执行或失败（仅影响「无订单但有曝光/会话」的子 ASIN），表内已有数据可正常展示。"
            )
        elif insert_ok:
            msg = (
                f"已从 online 查询 {rows_fetched} 条，插入 {rows_inserted} 条、更新 {rows_updated} 条到表 {table_name}，"
                f"当前表内共 {local_count} 条。"
            )
        else:
            msg = (
                f"警告：从 online 查询 {rows_fetched} 条，插入 {rows_inserted} 条、更新 {rows_updated} 条，"
                f"插入后表 {table_name} 行数为 {local_count}，请检查是否一致。"
            )

        logger.info(
            "Sync completed: fetched=%s, inserted=%s, updated=%s, table=%s, insert_ok=%s, step2_error=%s",
            rows_fetched, rows_inserted, rows_updated, table_name, insert_ok, bool(step2_error),
        )
        record_sync_run()
        return {
            "status": "ok",
            "rows_synced": rows_inserted + rows_updated,
            "message": msg,
            "check": {
                "rows_fetched_from_online": rows_fetched,
                "rows_inserted": rows_inserted,
                "rows_updated": rows_updated,
                "local_table_count_after": local_count,
                "table_name": table_name,
                "insert_ok": insert_ok,
                "step2_error": step2_error,
                "message": msg,
            },
        }
    except ValueError as e:
        logger.warning("Sync failed (config/validation): %s", e)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Sync failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
