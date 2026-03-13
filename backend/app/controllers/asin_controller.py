import csv
import logging
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from io import StringIO
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import func, text

from app.online_engine import get_online_engine

from app.config import settings
from app.database import get_db
from app.models import AsinPerformance
from app.views import (
    AsinPerformanceCreate,
    AsinPerformanceResponse,
    AsinPerformanceUpdate,
    SummaryRow,
    SummaryStatsResponse,
    WeekStatsRow,
    DetailChildRow,
    DetailResponse,
    SearchQueryRow,
    GroupFRow,
    GroupFResponse,
)
from app.services.group_f_spark import (
    get_group_f,
    compute_scan_weeks_list_for_api,
    _group_f_current_week_no,
    _group_f_to_mysql_week_no,
)

router = APIRouter(prefix="/api/asin-performances", tags=["asin-performances"])
logger = logging.getLogger(__name__)
_query_refresh_lock = threading.Lock()

# Group F 槽位：同一时刻只允许一个请求执行；可查询占用者与时长，支持手动释放
_group_f_slot = {"started_at": None, "request_id": None}
_group_f_slot_lock = threading.Lock()
_GROUP_F_STALE_SEC = 25 * 60   # 超过此时长视为过期，允许新请求
_GROUP_F_STUCK_SEC = 20 * 60   # 超过此时长在 status 中标记为 is_stuck


def _fetch_listing_meta_for_export(rows: list[AsinPerformance]) -> dict:
    """
    通过 (child_asin, store_id) 从 online 库补齐 pid/title/search_term。
    返回 key=(child_asin, store_id) -> {"pid": ..., "title": ..., "search_term": ...}
    """
    key_pairs = {
        (r.child_asin, int(r.store_id))
        for r in rows
        if r.child_asin and r.store_id is not None
    }
    if not key_pairs or not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        return {}

    store_ids = sorted({sid for _, sid in key_pairs})
    asins = sorted({asin for asin, _ in key_pairs})
    if not store_ids or not asins:
        return {}

    # 兼容不同环境中表名/字段名差异
    table_candidates = [
        ("ai_generated_amazon_listings", "search_terms"),
    ]
    online_engine = get_online_engine()
    for agal_table, search_col in table_candidates:
            meta_map = {}
            try:
                with online_engine.connect() as conn:
                    store_ph = ", ".join([f":s{i}" for i in range(len(store_ids))])
                    store_params = {f"s{i}": sid for i, sid in enumerate(store_ids)}
                    batch_size = 300
                    for i in range(0, len(asins), batch_size):
                        asin_batch = asins[i:i + batch_size]
                        asin_ph = ", ".join([f":a{j}" for j in range(len(asin_batch))])
                        params = dict(store_params)
                        for j, asin in enumerate(asin_batch):
                            params[f"a{j}"] = asin
                        q = text(
                            f"SELECT al.asin, al.store_id, MIN(al.pid) AS pid, "
                            f"MAX(agal.title) AS title, MAX(agal.{search_col}) AS search_term "
                            f"FROM amazon_listing al "
                            f"LEFT JOIN {agal_table} agal ON al.pid = agal.id "
                            f"WHERE al.store_id IN ({store_ph}) AND al.asin IN ({asin_ph}) "
                            f"GROUP BY al.asin, al.store_id"
                        )
                        result = conn.execute(q, params).fetchall()
                        for r in result:
                            key = (r[0], int(r[1]) if r[1] is not None else None)
                            if key[0] is None or key[1] is None:
                                continue
                            meta_map[key] = {
                                "pid": r[2],
                                "title": r[3],
                                "search_term": r[4],
                            }
                return meta_map
            except Exception as e:
                logger.warning("Export listing meta query failed with %s.%s: %s", agal_table, search_col, e)
                continue
    return {}


@router.get("/stats")
def get_table_stats(db: Session = Depends(get_db)):
    """返回 asin_performances 表总行数，用于检查是否已同步到数据。"""
    try:
        count = db.query(AsinPerformance).count()
        return {"count": count, "table": "asin_performances"}
    except Exception as e:
        logger.exception("Stats query failed: %s", e)
        raise HTTPException(status_code=500, detail=f"查询表行数失败（表可能尚未创建）: {e!s}")


@router.get("/summary-stats", response_model=SummaryStatsResponse)
def get_summary_stats(db: Session = Depends(get_db)):
    """按 week_no 统计：仅返回最新一周的数据（该 week_no 下父 ASIN 个数、总订单数）。"""
    try:
        latest_week = db.query(func.max(AsinPerformance.week_no)).scalar()
        if latest_week is None:
            return SummaryStatsResponse(by_week=[])
        sub = (
            db.query(
                AsinPerformance.parent_asin,
                AsinPerformance.week_no,
                AsinPerformance.store_id,
                func.max(AsinPerformance.parent_order_total).label("pot"),
            )
            .filter(
                AsinPerformance.parent_asin.isnot(None),
                AsinPerformance.parent_asin != "",
                AsinPerformance.week_no == latest_week,
            )
            .group_by(AsinPerformance.parent_asin, AsinPerformance.week_no, AsinPerformance.store_id)
            .having(func.max(AsinPerformance.parent_order_total) > 0)
            .subquery()
        )
        rows = (
            db.query(
                sub.c.week_no,
                func.count().label("parent_asin_count"),
                func.sum(sub.c.pot).label("total_orders"),
            )
            .group_by(sub.c.week_no)
            .all()
        )
        by_week = []
        for r in rows:
            week_no = r[0]
            if week_no is not None:
                try:
                    week_no = int(week_no)
                except (TypeError, ValueError):
                    week_no = None
            total_orders = r[2]
            if total_orders is not None and not isinstance(total_orders, Decimal):
                try:
                    total_orders = Decimal(str(total_orders))
                except Exception:
                    total_orders = None
            by_week.append(
                WeekStatsRow(
                    week_no=week_no,
                    parent_asin_count=int(r[1]) if r[1] is not None else 0,
                    total_orders=total_orders,
                )
            )
        return SummaryStatsResponse(by_week=by_week)
    except Exception as e:
        logger.exception("Summary-stats query failed: %s", e)
        raise HTTPException(status_code=500, detail=f"查询 week_no 统计失败: {e!s}")


@router.get("", response_model=List[AsinPerformanceResponse])
def list_asin_performances(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    items = db.query(AsinPerformance).offset(skip).limit(limit).all()
    return items


@router.get("/weeks", response_model=List[int])
def list_weeks(db: Session = Depends(get_db)):
    """返回表中存在的 week_no（降序，去重，过滤空值）。"""
    rows = (
        db.query(AsinPerformance.week_no)
        .filter(AsinPerformance.week_no.isnot(None))
        .distinct()
        .order_by(AsinPerformance.week_no.desc())
        .all()
    )
    out: List[int] = []
    for r in rows:
        try:
            out.append(int(r[0]))
        except (TypeError, ValueError):
            continue
    return out


@router.get("/summary", response_model=List[SummaryRow])
def list_summary(
    week_no: Optional[int] = Query(None, description="Week number, default latest week"),
    db: Session = Depends(get_db),
):
    """按 parent_asin + week_no + store_id 去重，返回指定周（默认最新一周）且有订单的父 ASIN。"""
    try:
        selected_week = week_no if week_no is not None else db.query(func.max(AsinPerformance.week_no)).scalar()
        if selected_week is None:
            return []
        sub = (
            db.query(
                AsinPerformance.parent_asin,
                AsinPerformance.week_no,
                AsinPerformance.store_id,
                func.max(AsinPerformance.parent_order_total).label("parent_order_total"),
                func.max(AsinPerformance.parent_asin_create_at).label("parent_asin_create_at"),
                func.max(AsinPerformance.operation_status).label("operation_status"),
                func.max(AsinPerformance.operated_at).label("operated_at"),
                func.max(AsinPerformance.checked_status).label("checked_status"),
                func.max(AsinPerformance.checked_at).label("checked_at"),
            )
            .filter(
                AsinPerformance.parent_asin.isnot(None),
                AsinPerformance.parent_asin != "",
                AsinPerformance.week_no == selected_week,
            )
            .group_by(AsinPerformance.parent_asin, AsinPerformance.week_no, AsinPerformance.store_id)
            .having(func.max(AsinPerformance.parent_order_total) > 0)
            .order_by(func.max(AsinPerformance.parent_order_total).desc())
            .subquery()
        )
        rows = (
            db.query(
                sub.c.parent_asin,
                sub.c.parent_asin_create_at,
                sub.c.parent_order_total,
                sub.c.week_no,
                sub.c.store_id,
                sub.c.operation_status,
                sub.c.operated_at,
                sub.c.checked_status,
                sub.c.checked_at,
            )
            .all()
        )
        out = []
        for r in rows:
            week_no = r[3]
            if week_no is not None:
                try:
                    week_no = int(week_no)
                except (TypeError, ValueError):
                    week_no = None
            store_id = r[4]
            if store_id is not None:
                try:
                    store_id = int(store_id)
                except (TypeError, ValueError):
                    store_id = None
            parent_order_total = r[2]
            if parent_order_total is not None and not isinstance(parent_order_total, Decimal):
                try:
                    parent_order_total = Decimal(str(parent_order_total))
                except Exception:
                    parent_order_total = None
            op_status = r[5]
            if op_status is not None and not isinstance(op_status, bool):
                op_status = bool(int(op_status)) if op_status is not None else False
            out.append(
                SummaryRow(
                    parent_asin=r[0] if r[0] is not None else None,
                    parent_asin_create_at=r[1],
                    parent_order_total=parent_order_total,
                    week_no=week_no,
                    store_id=store_id,
                    operation_status=op_status,
                    operated_at=r[6],
                    checked_status=r[7],
                    checked_at=r[8],
                )
            )
        return out
    except Exception as e:
        logger.exception("Summary query failed: %s", e)
        raise HTTPException(status_code=500, detail=f"查询 summary 失败: {e!s}")


class OperateBody(BaseModel):
    parent_asin: str
    week_no: int | str


@router.post("/operate")
def operate_by_parent_week(
    body: OperateBody,
    db: Session = Depends(get_db),
):
    """按 parent_asin 和 week_no 将符合条件的所有记录的 operation_status 置为 True，operated_at 置为当前时间。"""
    parent_asin = (body.parent_asin or "").strip()
    if parent_asin == "":
        raise HTTPException(status_code=400, detail="parent_asin 不能为空")
    # 接口兼容 "202609" / "202,609" 两种格式
    week_raw = str(body.week_no).strip().replace(",", "")
    if not week_raw.isdigit():
        raise HTTPException(status_code=400, detail="week_no 格式不合法")
    week_no = int(week_raw)
    # 存储为 UTC+8（Asia/Shanghai）本地时间
    now = datetime.now(timezone(timedelta(hours=8))).replace(tzinfo=None)
    n = (
        db.query(AsinPerformance)
        .filter(
            func.trim(AsinPerformance.parent_asin) == parent_asin,
            AsinPerformance.week_no == week_no,
        )
        .update(
            {"operation_status": True, "operated_at": now},
            synchronize_session=False,
        )
    )
    db.commit()
    return {"updated": n, "parent_asin": parent_asin, "week_no": week_no, "operated_at": now.isoformat()}


class RefreshQueryStatusBody(BaseModel):
    week_no: int | str


@router.post("/query-status/refresh")
def refresh_query_status(
    body: RefreshQueryStatusBody,
    db: Session = Depends(get_db),
):
    """
    轮询查询状态：
    - 针对指定 week_no 下各 (parent_asin, store_id)；
    - 仅检查该父 ASIN 下 impression>0 的子 ASIN；
    - 若子 ASIN 在 online.amazon_search 中状态全部为 3，则标记 completed；
    - completed 的父 ASIN 后续跳过；
    - 未 completed 的父 ASIN 每 8 分钟最多检查一次。
    """
    week_raw = str(body.week_no).strip().replace(",", "")
    if not week_raw.isdigit():
        raise HTTPException(status_code=400, detail="week_no 格式不合法")
    week_no = int(week_raw)

    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        raise HTTPException(status_code=400, detail="online_db 配置缺失，无法刷新查询状态")

    now = datetime.now(timezone(timedelta(hours=8))).replace(tzinfo=None)
    threshold = now - timedelta(minutes=8)

    groups = (
        db.query(
            AsinPerformance.parent_asin,
            AsinPerformance.store_id,
            func.max(AsinPerformance.checked_status).label("checked_status"),
            func.max(AsinPerformance.checked_at).label("checked_at"),
        )
        .filter(
            AsinPerformance.week_no == week_no,
            AsinPerformance.parent_asin.isnot(None),
            AsinPerformance.parent_asin != "",
        )
        .group_by(AsinPerformance.parent_asin, AsinPerformance.store_id)
        .all()
    )

    checked_groups = 0
    completed_groups = 0
    skipped_completed = 0
    skipped_by_interval = 0
    lock_acquired = _query_refresh_lock.acquire(blocking=False)
    if not lock_acquired:
        return {
            "week_no": week_no,
            "checked_groups": 0,
            "completed_groups": 0,
            "skipped_completed": 0,
            "skipped_by_interval": 0,
            "checked_at": now.isoformat(),
            "message": "refresh already running, skip this request",
        }
    online_engine = get_online_engine()
    try:
        with online_engine.connect() as conn:
            for pa, sid, q_status, checked_at in groups:
                if str(q_status or "").lower() == "completed":
                    skipped_completed += 1
                    continue
                if checked_at is not None and checked_at > threshold:
                    skipped_by_interval += 1
                    continue

                child_rows = (
                    db.query(AsinPerformance.child_asin)
                    .filter(
                        AsinPerformance.week_no == week_no,
                        AsinPerformance.parent_asin == pa,
                        AsinPerformance.store_id == sid,
                        AsinPerformance.child_impression_count > 0,
                        AsinPerformance.child_asin.isnot(None),
                        AsinPerformance.child_asin != "",
                    )
                    .distinct()
                    .all()
                )
                child_asins = [r[0] for r in child_rows if r[0]]
                if not child_asins:
                    # 无 impression 子 ASIN，保持 pending，只记录检查时间
                    db.query(AsinPerformance).filter(
                        AsinPerformance.week_no == week_no,
                        AsinPerformance.parent_asin == pa,
                        AsinPerformance.store_id == sid,
                    ).update(
                        {"checked_status": "pending", "checked_at": now},
                        synchronize_session=False,
                    )
                    checked_groups += 1
                    continue

                status_map = {}
                batch_size = 200
                for i in range(0, len(child_asins), batch_size):
                    batch = child_asins[i:i + batch_size]
                    asin_ph = ", ".join([f":a{j}" for j in range(len(batch))])
                    params = {"sid": sid, "week_no": str(week_no)}
                    for j, asin in enumerate(batch):
                        params[f"a{j}"] = asin
                    rows = conn.execute(
                        text(
                            f"SELECT asin, status FROM amazon_search "
                            f"WHERE store_id = :sid AND week_no = :week_no AND asin IN ({asin_ph})"
                        ),
                        params,
                    ).fetchall()
                    for r in rows:
                        status_map[str(r[0])] = r[1]

                done = all((asin in status_map and int(status_map[asin]) == 3) for asin in child_asins)
                new_status = "completed" if done else "pending"
                db.query(AsinPerformance).filter(
                    AsinPerformance.week_no == week_no,
                    AsinPerformance.parent_asin == pa,
                    AsinPerformance.store_id == sid,
                ).update(
                    {"checked_status": new_status, "checked_at": now},
                    synchronize_session=False,
                )
                checked_groups += 1
                if done:
                    completed_groups += 1
        db.commit()
    finally:
        if lock_acquired:
            _query_refresh_lock.release()

    return {
        "week_no": week_no,
        "checked_groups": checked_groups,
        "completed_groups": completed_groups,
        "skipped_completed": skipped_completed,
        "skipped_by_interval": skipped_by_interval,
        "checked_at": now.isoformat(),
    }


@router.get("/db-status")
def get_online_db_status():
    """
    查询 online 库连接状态：Threads_connected、Max_used_connections、当前运行中的查询数。
    用于排查 Group F 等长查询时的连接数问题。
    """
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        return {"error": "online_db 未配置", "threads_connected": None, "max_used_connections": None}
    try:
        engine = get_online_engine()
        with engine.connect() as conn:
            threads = conn.execute(text("SHOW GLOBAL STATUS LIKE 'Threads_connected'")).fetchone()
            max_used = conn.execute(text("SHOW GLOBAL STATUS LIKE 'Max_used_connections'")).fetchone()
            procs = conn.execute(text("SHOW PROCESSLIST")).fetchall()
        threads_connected = int(threads[1]) if threads and len(threads) > 1 else None
        max_used_connections = int(max_used[1]) if max_used and len(max_used) > 1 else None
        running = sum(1 for p in procs if p and len(p) >= 5 and str(p[4]).strip().lower() in ("query", "execute"))
        return {
            "threads_connected": threads_connected,
            "max_used_connections": max_used_connections,
            "processlist_count": len(procs),
            "running_queries": running,
        }
    except Exception as e:
        logger.exception("db-status failed: %s", e)
        return {"error": str(e), "threads_connected": None, "max_used_connections": None}


def _group_f_acquire_slot():
    """若当前无占用或已过期则占用槽位并返回 (request_id, True)，否则返回 (None, False)。"""
    with _group_f_slot_lock:
        now = time.time()
        if _group_f_slot["started_at"] is not None:
            age = now - _group_f_slot["started_at"]
            if age < _GROUP_F_STALE_SEC:
                return None, False
        rid = uuid.uuid4().hex[:12]
        _group_f_slot["started_at"] = now
        _group_f_slot["request_id"] = rid
        return rid, True


def _group_f_release_slot(request_id: str):
    """仅当槽位属于本 request_id 时清空。"""
    with _group_f_slot_lock:
        if _group_f_slot.get("request_id") == request_id:
            _group_f_slot["started_at"] = None
            _group_f_slot["request_id"] = None


@router.get("/group-f/status")
def get_group_f_lock_status():
    """
    查询 Group F 锁状态：谁占用、从何时开始、已运行多久、是否卡住。
    用于排查 429 与长时间无响应。
    """
    with _group_f_slot_lock:
        started_at = _group_f_slot.get("started_at")
        request_id = _group_f_slot.get("request_id")
    if started_at is None:
        return {
            "lock_held": False,
            "request_id": None,
            "started_at": None,
            "duration_seconds": None,
            "is_stuck": False,
            "message": "当前无占用",
        }
    duration = time.time() - started_at
    started_iso = datetime.fromtimestamp(started_at, tz=timezone.utc).isoformat()
    return {
        "lock_held": True,
        "request_id": request_id,
        "started_at": started_iso,
        "duration_seconds": round(duration, 1),
        "is_stuck": duration > _GROUP_F_STUCK_SEC,
        "message": "有请求在执行中" + ("（已超时，可能卡住）" if duration > _GROUP_F_STUCK_SEC else ""),
    }


@router.post("/group-f/release-lock")
def release_group_f_lock():
    """
    手动释放 Group F 槽位，允许新请求进入。
    注意：原请求若仍在执行会继续在后台跑直至完成或超时，不会中断。
    """
    with _group_f_slot_lock:
        had = _group_f_slot["started_at"] is not None
        req_id = _group_f_slot.get("request_id")
        _group_f_slot["started_at"] = None
        _group_f_slot["request_id"] = None
    logger.info("[Group F] 手动释放槽位，原 request_id=%s", req_id)
    return {
        "released": True,
        "had_lock": had,
        "previous_request_id": req_id,
        "message": "槽位已释放，新请求可发起" if had else "槽位本就空闲",
    }


@router.get("/group-f", response_model=GroupFResponse)
def get_group_f_candidates(
    scan_weeks: int = Query(4, ge=1, le=12, description="扫描周数（指定周为空时生效）"),
    week_nos: Optional[List[int]] = Query(None, description="指定周，如 202607，可多值；有值时忽略扫描周数"),
):
    """
    Group F：查询指定周或按扫描周数计算的周内创建的父 ASIN 状态。
    指定周：week_nos=202607 或 week_nos=202607&week_nos=202606；留空则按 scan_weeks 计算。
    """
    logger.info("[Group F] 请求已到达: scan_weeks=%s, week_nos=%s", scan_weeks, week_nos)
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        raise HTTPException(status_code=400, detail="online_db 配置缺失，无法查询 Group F 数据")

    request_id, acquired = _group_f_acquire_slot()
    if not acquired:
        logger.warning("[Group F] 请求被拒绝：已有查询在执行中，返回 429")
        raise HTTPException(
            status_code=429,
            detail="Group F 查询已在进行中，请等待完成或调用 POST /group-f/release-lock 后重试",
        )
    logger.info("[Group F] 已占用槽位 request_id=%s，开始执行", request_id)

    online_engine = get_online_engine()
    try:
        if week_nos:
            scan_weeks_list = [int(w) for w in week_nos]
        else:
            with online_engine.connect() as conn:
                search_max = conn.execute(
                    text(
                        "SELECT MAX(week_no) FROM amazon_search_data"
                        " WHERE store_id IN (1,7,12,25) AND week_no IS NOT NULL"
                    )
                ).scalar()
                traffic_max = conn.execute(
                    text(
                        "SELECT MAX(week_no) FROM amazon_sales_traffic"
                        " WHERE store_id IN (1,7,12,25) AND week_no IS NOT NULL"
                    )
                ).scalar()
            cands = [int(w) for w in (search_max, traffic_max) if w is not None]
            if not cands:
                return GroupFResponse(weeks=[], rows=[])
            db_max = max(cands)
            current_week = max(db_max, _group_f_current_week_no())
            scan_weeks_list = compute_scan_weeks_list_for_api(current_week, scan_weeks)
        mysql_weeks = [_group_f_to_mysql_week_no(w) for w in scan_weeks_list]
        logger.info("[Group F] request_id=%s 创建周（Group F）=%s，mysql_weeks=%s，调用 get_group_f...", request_id, scan_weeks_list, mysql_weeks)
        rows = get_group_f(mysql_weeks)
        logger.info("[Group F] request_id=%s 查询成功，返回 %d 条", request_id, len(rows))
        return GroupFResponse(
            weeks=scan_weeks_list,
            rows=[
                GroupFRow(
                    parent_asin=r[0],
                    created_at=r[1],
                    store_id=int(r[2]) if r[2] is not None else None,
                    impression_count_asin=str(r[3]) if r[3] else None,
                    order_asin=str(r[4]) if r[4] else None,
                    sessions_asin=str(r[5]) if r[5] else None,
                )
                for r in rows
            ],
        )
    except Exception as e:
        logger.exception("[Group F] request_id=%s 查询失败: %s", request_id, e)
        raise HTTPException(status_code=500, detail=f"查询 Group F 失败: {e!s}")
    finally:
        _group_f_release_slot(request_id)
        logger.info("[Group F] request_id=%s 槽位已释放", request_id)


@router.get("/export")
def export_week_data(
    week_no: int = Query(..., description="Week number"),
    parent_asins: Optional[List[str]] = Query(None, description="Optional parent ASIN filters"),
    db: Session = Depends(get_db),
):
    """下载指定 week_no 的 asin_performances 全量数据（CSV）。"""
    q = db.query(AsinPerformance).filter(AsinPerformance.week_no == week_no)
    if parent_asins:
        normalized = [x.strip() for x in parent_asins if x and x.strip()]
        if normalized:
            q = q.filter(AsinPerformance.parent_asin.in_(normalized))
    rows = q.order_by(
        AsinPerformance.parent_asin,
        AsinPerformance.child_asin,
        AsinPerformance.store_id,
        AsinPerformance.id,
    ).all()
    if not rows:
        raise HTTPException(status_code=404, detail=f"week_no={week_no} 无可导出数据")

    headers = [
        "id",
        "store_id",
        "parent_asin",
        "child_asin",
        "pid",
        "search_term",
        "title",
        "parent_asin_create_at",
        "parent_order_total",
        "order_num",
        "order_id",
        "week_no",
        "child_impression_count",
        "child_session_count",
        "search_query",
        "search_query_volume",
        "search_query_impression_count",
        "search_query_purchase_count",
        "search_query_total_impression",
        "search_query_click_count",
        "search_query_total_click",
    ]
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)
    meta_map = _fetch_listing_meta_for_export(rows)
    for r in rows:
        meta = meta_map.get((r.child_asin, int(r.store_id) if r.store_id is not None else None), {})
        writer.writerow([
            r.id,
            r.store_id,
            r.parent_asin,
            r.child_asin,
            meta.get("pid"),
            meta.get("search_term"),
            meta.get("title"),
            r.parent_asin_create_at.isoformat() if r.parent_asin_create_at else None,
            r.parent_order_total,
            r.order_num,
            r.order_id,
            r.week_no,
            r.child_impression_count,
            r.child_session_count,
            r.search_query,
            r.search_query_volume,
            r.search_query_impression_count,
            r.search_query_purchase_count,
            r.search_query_total_impression,
            r.search_query_click_count,
            r.search_query_total_click,
        ])
    output.seek(0)
    filename = f"asin_performances_week_{week_no}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/detail", response_model=DetailResponse)
def list_detail_by_parent_week(
    parent_asin: str = Query(..., description="Parent ASIN"),
    week_no: int = Query(..., description="Week number"),
    store_id: Optional[int] = Query(None, description="Store ID (optional filter)"),
    db: Session = Depends(get_db),
):
    """按 parent_asin + week_no（+ 可选 store_id）查询，按 child_asin 聚合，返回父 ASIN/总订单数及子 ASIN 列表与 search_query 明细。"""
    q = db.query(AsinPerformance).filter(
        AsinPerformance.parent_asin == parent_asin,
        AsinPerformance.week_no == week_no,
    )
    if store_id is not None:
        q = q.filter(AsinPerformance.store_id == store_id)
    rows = (
        q
        .order_by(AsinPerformance.child_asin, AsinPerformance.search_query)
        .all()
    )
    if not rows:
        return DetailResponse(parent_asin=parent_asin, week_no=week_no, children=[])

    parent_order_total = rows[0].parent_order_total
    # 按 child_asin 分组，保留每组第一条的 child_asin/impression/session，以及 search_queries 列表
    by_child: dict = {}
    for r in rows:
        key = (r.child_asin or "", r.child_impression_count, r.child_session_count)
        if key not in by_child:
            by_child[key] = {
                "child_asin": r.child_asin,
                "child_impression_count": r.child_impression_count,
                "child_session_count": r.child_session_count,
                "order_num": r.order_num,
                "order_id": r.order_id,
                "search_queries": [],
            }
        by_child[key]["search_queries"].append(
            SearchQueryRow(
                search_query=r.search_query,
                search_query_volume=r.search_query_volume,
                search_query_impression_count=r.search_query_impression_count,
                search_query_total_impression=r.search_query_total_impression,
                search_query_click_count=r.search_query_click_count,
                search_query_total_click=r.search_query_total_click,
                search_query_purchase_count=r.search_query_purchase_count,
            )
        )
    def _sort_key(row: SearchQueryRow):
        return (
            -(row.search_query_impression_count or 0),
            -(row.search_query_purchase_count or 0),
            -(row.search_query_volume or 0),
        )

    children = []
    for group in by_child.values():
        sorted_queries = sorted(group["search_queries"], key=_sort_key)
        children.append(
            DetailChildRow(
                child_asin=group["child_asin"],
                child_impression_count=group["child_impression_count"],
                child_session_count=group["child_session_count"],
                order_num=group.get("order_num"),
                order_id=group.get("order_id"),
                search_queries=sorted_queries,
            )
        )
    # 弹窗中按 child_impression_count 降序、child_session_count 降序
    children.sort(key=lambda c: (-(c.child_impression_count or 0), -(c.child_session_count or 0)))
    return DetailResponse(
        parent_asin=parent_asin,
        parent_order_total=parent_order_total,
        week_no=week_no,
        children=children,
    )


@router.get("/{item_id}", response_model=AsinPerformanceResponse)
def get_asin_performance(item_id: int, db: Session = Depends(get_db)):
    item = db.query(AsinPerformance).filter(AsinPerformance.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Not found")
    return item


@router.post("", response_model=AsinPerformanceResponse, status_code=201)
def create_asin_performance(
    payload: AsinPerformanceCreate,
    db: Session = Depends(get_db),
):
    item = AsinPerformance(**payload.model_dump())
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


@router.patch("/{item_id}", response_model=AsinPerformanceResponse)
def update_asin_performance(
    item_id: int,
    payload: AsinPerformanceUpdate,
    db: Session = Depends(get_db),
):
    item = db.query(AsinPerformance).filter(AsinPerformance.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Not found")
    data = payload.model_dump(exclude_unset=True)
    for k, v in data.items():
        setattr(item, k, v)
    db.commit()
    db.refresh(item)
    return item


@router.delete("/{item_id}", status_code=204)
def delete_asin_performance(item_id: int, db: Session = Depends(get_db)):
    item = db.query(AsinPerformance).filter(AsinPerformance.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Not found")
    db.delete(item)
    db.commit()
    return None
