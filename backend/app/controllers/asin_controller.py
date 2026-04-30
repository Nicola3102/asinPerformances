import csv
import logging
import re
import threading
import time
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import func, literal_column, or_, text, tuple_
from sqlalchemy.exc import OperationalError

from app.online_engine import get_online_reporting_engine

from app.config import settings
from app.database import get_db
from app.models import AsinPerformance, GroupA, ListingTracking
from app.views import (
    AsinPerformanceCreate,
    AsinPerformanceResponse,
    AsinPerformanceUpdate,
    SummaryRow,
    SummaryRowConsolidated,
    SummaryStatsResponse,
    WeekStatsRow,
    DetailChildRow,
    DetailResponse,
    SearchQueryRow,
    GroupFRow,
    GroupFResponse,
    GroupASummaryRow,
    GroupASummaryResponse,
    GroupADetailChildRow,
    GroupADetailResponse,
    GroupAOperateBody,
    MonitorParentItem,
    MonitorTrackRow,
    MonitorWeekStatus,
    MonitorTrackResponse,
    TrendBatchOption,
    TrendFilterOptions,
    TrendWeekPoint,
    TrendResponse,
)
from app.services.group_f_spark import (
    get_group_f,
    compute_scan_weeks_list_for_api,
    _group_f_current_week_no,
    _group_f_to_mysql_week_no,
)
from app.services.groupA_impression import (
    sync_group_a_impression,
    _get_sync_date_range,
    _date_to_week_no as _group_a_date_to_week_no,
)
from app.services.auto_monitor import check_parent_store_week_completed, get_parent_week_status_details

router = APIRouter(prefix="/api/asin-performances", tags=["asin-performances"])
logger = logging.getLogger(__name__)
_query_refresh_lock = threading.Lock()

# Group F 槽位：同一时刻只允许一个请求执行；可查询占用者与时长，支持手动释放
_group_f_slot = {"started_at": None, "request_id": None}
_group_f_slot_lock = threading.Lock()
_GROUP_F_STALE_SEC = 25 * 60   # 超过此时长视为过期，允许新请求
_GROUP_F_STUCK_SEC = 20 * 60   # 超过此时长在 status 中标记为 is_stuck

# Trend: filter options（store/batch/used_model/batch_title）全表 distinct + online title 查询较慢，做短 TTL 缓存
_trend_filter_cache_lock = threading.Lock()
_trend_filter_cache: dict = {"ts": 0.0, "value": None}
_TREND_FILTER_CACHE_TTL_SEC = 300.0


def _listing_tracking_week_start(d: date) -> date:
    days_since_sunday = (d.weekday() + 1) % 7
    return d - timedelta(days=days_since_sunday)


def _listing_tracking_week_no(d: date) -> int:
    # 与 services/listing_tracking.py 保持一致：周日到周六为一周，week_no 取该周周六所在 ISO 周。
    week_end = _listing_tracking_week_start(d) + timedelta(days=6)
    iso_year, iso_week, _ = week_end.isocalendar()
    return int(f"{iso_year}{iso_week:02d}")


def _normalize_group_a_sync_result(result: dict) -> dict:
    """Normalize Group A sync result keys for API consumers."""
    normalized = dict(result or {})
    if "skipped_parents_with_orders" not in normalized and "skipped_parents_existing" in normalized:
        normalized["skipped_parents_with_orders"] = normalized.pop("skipped_parents_existing")
    else:
        normalized.pop("skipped_parents_existing", None)
    return normalized


def _fetch_listing_meta_for_export(rows) -> dict:
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
    online_engine = get_online_reporting_engine()
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
                func.max(AsinPerformance.ad_check).label("ad_check"),
                func.max(AsinPerformance.ad_created_at).label("ad_created_at"),
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
                sub.c.ad_check,
                sub.c.ad_created_at,
                sub.c.operated_at,
                sub.c.checked_status,
                sub.c.checked_at,
            )
            .all()
        )
        previous_ad_rows = (
            db.query(
                AsinPerformance.parent_asin,
                func.max(AsinPerformance.ad_created_at).label("last_ad_created_at"),
            )
            .filter(
                AsinPerformance.parent_asin.isnot(None),
                AsinPerformance.parent_asin != "",
                AsinPerformance.week_no < selected_week,
                AsinPerformance.ad_check == True,
                AsinPerformance.ad_created_at.isnot(None),
            )
            .group_by(AsinPerformance.parent_asin)
            .all()
        )
        previous_operated_rows = (
            db.query(
                AsinPerformance.parent_asin,
                func.max(AsinPerformance.operated_at).label("last_operated_at"),
            )
            .filter(
                AsinPerformance.parent_asin.isnot(None),
                AsinPerformance.parent_asin != "",
                AsinPerformance.week_no < selected_week,
                AsinPerformance.operation_status == True,
                AsinPerformance.operated_at.isnot(None),
            )
            .group_by(AsinPerformance.parent_asin)
            .all()
        )
        previous_ad_map = {r[0]: r[1] for r in previous_ad_rows if r[0]}
        previous_operated_map = {r[0]: r[1] for r in previous_operated_rows if r[0]}
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
            ad_check = r[6]
            if ad_check is not None and not isinstance(ad_check, bool):
                ad_check = bool(int(ad_check)) if ad_check is not None else False
            out.append(
                SummaryRow(
                    parent_asin=r[0] if r[0] is not None else None,
                    parent_asin_create_at=r[1],
                    parent_order_total=parent_order_total,
                    week_no=week_no,
                    store_id=store_id,
                    operation_status=op_status,
                    last_operated_at=previous_operated_map.get(r[0]),
                    ad_check=ad_check,
                    ad_created_at=r[7],
                    last_ad_created_at=previous_ad_map.get(r[0]),
                    operated_at=r[8],
                    checked_status=r[9],
                    checked_at=r[10],
                )
            )
        return out
    except Exception as e:
        logger.exception("Summary query failed: %s", e)
        raise HTTPException(status_code=500, detail=f"查询 summary 失败: {e!s}")


@router.get("/summary/consolidated", response_model=List[SummaryRowConsolidated])
def list_summary_consolidated(
    week_no: Optional[int] = Query(None, description="Week number, default latest week"),
    db: Session = Depends(get_db),
):
    """按 parent_asin + week_no 汇总：同一父 ASIN 同周下多 store 合并为一行，parent_order_total 为各 store 之和，store_ids 罗列有订单的 store，child_asins_with_orders 罗列有订单的子 ASIN。"""
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
                func.max(AsinPerformance.ad_check).label("ad_check"),
                func.max(AsinPerformance.ad_created_at).label("ad_created_at"),
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
                sub.c.ad_check,
                sub.c.ad_created_at,
                sub.c.operated_at,
                sub.c.checked_status,
                sub.c.checked_at,
            )
            .order_by(sub.c.parent_order_total.desc())
            .all()
        )
        previous_ad_rows = (
            db.query(
                AsinPerformance.parent_asin,
                func.max(AsinPerformance.ad_created_at).label("last_ad_created_at"),
            )
            .filter(
                AsinPerformance.parent_asin.isnot(None),
                AsinPerformance.parent_asin != "",
                AsinPerformance.week_no < selected_week,
                AsinPerformance.ad_check == True,
                AsinPerformance.ad_created_at.isnot(None),
            )
            .group_by(AsinPerformance.parent_asin)
            .all()
        )
        previous_operated_rows = (
            db.query(
                AsinPerformance.parent_asin,
                func.max(AsinPerformance.operated_at).label("last_operated_at"),
            )
            .filter(
                AsinPerformance.parent_asin.isnot(None),
                AsinPerformance.parent_asin != "",
                AsinPerformance.week_no < selected_week,
                AsinPerformance.operation_status == True,
                AsinPerformance.operated_at.isnot(None),
            )
            .group_by(AsinPerformance.parent_asin)
            .all()
        )
        previous_ad_map = {r[0]: r[1] for r in previous_ad_rows if r[0]}
        previous_operated_map = {r[0]: r[1] for r in previous_operated_rows if r[0]}
        child_rows = (
            db.query(
                AsinPerformance.parent_asin,
                AsinPerformance.week_no,
                AsinPerformance.child_asin,
            )
            .filter(
                AsinPerformance.parent_asin.isnot(None),
                AsinPerformance.parent_asin != "",
                AsinPerformance.week_no == selected_week,
                AsinPerformance.child_asin.isnot(None),
                AsinPerformance.child_asin != "",
                AsinPerformance.order_num > 0,
            )
            .distinct()
            .all()
        )
        child_by_key = defaultdict(list)
        for r in child_rows:
            k = (r[0], r[1])
            if r[2] and r[2] not in child_by_key[k]:
                child_by_key[k].append(r[2])
        by_key = {}
        for r in rows:
            pa, create_at, pot, wn, sid, op_status, ad_status, ad_at, op_at, chk_status, chk_at = (
                r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10]
            )
            key = (pa, wn)
            if key not in by_key:
                week_no_int = int(wn) if wn is not None else None
                if pot is not None and not isinstance(pot, Decimal):
                    try:
                        pot = Decimal(str(pot))
                    except Exception:
                        pot = None
                op = op_status if isinstance(op_status, bool) else (bool(int(op_status)) if op_status is not None else False)
                ad = ad_status if isinstance(ad_status, bool) else (bool(int(ad_status)) if ad_status is not None else False)
                by_key[key] = {
                    "parent_asin": pa,
                    "parent_asin_create_at": create_at,
                    "parent_order_total": pot or Decimal(0),
                    "week_no": week_no_int,
                    "store_ids": [int(sid)] if sid is not None else [],
                    "operation_status": op,
                    "last_operated_at": previous_operated_map.get(pa),
                    "ad_check": ad,
                    "ad_created_at": ad_at,
                    "last_ad_created_at": previous_ad_map.get(pa),
                    "operated_at": op_at,
                    "checked_status": chk_status,
                    "checked_at": chk_at,
                    "child_asins_with_orders": child_by_key.get(key, []),
                }
            else:
                g = by_key[key]
                g["parent_order_total"] = (g["parent_order_total"] or Decimal(0)) + (pot if isinstance(pot, Decimal) else (Decimal(str(pot)) if pot is not None else Decimal(0)))
                if sid is not None and int(sid) not in g["store_ids"]:
                    g["store_ids"].append(int(sid))
                if op_status is not None and (isinstance(op_status, bool) and op_status or (not isinstance(op_status, bool) and int(op_status))):
                    g["operation_status"] = True
                if ad_status is not None and (isinstance(ad_status, bool) and ad_status or (not isinstance(ad_status, bool) and int(ad_status))):
                    g["ad_check"] = True
                if ad_at is not None and (g["ad_created_at"] is None or (ad_at > g["ad_created_at"])):
                    g["ad_created_at"] = ad_at
                if op_at is not None and (g["operated_at"] is None or (op_at > g["operated_at"])):
                    g["operated_at"] = op_at
                if chk_status == "completed":
                    g["checked_status"] = "completed"
                if chk_at is not None and (g["checked_at"] is None or (chk_at > g["checked_at"])):
                    g["checked_at"] = chk_at
        out = []
        for g in by_key.values():
            g["store_ids"].sort()
            out.append(
                SummaryRowConsolidated(
                    parent_asin=g["parent_asin"],
                    parent_asin_create_at=g["parent_asin_create_at"],
                    parent_order_total=g["parent_order_total"],
                    week_no=g["week_no"],
                    store_ids=g["store_ids"],
                    child_asins_with_orders=g["child_asins_with_orders"],
                    operation_status=g["operation_status"],
                    last_operated_at=g["last_operated_at"],
                    ad_check=g["ad_check"],
                    ad_created_at=g["ad_created_at"],
                    last_ad_created_at=g["last_ad_created_at"],
                    operated_at=g["operated_at"],
                    checked_status=g["checked_status"] or "pending",
                    checked_at=g["checked_at"],
                )
            )
        out.sort(key=lambda x: (-(x.parent_order_total or 0), x.parent_asin or ""))
        return out
    except Exception as e:
        logger.exception("Summary consolidated query failed: %s", e)
        raise HTTPException(status_code=500, detail=f"查询 summary 汇总失败: {e!s}")


class OperateBody(BaseModel):
    parent_asin: str
    week_no: int | str


class AdCheckBody(BaseModel):
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
            AsinPerformance.parent_asin == parent_asin,
            AsinPerformance.week_no == week_no,
        )
        .update(
            {"operation_status": True, "operated_at": now},
            synchronize_session=False,
        )
    )
    db.commit()
    return {"updated": n, "parent_asin": parent_asin, "week_no": week_no, "operated_at": now.isoformat()}


@router.post("/ad-check")
def ad_check_by_parent_week(
    body: AdCheckBody,
    db: Session = Depends(get_db),
):
    """按 parent_asin 和 week_no 将符合条件的所有记录的 ad_check 置为 True，ad_created_at 置为当前时间。"""
    parent_asin = (body.parent_asin or "").strip()
    if parent_asin == "":
        raise HTTPException(status_code=400, detail="parent_asin 不能为空")
    week_raw = str(body.week_no).strip().replace(",", "")
    if not week_raw.isdigit():
        raise HTTPException(status_code=400, detail="week_no 格式不合法")
    week_no = int(week_raw)
    now = datetime.now(timezone(timedelta(hours=8))).replace(tzinfo=None)
    n = (
        db.query(AsinPerformance)
        .filter(
            AsinPerformance.parent_asin == parent_asin,
            AsinPerformance.week_no == week_no,
        )
        .update(
            {"ad_check": True, "ad_created_at": now},
            synchronize_session=False,
        )
    )
    db.commit()
    return {"updated": n, "parent_asin": parent_asin, "week_no": week_no, "ad_created_at": now.isoformat()}


class RefreshQueryStatusBody(BaseModel):
    week_no: int | str


@router.post("/group-a")
def trigger_group_a_sync(
    week_no: Optional[str] = Query(None, description="Week number, optional; defaults to current script logic"),
):
    """触发 Group A 同步，将结果写入本地 group_A 表。"""
    try:
        if week_no is not None and str(week_no).strip():
            wk_str = str(week_no).strip().replace(",", "")
            if not wk_str.isdigit():
                raise HTTPException(status_code=400, detail="week_no 格式不合法")
        else:
            date_start_str, date_end_str = _get_sync_date_range()
            date_end_d = datetime.strptime(date_end_str, "%Y-%m-%d").date()
            reference_date = date_end_d - timedelta(days=1)
            wk_str, _ = _group_a_date_to_week_no(reference_date)
            logger.info(
                "[GroupA] API auto week_no by date_end-1: date_start=%s date_end=%s reference_date=%s -> week_no=%s",
                date_start_str,
                date_end_str,
                reference_date.strftime("%Y-%m-%d"),
                wk_str,
            )

        logger.info("[GroupA] API trigger start: week_no=%s", wk_str)
        out = sync_group_a_impression(wk_str)
        out = _normalize_group_a_sync_result(out)
        logger.info("[GroupA] API trigger done: week_no=%s result=%s", wk_str, out)
        return {
            "status": "ok",
            "week_no": int(wk_str),
            "message": f"Group A sync completed for week_no={wk_str}",
            "result": out,
        }
    except HTTPException:
        raise
    except ValueError as e:
        logger.warning("[GroupA] API trigger validation failed: %s", e)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("[GroupA] API trigger failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/group-a/weeks", response_model=List[int])
def list_group_a_weeks(db: Session = Depends(get_db)):
    rows = (
        db.query(GroupA.week_no)
        .filter(
            GroupA.week_no.isnot(None),
            GroupA.migrated_to_asin_performances == False,
        )
        .distinct()
        .order_by(GroupA.week_no.desc())
        .all()
    )
    out: List[int] = []
    for r in rows:
        try:
            out.append(int(r[0]))
        except (TypeError, ValueError):
            continue
    return out


@router.get("/group-a/summary", response_model=GroupASummaryResponse)
def list_group_a_summary(
    week_no: Optional[int] = Query(None, description="Week number, default latest week"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(30, ge=1, le=100, description="Page size"),
    db: Session = Depends(get_db),
):
    base_week_query = db.query(func.max(GroupA.week_no)).filter(GroupA.migrated_to_asin_performances == False)
    selected_week = week_no if week_no is not None else base_week_query.scalar()
    if selected_week is None:
        return GroupASummaryResponse(week_no=None, page=page, page_size=page_size, total=0, total_pages=0, rows=[])

    child_agg = (
        db.query(
            GroupA.parent_asin.label("parent_asin"),
            GroupA.store_id.label("store_id"),
            GroupA.week_no.label("week_no"),
            func.max(GroupA.parent_asin_created_at).label("created_at"),
            GroupA.child_asin.label("child_asin"),
            func.max(func.coalesce(GroupA.child_impression_count, 0)).label("child_impression_count"),
            func.max(func.coalesce(GroupA.child_cart, 0)).label("child_cart"),
            func.max(func.coalesce(GroupA.child_session_count, 0)).label("child_session_count"),
            func.max(func.coalesce(GroupA.operation_status, 0)).label("operation_status"),
            func.max(GroupA.operated_at).label("operated_at"),
        )
        .filter(
            GroupA.week_no == selected_week,
            GroupA.migrated_to_asin_performances == False,
            GroupA.parent_asin.isnot(None),
            GroupA.parent_asin != "",
            GroupA.store_id.isnot(None),
            GroupA.child_asin.isnot(None),
            GroupA.child_asin != "",
        )
        .group_by(GroupA.parent_asin, GroupA.store_id, GroupA.week_no, GroupA.child_asin)
        .subquery()
    )

    summary_query = (
        db.query(
            child_agg.c.parent_asin.label("parent_asin"),
            child_agg.c.store_id.label("store_id"),
            func.max(child_agg.c.created_at).label("created_at"),
            child_agg.c.week_no.label("week_no"),
            func.sum(child_agg.c.child_impression_count).label("total_impression_count"),
            func.sum(child_agg.c.child_cart).label("total_cart_count"),
            func.sum(child_agg.c.child_session_count).label("total_session_count"),
            func.max(child_agg.c.operation_status).label("operation_status"),
            func.max(child_agg.c.operated_at).label("operated_at"),
        )
        .group_by(child_agg.c.parent_asin, child_agg.c.store_id, child_agg.c.week_no)
    )

    summary_subq = summary_query.subquery()
    total = int(db.query(func.count()).select_from(summary_subq).scalar() or 0)
    total_pages = (total + page_size - 1) // page_size if total > 0 else 0

    rows = (
        db.query(summary_subq)
        .order_by(
            summary_subq.c.total_impression_count.desc(),
            summary_subq.c.total_cart_count.desc(),
            summary_subq.c.total_session_count.desc(),
            summary_subq.c.parent_asin.asc(),
            summary_subq.c.store_id.asc(),
        )
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    out_rows = [
        GroupASummaryRow(
            parent_asin=r.parent_asin,
            store_id=int(r.store_id) if r.store_id is not None else None,
            created_at=r.created_at,
            week_no=int(r.week_no) if r.week_no is not None else None,
            total_impression_count=int(r.total_impression_count or 0),
            total_cart_count=int(r.total_cart_count or 0),
            total_session_count=int(r.total_session_count or 0),
            operation_status=bool(int(r.operation_status)) if r.operation_status is not None else False,
            operated_at=r.operated_at,
        )
        for r in rows
    ]
    return GroupASummaryResponse(
        week_no=int(selected_week),
        page=page,
        page_size=page_size,
        total=total,
        total_pages=total_pages,
        rows=out_rows,
    )


@router.get("/group-a/detail", response_model=GroupADetailResponse)
def get_group_a_detail(
    parent_asin: str = Query(..., description="Parent ASIN"),
    week_no: int = Query(..., description="Week number"),
    store_id: int = Query(..., description="Store ID"),
    db: Session = Depends(get_db),
):
    rows = (
        db.query(GroupA)
        .filter(
            GroupA.parent_asin == parent_asin,
            GroupA.week_no == week_no,
            GroupA.store_id == store_id,
            GroupA.migrated_to_asin_performances == False,
        )
        .order_by(GroupA.child_asin, GroupA.search_query)
        .all()
    )
    if not rows:
        return GroupADetailResponse(
            parent_asin=parent_asin,
            week_no=week_no,
            store_id=store_id,
            children=[],
        )

    total_impression_count = 0
    total_cart_count = 0
    total_session_count = 0
    by_child: dict[tuple[str, int], dict] = {}
    for r in rows:
        child_asin = (r.child_asin or "").strip()
        if child_asin == "":
            continue
        key = (child_asin, int(r.store_id) if r.store_id is not None else store_id)
        if key not in by_child:
            child_impression_count = int(r.child_impression_count or 0)
            child_cart = int(r.child_cart or 0)
            child_session_count = int(r.child_session_count or 0)
            by_child[key] = {
                "child_asin": child_asin,
                "child_impression_count": child_impression_count,
                "child_cart": child_cart,
                "child_session_count": child_session_count,
                "search_queries": [],
            }
            total_impression_count += child_impression_count
            total_cart_count += child_cart
            total_session_count += child_session_count

        by_child[key]["search_queries"].append(
            SearchQueryRow(
                search_query=r.search_query,
                search_query_volume=r.search_query_volume,
                search_query_impression_count=r.search_query_impression_count,
                search_query_cart_count=r.search_query_cart_count,
                search_query_total_impression=r.search_query_total_impression_count,
                search_query_click_count=r.search_query_click_count,
                search_query_total_click=r.search_query_total_click_count,
            )
        )

    def _sort_key(row: SearchQueryRow):
        return (
            -(row.search_query_impression_count or 0),
            -(row.search_query_volume or 0),
            row.search_query or "",
        )

    children = []
    for group in by_child.values():
        sorted_queries = sorted(group["search_queries"], key=_sort_key)
        children.append(
            GroupADetailChildRow(
                child_asin=group["child_asin"],
                child_impression_count=group["child_impression_count"],
                child_cart=group["child_cart"],
                child_session_count=group["child_session_count"],
                search_queries=sorted_queries,
            )
        )
    children.sort(key=lambda c: (-(c.child_impression_count or 0), -(c.child_session_count or 0), c.child_asin or ""))

    return GroupADetailResponse(
        parent_asin=parent_asin,
        store_id=store_id,
        created_at=rows[0].parent_asin_created_at,
        week_no=week_no,
        total_impression_count=total_impression_count,
        total_cart_count=total_cart_count,
        total_session_count=total_session_count,
        children=children,
    )


@router.post("/group-a/operate")
def operate_group_a(
    body: GroupAOperateBody,
    db: Session = Depends(get_db),
):
    parent_asin = (body.parent_asin or "").strip()
    if parent_asin == "":
        raise HTTPException(status_code=400, detail="parent_asin 不能为空")
    week_raw = str(body.week_no).strip().replace(",", "")
    if not week_raw.isdigit():
        raise HTTPException(status_code=400, detail="week_no 格式不合法")
    week_no = int(week_raw)
    now = datetime.now(timezone(timedelta(hours=8))).replace(tzinfo=None)
    n = (
        db.query(GroupA)
        .filter(
            GroupA.parent_asin == parent_asin,
            GroupA.store_id == body.store_id,
            GroupA.week_no == week_no,
        )
        .update(
            {"operation_status": True, "operated_at": now},
            synchronize_session=False,
        )
    )
    db.commit()
    return {
        "updated": n,
        "parent_asin": parent_asin,
        "store_id": int(body.store_id),
        "week_no": week_no,
        "operated_at": now.isoformat(),
    }


@router.post("/query-status/refresh")
def refresh_query_status(
    body: RefreshQueryStatusBody,
    db: Session = Depends(get_db),
):
    """
    轮询查询状态：
    - 针对指定 week_no 下各 (parent_asin, store_id)；
    - 检查该父 ASIN 在对应 store 下的全部子 ASIN；
    - 若该周 amazon_search 对该父 ASIN 的全部子 ASIN 一条记录都没有，视为该周尚未开始处理，不允许通过；
    - 若子 ASIN 在 online.amazon_search 中状态为 3，或该周无记录，则视为该子 ASIN 已完成；
    - 仅当存在子 ASIN 查到记录但状态不为 3 时，才标记该父 ASIN 为 pending；
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
    threshold = now - timedelta(minutes=30)

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
    write_batches = 0
    pending_writes = 0

    def _is_lock_wait_timeout(exc: OperationalError) -> bool:
        # PyMySQL: args[0] = 1205
        orig = getattr(exc, "orig", None)
        args = getattr(orig, "args", ()) if orig is not None else ()
        try:
            return bool(args) and int(args[0]) == 1205
        except Exception:
            return False

    def _update_checked_status_with_retry(parent_asin: str, sid: int, new_status: str) -> bool:
        nonlocal pending_writes
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # 仅在需要时更新，减少无意义写入和行锁竞争
                db.query(AsinPerformance).filter(
                    AsinPerformance.week_no == week_no,
                    AsinPerformance.parent_asin == parent_asin,
                    AsinPerformance.store_id == sid,
                    or_(
                        AsinPerformance.checked_status != new_status,
                        AsinPerformance.checked_at.is_(None),
                        AsinPerformance.checked_at <= threshold,
                    ),
                ).update(
                    {"checked_status": new_status, "checked_at": now},
                    synchronize_session=False,
                )
                pending_writes += 1
                return True
            except OperationalError as exc:
                db.rollback()
                if _is_lock_wait_timeout(exc) and attempt < (max_retries - 1):
                    # 轻量退避重试，避免瞬时锁冲突导致整个接口失败
                    time.sleep(0.15 * (attempt + 1))
                    continue
                raise
        return False
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
    # 与后台同步主池分离，避免定时任务占满 QueuePool 后本接口排队 30s 超时
    online_engine = get_online_reporting_engine()
    try:
        with online_engine.connect() as conn:
            for pa, sid, q_status, checked_at in groups:
                if sid is None:
                    continue
                if str(q_status or "").lower() == "completed":
                    skipped_completed += 1
                    continue
                if checked_at is not None and checked_at > threshold:
                    skipped_by_interval += 1
                    continue

                done, child_count = check_parent_store_week_completed(conn, str(pa or ""), int(sid), week_no)
                if child_count == 0:
                    # 无子 ASIN，保持 pending，只记录检查时间
                    _update_checked_status_with_retry(str(pa or ""), int(sid), "pending")
                    checked_groups += 1
                    if pending_writes >= 50:
                        db.commit()
                        write_batches += 1
                        pending_writes = 0
                    continue
                new_status = "completed" if done else "pending"
                _update_checked_status_with_retry(str(pa or ""), int(sid), new_status)
                checked_groups += 1
                if done:
                    completed_groups += 1
                if pending_writes >= 50:
                    db.commit()
                    write_batches += 1
                    pending_writes = 0
        if pending_writes > 0:
            db.commit()
            write_batches += 1
    finally:
        if lock_acquired:
            _query_refresh_lock.release()

    logger.info(
        "[QueryStatusRefresh] week_no=%s checked_groups=%s completed_groups=%s skipped_completed=%s skipped_by_interval=%s groups_total=%s write_batches=%s",
        week_no,
        checked_groups,
        completed_groups,
        skipped_completed,
        skipped_by_interval,
        len(groups),
        write_batches,
    )
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
        engine = get_online_reporting_engine()
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
    week_nos: Optional[List[int]] = Query(None, description="指定 Group F 创建周，如 202612，可多值；有值时忽略扫描周数"),
):
    """
    Group F：查询指定周或按扫描周数计算的周内创建的父 ASIN 状态。
    注意：Group F 使用独立周号定义（包含 1 月 1 日的那周为 week 1），不同于其他页面/MySQL week_no。
    指定周：week_nos=202612 或 week_nos=202612&week_nos=202611；留空则按 scan_weeks 计算。
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

    online_engine = get_online_reporting_engine()
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
                return GroupFResponse(weeks=[], business_weeks=[], rows=[])
            db_max = max(cands)
            current_week = max(db_max, _group_f_current_week_no())
            scan_weeks_list = compute_scan_weeks_list_for_api(current_week, scan_weeks)
        business_weeks = [_group_f_to_mysql_week_no(w) for w in scan_weeks_list]
        logger.info("[Group F] request_id=%s 创建周（Group F）=%s，对应业务 week_no=%s，调用 get_group_f...", request_id, scan_weeks_list, business_weeks)
        rows = get_group_f(business_weeks)
        logger.info("[Group F] request_id=%s 查询成功，返回 %d 条", request_id, len(rows))
        return GroupFResponse(
            weeks=scan_weeks_list,
            business_weeks=business_weeks,
            rows=[
                GroupFRow(
                    variation_id=int(r[0]) if r[0] is not None else None,
                    parent_asin=r[1],
                    created_at=r[2],
                    store_id=int(r[3]) if r[3] is not None else None,
                    impression_count_asin=str(r[4]) if r[4] else None,
                    order_asin=str(r[5]) if r[5] else None,
                    sessions_asin=str(r[6]) if r[6] else None,
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


@router.get("/group-a/export")
def export_group_a_data(
    week_no: int = Query(..., description="Week number"),
    parent_store_keys: Optional[List[str]] = Query(None, description="Optional parent_asin||store_id filters"),
    db: Session = Depends(get_db),
):
    """下载指定 week_no 的 group_A 数据（CSV），支持按父 ASIN + store_id 过滤。"""
    q = db.query(GroupA).filter(GroupA.week_no == week_no)
    q = q.filter(GroupA.migrated_to_asin_performances == False)
    if parent_store_keys:
        normalized_pairs = []
        for raw in parent_store_keys:
            if not raw:
                continue
            parts = raw.split("||", 1)
            if len(parts) != 2:
                continue
            parent_asin = parts[0].strip()
            store_raw = parts[1].strip()
            if not parent_asin or not store_raw.isdigit():
                continue
            normalized_pairs.append((parent_asin, int(store_raw)))
        if normalized_pairs:
            q = q.filter(tuple_(GroupA.parent_asin, GroupA.store_id).in_(normalized_pairs))

    rows = q.order_by(
        GroupA.parent_asin,
        GroupA.store_id,
        GroupA.child_asin,
        GroupA.id,
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
        "parent_asin_created_at",
        "week_no",
        "child_impression_count",
        "child_cart",
        "child_session_count",
        "search_query",
        "search_query_volume",
        "search_query_impression_count",
        "search_query_cart_count",
        "search_query_total_impression_count",
        "search_query_click_count",
        "search_query_total_click_count",
        "operation_status",
        "operated_at",
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
            r.parent_asin_created_at.isoformat() if r.parent_asin_created_at else None,
            r.week_no,
            r.child_impression_count,
            r.child_cart,
            r.child_session_count,
            r.search_query,
            r.search_query_volume,
            r.search_query_impression_count,
            r.search_query_cart_count,
            r.search_query_total_impression_count,
            r.search_query_click_count,
            r.search_query_total_click_count,
            r.operation_status,
            r.operated_at.isoformat() if r.operated_at else None,
        ])
    output.seek(0)
    filename = f"group_a_week_{week_no}.csv"
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


@router.get("/monitor/parents")
def list_monitor_parents(db: Session = Depends(get_db)):
    """返回所有 operation_status=1 的父 ASIN 列表（去重），附最早 operated_at。"""
    rows = (
        db.query(
            AsinPerformance.parent_asin,
            func.min(AsinPerformance.operated_at).label("operated_at"),
        )
        .filter(AsinPerformance.operation_status == True)
        .filter(AsinPerformance.parent_asin.isnot(None), AsinPerformance.parent_asin != "")
        .group_by(AsinPerformance.parent_asin)
        .order_by(AsinPerformance.parent_asin)
        .all()
    )
    return [MonitorParentItem(parent_asin=r[0], operated_at=r[1]) for r in rows]


def _split_asin_values(raw_value: str | None) -> set[str]:
    if not raw_value:
        return set()
    return {part.strip() for part in str(raw_value).split(",") if part and part.strip()}


def _parse_parent_asin_filter_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    parts = re.split(r"[\s,;]+", str(raw).strip())
    return [p.strip() for p in parts if p and p.strip()]


def _fetch_trend_batch_options(batch_ids: list[int]) -> list[TrendBatchOption]:
    if not batch_ids:
        return []

    title_map: dict[int, str] = {}
    if settings.ONLINE_DB_HOST and settings.ONLINE_DB_USER:
        try:
            with get_online_reporting_engine().connect() as conn:
                placeholders = ", ".join([f":b{i}" for i in range(len(batch_ids))])
                params = {f"b{i}": int(batch_id) for i, batch_id in enumerate(batch_ids)}
                rows = conn.execute(
                    text(
                        f"SELECT id, test_title FROM ai_test_batch "
                        f"WHERE id IN ({placeholders})"
                    ),
                    params,
                ).fetchall()
                for row in rows:
                    try:
                        batch_id = int(row[0])
                    except (TypeError, ValueError):
                        continue
                    title_map[batch_id] = str(row[1] or "").strip()
        except Exception as exc:
            logger.warning("[Trend] failed to load ai_test_batch titles: %s", exc)

    return [
        TrendBatchOption(
            id=int(batch_id),
            label=f"{int(batch_id)}_{title_map.get(int(batch_id), '').strip()}".rstrip("_"),
        )
        for batch_id in batch_ids
    ]

def get_trend_data(
    store_id: Optional[int] = Query(None, description="店铺 ID"),
    used_model: Optional[str] = Query(None, description="used_text_model 或 used_image_model"),
    created_at_start: Optional[str] = Query(None, description="创建开始日期，格式 YYYY-MM-DD"),
    created_at_end: Optional[str] = Query(None, description="创建结束日期，格式 YYYY-MM-DD"),
    pid_min: Optional[int] = Query(None, description="PID 范围下限（含）"),
    pid_max: Optional[int] = Query(None, description="PID 范围上限（含）"),
    parent_asin: Optional[str] = Query(
        None, description="父 ASIN，多个用逗号、分号或空白分隔，精确匹配"
    ),
    week_no: Optional[List[int]] = Query(None, description="周序号，可重复传参多选"),
    batch_id: Optional[int] = Query(None, description="批次 ID"),
    db: Session = Depends(get_db),
):
    t0 = time.perf_counter()
    created_start_day = None
    created_end_day = None
    if created_at_start:
        try:
            created_start_day = datetime.strptime(created_at_start.strip(), "%Y-%m-%d").date()
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"created_at_start 格式错误，需为 YYYY-MM-DD: {exc}") from exc
    if created_at_end:
        try:
            created_end_day = datetime.strptime(created_at_end.strip(), "%Y-%m-%d").date()
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"created_at_end 格式错误，需为 YYYY-MM-DD: {exc}") from exc
    if created_start_day is not None and created_end_day is not None and created_start_day > created_end_day:
        raise HTTPException(status_code=400, detail="created_at_start 不能晚于 created_at_end")
    if pid_min is not None and pid_max is not None and int(pid_min) > int(pid_max):
        raise HTTPException(status_code=400, detail="pid_min 不能大于 pid_max")

    parent_asin_list = _parse_parent_asin_filter_list(parent_asin)

    def _apply_trend_listing_filters(q):
        if store_id is not None:
            q = q.filter(ListingTracking.store_id == store_id)
        if used_model:
            um = used_model.strip()
            if um:
                q = q.filter(
                    or_(
                        ListingTracking.used_text_model == um,
                        ListingTracking.used_image_model == um,
                    )
                )
        if created_start_day is not None:
            q = q.filter(func.date(ListingTracking.created_at) >= created_start_day)
        if created_end_day is not None:
            q = q.filter(func.date(ListingTracking.created_at) <= created_end_day)
        if pid_min is not None:
            q = q.filter(ListingTracking.pid >= int(pid_min))
        if pid_max is not None:
            q = q.filter(ListingTracking.pid <= int(pid_max))
        if parent_asin_list:
            q = q.filter(ListingTracking.parent_asin.in_(parent_asin_list))
        if week_no:
            q = q.filter(ListingTracking.week_no.in_(week_no))
        if batch_id is not None:
            q = q.filter(ListingTracking.batch_id == batch_id)
        return q

    t_parse = time.perf_counter()
    # 避免逐行拉取整表；在 MySQL 侧按 week_no 聚合。impression_asin 用 GROUP_CONCAT 合并后再解析。
    # new_asin_count 与 _listing_tracking_week_no 对齐：created_at 所在业务周周六的 ISO 周 = 行 week_no 时计入 pid_asin_count。
    db.execute(text("SET SESSION group_concat_max_len = 33554432"))
    _new_asin_case_sql = (
        "CASE WHEN listing_tracking.created_at IS NOT NULL AND listing_tracking.week_no = "
        "YEARWEEK(DATE_ADD(DATE_SUB(DATE(listing_tracking.created_at), "
        "INTERVAL ((WEEKDAY(DATE(listing_tracking.created_at)) + 1) % 7) DAY), INTERVAL 6 DAY), 3) "
        "THEN COALESCE(listing_tracking.pid_asin_count, 0) ELSE 0 END"
    )
    agg_q = db.query(
        ListingTracking.week_no,
        func.count().label("grp_n"),
        func.coalesce(func.sum(ListingTracking.total_impression), 0),
        func.coalesce(func.sum(ListingTracking.total_session), 0),
        func.coalesce(func.sum(ListingTracking.total_click), 0),
        func.coalesce(func.sum(ListingTracking.pid_asin_count), 0),
        func.coalesce(func.sum(ListingTracking.pid_active_asin_count), 0),
        func.sum(literal_column(_new_asin_case_sql)),
        literal_column(
            "GROUP_CONCAT(listing_tracking.impression_asin SEPARATOR '||')"
        ).label("imp_concat"),
    )
    agg_q = _apply_trend_listing_filters(agg_q)
    agg_q = agg_q.filter(ListingTracking.week_no.isnot(None))
    agg_q = agg_q.group_by(ListingTracking.week_no)
    agg_q = agg_q.order_by(ListingTracking.week_no.asc())
    t_sql = time.perf_counter()
    agg_rows = agg_q.all()
    t_sql_done = time.perf_counter()

    matched_row_count = 0
    weeks: List[int] = []
    series: List[TrendWeekPoint] = []
    for row in agg_rows:
        current_week = int(row[0])
        matched_row_count += int(row[1] or 0)
        total_impression = int(row[2] or 0)
        total_sessions = int(row[3] or 0)
        total_clicks = int(row[4] or 0)
        total_asin_count = int(row[5] or 0)
        active_asin_count = int(row[6] or 0)
        new_asin_count = int(row[7] or 0)
        impression_asins: set[str] = set()
        imp_blob = row[8]
        if imp_blob:
            for chunk in str(imp_blob).split("||"):
                impression_asins.update(_split_asin_values(chunk))
        impression_asin_count = len(impression_asins)
        weeks.append(current_week)
        series.append(
            TrendWeekPoint(
                week_no=current_week,
                new_asin_count=new_asin_count,
                total_impression=total_impression,
                total_sessions=total_sessions,
                total_clicks=total_clicks,
                total_asin_count=total_asin_count,
                active_asin_count=active_asin_count,
                impression_asin_count=impression_asin_count,
                related_click=total_sessions - total_clicks,
                impression_asin_rate=(float(total_impression) / impression_asin_count) if impression_asin_count > 0 else 0.0,
            )
        )
    t_agg = time.perf_counter()

    # Filter options: cache to avoid repeated full-table distinct + online title lookup
    now_ts = time.time()
    cached = None
    with _trend_filter_cache_lock:
        if (
            _trend_filter_cache.get("value") is not None
            and (now_ts - float(_trend_filter_cache.get("ts") or 0.0)) < _TREND_FILTER_CACHE_TTL_SEC
        ):
            cached = _trend_filter_cache["value"]

    if cached is not None:
        store_ids = cached["store_ids"]
        batch_ids = cached["batch_ids"]
        batch_options = cached["batch_options"]
        used_models = cached["used_models"]
        week_nos = []  # 由前端生成
    else:
        store_ids = [
            int(item[0])
            for item in db.query(ListingTracking.store_id)
            .filter(ListingTracking.store_id.isnot(None))
            .distinct()
            .order_by(ListingTracking.store_id.asc())
            .all()
            if item[0] is not None
        ]
        batch_ids = [
            int(item[0])
            for item in db.query(ListingTracking.batch_id)
            .filter(ListingTracking.batch_id.isnot(None))
            .distinct()
            .order_by(ListingTracking.batch_id.asc())
            .all()
            if item[0] is not None
        ]
        batch_options = _fetch_trend_batch_options(batch_ids)
        # 周次候选由前端按 listing_tracking 规则自 202515 生成至当前周，避免每次全表 distinct week_no。
        week_nos: list[int] = []
        used_model_rows = db.execute(
            text(
                "SELECT v FROM ( "
                "SELECT DISTINCT used_text_model AS v FROM listing_tracking "
                "WHERE used_text_model IS NOT NULL AND used_text_model != '' "
                "UNION "
                "SELECT DISTINCT used_image_model AS v FROM listing_tracking "
                "WHERE used_image_model IS NOT NULL AND used_image_model != '' "
                ") AS x ORDER BY v"
            )
        ).fetchall()
        used_models = sorted(
            {str(r[0]).strip() for r in used_model_rows if r[0] is not None and str(r[0]).strip()}
        )
        with _trend_filter_cache_lock:
            _trend_filter_cache["ts"] = now_ts
            _trend_filter_cache["value"] = {
                "store_ids": store_ids,
                "batch_ids": batch_ids,
                "batch_options": batch_options,
                "used_models": used_models,
            }
    t_opts = time.perf_counter()

    out = TrendResponse(
        matched_row_count=matched_row_count,
        weeks=weeks,
        filter_options=TrendFilterOptions(
            store_ids=store_ids,
            batch_ids=batch_ids,
            batch_options=batch_options,
            week_nos=week_nos,
            used_models=used_models,
        ),
        series=series,
    )
    t_done = time.perf_counter()
    try:
        resp_bytes = len(out.model_dump_json().encode("utf-8"))
    except Exception:
        resp_bytes = -1
    logger.info(
        "[Trend] /api/trend timings parse=%.3fs sql_build=%.3fs sql=%.3fs agg=%.3fs opts=%.3fs total=%.3fs weeks=%s rows=%s resp_kb=%.1f filters={store_id=%s used_model=%s week_no=%s batch_id=%s pid=[%s,%s] parent_asin_n=%s created=[%s,%s]}",
        (t_parse - t0),
        (t_sql - t_parse),
        (t_sql_done - t_sql),
        (t_agg - t_sql_done),
        (t_opts - t_agg),
        (t_done - t0),
        len(weeks),
        matched_row_count,
        (resp_bytes / 1024.0) if resp_bytes >= 0 else -1.0,
        store_id,
        used_model,
        len(week_no) if week_no else 0,
        batch_id,
        pid_min,
        pid_max,
        len(parent_asin_list),
        created_at_start,
        created_at_end,
    )
    return out


@router.get("/monitor/track", response_model=MonitorTrackResponse)
def get_monitor_track(
    parent_asin: str = Query(..., description="父 ASIN"),
    db: Session = Depends(get_db),
):
    """返回指定父 ASIN 下所有子 ASIN、所有 week_no 的 search_query 及 volume/impression/click，用于监控表格。"""
    parent_asin = (parent_asin or "").strip()
    if not parent_asin:
        raise HTTPException(status_code=400, detail="parent_asin 不能为空")
    rows = (
        db.query(
            AsinPerformance.child_asin,
            AsinPerformance.week_no,
            AsinPerformance.search_query,
            AsinPerformance.search_query_volume,
            AsinPerformance.search_query_impression_count,
            AsinPerformance.search_query_click_count,
        )
        .filter(
            AsinPerformance.parent_asin == parent_asin,
            AsinPerformance.child_asin.isnot(None),
        )
        .order_by(AsinPerformance.child_asin, AsinPerformance.week_no, AsinPerformance.search_query)
        .all()
    )
    weeks = sorted({r[1] for r in rows if r[1] is not None})
    if weeks:
        latest_week = max(weeks)
        latest_week_rows = [r for r in rows if r[1] == latest_week]
        latest_week_has_data = any(
            (r[2] is not None and str(r[2]).strip() != "")
            or r[3] is not None
            or r[4] is not None
            or r[5] is not None
            for r in latest_week_rows
        )
        latest_week_status_rows = (
            db.query(
                AsinPerformance.store_id,
                func.max(AsinPerformance.checked_status).label("checked_status"),
            )
            .filter(
                AsinPerformance.parent_asin == parent_asin,
                AsinPerformance.week_no == latest_week,
            )
            .group_by(AsinPerformance.store_id)
            .all()
        )
        all_pending = bool(latest_week_status_rows) and all(
            str(item[1] or "").lower() == "pending"
            for item in latest_week_status_rows
        )
        if all_pending or not latest_week_has_data:
            weeks = [w for w in weeks if w != latest_week]
            rows = [r for r in rows if r[1] != latest_week]
            logger.info(
                "[Monitor] hide latest week for parent_asin=%s latest_week=%s all_pending=%s has_data=%s",
                parent_asin,
                latest_week,
                all_pending,
                latest_week_has_data,
            )
    out_rows = [
        MonitorTrackRow(
            child_asin=r[0],
            week_no=r[1],
            search_query=r[2],
            search_query_volume=r[3],
            search_query_impression_count=r[4],
            search_query_click_count=r[5],
        )
        for r in rows
    ]
    status_rows = (
        db.query(
            AsinPerformance.week_no,
            AsinPerformance.store_id,
            AsinPerformance.checked_status,
            AsinPerformance.checked_at,
        )
        .filter(
            AsinPerformance.parent_asin == parent_asin,
            AsinPerformance.week_no.isnot(None),
        )
        .all()
    )
    latest_status_by_group = {}
    for week_no, store_id, checked_status, checked_at in status_rows:
        if week_no is None or store_id is None:
            continue
        key = (int(week_no), int(store_id))
        prev = latest_status_by_group.get(key)
        if prev is None or ((checked_at or datetime.min) >= (prev[1] or datetime.min)):
            latest_status_by_group[key] = (str(checked_status or "").lower(), checked_at)
    week_details = {}
    if weeks and settings.ONLINE_DB_HOST and settings.ONLINE_DB_USER:
        try:
            engine = get_online_reporting_engine()
            with engine.connect() as conn:
                week_details = get_parent_week_status_details(conn, parent_asin, weeks)
        except Exception as e:
            logger.warning("[Monitor] failed to load week status details for parent_asin=%s: %s", parent_asin, e)
    week_statuses = []
    for week_no in weeks:
        per_store = [val for (wk, _sid), val in latest_status_by_group.items() if wk == week_no]
        detail = week_details.get(week_no, {})
        completed = detail.get("completed")
        if completed is None:
            completed = bool(per_store) and all(status == "completed" for status, _checked_at in per_store)
        checked_at = max((item[1] for item in per_store if item[1] is not None), default=None)
        week_statuses.append(
            MonitorWeekStatus(
                week_no=week_no,
                completed=bool(completed),
                checked_at=checked_at,
                incomplete_count=int(detail.get("incomplete_count") or 0),
                incomplete_child_asins=detail.get("incomplete_child_asins") or [],
            )
        )
    return MonitorTrackResponse(parent_asin=parent_asin, weeks=weeks, week_statuses=week_statuses, rows=out_rows)


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
