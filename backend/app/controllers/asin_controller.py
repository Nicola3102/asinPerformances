import csv
import logging
from io import StringIO
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, text

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
)

router = APIRouter(prefix="/api/asin-performances", tags=["asin-performances"])
logger = logging.getLogger(__name__)


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
    connect_args = {"connect_timeout": 15, "read_timeout": 60, "write_timeout": 60}
    online_engine = create_engine(settings.online_database_url, pool_pre_ping=True, connect_args=connect_args)
    try:
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
    finally:
        try:
            online_engine.dispose()
        except Exception:
            pass
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
            out.append(
                SummaryRow(
                    parent_asin=r[0] if r[0] is not None else None,
                    parent_asin_create_at=r[1],
                    parent_order_total=parent_order_total,
                    week_no=week_no,
                    store_id=store_id,
                )
            )
        return out
    except Exception as e:
        logger.exception("Summary query failed: %s", e)
        raise HTTPException(status_code=500, detail=f"查询 summary 失败: {e!s}")


@router.get("/export")
def export_week_data(
    week_no: int = Query(..., description="Week number"),
    db: Session = Depends(get_db),
):
    """下载指定 week_no 的 asin_performances 全量数据（CSV）。"""
    rows = (
        db.query(AsinPerformance)
        .filter(AsinPerformance.week_no == week_no)
        .order_by(AsinPerformance.parent_asin, AsinPerformance.child_asin, AsinPerformance.store_id, AsinPerformance.id)
        .all()
    )
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
