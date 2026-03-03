import logging
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

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


@router.get("/summary", response_model=List[SummaryRow])
def list_summary(db: Session = Depends(get_db)):
    """按 parent_asin + week_no + store_id 去重，仅返回最新一周且有订单的父 ASIN。"""
    try:
        latest_week = db.query(func.max(AsinPerformance.week_no)).scalar()
        if latest_week is None:
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
                AsinPerformance.week_no == latest_week,
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
