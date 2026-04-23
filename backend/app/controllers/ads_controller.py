import csv
import logging
from datetime import date, datetime
from io import StringIO
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from decimal import Decimal

from sqlalchemy import and_, func
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.config import settings
from app.database import get_db
from app.models import DailyAdCostSales
from app.online_engine import get_online_reporting_engine
from app.services.weekly_profit import (
    DEFAULT_PROFIT_START,
    fetch_profit_latest_invoice_date,
    fetch_profit_report,
)
from app.services.daily_ad_cost_sales import ensure_latest_ad_cost_sales_data

router = APIRouter(prefix="/api/ads", tags=["ads"])
logger = logging.getLogger(__name__)

_SORT_FIELDS = {
    "ad_cost": DailyAdCostSales.ad_cost,
    "sales_1d": DailyAdCostSales.sales_1d,
    "ad_sales_1d": DailyAdCostSales.ad_sales_1d,
    "tad_sales": DailyAdCostSales.tad_sales,
    "tsales": DailyAdCostSales.tsales,
}


def _bg_ensure_latest_ad_sales() -> None:
    try:
        out = ensure_latest_ad_cost_sales_data()
        logger.info("[Ads] background ensure_latest finished: %s", out)
    except Exception as exc:
        logger.warning("[Ads] background ensure_latest failed: %s", exc)


def _num_to_float(val) -> float:
    if val is None:
        return 0.0
    if isinstance(val, Decimal):
        return float(val)
    try:
        return float(val)
    except Exception:
        return 0.0


def _parse_sort_or_400(raw: str | None) -> list:
    """
    sort 格式：field:asc,field2:desc
    - field 必须在 _SORT_FIELDS 中
    - direction 缺省为 desc
    """
    if raw is None:
        return []
    s = str(raw).strip()
    if not s:
        return []
    parts = [p.strip() for p in s.split(",") if p.strip()]
    out = []
    for p in parts:
        if ":" in p:
            field, direction = [x.strip() for x in p.split(":", 1)]
        else:
            field, direction = p.strip(), "desc"
        if field not in _SORT_FIELDS:
            raise HTTPException(status_code=400, detail=f"sort 字段不支持: {field}")
        d = direction.lower()
        col = _SORT_FIELDS[field]
        if d in ("asc", "a", "1"):
            out.append(col.asc())
        elif d in ("desc", "d", "-1"):
            out.append(col.desc())
        else:
            raise HTTPException(status_code=400, detail=f"sort direction 不支持: {direction}")
    return out


def _parse_ymd_or_400(raw: str | None, field: str) -> date | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail=f"{field} 格式不合法，需 YYYY-MM-DD")


def _order_item_purchase_date_sql(alias: str = "oi") -> str:
    """将 purchase_utc_date 转成 PST 后取日历日。"""
    return f"DATE(CONVERT_TZ({alias}.purchase_utc_date, '+00:00', '-07:00'))"


def _fetch_ads_report_max_current_date() -> date | None:
    """读取 amazon_ads_ad_group_ad_report 当前可用的最大报表日。"""
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        return None
    sql = text(
        """
        SELECT MAX(DATE(r.`current_date`)) AS max_current_date
        FROM amazon_ads_ad_group_ad_report r
        WHERE r.`current_date` IS NOT NULL
        """
    )
    with get_online_reporting_engine().connect() as conn:
        raw = conn.execute(sql).scalar()
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    try:
        return datetime.strptime(str(raw)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _fetch_order_item_ad_asin_sales(
    db: Session,
    *,
    store_id: int | None,
    sd: date | None,
    ed: date | None,
) -> tuple[float, dict[str, float]]:
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        return 0.0, {}

    ads_q = db.query(
        DailyAdCostSales.purchase_date,
        DailyAdCostSales.store_id,
        func.trim(DailyAdCostSales.ad_asin),
    ).filter(
        DailyAdCostSales.purchase_date.is_not(None),
        DailyAdCostSales.store_id.is_not(None),
        DailyAdCostSales.ad_asin.is_not(None),
        func.trim(DailyAdCostSales.ad_asin) != "",
    )
    if store_id is not None:
        ads_q = ads_q.filter(DailyAdCostSales.store_id == int(store_id))
    if sd is not None and ed is not None:
        ads_q = ads_q.filter(
            DailyAdCostSales.purchase_date >= sd,
            DailyAdCostSales.purchase_date <= ed,
        )

    ad_asin_keys: set[tuple[int, str]] = set()
    for _d, sid, asin in ads_q.distinct().all():
        if sid is None:
            continue
        asin_key = str(asin or "").strip()
        if not asin_key:
            continue
        ad_asin_keys.add((int(sid), asin_key))
    if not ad_asin_keys:
        return 0.0, {}

    params: dict[str, object] = {}
    purchase_day_sql = _order_item_purchase_date_sql("oi")
    report_max_day = _fetch_ads_report_max_current_date()
    where_parts = [
        "oi.order_status != 'Canceled'",
        "oi.purchase_utc_date IS NOT NULL",
        "oi.asin IS NOT NULL",
        "TRIM(oi.asin) <> ''",
    ]
    if store_id is not None:
        where_parts.append("oi.store_id = :store_id")
        params["store_id"] = int(store_id)
    if sd is not None and ed is not None:
        effective_ed = min(ed, report_max_day) if report_max_day is not None else ed
        if sd > effective_ed:
            return 0.0, {}
        where_parts.append(f"{purchase_day_sql} BETWEEN :sd AND :ed")
        params["sd"] = sd
        params["ed"] = effective_ed
    elif report_max_day is not None:
        where_parts.append(f"{purchase_day_sql} <= :report_max_day")
        params["report_max_day"] = report_max_day

    online_sql = text(
        f"""
        SELECT {purchase_day_sql} AS d,
               oi.store_id AS sid,
               TRIM(oi.asin) AS asin,
               SUM(COALESCE(oi.total_amount, 0)) AS amt
        FROM order_item oi
        WHERE {' AND '.join(where_parts)}
        GROUP BY {purchase_day_sql}, oi.store_id, TRIM(oi.asin)
        ORDER BY {purchase_day_sql} ASC
        """
    )
    with get_online_reporting_engine().connect() as conn:
        rows = conn.execute(online_sql, params).fetchall()

    by_day: dict[str, float] = {}
    total = 0.0
    for d, sid, asin, amt in rows:
        if d is None or sid is None:
            continue
        key = d.isoformat() if hasattr(d, "isoformat") else str(d)[:10]
        asin_key = str(asin or "").strip()
        if (int(sid), asin_key) not in ad_asin_keys:
            continue
        val = _num_to_float(amt)
        by_day[key] = _num_to_float(by_day.get(key, 0.0)) + val
        total += val
    return total, by_day


@router.get("/ad-sales")
def list_ad_sales(
    store_id: Optional[int] = Query(None),
    start_date: Optional[str] = Query(None, description="purchase_date 起始 YYYY-MM-DD（含）"),
    end_date: Optional[str] = Query(None, description="purchase_date 结束 YYYY-MM-DD（含）"),
    ensure_latest: bool = Query(
        False,
        description="为 true 时，请求前先执行 daily_ad_cost_sales 增量同步：补缺失报表日，并重算最近 7 天实际存在的线上报表日",
    ),
    sort: Optional[str] = Query(None, description="排序：field:asc,field2:desc"),
    page: int = Query(1, ge=1),
    page_size: int = Query(30, ge=1, le=200),
    db: Session = Depends(get_db),
):
    sync_info = None
    if ensure_latest:
        sync_info = ensure_latest_ad_cost_sales_data()

    sd = _parse_ymd_or_400(start_date, "start_date")
    ed = _parse_ymd_or_400(end_date, "end_date")
    if (sd is None) ^ (ed is None):
        raise HTTPException(status_code=400, detail="start_date 与 end_date 需同时提供或同时省略")
    if sd is not None and ed is not None and sd > ed:
        raise HTTPException(status_code=400, detail="start_date 不能晚于 end_date")

    conds = []
    if store_id is not None:
        conds.append(DailyAdCostSales.store_id == int(store_id))
    if sd is not None and ed is not None:
        conds.append(and_(DailyAdCostSales.purchase_date >= sd, DailyAdCostSales.purchase_date <= ed))

    q = db.query(DailyAdCostSales)
    if conds:
        q = q.filter(and_(*conds))

    summary_row = q.with_entities(
        func.coalesce(func.sum(DailyAdCostSales.clicks), 0),
        func.coalesce(func.sum(DailyAdCostSales.impressions), 0),
        func.coalesce(func.sum(DailyAdCostSales.ad_cost), 0),
        func.coalesce(func.sum(DailyAdCostSales.sales_1d), 0),
        func.coalesce(func.sum(DailyAdCostSales.purchases), 0),
    ).first()
    total_ad_asin_count = (
        q.filter(
            DailyAdCostSales.ad_asin.is_not(None),
            func.trim(DailyAdCostSales.ad_asin) != "",
        )
        .with_entities(func.count(func.distinct(func.trim(DailyAdCostSales.ad_asin))))
        .scalar()
        or 0
    )
    total_clicks = int(summary_row[0] or 0) if summary_row else 0
    total_impressions = int(summary_row[1] or 0) if summary_row else 0
    total_ad_cost = _num_to_float(summary_row[2] if summary_row else 0)
    total_sales_1d = _num_to_float(summary_row[3] if summary_row else 0)
    total_purchases = int(summary_row[4] or 0) if summary_row else 0
    total_order_item_sales, order_item_sales_by_day = _fetch_order_item_ad_asin_sales(
        db,
        store_id=store_id,
        sd=sd,
        ed=ed,
    )
    summary = {
        "clicks": total_clicks,
        "impressions": total_impressions,
        "ad_cost": total_ad_cost,
        "sales_1d": total_sales_1d,
        "order_item_sales": total_order_item_sales,
        "tacos": (total_ad_cost / total_order_item_sales * 100.0) if total_order_item_sales > 0 else 0.0,
        "ad_asin_count": int(total_ad_asin_count),
        "cpc": (total_ad_cost / total_clicks) if total_clicks > 0 else 0.0,
        "acos": (total_ad_cost / total_sales_1d * 100.0) if total_sales_1d > 0 else 0.0,
        "cvr": (total_purchases / total_clicks * 100.0) if total_clicks > 0 else 0.0,
        "purchases": total_purchases,
    }

    daily_rows = (
        q.with_entities(
            DailyAdCostSales.purchase_date,
            func.coalesce(func.sum(DailyAdCostSales.clicks), 0),
            func.coalesce(func.sum(DailyAdCostSales.impressions), 0),
            func.coalesce(func.sum(DailyAdCostSales.ad_cost), 0),
            func.coalesce(func.sum(DailyAdCostSales.sales_1d), 0),
            func.coalesce(func.sum(DailyAdCostSales.purchases), 0),
            func.count(func.distinct(func.trim(DailyAdCostSales.ad_asin))),
        )
        .filter(DailyAdCostSales.purchase_date.is_not(None))
        .group_by(DailyAdCostSales.purchase_date)
        .order_by(DailyAdCostSales.purchase_date.asc())
        .all()
    )
    daily_series = []
    for d, clicks_sum, impressions_sum, ad_cost_sum, sales_sum, purchases_sum, ad_asin_count in daily_rows:
        clicks_i = int(clicks_sum or 0)
        impressions_i = int(impressions_sum or 0)
        ad_cost_f = _num_to_float(ad_cost_sum)
        sales_f = _num_to_float(sales_sum)
        day_key = d.isoformat() if d else ""
        order_item_sales_f = _num_to_float(order_item_sales_by_day.get(day_key, 0))
        daily_series.append(
            {
                "date": day_key or None,
                "clicks": clicks_i,
                "impressions": impressions_i,
                "ad_cost": ad_cost_f,
                "sales_1d": sales_f,
                "order_item_sales": order_item_sales_f,
                "tacos": (ad_cost_f / order_item_sales_f * 100.0) if order_item_sales_f > 0 else 0.0,
                "ad_asin_count": int(ad_asin_count or 0),
                "cpc": (ad_cost_f / clicks_i) if clicks_i > 0 else 0.0,
                "acos": (ad_cost_f / sales_f * 100.0) if sales_f > 0 else 0.0,
                "cvr": (int(purchases_sum or 0) / clicks_i * 100.0) if clicks_i > 0 else 0.0,
                "purchases": int(purchases_sum or 0),
            }
        )

    rows_q = q.filter(func.coalesce(DailyAdCostSales.purchases, 0) > 0)
    sort_exprs = _parse_sort_or_400(sort)
    total = rows_q.with_entities(func.count(DailyAdCostSales.id)).scalar() or 0
    rows = (
        rows_q.order_by(
            *(
                sort_exprs
                if sort_exprs
                else [DailyAdCostSales.purchase_date.desc(), DailyAdCostSales.id.desc()]
            )
        )
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    items = [
        {
            "id": r.id,
            "ad_asin": r.ad_asin,
            "store_id": r.store_id,
            "purchase_date": r.purchase_date.isoformat() if r.purchase_date else None,
            "clicks": int(r.clicks or 0) if r.clicks is not None else 0,
            "impressions": int(r.impressions or 0) if r.impressions is not None else 0,
            "purchases": int(r.purchases or 0) if r.purchases is not None else 0,
            "ad_cost": float(r.ad_cost) if r.ad_cost is not None else None,
            "sales_1d": float(r.sales_1d) if r.sales_1d is not None else None,
            "ad_sales_1d": r.ad_sales_1d,
            "tad_sales": r.tad_sales,
            "tsales": float(r.tsales) if r.tsales is not None else None,
        }
        for r in rows
    ]
    return {
        "items": items,
        "page": page,
        "page_size": page_size,
        "total": int(total),
        "summary": summary,
        "daily_series": daily_series,
        "sync_info": sync_info,
    }


@router.post("/ad-sales/ensure-latest")
def trigger_ad_sales_ensure_latest(background_tasks: BackgroundTasks):
    """
    后台触发一次 ad-sales 最新数据补齐，不阻塞当前页面请求。
    实际执行仍受 daily_ad_cost_sales 内部全局锁保护；若已有任务在跑，本次会被自动跳过。
    """
    background_tasks.add_task(_bg_ensure_latest_ad_sales)
    return {
        "status": "accepted",
        "message": "Ad-Sales 最新数据已在后台检查/刷新，可先查看本地数据，稍后手动刷新页面。",
    }


@router.get("/ad-sales/export")
def export_ad_sales(
    ids: List[int] = Query(..., description="选中的记录 id，可重复传参 ids=1&ids=2"),
    db: Session = Depends(get_db),
):
    wanted = [int(x) for x in ids if x is not None]
    wanted = [x for x in wanted if x > 0]
    if not wanted:
        raise HTTPException(status_code=400, detail="ids 不能为空")

    rows = (
        db.query(DailyAdCostSales)
        .filter(DailyAdCostSales.id.in_(wanted))
        .order_by(DailyAdCostSales.purchase_date.desc(), DailyAdCostSales.id.desc())
        .all()
    )
    if not rows:
        raise HTTPException(status_code=404, detail="未匹配到可导出的记录")

    headers = [
        "id",
        "ad_asin",
        "store_id",
        "pid",
        "variation_id",
        "purchase_date",
        "clicks",
        "impressions",
        "purchases",
        "ad_cost",
        "sales_1d",
        "ad_sales_1d",
        "tad_sales",
        "tsales",
    ]
    output = StringIO()
    w = csv.writer(output)
    w.writerow(headers)
    for r in rows:
        w.writerow(
            [
                r.id,
                r.ad_asin,
                r.store_id,
                r.pid,
                r.variation_id,
                r.purchase_date.isoformat() if r.purchase_date else None,
                int(r.clicks or 0) if r.clicks is not None else 0,
                int(r.impressions or 0) if r.impressions is not None else 0,
                int(r.purchases or 0) if r.purchases is not None else 0,
                float(r.ad_cost) if r.ad_cost is not None else None,
                float(r.sales_1d) if r.sales_1d is not None else None,
                r.ad_sales_1d,
                r.tad_sales,
                float(r.tsales) if r.tsales is not None else None,
            ]
        )
    output.seek(0)
    filename = "ad_sales.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/profit")
def get_ads_profit(
    store_id: Optional[int] = Query(None, description="按 order_profit.store_id 过滤"),
    start_date: Optional[str] = Query(None, description="invoice_date 起始 YYYY-MM-DD（含），默认 2026-02-23"),
    end_date: Optional[str] = Query(None, description="invoice_date 结束 YYYY-MM-DD（含），默认最新 invoice_date"),
):
    sd = _parse_ymd_or_400(start_date, "start_date") or DEFAULT_PROFIT_START
    ed = _parse_ymd_or_400(end_date, "end_date") or fetch_profit_latest_invoice_date()
    if sd > ed:
        raise HTTPException(status_code=400, detail="start_date 不能晚于 end_date")
    return fetch_profit_report(sd, ed, store_id)

