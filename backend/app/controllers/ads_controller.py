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
revenue_router = APIRouter(prefix="/api", tags=["revenue"])
logger = logging.getLogger(__name__)


def _normalize_week_start_key(ws: object) -> str:
    """
    统一周键为 YYYY-MM-DD。

    线上/本地 MySQL 驱动可能对 DATE_SUB 返回 date 或 datetime，isoformat() 分别为
    「2026-03-02」与「2026-03-02T00:00:00」，直接用作 dict 键会导致广告合并失败（显示 0）。
    """
    if ws is None:
        return ""
    if isinstance(ws, str):
        s = ws.strip()
        return s[:10] if len(s) >= 10 else s
    if hasattr(ws, "isoformat"):
        return ws.isoformat()[:10]
    return str(ws)[:10]


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


def _fetch_weekly_weighted_fx_order_profit_online(
    start_date: date,
    end_date: date,
    store_id: Optional[int],
) -> dict[str, float]:
    """
    线上 order_profit：按 invoice_date 所在自然周（周一）汇总，
    销售额加权汇率 = SUM(net_revenue*qty*exchange_rate) / SUM(net_revenue*qty)，
    仅使用 exchange_rate > 6 的记录参与加权，
    用于将广告美元花费换算到与订单一致的本币口径。
    """
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        raise ValueError("Online DB 未配置")
    store_clause = " AND op.store_id = :store_id" if store_id is not None else ""
    sql = text(
        f"""
        SELECT
            DATE_SUB(op.invoice_date, INTERVAL WEEKDAY(op.invoice_date) DAY) AS week_start,
            SUM(
                CASE
                    WHEN COALESCE(op.exchange_rate, 0) > 6
                    THEN COALESCE(op.net_revenue, 0) * COALESCE(op.qty, 0)
                    ELSE 0
                END
            ) AS rev_sum,
            SUM(
                CASE
                    WHEN COALESCE(op.exchange_rate, 0) > 6
                    THEN COALESCE(op.net_revenue, 0) * COALESCE(op.qty, 0) * COALESCE(op.exchange_rate, 1)
                    ELSE 0
                END
            ) AS rev_fx_sum
        FROM order_profit op
        WHERE op.invoice_date >= :start_date
          AND op.invoice_date <= :end_date
          AND op.order_id <> ''
          {store_clause}
        GROUP BY DATE_SUB(op.invoice_date, INTERVAL WEEKDAY(op.invoice_date) DAY)
        """
    )
    params: dict[str, object] = {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
    }
    if store_id is not None:
        params["store_id"] = int(store_id)
    out: dict[str, float] = {}
    with get_online_reporting_engine().connect() as conn:
        for row in conn.execute(sql, params).mappings().all():
            ws = row["week_start"]
            key = _normalize_week_start_key(ws)
            if not key:
                continue
            rev_sum = float(row["rev_sum"] or 0)
            rev_fx_sum = float(row["rev_fx_sum"] or 0)
            if rev_sum > 0:
                out[key] = rev_fx_sum / rev_sum
            else:
                out[key] = 1.0
    return out


def _fetch_weekly_ads_cost_usd_online(
    start_date: date,
    end_date: date,
    store_id: Optional[int],
) -> dict[str, float]:
    """
    线上 amazon_ads_ad_group_ad_report：cost 为美元；按 DATE(current_date) 落入的周（周一）汇总 SUM(cost)。
    """
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        raise ValueError("Online DB 未配置")
    store_clause = " AND r.store_id = :store_id" if store_id is not None else ""
    sql = text(
        f"""
        SELECT
            DATE_SUB(DATE(r.current_date), INTERVAL WEEKDAY(DATE(r.current_date)) DAY) AS week_start,
            COALESCE(SUM(COALESCE(r.cost, 0)), 0) AS cost_usd
        FROM amazon_ads_ad_group_ad_report r
        WHERE r.current_date >= :start_date
          AND r.current_date <= :end_date
          {store_clause}
        GROUP BY DATE_SUB(DATE(r.current_date), INTERVAL WEEKDAY(DATE(r.current_date)) DAY)
        """
    )
    params: dict[str, object] = {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
    }
    if store_id is not None:
        params["store_id"] = int(store_id)
    out: dict[str, float] = {}
    with get_online_reporting_engine().connect() as conn:
        for row in conn.execute(sql, params).mappings().all():
            ws = row["week_start"]
            key = _normalize_week_start_key(ws)
            if not key:
                continue
            out[key] = float(row["cost_usd"] or 0)
    return out


def _weekly_ad_cost_local_usd_fx(
    start_date: date,
    end_date: date,
    store_id: Optional[int],
) -> tuple[dict[str, float], dict[str, float], dict[str, float]]:
    """
    返回 (本币广告费按周, 美元广告费按周, 当周加权汇率)。
    本币 = cost_usd * fx_week；fx_week 无订单数据时为 1.0。
    """
    usd_map = _fetch_weekly_ads_cost_usd_online(start_date, end_date, store_id)
    fx_map = _fetch_weekly_weighted_fx_order_profit_online(start_date, end_date, store_id)
    local: dict[str, float] = {}
    all_keys = set(usd_map.keys()) | set(fx_map.keys())
    for key in all_keys:
        usd = float(usd_map.get(key, 0.0))
        fx = float(fx_map.get(key, 1.0))
        if fx <= 0:
            fx = 1.0
        local[key] = usd * fx
    return local, usd_map, fx_map


def _merge_weekly_ad_cost_into_report(
    report: dict,
    ad_local_by_week: dict[str, float],
    *,
    ad_usd_by_week: Optional[dict[str, float]] = None,
    fx_by_week: Optional[dict[str, float]] = None,
) -> None:
    """写入 summary 与 weekly_series：ad_cost 为本币；附带 ad_cost_usd、ad_fx_rate。

    净收益额（sales_amount）保持 order_profit 周汇总 net_revenue，不扣广告。
    仅毛利 gross_profit、gross_profit_after_return 减去当周本币广告费；毛利率分母仍为未扣广告的销售额 / mature_sales。
    费销比 = 当周广告 ÷ 当周净收益额（未扣广告）。
    """
    summary = report.setdefault("summary", {})
    weekly = report.get("weekly_series") or []

    total_local = 0.0
    total_usd = 0.0
    for row in weekly:
        ws = row.get("week_start")
        key = _normalize_week_start_key(ws)
        loc = float(ad_local_by_week.get(key, 0.0))
        usd = float(ad_usd_by_week.get(key, 0.0)) if ad_usd_by_week else 0.0
        total_local += loc
        total_usd += usd

    summary["ad_cost"] = round(float(total_local), 2)
    summary["ad_cost_usd"] = round(float(total_usd), 2)
    sales_orig_summary = float(summary.get("sales_amount") or 0)
    summary["ad_cost_to_sales_pct"] = (
        round((total_local / sales_orig_summary * 100.0), 2) if sales_orig_summary > 0 else 0.0
    )

    for row in weekly:
        ws = row.get("week_start")
        key = _normalize_week_start_key(ws)
        ac = float(ad_local_by_week.get(key, 0.0))
        row["ad_cost"] = round(ac, 2)
        if ad_usd_by_week is not None:
            row["ad_cost_usd"] = round(float(ad_usd_by_week.get(key, 0.0)), 2)
        else:
            row["ad_cost_usd"] = 0.0
        if fx_by_week is not None:
            row["ad_fx_rate"] = round(float(fx_by_week.get(key, 1.0)), 6)
        else:
            row["ad_fx_rate"] = 1.0

        sa0 = float(row.get("sales_amount") or 0)
        ms0 = float(row.get("mature_sales_amount") or 0)
        gp0 = float(row.get("gross_profit") or 0)
        gpar0 = float(row.get("gross_profit_after_return") or 0)
        refund_total = float(row.get("refund_amount") or 0)

        row["ad_cost_to_sales_pct"] = round((ac / sa0 * 100.0), 2) if sa0 > 0 else 0.0

        row["gross_profit"] = round(gp0 - ac, 2)
        row["gross_profit_after_return"] = round(gpar0 - ac, 2)

        row["gross_margin_rate"] = (
            round((float(row["gross_profit"]) / sa0 * 100.0), 2) if sa0 > 0 else 0.0
        )
        row["gross_margin_after_return_rate"] = (
            round((float(row["gross_profit_after_return"]) / ms0 * 100.0), 2) if ms0 > 0 else 0.0
        )

        # 展示用含退货毛利率：按「真实+预估」退货金额口径计算，分母为当周净收益额（sales_amount）。
        gpar_display = float(row["gross_profit"]) - refund_total
        row["gross_profit_after_return_display"] = round(gpar_display, 2)
        row["gross_margin_after_return_rate_display"] = round((gpar_display / sa0 * 100.0), 2) if sa0 > 0 else 0.0

    if weekly:
        s_sales_raw = sum(float(r.get("sales_amount") or 0) for r in weekly)
        s_refund = sum(float(r.get("refund_amount") or 0) for r in weekly)
        s_sales = s_sales_raw - s_refund
        s_gp_before_refund = sum(float(r.get("gross_profit") or 0) for r in weekly)
        s_gp = s_gp_before_refund - s_refund
        s_gpar = sum(float(r.get("gross_profit_after_return_display") or 0) for r in weekly)
        summary["sales_amount"] = round(s_sales, 2)
        summary["refund_amount"] = round(s_refund, 2)
        summary["gross_profit"] = round(s_gp, 2)
        summary["gross_profit_after_return"] = round(s_gpar, 2)
        summary["gross_margin_rate"] = round((s_gp / s_sales * 100.0), 2) if s_sales > 0 else 0.0
        summary["return_rate"] = round((s_refund / s_sales * 100.0), 2) if s_sales > 0 else 0.0
        summary["gross_margin_after_return_rate"] = round((s_gpar / s_sales * 100.0), 2) if s_sales > 0 else 0.0


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
) -> tuple[float, dict[str, float], int | None]:
    """
    广告 ASIN 销售额 / 广告订单（条数）：online order_item × amazon_ads_ad_group_ad。
    - 销售额：DISTINCT 行上 (item_price_amount * quantity_ordered) 之和；
    - 广告订单：上述 DISTINCT 结果行数。
    与本地 daily_ad_cost_sales 是否已有广告行无关。
    """
    del db  # 口径仅依赖线上库；保留参数以兼容调用方
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        return 0.0, {}, None

    purchase_day_sql = _order_item_purchase_date_sql("oi")
    line_total_sql = "(COALESCE(oi.item_price_amount, 0) * COALESCE(oi.quantity_ordered, 0))"

    where_parts = [
        "oi.order_status != 'Canceled'",
        "COALESCE(oi.is_cancel, 0) = 0",
        "oi.purchase_utc_date IS NOT NULL",
        "oi.asin <> ''",
    ]
    params: dict[str, object] = {}
    if store_id is not None:
        where_parts.append("oi.store_id = :store_id")
        params["store_id"] = int(store_id)
    if sd is not None and ed is not None:
        where_parts.append(f"{purchase_day_sql} BETWEEN :sd AND :ed")
        params["sd"] = sd
        params["ed"] = ed
    else:
        report_max_day = _fetch_ads_report_max_current_date()
        if report_max_day is not None:
            where_parts.append(f"{purchase_day_sql} <= :report_max_day")
            params["report_max_day"] = report_max_day

    where_sql = " AND ".join(where_parts)
    inner_from = f"""
        FROM amazon_ads_ad_group_ad aaag
        INNER JOIN order_item oi ON oi.asin = aaag.asin AND oi.store_id = aaag.store_id
        WHERE {where_sql}
    """

    summary_sql = text(
        f"""
        SELECT COUNT(*) AS cnt, COALESCE(SUM(x.line_total), 0) AS total_amt
        FROM (
            SELECT DISTINCT
                oi.order_id,
                oi.asin,
                oi.item_price_amount,
                oi.quantity_ordered,
                {line_total_sql} AS line_total
            {inner_from}
        ) AS x
        """
    )
    daily_sql = text(
        f"""
        SELECT x.d AS d, COALESCE(SUM(x.line_total), 0) AS day_amt
        FROM (
            SELECT DISTINCT
                oi.order_id,
                oi.asin,
                oi.item_price_amount,
                oi.quantity_ordered,
                {line_total_sql} AS line_total,
                {purchase_day_sql} AS d
            {inner_from}
        ) AS x
        GROUP BY x.d
        ORDER BY x.d ASC
        """
    )

    with get_online_reporting_engine().connect() as conn:
        sum_row = conn.execute(summary_sql, params).fetchone()
        daily_rows = conn.execute(daily_sql, params).fetchall()

    row_cnt = int(sum_row[0] or 0) if sum_row else 0
    total = _num_to_float(sum_row[1] if sum_row else 0)

    by_day: dict[str, float] = {}
    for r in daily_rows:
        d, amt = r[0], r[1]
        if d is None:
            continue
        key = d.isoformat() if hasattr(d, "isoformat") else str(d)[:10]
        by_day[key] = _num_to_float(amt)

    return total, by_day, row_cnt


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
            DailyAdCostSales.ad_asin != "",
        )
        .with_entities(func.count(func.distinct(DailyAdCostSales.ad_asin)))
        .scalar()
        or 0
    )
    total_clicks = int(summary_row[0] or 0) if summary_row else 0
    total_impressions = int(summary_row[1] or 0) if summary_row else 0
    total_ad_cost = _num_to_float(summary_row[2] if summary_row else 0)
    total_sales_1d = _num_to_float(summary_row[3] if summary_row else 0)
    total_purchases = int(summary_row[4] or 0) if summary_row else 0
    total_order_item_sales, order_item_sales_by_day, ad_order_count = _fetch_order_item_ad_asin_sales(
        db,
        store_id=store_id,
        sd=sd,
        ed=ed,
    )
    summary_purchases = int(total_purchases)
    if ad_order_count is not None:
        summary_purchases = int(ad_order_count)
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
        "cvr": (summary_purchases / total_clicks * 100.0) if total_clicks > 0 else 0.0,
        "purchases": summary_purchases,
    }

    daily_rows = (
        q.with_entities(
            DailyAdCostSales.purchase_date,
            func.coalesce(func.sum(DailyAdCostSales.clicks), 0),
            func.coalesce(func.sum(DailyAdCostSales.impressions), 0),
            func.coalesce(func.sum(DailyAdCostSales.ad_cost), 0),
            func.coalesce(func.sum(DailyAdCostSales.sales_1d), 0),
            func.coalesce(func.sum(DailyAdCostSales.purchases), 0),
            func.count(func.distinct(DailyAdCostSales.ad_asin)),
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


@revenue_router.get("/revenue")
@router.get("/revenue", include_in_schema=False)
def get_ads_profit(
    store_id: Optional[int] = Query(None, description="按 order_profit.store_id / 线上广告报表 store_id 过滤"),
    start_date: Optional[str] = Query(None, description="invoice_date 起始 YYYY-MM-DD（含），默认 2026-02-23"),
    end_date: Optional[str] = Query(None, description="invoice_date 结束 YYYY-MM-DD（含），默认最新 invoice_date"),
):
    sd = _parse_ymd_or_400(start_date, "start_date") or DEFAULT_PROFIT_START
    ed = _parse_ymd_or_400(end_date, "end_date") or fetch_profit_latest_invoice_date()
    if sd > ed:
        raise HTTPException(status_code=400, detail="start_date 不能晚于 end_date")
    report = fetch_profit_report(sd, ed, store_id)
    try:
        local_map, usd_map, fx_map = _weekly_ad_cost_local_usd_fx(sd, ed, store_id)
        _merge_weekly_ad_cost_into_report(
            report,
            local_map,
            ad_usd_by_week=usd_map,
            fx_by_week=fx_map,
        )
    except Exception as exc:
        logger.exception("[Ads] weekly ad_cost (online USD × FX) merge failed")
        raise HTTPException(
            status_code=503,
            detail="广告成本数据暂时不可用，请稍后重试",
        ) from exc
    return report


@router.get("/profit", include_in_schema=False)
def get_ads_profit_legacy(
    store_id: Optional[int] = Query(None, description="按 order_profit.store_id / 线上广告报表 store_id 过滤"),
    start_date: Optional[str] = Query(None, description="invoice_date 起始 YYYY-MM-DD（含），默认 2026-02-23"),
    end_date: Optional[str] = Query(None, description="invoice_date 结束 YYYY-MM-DD（含），默认最新 invoice_date"),
):
    return get_ads_profit(store_id=store_id, start_date=start_date, end_date=end_date)

