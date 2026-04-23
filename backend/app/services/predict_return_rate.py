"""
基于 order_return 的历史 return_request -> 实际退货转化，按周预测最近 45 天请求的最终退货率。

数据来源（online DB）：
- ``order_return.order_date``：视为订单购买日期；
- ``order_return.request_date``：视为 return_request 发起日期；
- ``order_return.state = 5 OR order_return.track_status = '-'``：视为已真正退货。

口径：
- 历史成熟样本：request_date 早于最近 N 天窗口的请求；
- 预测窗口：最近 N 天（默认 45 天）request_date；
- 先按 ``购买日期 -> request_date`` 的周间隔（lag_week）统计历史真实退货率；
- 再把最近窗口内每条 request 按其 lag_week 匹配历史退货率，预测其最终会否成为真实退货；
- 最终按 request_week（周一~周日）聚合输出。

用法（backend 目录）：
  python3.11 -m app.services.predict_return_rate
  python3.11 -m app.services.predict_return_rate --lookback-days 45 --store-id 1 --pretty
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta

from sqlalchemy import text

from app.config import settings
from app.online_engine import get_online_reporting_engine


def _quantize_pct(val: float) -> float:
    return round(float(val), 2)


def _quantize_num(val: float) -> float:
    return round(float(val), 2)


def _rate(numerator: float, denominator: float) -> float:
    if not denominator:
        return 0.0
    return numerator / denominator


def _week_start_sql(expr: str) -> str:
    return f"DATE_SUB({expr}, INTERVAL WEEKDAY({expr}) DAY)"


def _build_where_clause(store_id: int | None) -> tuple[str, dict[str, object]]:
    params: dict[str, object] = {}
    where = "WHERE r.request_date IS NOT NULL"
    if store_id is not None:
        where += " AND r.store_id = :store_id"
        params["store_id"] = int(store_id)
    return where, params


def predict_recent_return_rate(
    *,
    lookback_days: int = 45,
    store_id: int | None = None,
    as_of_date: date | None = None,
) -> dict:
    if lookback_days <= 0:
        raise ValueError("lookback_days 必须大于 0")
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        raise ValueError("Online DB 未配置：需设置 online_db_host, online_db_user 等")

    today = as_of_date or date.today()
    recent_end = today
    recent_start = today - timedelta(days=lookback_days - 1)
    mature_end = recent_start - timedelta(days=1)

    where_sql, params = _build_where_clause(store_id)
    params.update(
        {
            "recent_start": recent_start.strftime("%Y-%m-%d"),
            "recent_end": recent_end.strftime("%Y-%m-%d"),
            "mature_end": mature_end.strftime("%Y-%m-%d"),
        }
    )

    summary_sql = text(
        f"""
        WITH base AS (
            SELECT
                r.store_id,
                r.order_date,
                r.request_date,
                r.amazon_order_id,
                COALESCE(r.order_amount, 0) AS order_amount,
                CASE
                    WHEN COALESCE(r.state, 0) = 5 OR COALESCE(r.track_status, '') = '-' THEN 1
                    ELSE 0
                END AS actual_refund
            FROM order_return r
            {where_sql}
              AND r.order_date IS NOT NULL
              AND r.request_date >= r.order_date
        ),
        historical_rates AS (
            SELECT
                GREATEST(0, TIMESTAMPDIFF(WEEK, b.order_date, b.request_date)) AS lag_week,
                COUNT(*) AS request_rows,
                SUM(b.actual_refund) AS actual_refund_rows
            FROM base b
            WHERE b.request_date <= :mature_end
            GROUP BY GREATEST(0, TIMESTAMPDIFF(WEEK, b.order_date, b.request_date))
        ),
        historical AS (
            SELECT
                COUNT(*) AS historical_request_rows,
                COUNT(DISTINCT CASE WHEN amazon_order_id IS NOT NULL AND amazon_order_id <> '' THEN amazon_order_id END) AS historical_request_orders,
                SUM(actual_refund) AS historical_actual_refund_rows,
                COUNT(DISTINCT CASE
                    WHEN actual_refund = 1 AND amazon_order_id IS NOT NULL AND amazon_order_id <> ''
                    THEN amazon_order_id
                END) AS historical_actual_refund_orders,
                SUM(order_amount) AS historical_request_amount
            FROM base
            WHERE request_date <= :mature_end
        ),
        recent AS (
            SELECT
                COUNT(*) AS recent_request_rows,
                COUNT(DISTINCT CASE WHEN amazon_order_id IS NOT NULL AND amazon_order_id <> '' THEN amazon_order_id END) AS recent_request_orders,
                SUM(actual_refund) AS observed_actual_refund_rows,
                COUNT(DISTINCT CASE
                    WHEN actual_refund = 1 AND amazon_order_id IS NOT NULL AND amazon_order_id <> ''
                    THEN amazon_order_id
                END) AS observed_actual_refund_orders,
                SUM(order_amount) AS recent_request_amount,
                SUM(
                    COALESCE(hr.actual_refund_rows / NULLIF(hr.request_rows, 0), 0)
                ) AS predicted_recent_actual_refund_rows
            FROM base
            LEFT JOIN historical_rates hr
              ON hr.lag_week = GREATEST(0, TIMESTAMPDIFF(WEEK, base.order_date, base.request_date))
            WHERE request_date BETWEEN :recent_start AND :recent_end
        )
        SELECT
            COALESCE(h.historical_request_rows, 0) AS historical_request_rows,
            COALESCE(h.historical_request_orders, 0) AS historical_request_orders,
            COALESCE(h.historical_actual_refund_rows, 0) AS historical_actual_refund_rows,
            COALESCE(h.historical_actual_refund_orders, 0) AS historical_actual_refund_orders,
            COALESCE(h.historical_request_amount, 0) AS historical_request_amount,
            COALESCE(r.recent_request_rows, 0) AS recent_request_rows,
            COALESCE(r.recent_request_orders, 0) AS recent_request_orders,
            COALESCE(r.observed_actual_refund_rows, 0) AS observed_actual_refund_rows,
            COALESCE(r.observed_actual_refund_orders, 0) AS observed_actual_refund_orders,
            COALESCE(r.recent_request_amount, 0) AS recent_request_amount,
            COALESCE(r.predicted_recent_actual_refund_rows, 0) AS predicted_recent_actual_refund_rows
        FROM historical h
        CROSS JOIN recent r
        """
    )

    weekly_sql = text(
        f"""
        WITH base AS (
            SELECT
                r.order_date,
                r.request_date,
                {_week_start_sql("r.order_date")} AS order_week_start,
                {_week_start_sql("r.request_date")} AS request_week_start,
                r.amazon_order_id,
                COALESCE(r.order_amount, 0) AS order_amount,
                GREATEST(0, TIMESTAMPDIFF(WEEK, r.order_date, r.request_date)) AS lag_week,
                CASE
                    WHEN COALESCE(r.state, 0) = 5 OR COALESCE(r.track_status, '') = '-' THEN 1
                    ELSE 0
                END AS actual_refund
            FROM order_return r
            {where_sql}
              AND r.order_date IS NOT NULL
              AND r.request_date >= r.order_date
        ),
        historical_rates AS (
            SELECT
                b.lag_week,
                COUNT(*) AS request_rows,
                SUM(b.actual_refund) AS actual_refund_rows
            FROM base b
            WHERE b.request_date <= :mature_end
            GROUP BY b.lag_week
        )
        SELECT
            b.request_week_start,
            DATE_ADD(b.request_week_start, INTERVAL 6 DAY) AS request_week_end,
            COUNT(*) AS request_rows,
            COUNT(DISTINCT CASE WHEN b.amazon_order_id IS NOT NULL AND b.amazon_order_id <> '' THEN b.amazon_order_id END) AS request_orders,
            COUNT(DISTINCT b.order_week_start) AS purchase_weeks,
            SUM(b.actual_refund) AS observed_actual_refund_rows,
            COUNT(DISTINCT CASE
                WHEN b.actual_refund = 1 AND b.amazon_order_id IS NOT NULL AND b.amazon_order_id <> ''
                THEN b.amazon_order_id
            END) AS observed_actual_refund_orders,
            SUM(b.order_amount) AS request_amount,
            SUM(COALESCE(hr.actual_refund_rows / NULLIF(hr.request_rows, 0), 0)) AS predicted_actual_refund_rows
        FROM base b
        LEFT JOIN historical_rates hr
          ON hr.lag_week = b.lag_week
        WHERE b.request_date BETWEEN :recent_start AND :recent_end
        GROUP BY b.request_week_start
        ORDER BY b.request_week_start ASC
        """
    )

    lag_rate_sql = text(
        f"""
        WITH base AS (
            SELECT
                r.order_date,
                r.request_date,
                GREATEST(0, TIMESTAMPDIFF(WEEK, r.order_date, r.request_date)) AS lag_week,
                CASE
                    WHEN COALESCE(r.state, 0) = 5 OR COALESCE(r.track_status, '') = '-' THEN 1
                    ELSE 0
                END AS actual_refund
            FROM order_return r
            {where_sql}
              AND r.order_date IS NOT NULL
              AND r.request_date >= r.order_date
        )
        SELECT
            lag_week,
            COUNT(*) AS request_rows,
            SUM(actual_refund) AS actual_refund_rows
        FROM base
        WHERE request_date <= :mature_end
        GROUP BY lag_week
        ORDER BY lag_week ASC
        """
    )

    with get_online_reporting_engine().connect() as conn:
        summary_row = conn.execute(summary_sql, params).mappings().one()
        weekly_rows = conn.execute(weekly_sql, params).mappings().all()
        lag_rate_rows = conn.execute(lag_rate_sql, params).mappings().all()

    historical_request_rows = int(summary_row["historical_request_rows"] or 0)
    historical_request_orders = int(summary_row["historical_request_orders"] or 0)
    historical_actual_refund_rows = int(summary_row["historical_actual_refund_rows"] or 0)
    historical_actual_refund_orders = int(summary_row["historical_actual_refund_orders"] or 0)
    recent_request_rows = int(summary_row["recent_request_rows"] or 0)
    recent_request_orders = int(summary_row["recent_request_orders"] or 0)
    observed_actual_refund_rows = int(summary_row["observed_actual_refund_rows"] or 0)
    observed_actual_refund_orders = int(summary_row["observed_actual_refund_orders"] or 0)
    predicted_recent_actual_refund_rows = float(summary_row["predicted_recent_actual_refund_rows"] or 0)

    historical_row_rate = _rate(historical_actual_refund_rows, historical_request_rows)
    historical_order_rate = _rate(historical_actual_refund_orders, historical_request_orders)
    observed_recent_row_rate = _rate(observed_actual_refund_rows, recent_request_rows)
    observed_recent_order_rate = _rate(observed_actual_refund_orders, recent_request_orders)
    predicted_recent_actual_refund_orders = recent_request_orders * historical_order_rate
    predicted_additional_refund_rows = max(predicted_recent_actual_refund_rows - observed_actual_refund_rows, 0.0)
    predicted_additional_refund_orders = max(predicted_recent_actual_refund_orders - observed_actual_refund_orders, 0.0)

    weekly_series: list[dict] = []
    for row in weekly_rows:
        request_rows = int(row["request_rows"] or 0)
        request_orders = int(row["request_orders"] or 0)
        observed_rows = int(row["observed_actual_refund_rows"] or 0)
        observed_orders = int(row["observed_actual_refund_orders"] or 0)
        predicted_rows = float(row["predicted_actual_refund_rows"] or 0)
        weekly_series.append(
            {
                "request_week_start": row["request_week_start"].isoformat() if row["request_week_start"] is not None else None,
                "request_week_end": row["request_week_end"].isoformat() if row["request_week_end"] is not None else None,
                "request_rows": request_rows,
                "request_orders": request_orders,
                "purchase_weeks": int(row["purchase_weeks"] or 0),
                "observed_actual_refund_rows": observed_rows,
                "observed_actual_refund_orders": observed_orders,
                "request_amount": _quantize_num(row["request_amount"] or 0),
                "observed_actual_refund_rate": _quantize_pct(_rate(observed_rows, request_rows) * 100.0),
                "predicted_actual_refund_rows": _quantize_num(predicted_rows),
                "predicted_actual_refund_orders": _quantize_num(request_orders * historical_order_rate),
                "predicted_return_rate": _quantize_pct(_rate(predicted_rows, request_rows) * 100.0),
            }
        )

    lag_rate_series = [
        {
            "lag_week": int(row["lag_week"] or 0),
            "request_rows": int(row["request_rows"] or 0),
            "actual_refund_rows": int(row["actual_refund_rows"] or 0),
            "actual_refund_rate": _quantize_pct(
                _rate(float(row["actual_refund_rows"] or 0), float(row["request_rows"] or 0)) * 100.0
            ),
        }
        for row in lag_rate_rows
    ]

    return {
        "as_of_date": today.isoformat(),
        "store_id": int(store_id) if store_id is not None else None,
        "lookback_days": int(lookback_days),
        "recent_start_date": recent_start.isoformat(),
        "recent_end_date": recent_end.isoformat(),
        "mature_end_date": mature_end.isoformat(),
        "refund_rule": "state = 5 OR track_status = '-'",
        "prediction_basis": "历史成熟样本中，按购买日期(order_date)到请求日期(request_date)的 lag_week 统计真实退货率；最近45天按 request_week 聚合预测",
        "summary": {
            "historical_request_rows": historical_request_rows,
            "historical_request_orders": historical_request_orders,
            "historical_actual_refund_rows": historical_actual_refund_rows,
            "historical_actual_refund_orders": historical_actual_refund_orders,
            "historical_request_amount": _quantize_num(summary_row["historical_request_amount"] or 0),
            "historical_actual_refund_row_rate": _quantize_pct(historical_row_rate * 100.0),
            "historical_actual_refund_order_rate": _quantize_pct(historical_order_rate * 100.0),
            "recent_request_rows": recent_request_rows,
            "recent_request_orders": recent_request_orders,
            "recent_request_amount": _quantize_num(summary_row["recent_request_amount"] or 0),
            "observed_actual_refund_rows": observed_actual_refund_rows,
            "observed_actual_refund_orders": observed_actual_refund_orders,
            "observed_recent_actual_refund_row_rate": _quantize_pct(observed_recent_row_rate * 100.0),
            "observed_recent_actual_refund_order_rate": _quantize_pct(observed_recent_order_rate * 100.0),
            "predicted_recent_actual_refund_rows": _quantize_num(predicted_recent_actual_refund_rows),
            "predicted_recent_actual_refund_orders": _quantize_num(predicted_recent_actual_refund_orders),
            "predicted_additional_refund_rows": _quantize_num(predicted_additional_refund_rows),
            "predicted_additional_refund_orders": _quantize_num(predicted_additional_refund_orders),
            "predicted_recent_return_rate": _quantize_pct(_rate(predicted_recent_actual_refund_rows, recent_request_rows) * 100.0),
        },
        "lag_rate_series": lag_rate_series,
        "weekly_series": weekly_series,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="预测最近 45 天 return_request 的最终退货率")
    parser.add_argument("--lookback-days", type=int, default=45, help="最近窗口天数，默认 45")
    parser.add_argument("--store-id", type=int, default=None, help="可选：仅统计指定店铺")
    parser.add_argument("--as-of-date", type=str, default="", help="可选：按指定日期作为 today，格式 YYYY-MM-DD")
    parser.add_argument("--pretty", action="store_true", help="以更易读的 JSON 格式输出")
    args = parser.parse_args()

    as_of_date = (
        datetime.strptime(str(args.as_of_date).strip(), "%Y-%m-%d").date()
        if str(args.as_of_date or "").strip()
        else None
    )
    result = predict_recent_return_rate(
        lookback_days=int(args.lookback_days),
        store_id=args.store_id,
        as_of_date=as_of_date,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2 if args.pretty else None))


if __name__ == "__main__":
    main()
