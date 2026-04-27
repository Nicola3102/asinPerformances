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
import math
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


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _train_logistic_regression(
    rows: list[dict[str, float]],
    *,
    epochs: int = 350,
    lr: float = 0.08,
    l2: float = 1e-3,
) -> tuple[list[float], list[float], list[float]]:
    """
    训练一个轻量 logistic regression（二分类）：
    - 特征：lag_week, ln(1+order_amount)
    - 返回：(weights, means, stds)
    """
    if not rows:
        return [0.0, 0.0, 0.0], [0.0, 0.0], [1.0, 1.0]

    x1 = [float(r["lag_week"]) for r in rows]
    x2 = [math.log1p(max(float(r["order_amount"]), 0.0)) for r in rows]
    y = [float(r["actual_refund"]) for r in rows]
    n = float(len(rows))

    m1 = sum(x1) / n
    m2 = sum(x2) / n
    v1 = max(sum((v - m1) ** 2 for v in x1) / n, 1e-8)
    v2 = max(sum((v - m2) ** 2 for v in x2) / n, 1e-8)
    s1 = math.sqrt(v1)
    s2 = math.sqrt(v2)

    w0 = 0.0
    w1 = 0.0
    w2 = 0.0

    for _ in range(max(50, int(epochs))):
        g0 = 0.0
        g1 = 0.0
        g2 = 0.0
        for i in range(len(rows)):
            z1 = (x1[i] - m1) / s1
            z2 = (x2[i] - m2) / s2
            p = _sigmoid(w0 + w1 * z1 + w2 * z2)
            err = p - y[i]
            g0 += err
            g1 += err * z1
            g2 += err * z2

        inv_n = 1.0 / n
        # L2 正则仅作用于非偏置项
        w0 -= lr * (g0 * inv_n)
        w1 -= lr * ((g1 * inv_n) + l2 * w1)
        w2 -= lr * ((g2 * inv_n) + l2 * w2)

    return [w0, w1, w2], [m1, m2], [s1, s2]


def _predict_logistic_prob(
    lag_week: float,
    order_amount: float,
    *,
    weights: list[float],
    means: list[float],
    stds: list[float],
) -> float:
    z1 = (float(lag_week) - means[0]) / stds[0]
    z2 = (math.log1p(max(float(order_amount), 0.0)) - means[1]) / stds[1]
    return _sigmoid(weights[0] + weights[1] * z1 + weights[2] * z2)


def predict_recent_return_rate(
    *,
    lookback_days: int = 45,
    store_id: int | None = None,
    as_of_date: date | None = None,
    bayes_prior_strength: float = 20.0,
    ml_weight: float = 0.7,
) -> dict:
    if lookback_days <= 0:
        raise ValueError("lookback_days 必须大于 0")
    if bayes_prior_strength <= 0:
        raise ValueError("bayes_prior_strength 必须大于 0")
    if not (0.0 <= ml_weight <= 1.0):
        raise ValueError("ml_weight 必须在 [0,1] 范围内")
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

    rows_sql = text(
        f"""
        WITH base AS (
            SELECT
                r.store_id,
                r.order_date,
                r.request_date,
                DATE_SUB(r.request_date, INTERVAL WEEKDAY(r.request_date) DAY) AS request_week_start,
                DATE_SUB(r.order_date, INTERVAL WEEKDAY(r.order_date) DAY) AS order_week_start,
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
        )
        SELECT
            store_id,
            order_date,
            request_date,
            order_week_start,
            request_week_start,
            amazon_order_id,
            order_amount,
            lag_week,
            actual_refund
        FROM base
        """
    )

    with get_online_reporting_engine().connect() as conn:
        all_rows = conn.execute(rows_sql, params).mappings().all()

    historical_rows = [r for r in all_rows if r["request_date"] <= mature_end]
    recent_rows = [r for r in all_rows if recent_start <= r["request_date"] <= recent_end]

    historical_request_rows = len(historical_rows)
    recent_request_rows = len(recent_rows)
    historical_actual_refund_rows = int(sum(int(r["actual_refund"] or 0) for r in historical_rows))
    observed_actual_refund_rows = int(sum(int(r["actual_refund"] or 0) for r in recent_rows))

    hist_order_ids = {
        str(r["amazon_order_id"]).strip()
        for r in historical_rows
        if r["amazon_order_id"] is not None and str(r["amazon_order_id"]).strip()
    }
    hist_refund_order_ids = {
        str(r["amazon_order_id"]).strip()
        for r in historical_rows
        if int(r["actual_refund"] or 0) == 1 and r["amazon_order_id"] is not None and str(r["amazon_order_id"]).strip()
    }
    recent_order_ids = {
        str(r["amazon_order_id"]).strip()
        for r in recent_rows
        if r["amazon_order_id"] is not None and str(r["amazon_order_id"]).strip()
    }
    recent_refund_order_ids = {
        str(r["amazon_order_id"]).strip()
        for r in recent_rows
        if int(r["actual_refund"] or 0) == 1 and r["amazon_order_id"] is not None and str(r["amazon_order_id"]).strip()
    }
    historical_request_orders = len(hist_order_ids)
    historical_actual_refund_orders = len(hist_refund_order_ids)
    recent_request_orders = len(recent_order_ids)
    observed_actual_refund_orders = len(recent_refund_order_ids)

    historical_request_amount = float(sum(float(r["order_amount"] or 0) for r in historical_rows))
    recent_request_amount = float(sum(float(r["order_amount"] or 0) for r in recent_rows))

    # 贝叶斯平滑（按 lag_week）：后验均值 = (success + alpha) / (count + alpha + beta)
    global_hist_rate = _rate(historical_actual_refund_rows, historical_request_rows)
    alpha = max(global_hist_rate * bayes_prior_strength, 1e-6)
    beta = max((1.0 - global_hist_rate) * bayes_prior_strength, 1e-6)
    lag_stats: dict[int, dict[str, float]] = {}
    for r in historical_rows:
        lag = int(r["lag_week"] or 0)
        bucket = lag_stats.setdefault(lag, {"request_rows": 0.0, "actual_refund_rows": 0.0})
        bucket["request_rows"] += 1.0
        bucket["actual_refund_rows"] += float(int(r["actual_refund"] or 0))

    lag_smoothed_rate: dict[int, float] = {}
    for lag, s in lag_stats.items():
        lag_smoothed_rate[lag] = (s["actual_refund_rows"] + alpha) / (s["request_rows"] + alpha + beta)

    default_smoothed_rate = alpha / (alpha + beta)

    # 轻量 ML（logistic regression）+ 贝叶斯平滑融合
    train_rows = [
        {
            "lag_week": float(int(r["lag_week"] or 0)),
            "order_amount": float(r["order_amount"] or 0),
            "actual_refund": float(int(r["actual_refund"] or 0)),
        }
        for r in historical_rows
    ]
    weights, means, stds = _train_logistic_regression(train_rows)

    predicted_recent_actual_refund_rows = 0.0
    weekly_acc: dict[str, dict[str, object]] = {}
    for r in recent_rows:
        lag = int(r["lag_week"] or 0)
        amount = float(r["order_amount"] or 0)
        p_ml = _predict_logistic_prob(lag, amount, weights=weights, means=means, stds=stds)
        p_bayes = lag_smoothed_rate.get(lag, default_smoothed_rate)
        p = (ml_weight * p_ml) + ((1.0 - ml_weight) * p_bayes)
        p = min(max(p, 0.0), 1.0)
        predicted_recent_actual_refund_rows += p

        wk = r["request_week_start"].isoformat()
        it = weekly_acc.setdefault(
            wk,
            {
                "request_week_start": r["request_week_start"],
                "request_week_end": r["request_week_start"] + timedelta(days=6),
                "request_rows": 0,
                "request_orders_set": set(),
                "purchase_weeks_set": set(),
                "observed_actual_refund_rows": 0,
                "observed_refund_orders_set": set(),
                "request_amount": 0.0,
                "predicted_actual_refund_rows": 0.0,
            },
        )
        it["request_rows"] = int(it["request_rows"]) + 1
        if r["amazon_order_id"] is not None and str(r["amazon_order_id"]).strip():
            order_id = str(r["amazon_order_id"]).strip()
            it["request_orders_set"].add(order_id)
            if int(r["actual_refund"] or 0) == 1:
                it["observed_refund_orders_set"].add(order_id)
        it["purchase_weeks_set"].add(r["order_week_start"].isoformat())
        it["observed_actual_refund_rows"] = int(it["observed_actual_refund_rows"]) + int(r["actual_refund"] or 0)
        it["request_amount"] = float(it["request_amount"]) + amount
        it["predicted_actual_refund_rows"] = float(it["predicted_actual_refund_rows"]) + p

    historical_row_rate = _rate(historical_actual_refund_rows, historical_request_rows)
    historical_order_rate = _rate(historical_actual_refund_orders, historical_request_orders)
    observed_recent_row_rate = _rate(observed_actual_refund_rows, recent_request_rows)
    observed_recent_order_rate = _rate(observed_actual_refund_orders, recent_request_orders)
    predicted_recent_actual_refund_orders = recent_request_orders * historical_order_rate
    predicted_additional_refund_rows = max(predicted_recent_actual_refund_rows - observed_actual_refund_rows, 0.0)
    predicted_additional_refund_orders = max(predicted_recent_actual_refund_orders - observed_actual_refund_orders, 0.0)

    weekly_series: list[dict] = []
    for k in sorted(weekly_acc.keys()):
        row = weekly_acc[k]
        request_rows = int(row["request_rows"] or 0)
        request_orders = len(row["request_orders_set"])
        observed_rows = int(row["observed_actual_refund_rows"] or 0)
        observed_orders = len(row["observed_refund_orders_set"])
        predicted_rows = float(row["predicted_actual_refund_rows"] or 0)
        weekly_series.append(
            {
                "request_week_start": row["request_week_start"].isoformat() if row["request_week_start"] is not None else None,
                "request_week_end": row["request_week_end"].isoformat() if row["request_week_end"] is not None else None,
                "request_rows": request_rows,
                "request_orders": request_orders,
                "purchase_weeks": len(row["purchase_weeks_set"]),
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
            "lag_week": int(lag),
            "request_rows": int(stat["request_rows"] or 0),
            "actual_refund_rows": int(stat["actual_refund_rows"] or 0),
            "actual_refund_rate": _quantize_pct(
                _rate(float(stat["actual_refund_rows"] or 0), float(stat["request_rows"] or 0)) * 100.0
            ),
            "bayes_smoothed_rate": _quantize_pct(float(lag_smoothed_rate.get(lag, default_smoothed_rate)) * 100.0),
        }
        for lag, stat in sorted(lag_stats.items(), key=lambda x: x[0])
    ]

    return {
        "as_of_date": today.isoformat(),
        "store_id": int(store_id) if store_id is not None else None,
        "lookback_days": int(lookback_days),
        "ml_model": "logistic_regression(lag_week, log1p(order_amount))",
        "ml_weight": _quantize_num(ml_weight),
        "bayes_prior_strength": _quantize_num(bayes_prior_strength),
        "recent_start_date": recent_start.isoformat(),
        "recent_end_date": recent_end.isoformat(),
        "mature_end_date": mature_end.isoformat(),
        "refund_rule": "state = 5 OR track_status = '-'",
        "prediction_basis": "历史成熟样本训练 logistic regression，并按 lag_week 执行贝叶斯平滑；执行期按 ml_weight 融合预测，最近窗口按 request_week 聚合",
        "summary": {
            "historical_request_rows": historical_request_rows,
            "historical_request_orders": historical_request_orders,
            "historical_actual_refund_rows": historical_actual_refund_rows,
            "historical_actual_refund_orders": historical_actual_refund_orders,
            "historical_request_amount": _quantize_num(historical_request_amount),
            "historical_actual_refund_row_rate": _quantize_pct(historical_row_rate * 100.0),
            "historical_actual_refund_order_rate": _quantize_pct(historical_order_rate * 100.0),
            "recent_request_rows": recent_request_rows,
            "recent_request_orders": recent_request_orders,
            "recent_request_amount": _quantize_num(recent_request_amount),
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
    parser.add_argument(
        "--bayes-prior-strength",
        type=float,
        default=20.0,
        help="贝叶斯平滑先验强度（默认 20）",
    )
    parser.add_argument(
        "--ml-weight",
        type=float,
        default=0.7,
        help="执行期融合权重：final = ml_weight*ML + (1-ml_weight)*Bayes，默认 0.7",
    )
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
        bayes_prior_strength=float(args.bayes_prior_strength),
        ml_weight=float(args.ml_weight),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2 if args.pretty else None))


if __name__ == "__main__":
    main()
