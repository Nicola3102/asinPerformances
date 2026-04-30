"""
基于 ``order_return`` 与 ``order_item`` 的退货预测：双表口径 + 机器学习。

- ``order_return``：按 ``request_date`` 周聚合申请条数；``CAST(state AS SIGNED)=5`` 或 ``track_status`` 含 ``'-'`` 计已退货。
- ``order_item``：``purchase_utc_date`` 转 PST 日历日后与 **同一自然周（周一为起点）** 对齐统计
  ``SUM(total_amount)``（非取消单）作为 **当周订单销售额**；可同时输出行数便于对照。

周口径指标（与业务对齐）：
- **return_request_completion_rate**：已退货 / return 申请（行，例 21/37）。
- **return_request_penetration_rate**：return 申请行数 / 订单行（例 37/179）。
- **order_level_return_amount_rate**：当周 **已退货金额**（满足退货规则的 ``order_amount``）/ 当周 **订单销售额**（岭回归目标与主预测口径）。

机器学习：
- **行级**：logistic（``lag_week``, ``log1p(order_amount)``）+ 按 lag 的贝叶斯平滑。
- **周级（双表）**：成熟历史上以 ``[1, 申请侧 order_amount 之和/当周销售额, log1p(当周销售额)]`` 预测 **退货金额占当周销售额比例**（岭回归，目标 ``min(1, 已退金额/销售额)``）。

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


def _order_item_purchase_day_expr(alias: str = "oi") -> str:
    """PST 日历日，与 ads_controller._order_item_purchase_date_sql 一致。"""
    return f"DATE(CONVERT_TZ({alias}.purchase_utc_date, '+00:00', '-07:00'))"


def _monday_of_date_py(d: date) -> date:
    return d - timedelta(days=int(d.weekday()))


def _gaussian_elimination_solve(a: list[list[float]], b: list[float]) -> list[float] | None:
    """解 A w = b，A 为 n×n 方阵；奇异则返回 None。"""
    n = len(b)
    mat = [row[:] + [b[i]] for i, row in enumerate(a)]
    for col in range(n):
        pivot = None
        for r in range(col, n):
            if abs(mat[r][col]) > 1e-12:
                pivot = r
                break
        if pivot is None:
            return None
        if pivot != col:
            mat[col], mat[pivot] = mat[pivot], mat[col]
        piv = mat[col][col]
        for j in range(col, n + 1):
            mat[col][j] /= piv
        for r in range(n):
            if r == col:
                continue
            f = mat[r][col]
            if abs(f) < 1e-15:
                continue
            for j in range(col, n + 1):
                mat[r][j] -= f * mat[col][j]
    return [mat[i][n] for i in range(n)]


def _ridge_train_order_level_weekly(
    samples: list[tuple[list[float], float]],
    *,
    lam: float = 2.0,
) -> list[float] | None:
    """岭回归：y ≈ w·x，x 已含截距项；最小化 ||Xw - y||² + λ||w||²。"""
    if len(samples) < 4:
        return None
    d = len(samples[0][0])
    xtx = [[0.0 for _ in range(d)] for _ in range(d)]
    xty = [0.0 for _ in range(d)]
    for x, y in samples:
        for i in range(d):
            xty[i] += x[i] * y
            for j in range(d):
                xtx[i][j] += x[i] * x[j]
    for i in range(d):
        xtx[i][i] += float(lam)
    return _gaussian_elimination_solve(xtx, xty)


def _fit_linear_trend(points: list[tuple[float, float]]) -> tuple[float, float] | None:
    """最小二乘直线 y = a + b*x。样本不足或退化时返回 None。"""
    if len(points) < 2:
        return None
    n = float(len(points))
    sx = sum(p[0] for p in points)
    sy = sum(p[1] for p in points)
    sxx = sum(p[0] * p[0] for p in points)
    sxy = sum(p[0] * p[1] for p in points)
    den = (n * sxx) - (sx * sx)
    if abs(den) < 1e-12:
        return None
    b = ((n * sxy) - (sx * sy)) / den
    a = (sy - (b * sx)) / n
    return a, b


def _fetch_order_item_rows_and_sales_by_week(
    conn,
    *,
    store_id: int | None,
    purchase_day_from: date,
    purchase_day_to: date,
) -> tuple[dict[str, int], dict[str, float]]:
    """周键为周一 YYYY-MM-DD；销售额为 ``SUM(COALESCE(total_amount,0))``（排除已取消订单行）。"""
    day_expr = _order_item_purchase_day_expr("oi")
    store_sql = ""
    bind: dict[str, object] = {
        "oi_from": purchase_day_from.strftime("%Y-%m-%d"),
        "oi_to": purchase_day_to.strftime("%Y-%m-%d"),
    }
    if store_id is not None:
        store_sql = " AND oi.store_id = :store_id"
        bind["store_id"] = int(store_id)
    sql = text(
        f"""
        SELECT
            DATE_SUB({day_expr}, INTERVAL WEEKDAY({day_expr}) DAY) AS week_start,
            COUNT(*) AS n_order_item_rows,
            COALESCE(SUM(COALESCE(oi.total_amount, 0)), 0) AS order_item_sales
        FROM order_item oi
        WHERE oi.purchase_utc_date IS NOT NULL
          AND COALESCE(oi.order_status, '') != 'Canceled'
          AND {day_expr} >= :oi_from
          AND {day_expr} <= :oi_to
          {store_sql}
        GROUP BY week_start
        """
    )
    rows: dict[str, int] = {}
    sales: dict[str, float] = {}
    for row in conn.execute(sql, bind).mappings().all():
        ws = row["week_start"]
        if ws is None:
            continue
        if isinstance(ws, datetime):
            ws = ws.date()
        key = ws.isoformat()
        rows[key] = int(row["n_order_item_rows"] or 0)
        sales[key] = float(row["order_item_sales"] or 0.0)
    return rows, sales


def _returns_agg_by_request_week(rows: list) -> dict[str, dict[str, float | int]]:
    acc: dict[str, dict[str, float | int]] = {}
    for r in rows:
        wk = r["request_week_start"]
        if wk is None:
            continue
        if isinstance(wk, datetime):
            wk = wk.date()
        key = wk.isoformat()
        it = acc.setdefault(
            key,
            {
                "request_rows": 0,
                "actual_refund_rows": 0,
                "request_order_amount": 0.0,
                "actual_refund_amount": 0.0,
            },
        )
        it["request_rows"] = int(it["request_rows"]) + 1
        it["request_order_amount"] = float(it["request_order_amount"]) + float(r["order_amount"] or 0)
        ar = int(r["actual_refund"] or 0)
        it["actual_refund_rows"] = int(it["actual_refund_rows"]) + ar
        if ar == 1:
            it["actual_refund_amount"] = float(it["actual_refund_amount"]) + float(r.get("order_amount") or 0.0)
    return acc


def _week_max_request_date(rows: list) -> dict[str, date]:
    mx: dict[str, date] = {}
    for r in rows:
        wk = r["request_week_start"]
        if wk is None:
            continue
        if isinstance(wk, datetime):
            wk = wk.date()
        key = wk.isoformat()
        rd = r["request_date"]
        if isinstance(rd, datetime):
            rd = rd.date()
        if key not in mx or rd > mx[key]:
            mx[key] = rd
    return mx


def _recent_week_monday_keys(start: date, end: date) -> list[str]:
    cur = _monday_of_date_py(start)
    out: list[str] = []
    while cur <= end:
        out.append(cur.isoformat())
        cur += timedelta(days=7)
    return out


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
                DATE_SUB(
                    COALESCE(r.order_date, r.request_date),
                    INTERVAL WEEKDAY(COALESCE(r.order_date, r.request_date)) DAY
                ) AS order_week_start,
                r.amazon_order_id,
                COALESCE(r.order_amount, 0) AS order_amount,
                CASE
                    WHEN r.order_date IS NOT NULL AND r.request_date >= r.order_date THEN
                        GREATEST(0, TIMESTAMPDIFF(WEEK, r.order_date, r.request_date))
                    ELSE 0
                END AS lag_week,
                CASE
                    WHEN COALESCE(CAST(r.state AS SIGNED), 0) = 5
                         OR LOCATE('-', COALESCE(r.track_status, '')) > 0
                    THEN 1
                    ELSE 0
                END AS actual_refund
            FROM order_return r
            {where_sql}
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
        if all_rows:
            earliest_req_monday = min(
                (r["request_week_start"].date() if isinstance(r["request_week_start"], datetime) else r["request_week_start"])
                for r in all_rows
                if r["request_week_start"] is not None
            )
        else:
            earliest_req_monday = _monday_of_date_py(recent_start)
        oi_from = min(earliest_req_monday, _monday_of_date_py(recent_start))
        oi_rows_by_week, oi_sales_by_week = _fetch_order_item_rows_and_sales_by_week(
            conn, store_id=store_id, purchase_day_from=oi_from, purchase_day_to=recent_end
        )

    historical_rows = [r for r in all_rows if r["request_date"] <= mature_end]
    recent_rows = [r for r in all_rows if recent_start <= r["request_date"] <= recent_end]
    historical_start_date = min((r["request_date"] for r in historical_rows), default=None)
    historical_end_date = max((r["request_date"] for r in historical_rows), default=None)

    historical_request_rows = len(historical_rows)
    recent_request_rows = len(recent_rows)
    historical_actual_refund_rows = int(sum(int(r["actual_refund"] or 0) for r in historical_rows))
    observed_actual_refund_rows = int(sum(int(r["actual_refund"] or 0) for r in recent_rows))
    observed_actual_refund_amount = float(
        sum(float(r.get("order_amount") or 0) for r in recent_rows if int(r["actual_refund"] or 0) == 1)
    )

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

    hist_by_week = _returns_agg_by_request_week(historical_rows)
    week_max_req_raw = _week_max_request_date(all_rows)

    def _as_date_val(v: date | datetime) -> date:
        return v.date() if isinstance(v, datetime) else v

    mature_week_keys = {wk for wk, mx in week_max_req_raw.items() if _as_date_val(mx) <= mature_end}

    ridge_samples: list[tuple[list[float], float]] = []
    sum_mature_refund_amt = 0.0
    sum_mature_oi_sales = 0.0
    mature_week_pen_amt: dict[str, float] = {}
    prev_pen_amt = 0.0
    for wk in sorted(mature_week_keys):
        oi_sales = float(oi_sales_by_week.get(wk, 0.0))
        if oi_sales <= 0.0:
            continue
        agg = hist_by_week.get(
            wk,
            {"request_rows": 0, "actual_refund_rows": 0, "request_order_amount": 0.0, "actual_refund_amount": 0.0},
        )
        ref_amt = float(agg["actual_refund_amount"])
        req_amt = float(agg["request_order_amount"])
        y = min(1.0, _rate(ref_amt, oi_sales))
        pen_amt = _rate(req_amt, oi_sales)
        momentum_pen_amt = pen_amt - prev_pen_amt
        ridge_samples.append(([1.0, pen_amt, momentum_pen_amt, math.log1p(oi_sales)], y))
        mature_week_pen_amt[wk] = pen_amt
        prev_pen_amt = pen_amt
        sum_mature_refund_amt += ref_amt
        sum_mature_oi_sales += oi_sales

    ridge_w = _ridge_train_order_level_weekly(ridge_samples, lam=2.0)
    fallback_hist_order_level_rate = (
        _rate(sum_mature_refund_amt, sum_mature_oi_sales) if sum_mature_oi_sales > 0.0 else global_hist_rate
    )
    mature_week_sorted = sorted(mature_week_keys)
    month_rate_by_ym: dict[str, float] = {}
    # 先累计分子分母，随后再转比例（按月聚合 mature 周）。
    month_refund_sum: dict[str, float] = {}
    month_sales_sum: dict[str, float] = {}
    for wk in mature_week_sorted:
        oi_sales = float(oi_sales_by_week.get(wk, 0.0))
        if oi_sales <= 0.0:
            continue
        agg = hist_by_week.get(
            wk,
            {"request_rows": 0, "actual_refund_rows": 0, "request_order_amount": 0.0, "actual_refund_amount": 0.0},
        )
        ym = wk[:7]
        month_refund_sum[ym] = float(month_refund_sum.get(ym, 0.0)) + float(agg["actual_refund_amount"])
        month_sales_sum[ym] = float(month_sales_sum.get(ym, 0.0)) + oi_sales
    for ym, s in month_sales_sum.items():
        month_rate_by_ym[ym] = _rate(float(month_refund_sum.get(ym, 0.0)), float(s))
    sorted_month_keys = sorted(month_rate_by_ym.keys())

    def _prior_from_prev_months(ym: str, *, n_months: int = 3) -> float:
        if ym not in sorted_month_keys:
            # 若当月不在成熟历史中，则取截至该月前最近 n 个月
            candidates = [m for m in sorted_month_keys if m < ym]
        else:
            idx = sorted_month_keys.index(ym)
            candidates = sorted_month_keys[:idx]
        if not candidates:
            return float(fallback_hist_order_level_rate)
        tail = candidates[-max(1, int(n_months)) :]
        vals = [float(month_rate_by_ym[m]) for m in tail]
        return sum(vals) / float(len(vals))

    all_week_keys_sorted = sorted(set(mature_week_keys) | set(_recent_week_monday_keys(recent_start, recent_end)))
    week_idx_map = {wk: idx for idx, wk in enumerate(all_week_keys_sorted)}
    trend_points: list[tuple[float, float]] = []
    for wk in sorted(mature_week_keys):
        oi_sales = float(oi_sales_by_week.get(wk, 0.0))
        if oi_sales <= 0.0:
            continue
        agg = hist_by_week.get(
            wk,
            {"request_rows": 0, "actual_refund_rows": 0, "request_order_amount": 0.0, "actual_refund_amount": 0.0},
        )
        y = _rate(float(agg["actual_refund_amount"]), oi_sales)
        trend_points.append((float(week_idx_map.get(wk, 0)), y))
    trend_ab = _fit_linear_trend(trend_points)

    def _trend_rate_for_week(wk: str) -> float:
        if trend_ab is None:
            return float(fallback_hist_order_level_rate)
        a, b = trend_ab
        x = float(week_idx_map.get(wk, 0))
        y = a + (b * x)
        return min(max(y, 0.0), 1.0)

    predicted_recent_actual_refund_rows = 0.0
    weekly_acc: dict[str, dict[str, object]] = {}
    for r in recent_rows:
        lag = int(r["lag_week"] or 0)
        amount = float(r["order_amount"] or 0)
        p_ml = _predict_logistic_prob(lag, amount, weights=weights, means=means, stds=stds)
        p_bayes = lag_smoothed_rate.get(lag, default_smoothed_rate)
        p = (ml_weight * p_ml) + ((1.0 - ml_weight) * p_bayes)
        p = min(max(p, 0.0), 1.0)
        actual_flag = int(r["actual_refund"] or 0)
        # 最近窗口纯预测：不使用近期真实值修正 p。
        p_final = p
        predicted_recent_actual_refund_rows += p_final

        wk_raw = r["request_week_start"]
        wk_d = wk_raw.date() if isinstance(wk_raw, datetime) else wk_raw
        wk = wk_d.isoformat()
        it = weekly_acc.setdefault(
            wk,
            {
                "request_week_start": wk_d,
                "request_week_end": wk_d + timedelta(days=6),
                "request_rows": 0,
                "request_orders_set": set(),
                "purchase_weeks_set": set(),
                "observed_actual_refund_rows": 0,
                "observed_refund_orders_set": set(),
                "request_amount": 0.0,
                "observed_actual_refund_amount": 0.0,
                "predicted_actual_refund_rows": 0.0,
            },
        )
        it["request_rows"] = int(it["request_rows"]) + 1
        if r["amazon_order_id"] is not None and str(r["amazon_order_id"]).strip():
            order_id = str(r["amazon_order_id"]).strip()
            it["request_orders_set"].add(order_id)
            if int(r["actual_refund"] or 0) == 1:
                it["observed_refund_orders_set"].add(order_id)
        ow = r["order_week_start"]
        if ow is not None:
            od = ow.date() if isinstance(ow, datetime) else ow
            it["purchase_weeks_set"].add(od.isoformat())
        it["observed_actual_refund_rows"] = int(it["observed_actual_refund_rows"]) + actual_flag
        if actual_flag == 1:
            it["observed_actual_refund_amount"] = float(it["observed_actual_refund_amount"]) + float(
                r.get("order_amount") or 0
            )
        it["request_amount"] = float(it["request_amount"]) + amount
        it["predicted_actual_refund_rows"] = float(it["predicted_actual_refund_rows"]) + p_final

    historical_row_rate = _rate(historical_actual_refund_rows, historical_request_rows)
    historical_order_rate = _rate(historical_actual_refund_orders, historical_request_orders)
    observed_recent_row_rate = _rate(observed_actual_refund_rows, recent_request_rows)
    observed_recent_order_rate = _rate(observed_actual_refund_orders, recent_request_orders)
    predicted_recent_actual_refund_orders = recent_request_orders * historical_order_rate
    predicted_additional_refund_rows = 0.0
    predicted_additional_refund_orders = 0.0

    def _week_intersects_recent(ws: date, we: date) -> bool:
        return we >= recent_start and ws <= recent_end

    def _pred_amount_ridge(pen_amt: float, momentum_pen_amt: float, oi_sales: float) -> float:
        if oi_sales <= 0.0 or ridge_w is None:
            return float(fallback_hist_order_level_rate)
        y_hat = ridge_w[0] + ridge_w[1] * pen_amt + ridge_w[2] * momentum_pen_amt + ridge_w[3] * math.log1p(oi_sales)
        return min(max(y_hat, 0.0), 1.0)

    recent_week_keys_set = set(_recent_week_monday_keys(recent_start, recent_end))
    order_item_rows_window_total = sum(int(oi_rows_by_week.get(k, 0)) for k in recent_week_keys_set)
    order_item_sales_window_total = sum(float(oi_sales_by_week.get(k, 0.0)) for k in recent_week_keys_set)

    week_keys_out = sorted(
        set(weekly_acc.keys()) | {kk for kk in recent_week_keys_set if float(oi_sales_by_week.get(kk, 0.0)) > 0.0}
    )

    weekly_series: list[dict] = []
    pred_order_level_weighted_num = 0.0
    pred_order_level_weighted_den = 0.0
    prev_week_pred_order_level = float(fallback_hist_order_level_rate)
    prev_week_observed_order_level = float(fallback_hist_order_level_rate)
    for k in week_keys_out:
        row = weekly_acc.get(k)
        if row is None:
            wk_d = date.fromisoformat(k)
            row = {
                "request_week_start": wk_d,
                "request_week_end": wk_d + timedelta(days=6),
                "request_rows": 0,
                "request_orders_set": set(),
                "purchase_weeks_set": set(),
                "observed_actual_refund_rows": 0,
                "observed_refund_orders_set": set(),
                "request_amount": 0.0,
                "observed_actual_refund_amount": 0.0,
                "predicted_actual_refund_rows": 0.0,
            }
        ws = row["request_week_start"]
        if isinstance(ws, datetime):
            ws = ws.date()
        we = row["request_week_end"]
        if isinstance(we, datetime):
            we = we.date()
        if not _week_intersects_recent(ws, we):
            continue

        request_rows = int(row["request_rows"] or 0)
        request_orders = len(row["request_orders_set"])
        observed_rows = int(row["observed_actual_refund_rows"] or 0)
        observed_orders = len(row["observed_refund_orders_set"])
        predicted_rows = float(row["predicted_actual_refund_rows"] or 0)
        oi_rows = int(oi_rows_by_week.get(k, 0))
        oi_sales = float(oi_sales_by_week.get(k, 0.0))
        req_amt = float(row["request_amount"] or 0)
        ref_amt_week = float(row.get("observed_actual_refund_amount") or 0.0)

        pen_amt = _rate(req_amt, oi_sales) if oi_sales > 0.0 else 0.0
        prev_week_key = (ws - timedelta(days=7)).isoformat() if ws is not None else ""
        prev_pen_amt_recent = 0.0
        if prev_week_key in weekly_acc:
            prev_row = weekly_acc[prev_week_key]
            prev_req_amt = float(prev_row.get("request_amount") or 0.0)
            prev_oi_sales = float(oi_sales_by_week.get(prev_week_key, 0.0))
            prev_pen_amt_recent = _rate(prev_req_amt, prev_oi_sales) if prev_oi_sales > 0.0 else 0.0
        else:
            prev_pen_amt_recent = float(mature_week_pen_amt.get(prev_week_key, 0.0))
        momentum_pen_amt = pen_amt - prev_pen_amt_recent
        pred_order_level = _pred_amount_ridge(pen_amt, momentum_pen_amt, oi_sales)
        ym = ws.strftime("%Y-%m") if ws is not None else ""
        month_prior_rate = _prior_from_prev_months(ym, n_months=3) if ym else float(fallback_hist_order_level_rate)
        # 先向“前几个月月度退货率”收敛，再按离今天远近调整波动。
        pred_order_level = (0.7 * pred_order_level) + (0.3 * month_prior_rate)
        days_to_today = max((today - ws).days if ws is not None else 0, 0)
        recency = 1.0 - min(max(days_to_today / float(max(lookback_days, 1)), 0.0), 1.0)
        vol_scale = 0.75 + (0.7 * recency)  # 近周≈1.45，远周≈0.75
        pred_order_level = month_prior_rate + ((pred_order_level - month_prior_rate) * vol_scale)
        pred_order_level = min(max(pred_order_level, 0.0), 1.0)
        observed_order_level_amt_rate = _rate(ref_amt_week, oi_sales) if oi_sales > 0.0 else 0.0
        # 真实值高权重融合：先基于表内已观测金额退货率，再由模型补充未完成部分。
        # 有已观测值时权重 75%~85%（观察期更久权重更高）；无已观测值时退化为模型值。
        days_since_week_end = (today - we).days if we is not None else 0
        if observed_order_level_amt_rate > 0.0:
            observed_weight = 0.85 if days_since_week_end >= 14 else 0.75
            pred_order_level = (observed_weight * observed_order_level_amt_rate) + (
                (1.0 - observed_weight) * pred_order_level
            )
            pred_order_level = min(max(pred_order_level, 0.0), 1.0)
        # 增大上一周预测值对当周预测的影响（时间连续性约束）。
        prev_pred_weight = 0.6 if days_since_week_end <= 21 else 0.4
        pred_order_level = (prev_pred_weight * prev_week_pred_order_level) + (
            (1.0 - prev_pred_weight) * pred_order_level
        )
        # 2026-04-20 之后提高最近周权重：结合“上一周真实值+上一周预测值”进一步抬升近周预测。
        if ws is not None and ws >= date(2026, 4, 20):
            recent_mix = 0.65 * prev_week_observed_order_level + 0.35 * prev_week_pred_order_level
            # 越逼近今天，越提高 recent_mix 权重（动态权重）。
            days_to_today = max((today - ws).days, 0)
            proximity = 1.0 - min(max(days_to_today / float(max(lookback_days, 1)), 0.0), 1.0)
            recent_boost_weight = 0.45 + (0.35 * proximity)  # [0.45, 0.80]
            pred_order_level = ((1.0 - recent_boost_weight) * pred_order_level) + (
                recent_boost_weight * recent_mix
            )
        # 基于历史真实退货率拟合回归线：让近期预测围绕回归线动态波动。
        wk_key = ws.isoformat() if ws is not None else ""
        trend_rate = _trend_rate_for_week(wk_key) if wk_key else float(fallback_hist_order_level_rate)
        days_to_today = max((today - ws).days if ws is not None else 0, 0)
        proximity = 1.0 - min(max(days_to_today / float(max(lookback_days, 1)), 0.0), 1.0)
        band = 0.015 + (0.06 * proximity)  # 越近允许越大波动带
        swing_scale = 0.8 + (0.5 * proximity)  # 越近对偏离放大越明显
        pred_order_level = trend_rate + ((pred_order_level - trend_rate) * swing_scale)
        pred_order_level = min(max(pred_order_level, trend_rate - band), trend_rate + band)
        pred_order_level = min(max(pred_order_level, 0.0), 1.0)
        # 硬约束：预测值不得低于当前表内已观测金额退货率。
        # 业务含义：未来累计退货金额只会增加，不会回退。
        pred_order_level = max(pred_order_level, observed_order_level_amt_rate)
        # 若已有真实值，预测需高于真实值（留出未来继续发生退货的增量空间）。
        if observed_order_level_amt_rate > 0.0:
            pred_order_level = max(pred_order_level, observed_order_level_amt_rate * 1.02)
        pred_order_level = min(max(pred_order_level, 0.0), 1.0)
        prev_week_pred_order_level = pred_order_level
        prev_week_observed_order_level = observed_order_level_amt_rate
        if oi_sales > 0.0:
            pred_order_level_weighted_num += pred_order_level * oi_sales
            pred_order_level_weighted_den += oi_sales

        predicted_refund_amount_week = pred_order_level * oi_sales if oi_sales > 0.0 else 0.0
        weekly_series.append(
            {
                "request_week_start": ws.isoformat() if ws is not None else None,
                "request_week_end": we.isoformat() if we is not None else None,
                "request_rows": request_rows,
                "request_orders": request_orders,
                "order_item_rows": oi_rows,
                "order_item_sales": _quantize_num(oi_sales),
                "purchase_weeks": len(row["purchase_weeks_set"]),
                "observed_actual_refund_rows": observed_rows,
                "observed_actual_refund_orders": observed_orders,
                "request_amount": _quantize_num(row["request_amount"] or 0),
                "observed_actual_refund_amount": _quantize_num(ref_amt_week),
                "observed_return_request_completion_rate_pct": (
                    _quantize_pct(_rate(float(observed_rows), float(request_rows)) * 100.0) if request_rows else None
                ),
                "observed_return_request_penetration_rate_pct": (
                    _quantize_pct(_rate(float(request_rows), float(oi_rows)) * 100.0) if oi_rows else None
                ),
                "observed_return_request_amount_penetration_pct": (
                    _quantize_pct(_rate(req_amt, oi_sales) * 100.0) if oi_sales > 0 else None
                ),
                "observed_order_level_return_row_rate_pct": (
                    _quantize_pct(_rate(float(observed_rows), float(oi_rows)) * 100.0) if oi_rows else None
                ),
                "observed_order_level_return_completion_rate_pct": (
                    _quantize_pct(_rate(ref_amt_week, oi_sales) * 100.0) if oi_sales > 0 else None
                ),
                "observed_actual_refund_rate": _quantize_pct(_rate(observed_rows, request_rows) * 100.0),
                "predicted_actual_refund_rows": _quantize_num(predicted_rows),
                "predicted_actual_refund_orders": _quantize_num(request_orders * historical_order_rate),
                "predicted_refund_amount": _quantize_num(predicted_refund_amount_week),
                "predicted_row_return_rate_pct": _quantize_pct(_rate(predicted_rows, request_rows) * 100.0),
                "predicted_return_rate": (
                    _quantize_pct(pred_order_level * 100.0) if oi_sales > 0 else None
                ),
                "predicted_order_level_return_completion_rate_pct": (
                    _quantize_pct(pred_order_level * 100.0) if oi_sales > 0 else None
                ),
            }
        )

    predicted_recent_order_level_return_rate = (
        _rate(pred_order_level_weighted_num, pred_order_level_weighted_den)
        if pred_order_level_weighted_den > 0.0
        else float(fallback_hist_order_level_rate)
    )
    observed_recent_order_level_return_rate = (
        _rate(observed_actual_refund_amount, order_item_sales_window_total)
        if order_item_sales_window_total > 0.0
        else 0.0
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

    ridge_weekly_model = {
        "type": "ridge_regression",
        "features": [
            "1",
            "SUM(return_request order_amount)/order_item_sales",
            "weekly_delta_penetration = 当前申请金额渗透 - 上周申请金额渗透",
            "log1p(order_item_sales)",
        ],
        "target": "min(1, SUM(order_amount on completed returns)/order_item_sales) per mature request_week; 销售额为当周 PST 购买周 SUM(order_item.total_amount)",
        "mature_weeks_in_regression": len(ridge_samples),
        "lambda": 2.0,
        "weights": [_quantize_num(float(w)) for w in ridge_w] if ridge_w else None,
        "fallback_order_level_rate_pct": _quantize_pct(fallback_hist_order_level_rate * 100.0),
    }

    return {
        "as_of_date": today.isoformat(),
        "store_id": int(store_id) if store_id is not None else None,
        "lookback_days": int(lookback_days),
        "ml_model": "row: logistic_regression(lag_week, log1p(order_amount)); week: ridge(1, request_amount/sales, weekly_delta_penetration, log1p(sales)) -> completed_return_order_amount/sales",
        "ridge_weekly_model": ridge_weekly_model,
        "ml_weight": _quantize_num(ml_weight),
        "bayes_prior_strength": _quantize_num(bayes_prior_strength),
        "recent_start_date": recent_start.isoformat(),
        "recent_end_date": recent_end.isoformat(),
        "mature_end_date": mature_end.isoformat(),
        "historical_train_start_date": historical_start_date.isoformat() if historical_start_date else None,
        "historical_train_end_date": historical_end_date.isoformat() if historical_end_date else None,
        "refund_rule": "CAST(state AS SIGNED)=5 OR LOCATE('-', COALESCE(track_status,''))>0；含 order_date 为空或 request_date<order_date 的请求（lag_week=0）",
        "prediction_basis": "主预测口径为金额：周级使用 order_item 当周销售额（SUM total_amount，非 Canceled）与 order_return 同 request_week 对齐，成熟周上岭回归预测「已退金额/当周销售额」，特征含申请侧 order_amount 渗透、每周申请金额渗透动量（本周-上周）与 log1p(销售额)；并加入时间机制：当月预测先参考前 3 个月月度退货率，再按离今天远近调整波动（越近波动越高，越远越低）。最终预测采用“真实值高权重融合”（有已观测金额退货率时占 75%~85%，模型补充其余）+“上一周预测值连续性融合”（近周权重更高）；另外对 2026-04-20 之后周额外强化最近周影响，联合上一周真实值与预测值加权提升近周预测，且该增强权重随时间逼近动态提高。并基于历史真实退货率拟合周级回归线，使近期预测围绕回归线上下波动（越近波动带越宽）。且对最近45天每一周施加硬约束：预测值不低于该周已观测金额退货率，并在有真实值时至少高于真实值 2%。行级 logistic+Bayes 仅保留为辅助对照（row_rate）。",
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
            "order_item_rows_recent": int(order_item_rows_window_total),
            "order_item_sales_recent": _quantize_num(order_item_sales_window_total),
            "observed_actual_refund_amount": _quantize_num(observed_actual_refund_amount),
            "observed_actual_refund_rows": observed_actual_refund_rows,
            "observed_actual_refund_orders": observed_actual_refund_orders,
            "observed_recent_actual_refund_row_rate": _quantize_pct(observed_recent_row_rate * 100.0),
            "observed_recent_actual_refund_order_rate": _quantize_pct(observed_recent_order_rate * 100.0),
            "observed_recent_order_level_return_completion_rate_pct": _quantize_pct(
                observed_recent_order_level_return_rate * 100.0
            ),
            "predicted_recent_actual_refund_rows": _quantize_num(predicted_recent_actual_refund_rows),
            "predicted_recent_actual_refund_orders": _quantize_num(predicted_recent_actual_refund_orders),
            "predicted_additional_refund_rows": _quantize_num(predicted_additional_refund_rows),
            "predicted_additional_refund_orders": _quantize_num(predicted_additional_refund_orders),
            "predicted_recent_refund_amount": _quantize_num(
                predicted_recent_order_level_return_rate * order_item_sales_window_total
            ),
            "predicted_recent_row_return_rate_pct": _quantize_pct(
                _rate(predicted_recent_actual_refund_rows, recent_request_rows) * 100.0
            ),
            "predicted_recent_return_rate": _quantize_pct(predicted_recent_order_level_return_rate * 100.0),
            "predicted_recent_order_level_return_completion_rate_pct": _quantize_pct(
                predicted_recent_order_level_return_rate * 100.0
            ),
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
