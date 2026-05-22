"""
按 invoice_date 汇总一段时间内的销售/毛利/退货数据。

数据来源（online DB）：
- ``order_profit``：按 ``invoice_date`` 过滤，汇总 ``net_revenue * qty`` / ``gross_profit * qty``；
- ``order_item``：作为订单桥接表，使用 ``amazon_order_id``；
- ``order_return``：对筛出的订单，统计 ``is_refund = 1`` 或 ``track_status = '-'`` 的退货金额。

口径：
- 销售金额 = SUM(order_profit.net_revenue )
- 毛利 = SUM(order_profit.gross_profit )
- 毛利率（不含退货） = 毛利 / 销售金额
- 退货金额 = SUM(order_return.refund_amount * 对应订单 exchange_rate)
- 退货率 = 退货金额 / 销售金额
- 毛利率（包含退货） = (毛利 - 退货金额) / 销售金额

退货相关指标（退货金额 / 退货率 / 退货订单数 / 含退货毛利率）只统计「当前日期前 45 天」的成熟数据，
避免展示仍在退货窗口内的实时值。

默认汇总全部店铺；传 ``store_id`` 时仅统计该店铺。

用法（backend 目录）：
  python3.11 -m app.services.weekly_profit --start-date 2026-04-01 --end-date 2026-04-15
  python3.11 -m app.services.weekly_profit --start-date 2026-04-01 --end-date 2026-04-15 --store-id 7
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import text

from app.config import settings
from app.online_engine import get_online_reporting_engine
from app.services.predict_return_rate import predict_recent_return_rate

DEFAULT_PROFIT_START = date(2026, 2, 23)


def _parse_ymd(raw: str) -> date:
    return datetime.strptime(str(raw).strip(), "%Y-%m-%d").date()


def _to_decimal(val) -> Decimal:
    if val is None:
        return Decimal("0")
    if isinstance(val, Decimal):
        return val
    return Decimal(str(val))


def _pct(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator == 0:
        return Decimal("0")
    return (numerator / denominator) * Decimal("100")


def _quantize_money(val: Decimal) -> float:
    return float(val.quantize(Decimal("0.01")))


def _quantize_pct(val: Decimal) -> float:
    return float(val.quantize(Decimal("0.01")))


def _mature_return_end(end_date: date) -> date:
    """退货相关指标只统计当前日期前 45 天的成熟区间。"""
    return min(end_date, _today_pst_date() - timedelta(days=45))


def _today_pst_date() -> date:
    """以 PST（固定 -07:00，与本模块 SQL 口径一致）计算当前日历日。"""
    pst_tz = timezone(timedelta(hours=-7))
    return datetime.now(timezone.utc).astimezone(pst_tz).date()


def _fetch_recent_predicted_return_rate_by_week(
    *,
    as_of_date: date,
    store_id: int | None,
    lookback_days: int = 45,
) -> dict[str, float]:
    """
    拉取 predict_return_rate 的周预测退货率（金额口径，百分比）。
    """
    pred = predict_recent_return_rate(
        lookback_days=int(lookback_days),
        store_id=store_id,
        as_of_date=as_of_date,
    )
    out: dict[str, float] = {}
    for row in pred.get("weekly_series", []) if isinstance(pred, dict) else []:
        wk = row.get("request_week_start")
        rt = row.get("predicted_return_rate")
        if wk is None or rt is None:
            continue
        try:
            out[str(wk)] = float(rt)
        except (TypeError, ValueError):
            continue
    return out


def _build_scoped_profit_parts(
    start_date: date,
    end_date: date,
    store_id: int | None = None,
) -> tuple[dict[str, object], str]:
    if start_date > end_date:
        raise ValueError("start_date 不能晚于 end_date")
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        raise ValueError("Online DB 未配置：需设置 online_db_host, online_db_user 等")

    params: dict[str, object] = {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
    }
    store_filter = ""
    if store_id is not None:
        params["store_id"] = int(store_id)
        store_filter = " AND op.store_id = :store_id"
    return params, store_filter


def fetch_profit_latest_invoice_date() -> date:
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        raise ValueError("Online DB 未配置：需设置 online_db_host, online_db_user 等")

    sql = text("SELECT MAX(invoice_date) AS max_invoice_date FROM order_profit")
    with get_online_reporting_engine().connect() as conn:
        row = conn.execute(sql).fetchone()
    raw = row[0] if row else None
    if raw is None:
        return DEFAULT_PROFIT_START
    if isinstance(raw, datetime):
        return raw.date()
    return raw


def fetch_profit_store_ids(
    start_date: date,
    end_date: date,
) -> list[int]:
    params, _ = _build_scoped_profit_parts(start_date, end_date, None)
    sql = text(
        """
        SELECT DISTINCT op.store_id
        FROM order_profit op
        WHERE op.invoice_date >= :start_date
          AND op.invoice_date <= :end_date
        ORDER BY op.store_id ASC
        """
    )
    with get_online_reporting_engine().connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [int(r[0]) for r in rows if r and r[0] is not None]


def fetch_profit_summary(
    start_date: date,
    end_date: date,
    store_id: int | None = None,
) -> dict:
    params, store_filter = _build_scoped_profit_parts(start_date, end_date, store_id)
    mature_end = _mature_return_end(end_date)

    profit_sql = text(
        f"""
        SELECT
            COUNT(DISTINCT op.order_id) AS order_count,
            COALESCE(SUM(COALESCE(op.net_revenue, 0) ), 0) AS sales_amount,
            COALESCE(SUM(COALESCE(op.gross_profit, 0) ), 0) AS gross_profit
        FROM order_profit op
        WHERE op.invoice_date >= :start_date
          AND op.invoice_date <= :end_date
          AND op.order_id IS NOT NULL
          AND op.order_id <> ''
          {store_filter}
        """
    )

    with get_online_reporting_engine().connect() as conn:
        profit_row = conn.execute(profit_sql, params).mappings().one()
        if start_date <= mature_end:
            mature_params, mature_store_filter = _build_scoped_profit_parts(start_date, mature_end, store_id)
            mature_profit_sql_run = text(
                f"""
                SELECT
                    COALESCE(SUM(COALESCE(op.net_revenue, 0)), 0) AS sales_amount,
                    COALESCE(SUM(COALESCE(op.gross_profit, 0)), 0) AS gross_profit
                FROM order_profit op
                WHERE op.invoice_date >= :start_date
                  AND op.invoice_date <= :end_date
                  AND op.order_id <> ''
                  {mature_store_filter}
                """
            )
            return_sql_run = text(
                f"""
                WITH scoped_orders AS (
                    SELECT
                        op.store_id,
                        op.order_id AS amazon_order_id,
                        MAX(COALESCE(op.exchange_rate, 1)) AS exchange_rate
                    FROM order_profit op
                    WHERE op.invoice_date >= :start_date
                      AND op.invoice_date <= :end_date
                      AND op.order_id <> ''
                      {mature_store_filter}
                    GROUP BY op.store_id, op.order_id
                )
                SELECT
                    COUNT(DISTINCT r.amazon_order_id) AS returned_order_count,
                    COUNT(*) AS return_row_count,
                    COALESCE(SUM(COALESCE(r.refund_amount, 0) * COALESCE(scoped_items.exchange_rate, 1)), 0) AS refund_amount
                FROM order_return r
                INNER JOIN (
                    SELECT DISTINCT oi.store_id, oi.amazon_order_id, so.exchange_rate
                    FROM order_item oi
                    INNER JOIN scoped_orders so
                        ON so.store_id = oi.store_id
                       AND so.amazon_order_id = oi.amazon_order_id
                    WHERE oi.amazon_order_id <> ''
                ) scoped_items
                    ON scoped_items.store_id = r.store_id
                   AND scoped_items.amazon_order_id = r.amazon_order_id
                WHERE COALESCE(r.is_refund, 0) = 1
                   OR COALESCE(r.track_status, '') = '-'
                """
            )
            mature_profit_row = conn.execute(mature_profit_sql_run, mature_params).mappings().one()
            return_row = conn.execute(return_sql_run, mature_params).mappings().one()
        else:
            mature_profit_row = {"sales_amount": Decimal("0"), "gross_profit": Decimal("0")}
            return_row = {
                "returned_order_count": 0,
                "return_row_count": 0,
                "refund_amount": Decimal("0"),
            }

    sales_amount = _to_decimal(profit_row["sales_amount"])
    gross_profit = _to_decimal(profit_row["gross_profit"])
    mature_sales_amount = _to_decimal(mature_profit_row["sales_amount"])
    mature_gross_profit = _to_decimal(mature_profit_row["gross_profit"])
    refund_amount = _to_decimal(return_row["refund_amount"])

    gross_margin_rate = _pct(gross_profit, sales_amount)
    return_rate = _pct(refund_amount, mature_sales_amount)
    gross_profit_after_return = mature_gross_profit - refund_amount
    gross_margin_after_return_rate = _pct(gross_profit_after_return, mature_sales_amount)

    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "store_id": int(store_id) if store_id is not None else None,
        "order_count": int(profit_row["order_count"] or 0),
        "returned_order_count": int(return_row["returned_order_count"] or 0),
        "return_row_count": int(return_row["return_row_count"] or 0),
        "sales_amount": _quantize_money(sales_amount),
        "refund_amount": _quantize_money(refund_amount),
        "gross_profit": _quantize_money(gross_profit),
        "gross_profit_after_return": _quantize_money(gross_profit_after_return),
        "gross_margin_rate": _quantize_pct(gross_margin_rate),
        "gross_margin_after_return_rate": _quantize_pct(gross_margin_after_return_rate),
        "return_rate": _quantize_pct(return_rate),
    }


def fetch_profit_weekly_series(
    start_date: date,
    end_date: date,
    store_id: int | None = None,
) -> list[dict]:
    params, store_filter = _build_scoped_profit_parts(start_date, end_date, store_id)
    params["mature_end_date"] = _mature_return_end(end_date).strftime("%Y-%m-%d")
    as_of_pst = _today_pst_date()
    # 业务分界：当前日期前 45 天截止到前一日（例如 2026-04-30 -> 2026-03-15 为真实截止）
    mature_curve_end = as_of_pst - timedelta(days=46)
    predicted_by_week: dict[str, float] = {}
    try:
        predicted_by_week = _fetch_recent_predicted_return_rate_by_week(
            as_of_date=as_of_pst,
            store_id=store_id,
            lookback_days=45,
        )
    except Exception:
        # 预测失败不影响主报表：降级为全真实曲线
        predicted_by_week = {}
    sql = text(
        f"""
        WITH scoped_profit AS (
            SELECT
                op.store_id,
                op.order_id,
                op.invoice_date,
                DATE_SUB(op.invoice_date, INTERVAL WEEKDAY(op.invoice_date) DAY) AS week_start,
                COALESCE(op.net_revenue, 0)  AS net_revenue,
                COALESCE(op.gross_profit, 0) AS gross_profit,
                COALESCE(op.exchange_rate, 1) AS exchange_rate
            FROM order_profit op
            WHERE op.invoice_date >= :start_date
              AND op.invoice_date <= :end_date
              AND op.order_id <> ''
              {store_filter}
        ),
        profit_by_week AS (
            SELECT
                sp.week_start,
                COUNT(DISTINCT sp.order_id) AS order_count,
                SUM(sp.net_revenue) AS sales_amount,
                SUM(sp.gross_profit) AS gross_profit
            FROM scoped_profit sp
            GROUP BY sp.week_start
        ),
        mature_scoped_profit AS (
            SELECT *
            FROM scoped_profit
            WHERE invoice_date <= :mature_end_date
        ),
        mature_profit_by_week AS (
            SELECT
                sp.week_start,
                SUM(sp.net_revenue) AS mature_sales_amount,
                SUM(sp.gross_profit) AS mature_gross_profit
            FROM mature_scoped_profit sp
            GROUP BY sp.week_start
        ),
        valid_orders AS (
            SELECT
                sp.store_id,
                sp.order_id AS amazon_order_id,
                MIN(sp.week_start) AS week_start,
                MAX(sp.exchange_rate) AS exchange_rate
            FROM mature_scoped_profit sp
            WHERE EXISTS (
                SELECT 1
                FROM order_item oi
                WHERE oi.store_id = sp.store_id
                  AND oi.amazon_order_id = sp.order_id
            )
            GROUP BY sp.store_id, sp.order_id
        ),
        refund_by_week AS (
            SELECT
                vo.week_start,
                COUNT(DISTINCT r.amazon_order_id) AS returned_order_count,
                COUNT(*) AS return_row_count,
                SUM(COALESCE(r.refund_amount, 0) * COALESCE(vo.exchange_rate, 1)) AS refund_amount
            FROM valid_orders vo
            INNER JOIN order_return r
                ON r.store_id = vo.store_id
               AND r.amazon_order_id = vo.amazon_order_id
            WHERE COALESCE(r.is_refund, 0) = 1
               OR COALESCE(r.track_status, '') = '-'
            GROUP BY vo.week_start
        )
        SELECT
            pbw.week_start AS week_start,
            DATE_ADD(pbw.week_start, INTERVAL 6 DAY) AS week_end,
            pbw.order_count AS order_count,
            COALESCE(rbw.returned_order_count, 0) AS returned_order_count,
            COALESCE(rbw.return_row_count, 0) AS return_row_count,
            COALESCE(pbw.sales_amount, 0) AS sales_amount,
            COALESCE(mpbw.mature_sales_amount, 0) AS mature_sales_amount,
            COALESCE(rbw.refund_amount, 0) AS refund_amount,
            COALESCE(pbw.gross_profit, 0) AS gross_profit,
            COALESCE(mpbw.mature_gross_profit, 0) AS mature_gross_profit
        FROM profit_by_week pbw
        LEFT JOIN mature_profit_by_week mpbw
            ON mpbw.week_start = pbw.week_start
        LEFT JOIN refund_by_week rbw
            ON rbw.week_start = pbw.week_start
        ORDER BY pbw.week_start ASC
        """
    )
    with get_online_reporting_engine().connect() as conn:
        rows = conn.execute(sql, params).mappings().all()

    out: list[dict] = []
    for r in rows:
        sales_amount = _to_decimal(r["sales_amount"])
        mature_sales_amount = _to_decimal(r["mature_sales_amount"])
        refund_amount_actual = _to_decimal(r["refund_amount"])
        gross_profit = _to_decimal(r["gross_profit"])
        mature_gross_profit = _to_decimal(r["mature_gross_profit"])
        gross_profit_after_return = mature_gross_profit - refund_amount_actual
        week_start_obj = r["week_start"]
        week_start_str = week_start_obj.isoformat() if week_start_obj is not None else None
        actual_rate = _quantize_pct(_pct(refund_amount_actual, mature_sales_amount))
        predicted_rate = predicted_by_week.get(week_start_str or "") if week_start_str else None
        week_end_obj = r["week_end"]
        use_predicted = bool(week_end_obj is not None and week_end_obj > mature_curve_end and predicted_rate is not None)

        # 退货金额：45 天前用真实退货额；45 天后按预测退货率估算整周退货额，并拆成「真实 + 预估补足」两段。
        # 若一周跨越成熟分界，refund_amount_actual 仅包含成熟部分，预估部分 = 预测整周退货额 - 真实退货额。
        refund_amount_predicted = Decimal("0")
        if use_predicted and predicted_rate is not None:
            try:
                pr = Decimal(str(predicted_rate)) / Decimal("100")
            except Exception:
                pr = Decimal("0")
            predicted_total = pr * sales_amount if sales_amount > 0 else Decimal("0")
            refund_amount_predicted = predicted_total - refund_amount_actual
            if refund_amount_predicted < 0:
                refund_amount_predicted = Decimal("0")
        refund_amount_total = refund_amount_actual + refund_amount_predicted

        # 展示用「含退货毛利率」：以当周净收益额为分母，退货金额使用真实+预估总额，
        # 用于前端在存在预估退货金额时仍能绘制曲线（不依赖 mature_* 字段）。
        gross_profit_after_return_display = gross_profit - refund_amount_total
        gross_margin_after_return_rate_display = _quantize_pct(_pct(gross_profit_after_return_display, sales_amount))

        display_rate = float(predicted_rate) if use_predicted else float(actual_rate)
        curve_type = "predicted" if use_predicted else "actual"
        curve_color = "#f59e0b" if use_predicted else "#8b5cf6"
        out.append(
            {
                "week_start": week_start_str,
                "week_end": r["week_end"].isoformat() if r["week_end"] is not None else None,
                "order_count": int(r["order_count"] or 0),
                "returned_order_count": int(r["returned_order_count"] or 0),
                "return_row_count": int(r["return_row_count"] or 0),
                "sales_amount": _quantize_money(sales_amount),
                "mature_sales_amount": _quantize_money(mature_sales_amount),
                # 兼容：refund_amount 为总退货金额（真实+预估）；另附带拆分字段用于前端堆叠展示
                "refund_amount": _quantize_money(refund_amount_total),
                "refund_amount_actual": _quantize_money(refund_amount_actual),
                "refund_amount_predicted": _quantize_money(refund_amount_predicted),
                "gross_profit": _quantize_money(gross_profit),
                "gross_profit_after_return": _quantize_money(gross_profit_after_return),
                "gross_profit_after_return_display": _quantize_money(gross_profit_after_return_display),
                "gross_margin_rate": _quantize_pct(_pct(gross_profit, sales_amount)),
                "gross_margin_after_return_rate": _quantize_pct(_pct(gross_profit_after_return, mature_sales_amount)),
                "gross_margin_after_return_rate_display": gross_margin_after_return_rate_display,
                "return_rate_actual": float(actual_rate),
                "return_rate_predicted": float(predicted_rate) if predicted_rate is not None else None,
                "return_rate_curve_type": curve_type,
                "return_rate_curve_color": curve_color,
                "return_rate": display_rate,
            }
        )
    return out


def fetch_profit_report(
    start_date: date,
    end_date: date,
    store_id: int | None = None,
) -> dict:
    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "store_id": int(store_id) if store_id is not None else None,
        "store_ids": fetch_profit_store_ids(start_date, end_date),
        "latest_invoice_date": fetch_profit_latest_invoice_date().isoformat(),
        "summary": fetch_profit_summary(start_date, end_date, store_id),
        "weekly_series": fetch_profit_weekly_series(start_date, end_date, store_id),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="汇总指定 invoice_date 区间内的销售/毛利/退货数据")
    parser.add_argument("--start-date", required=True, help="开始日期，格式 YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="结束日期，格式 YYYY-MM-DD")
    parser.add_argument("--store-id", type=int, default=None, help="可选：只统计指定店铺")
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="以更易读的 JSON 格式输出",
    )
    args = parser.parse_args()

    summary = fetch_profit_report(
        start_date=_parse_ymd(args.start_date),
        end_date=_parse_ymd(args.end_date),
        store_id=args.store_id,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2 if args.pretty else None))


if __name__ == "__main__":
    main()
