"""
从线上 amazon_ads_ad_group_ad_report 同步广告花费与多窗口销售额到本地 daily_ad_cost_sales，
报表中的 ASIN 列（按表结构自动识别：advertise_asin / ad_asin / asin 等）写入本地 **ad_asin**，
并通过 amazon_listing（asin + store_id）补齐 pid、variation_id。

线上报表日口径：
- 若存在列 ``date``，使用 ``DATE(COALESCE(r.`date`, r.`current_date`))``；
- 否则使用 ``DATE(r.`current_date`)``（与 weekly_upload 中广告汇总一致）。

用法（在 backend 目录下）：
  python3 -m app.services.daily_ad_cost_sales --start-date 2026-03-01 --end-date 2026-03-31
  python3 -m app.services.daily_ad_cost_sales
      # 无参：对比本地 purchase_date 与线上报表日，仅同步「线上有而本地尚无」的日期（默认回看窗口见 default_gap_day_bounds）
"""

from __future__ import annotations

import argparse
import logging
import sys
import threading
from datetime import date, datetime, timedelta
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.dialects.mysql import insert as mysql_insert

from app.config import settings
from app.database import SessionLocal, init_db
from app.logging_config import setup_logging
from app.models.daily_ad_cost_sales import DailyAdCostSales
from app.online_engine import get_online_engine
from app.sync_run_record import (
    record_daily_ad_cost_sales_run,
    should_run_daily_ad_cost_sales_sync,
)

logger = logging.getLogger(__name__)

# 本地尚无数据时，只从线上最近 N 个自然日拉取，避免首次全表扫过多年
_DEFAULT_FIRST_SYNC_SPAN_DAYS = 35

# order_item 按日聚合时，需多取 purchase_date 之前若干天，才能算 30 日滚动窗口合计
_ORDER_ITEM_LOOKBACK_DAYS = 29

# 线上报表 ASIN 列名因库而异，按顺序探测；取值经 TRIM 后写入本地 ad_asin
_REPORT_ASIN_COLUMN_CANDIDATES = (
    "advertise_asin",
    "ad_asin",
    "asin",
    "advertised_asin",
    "child_asin",
)


def _parse_ymd(s: str) -> date:
    s = str(s or "").strip()
    return datetime.strptime(s, "%Y-%m-%d").date()


def _cell_date(val) -> date | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    head = str(val).strip()[:10]
    if not head or head.startswith("0000-00"):
        return None
    try:
        return _parse_ymd(head)
    except ValueError:
        return None


def _dec(val) -> Decimal | None:
    if val is None:
        return None
    if isinstance(val, Decimal):
        return val
    try:
        return Decimal(str(val))
    except Exception:
        return None


def _to_decimal(val) -> Decimal | None:
    if val is None:
        return None
    if isinstance(val, Decimal):
        return val
    try:
        return Decimal(str(val))
    except Exception:
        return None


def _ratio_float(ad_cost: Decimal | None, sales: Decimal | None) -> float:
    """ad_cost / sales；分母为 0 或缺失时返回 0。"""
    if ad_cost is None or sales is None:
        return 0.0
    if sales == 0:
        return 0.0
    return float(ad_cost / sales)


def _tad_over_order_sum(ad_cost: Decimal | None, order_sum: Decimal) -> float:
    """ad_cost / order_sum；ad_cost 缺失或分母<=0 时返回 0。"""
    if ad_cost is None:
        return 0.0
    if order_sum <= 0:
        return 0.0
    return float(ad_cost / order_sum)


def _sum_order_amount_window(
    order_by_day_store_asin: dict[tuple[date, int, str], Decimal],
    pday: date,
    store_id: int,
    asin: str,
    n_calendar_days: int,
) -> Decimal:
    """
    同店同 ASIN：DATE(purchase_utc_date) 落在 [pday-(n-1), pday]（含端点、共 n 个日历日）的 order_item total_amount 之和。
    """
    total = Decimal(0)
    for i in range(max(1, n_calendar_days)):
        d = pday - timedelta(days=i)
        total += order_by_day_store_asin.get((d, store_id, asin), Decimal(0))
    return total


def _fetch_order_totals_by_day_store_asin(
    online_conn,
    low: date,
    high: date,
) -> dict[tuple[date, int, str], Decimal]:
    """
    order_item：与报表日对齐的「店铺 + ASIN + 日」SUM(total_amount)，order_status!=Canceled。
    键 (DATE(purchase_utc_date), store_id, TRIM(asin))。
    """
    rows = online_conn.execute(
        text(
            """
            SELECT DATE(oi.purchase_utc_date) AS d, oi.store_id,
                   oi.asin AS asin,
                   SUM(COALESCE(oi.total_amount, 0)) AS amt
            FROM order_item oi
            WHERE oi.order_status != 'Canceled'
              AND oi.purchase_utc_date IS NOT NULL
              AND DATE(oi.purchase_utc_date) BETWEEN :a AND :b
              AND oi.asin IS NOT NULL AND oi.asin <> ''
            GROUP BY DATE(oi.purchase_utc_date), oi.store_id, oi.asin
            """
        ),
        {"a": low, "b": high},
    ).fetchall()
    out: dict[tuple[date, int, str], Decimal] = {}
    for row in rows:
        d = _cell_date(row[0])
        if d is None or row[1] is None:
            continue
        asin_key = (row[2] or "").strip()
        if not asin_key:
            continue
        try:
            sid = int(row[1])
        except (TypeError, ValueError):
            continue
        out[(d, sid, asin_key)] = _to_decimal(row[3]) or Decimal(0)
    return out


def _resolve_report_day_sql(online_conn) -> str:
    """返回用于 SELECT/WHERE/GROUP BY 的报表日历日表达式（别名 r）。"""
    schema = online_conn.execute(text("SELECT DATABASE()")).scalar()
    schema = (schema or settings.ONLINE_DB_NAME or "").strip()
    if not schema:
        return "DATE(r.`current_date`)"
    n = online_conn.execute(
        text(
            "SELECT COUNT(*) FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = 'amazon_ads_ad_group_ad_report' AND COLUMN_NAME = 'date'"
        ),
        {"schema": schema},
    ).scalar()
    if int(n or 0) > 0:
        return "DATE(COALESCE(r.`date`, r.`current_date`))"
    return "DATE(r.`current_date`)"


def _resolve_report_asin_trim_expr(online_conn) -> str:
    """
    返回 SQL 片段 ``TRIM(r.`列名`)``，用于 SELECT / WHERE / GROUP BY。
    线上表若无 advertise_asin，会依次尝试 _REPORT_ASIN_COLUMN_CANDIDATES 中存在的列；
    查询结果始终映射到本地 daily_ad_cost_sales.ad_asin。
    """
    schema = online_conn.execute(text("SELECT DATABASE()")).scalar()
    schema = (schema or settings.ONLINE_DB_NAME or "").strip()
    if not schema:
        raise ValueError("无法解析线上库 schema，不能确定 amazon_ads_ad_group_ad_report 的 ASIN 列")
    for col in _REPORT_ASIN_COLUMN_CANDIDATES:
        n = online_conn.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = 'amazon_ads_ad_group_ad_report' AND COLUMN_NAME = :col"
            ),
            {"schema": schema, "col": col},
        ).scalar()
        if int(n or 0) > 0:
            logger.info(
                "[DailyAdCostSales] 线上报表 ASIN 源列=%s → 写入本地 daily_ad_cost_sales.ad_asin",
                col,
            )
            return f"TRIM(r.`{col}`)"
    raise ValueError(
        "amazon_ads_ad_group_ad_report 未找到 ASIN 相关列，已尝试: "
        + ", ".join(_REPORT_ASIN_COLUMN_CANDIDATES)
    )


def _online_min_max_day(online_conn, day_sql: str) -> tuple[date | None, date | None]:
    row = online_conn.execute(
        text(
            f"SELECT MIN({day_sql}), MAX({day_sql}) "
            "FROM amazon_ads_ad_group_ad_report r "
            f"WHERE {day_sql} IS NOT NULL"
        )
    ).fetchone()
    if not row:
        return None, None
    return _cell_date(row[0]), _cell_date(row[1])


def _distinct_local_purchase_days(local_db, low: date, high: date) -> set[date]:
    rows = local_db.execute(
        text(
            "SELECT DISTINCT purchase_date FROM daily_ad_cost_sales "
            "WHERE purchase_date IS NOT NULL AND purchase_date BETWEEN :a AND :b"
        ),
        {"a": low, "b": high},
    ).fetchall()
    out: set[date] = set()
    for (d,) in rows:
        cd = _cell_date(d)
        if cd:
            out.add(cd)
    return out


def _distinct_online_report_days(online_conn, day_sql: str, low: date, high: date) -> set[date]:
    rows = online_conn.execute(
        text(
            f"SELECT DISTINCT {day_sql} AS d FROM amazon_ads_ad_group_ad_report r "
            f"WHERE {day_sql} IS NOT NULL AND {day_sql} BETWEEN :a AND :b"
        ),
        {"a": low, "b": high},
    ).fetchall()
    out: set[date] = set()
    for (d,) in rows:
        cd = _cell_date(d)
        if cd:
            out.add(cd)
    return out


def default_gap_days_to_sync(local_db, online_conn, day_sql: str) -> list[date]:
    """
    无 CLI 起止日时：找出「线上报表日存在、本地 purchase_date 尚未覆盖」的日期。
    - 若本地无任何行：仅考虑线上 [MAX(online_min, online_max-34), online_max] 内的报表日。
    - 若本地有数据：在 [min(online_min, local_min), online_max] 内做集合差。
    """
    omin, omax = _online_min_max_day(online_conn, day_sql)
    if omax is None:
        logger.warning("[DailyAdCostSales] 线上 amazon_ads_ad_group_ad_report 无有效报表日，跳过")
        return []
    if omin is None:
        omin = omax

    lr = local_db.execute(
        text("SELECT MIN(purchase_date), MAX(purchase_date) FROM daily_ad_cost_sales")
    ).fetchone()
    lmin_raw, lmax_raw = lr[0] if lr else None, lr[1] if lr else None
    lmin = _cell_date(lmin_raw)
    lmax = _cell_date(lmax_raw)

    if lmax is None:
        low = max(omin, omax - timedelta(days=_DEFAULT_FIRST_SYNC_SPAN_DAYS - 1))
        high = omax
    else:
        low = min(omin, lmin) if lmin is not None else omin
        high = omax

    if low > high:
        return []

    s_online = _distinct_online_report_days(online_conn, day_sql, low, high)
    s_local = _distinct_local_purchase_days(local_db, low, high)
    missing = sorted(s_online - s_local)
    logger.info(
        "[DailyAdCostSales] 默认差距同步: online_range=[%s..%s] local_distinct_days=%s online_distinct_days=%s missing_days=%s",
        low.isoformat(),
        high.isoformat(),
        len(s_local),
        len(s_online),
        len(missing),
    )
    return missing


def _fetch_aggregated_for_day_filter(
    online_conn,
    day_sql: str,
    asin_trim_sql: str,
    *,
    start: date | None = None,
    end: date | None = None,
    days: list[date] | None = None,
) -> list[tuple]:
    if days is not None:
        if not days:
            return []
        ph = ", ".join([f":d{i}" for i in range(len(days))])
        params = {f"d{i}": days[i] for i in range(len(days))}
        where = f"{day_sql} IN ({ph})"
    else:
        if start is None or end is None:
            return []
        where = f"{day_sql} BETWEEN :start AND :end"
        params = {"start": start, "end": end}

    sql = text(
        f"""
        SELECT {asin_trim_sql} AS ad_asin,
               r.store_id,
               {day_sql} AS purchase_day,
               SUM(COALESCE(r.cost, 0)) AS ad_cost,
               SUM(COALESCE(r.sales_1d, 0)) AS sales_1d,
               SUM(COALESCE(r.sales_7d, 0)) AS sales_7d,
               SUM(COALESCE(r.sales_14d, 0)) AS sales_14d,
               SUM(COALESCE(r.sales_30d, 0)) AS sales_30d
        FROM amazon_ads_ad_group_ad_report r
        WHERE {asin_trim_sql} IS NOT NULL AND {asin_trim_sql} <> ''
          AND {where}
        GROUP BY {asin_trim_sql}, r.store_id, {day_sql}
        """
    )
    return online_conn.execute(sql, params).fetchall()


def _fetch_listing_pid_map(
    online_conn,
    pairs: list[tuple[str, int | None]],
    batch_size: int = 400,
) -> dict[tuple[str, int | None], dict]:
    """
    (asin, store_id) -> {pid, variation_id}，同一店铺+asin 多行时取 MIN(id) 对应行。
    store_id 为 NULL 时按「店铺为空 + asin」选 MIN(id)。
    """
    out: dict[tuple[str, int | None], dict] = {}
    uniq: list[tuple[str, int | None]] = []
    seen: set[tuple[str, int | None]] = set()
    for a, sid in pairs:
        key = (a, sid)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(key)

    with_store = [(a, s) for a, s in uniq if s is not None]
    null_store = [(a, s) for a, s in uniq if s is None]

    for i in range(0, len(with_store), batch_size):
        chunk = with_store[i : i + batch_size]
        ph = ", ".join([f"(:s{j}, :a{j})" for j in range(len(chunk))])
        params: dict = {}
        for j, (asin, sid) in enumerate(chunk):
            params[f"a{j}"] = asin
            params[f"s{j}"] = int(sid)
        sql = text(
            f"""
            SELECT al.store_id, al.asin AS asin, al.pid, al.variation_id
            FROM amazon_listing al
            INNER JOIN (
                SELECT store_id, asin AS asin_b, MIN(id) AS mid
                FROM amazon_listing
                WHERE (store_id, asin) IN ({ph})
                GROUP BY store_id, asin
            ) t ON al.store_id = t.store_id AND al.asin = t.asin_b AND al.id = t.mid
            """
        )
        for row in online_conn.execute(sql, params).fetchall():
            sid = int(row[0]) if row[0] is not None else None
            asin = (row[1] or "").strip()
            if not asin:
                continue
            pid = int(row[2]) if row[2] is not None else None
            vid = int(row[3]) if row[3] is not None else None
            out[(asin, sid)] = {"pid": pid, "variation_id": vid}

    for i in range(0, len(null_store), batch_size):
        chunk = null_store[i : i + batch_size]
        asins = list({a for a, _ in chunk})
        ph = ", ".join([f":a{j}" for j in range(len(asins))])
        params = {f"a{j}": asins[j] for j in range(len(asins))}
        sql = text(
            f"""
            SELECT al.store_id, al.asin AS asin, al.pid, al.variation_id
            FROM amazon_listing al
            INNER JOIN (
                SELECT asin AS asin_b, MIN(id) AS mid
                FROM amazon_listing
                WHERE store_id IS NULL AND asin IN ({ph})
                GROUP BY asin
            ) t ON al.store_id IS NULL AND al.asin = t.asin_b AND al.id = t.mid
            """
        )
        for row in online_conn.execute(sql, params).fetchall():
            asin = (row[1] or "").strip()
            if not asin:
                continue
            pid = int(row[2]) if row[2] is not None else None
            vid = int(row[3]) if row[3] is not None else None
            out[(asin, None)] = {"pid": pid, "variation_id": vid}

    return out


def _upsert_rows(local_db, rows: list[dict]) -> dict:
    if not rows:
        return {"inserted": 0, "updated": 0}
    # 唯一键 (ad_asin, store_id, pid, purchase_date)：冲突时只更新非键字段
    stmt = mysql_insert(DailyAdCostSales).values(rows)
    stmt = stmt.on_duplicate_key_update(
        variation_id=stmt.inserted.variation_id,
        ad_cost=stmt.inserted.ad_cost,
        sales_1d=stmt.inserted.sales_1d,
        sales_7d=stmt.inserted.sales_7d,
        sales_14d=stmt.inserted.sales_14d,
        sales_30d=stmt.inserted.sales_30d,
        ad_sales_1d=stmt.inserted.ad_sales_1d,
        ad_sales_7d=stmt.inserted.ad_sales_7d,
        ad_sales_14d=stmt.inserted.ad_sales_14d,
        ad_sales_30d=stmt.inserted.ad_sales_30d,
        tad_sales=stmt.inserted.tad_sales,
        tad_sales_7d=stmt.inserted.tad_sales_7d,
        tad_sales_14d=stmt.inserted.tad_sales_14d,
        tad_sales_30d=stmt.inserted.tad_sales_30d,
        tsales=stmt.inserted.tsales,
        tsales_7d=stmt.inserted.tsales_7d,
        tsales_14d=stmt.inserted.tsales_14d,
        tsales_30d=stmt.inserted.tsales_30d,
    )
    res = local_db.execute(stmt)
    rc = int(getattr(res, "rowcount", 0) or 0)
    return {"inserted": rc, "updated": rc}


def sync_ad_cost_sales(
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    gap_days: list[date] | None = None,
) -> dict:
    """
    gap_days 与 (start_date, end_date) 二选一：前者为离散日列表；后者为连续闭区间。
    """
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        raise ValueError("Online DB 未配置：需设置 online_db_host, online_db_user 等")

    init_db()
    local_db = SessionLocal()
    day_sql_used = "DATE(r.`current_date`)"
    asin_trim_used = "r.`asin`"
    try:
        online_engine = get_online_engine()
        with online_engine.connect() as online_conn:
            day_sql_used = _resolve_report_day_sql(online_conn)
            day_sql = day_sql_used
            asin_trim_used = _resolve_report_asin_trim_expr(online_conn)

            if gap_days is not None:
                days_to_process = sorted(set(gap_days))
                mode = "gap"
            else:
                if start_date is None or end_date is None:
                    raise ValueError("start_date/end_date 与 gap_days 需择一传入")
                if start_date > end_date:
                    return {
                        "mode": "range",
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat(),
                        "rows_upsert": 0,
                        "skipped": True,
                    }
                days_to_process = None
                mode = "range"

            total_rows = 0
            day_batches = 50
            if days_to_process is not None:
                ranges_desc = f"{len(days_to_process)} gap days"
                iter_chunks: list[tuple[date | None, date | None, list[date] | None]] = []
                for i in range(0, len(days_to_process), day_batches):
                    iter_chunks.append((None, None, days_to_process[i : i + day_batches]))
            else:
                ranges_desc = f"{start_date.isoformat()}..{end_date.isoformat()}"
                span = (end_date - start_date).days + 1
                if span <= day_batches:
                    iter_chunks = [(start_date, end_date, None)]
                else:
                    iter_chunks = []
                    cur = start_date
                    while cur <= end_date:
                        nxt = min(cur + timedelta(days=day_batches - 1), end_date)
                        iter_chunks.append((cur, nxt, None))
                        cur = nxt + timedelta(days=1)

            for low, high, dlist in iter_chunks:
                if dlist is not None:
                    raw = _fetch_aggregated_for_day_filter(
                        online_conn, day_sql, asin_trim_used, days=dlist
                    )
                    chunk_low = min(dlist)
                    chunk_high = max(dlist)
                else:
                    raw = _fetch_aggregated_for_day_filter(
                        online_conn, day_sql, asin_trim_used, start=low, end=high
                    )
                    chunk_low, chunk_high = low, high
                if not raw:
                    continue
                order_low = chunk_low - timedelta(days=_ORDER_ITEM_LOOKBACK_DAYS)
                order_by_day_store_asin = _fetch_order_totals_by_day_store_asin(
                    online_conn, order_low, chunk_high
                )
                pairs: list[tuple[str, int | None]] = []
                for r in raw:
                    asin = (r[0] or "").strip()
                    sid = int(r[1]) if r[1] is not None else None
                    if asin:
                        pairs.append((asin, sid))
                lmap = _fetch_listing_pid_map(online_conn, pairs)
                payloads = []
                for r in raw:
                    asin = (r[0] or "").strip()
                    sid = int(r[1]) if r[1] is not None else None
                    pday = _cell_date(r[2])
                    if not asin or pday is None:
                        continue
                    meta = lmap.get((asin, sid), {})
                    ad_cost = _dec(r[3])
                    s1 = _dec(r[4])
                    s7 = _dec(r[5])
                    s14 = _dec(r[6])
                    s30 = _dec(r[7])
                    if sid is not None:
                        sid_i = int(sid)
                        order_day = order_by_day_store_asin.get((pday, sid_i, asin), Decimal(0))
                        order_7 = _sum_order_amount_window(order_by_day_store_asin, pday, sid_i, asin, 7)
                        order_14 = _sum_order_amount_window(order_by_day_store_asin, pday, sid_i, asin, 14)
                        order_30 = _sum_order_amount_window(order_by_day_store_asin, pday, sid_i, asin, 30)
                    else:
                        order_day = order_7 = order_14 = order_30 = Decimal(0)
                    payloads.append(
                        {
                            "ad_asin": asin[:32],
                            "store_id": sid,
                            "purchase_date": pday,
                            "pid": meta.get("pid"),
                            "variation_id": meta.get("variation_id"),
                            "ad_cost": ad_cost,
                            "sales_1d": s1,
                            "sales_7d": s7,
                            "sales_14d": s14,
                            "sales_30d": s30,
                            "ad_sales_1d": _ratio_float(ad_cost, s1),
                            "ad_sales_7d": _ratio_float(ad_cost, s7),
                            "ad_sales_14d": _ratio_float(ad_cost, s14),
                            "ad_sales_30d": _ratio_float(ad_cost, s30),
                            "tad_sales": _tad_over_order_sum(ad_cost, order_day),
                            "tad_sales_7d": _tad_over_order_sum(ad_cost, order_7),
                            "tad_sales_14d": _tad_over_order_sum(ad_cost, order_14),
                            "tad_sales_30d": _tad_over_order_sum(ad_cost, order_30),
                            "tsales": order_day,
                            "tsales_7d": order_7,
                            "tsales_14d": order_14,
                            "tsales_30d": order_30,
                        }
                    )
                if payloads:
                    st = _upsert_rows(local_db, payloads)
                    local_db.commit()
                    total_rows += len(payloads)
                    logger.info(
                        "[DailyAdCostSales] upsert chunk mode=%s range=%s rows=%s rowcount=%s",
                        mode,
                        ranges_desc,
                        len(payloads),
                        st,
                    )

        out = {
            "mode": mode,
            "report_day_sql": day_sql_used,
            "report_asin_trim_sql": asin_trim_used,
            "ranges": ranges_desc,
            "rows_upsert": total_rows,
        }
        if mode == "range" and start_date and end_date:
            out["start_date"] = start_date.isoformat()
            out["end_date"] = end_date.isoformat()
        logger.info("[DailyAdCostSales] done %s", out)
        return out
    finally:
        local_db.close()


_scheduled_ad_sales_lock = threading.Lock()


def run_daily_ad_cost_sales_scheduled(*, force: bool = False) -> dict | None:
    """
    定时任务入口：与命令行无参一致，仅同步「线上有而本地尚无」的报表日（gap）。
    force=True 时跳过「本分钟已跑」检查；仍受锁约束。
    """
    if not _scheduled_ad_sales_lock.acquire(blocking=False):
        logger.info("[DailyAdCostSales] scheduled job skipped: previous run still in progress")
        return None
    try:
        if not force and not should_run_daily_ad_cost_sales_sync():
            logger.info(
                "[DailyAdCostSales] scheduled job skipped: already run in this minute slot (Asia/Shanghai)"
            )
            return None
        if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
            logger.warning("[DailyAdCostSales] scheduled job skipped: online DB not configured")
            return None
        init_db()
        ldb = SessionLocal()
        try:
            online_engine = get_online_engine()
            with online_engine.connect() as oc:
                day_sql = _resolve_report_day_sql(oc)
                missing = default_gap_days_to_sync(ldb, oc, day_sql)
        finally:
            ldb.close()
        if not missing:
            logger.info("[DailyAdCostSales] scheduled job: no gap days to sync")
            record_daily_ad_cost_sales_run()
            return {"mode": "gap", "rows_upsert": 0, "skipped": True}
        out = sync_ad_cost_sales(gap_days=missing)
        record_daily_ad_cost_sales_run()
        return out
    except Exception as e:
        logger.exception("[DailyAdCostSales] scheduled job failed: %s", e)
        return None
    finally:
        try:
            _scheduled_ad_sales_lock.release()
        except Exception:
            pass


def main(argv: list[str]) -> int:
    setup_logging(level=logging.INFO)
    p = argparse.ArgumentParser(description="同步 amazon_ads_ad_group_ad_report -> daily_ad_cost_sales")
    p.add_argument("--start-date", type=str, default="", help="开始日期 YYYY-MM-DD（与 --end-date 成对）")
    p.add_argument("--end-date", type=str, default="", help="结束日期 YYYY-MM-DD（含）")
    args = p.parse_args(argv)

    has_start = bool(str(args.start_date or "").strip())
    has_end = bool(str(args.end_date or "").strip())
    if has_start ^ has_end:
        p.error("请同时指定 --start-date 与 --end-date，或两者皆省略以按「本地与线上日期差距」同步")

    try:
        if not has_start and not has_end:
            init_db()
            ldb = SessionLocal()
            try:
                online_engine = get_online_engine()
                with online_engine.connect() as oc:
                    day_sql = _resolve_report_day_sql(oc)
                    missing = default_gap_days_to_sync(ldb, oc, day_sql)
            finally:
                ldb.close()
            if not missing:
                logger.info("[DailyAdCostSales] 无待同步日期，退出")
                return 0
            sync_ad_cost_sales(gap_days=missing)
            return 0

        start_d = _parse_ymd(args.start_date)
        end_d = _parse_ymd(args.end_date)
        sync_ad_cost_sales(start_date=start_d, end_date=end_d)
        return 0
    except Exception as e:
        logger.exception("[DailyAdCostSales] failed: %s", e)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
