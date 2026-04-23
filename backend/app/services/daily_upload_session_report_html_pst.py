"""
每日上新 ASIN session 堆叠图 — HTML 交互报表（Chart.js）。

- 默认「全部店铺」聚合；可按 store_id 切换。
- 堆叠柱 + 折线：折线为每日 sessions 合计（与柱顶一致），双 Y 轴同刻度对齐。
- 悬停：各批次 sessions、当日合计、当日上新 ASIN 数（均可在 tooltip 查看）。
- KPI（Total / Active Asins）为线上 **amazon_listing**：`COUNT(*)`，`DATE(open_date) > listing_since`；Active 另加 `status = 'Active'`（与手写 SQL 对账）。
  Online 不可用时回退本地表仍为近似口径（本地表按 session 行展开，非 listing 行数）。

本地库表（仅 session 堆叠与回退）：daily_upload_asin_dates。
图表下表格：第二列为 amazon_listing 的 COUNT(*), DATE(open_date)（条件 asin IS NOT NULL）；第 1～30 列为本地
daily_upload_asin_dates 中 open_date(PST)=该上新日 的 session_date 汇总。

静态导出（backend 目录）：
  python3.11 -m app.services.daily_upload_session_report_html_pst --out ./charts/report_pst.html

若要与手写 SQL `DATE(open_date), COUNT(*), store_id` 对账，请使用本模块（`_pst`），不要用
`daily_upload_session_report_html.py`（该文件按 DATE(created_at) 统计，口径不同）。

若请求区间内本地表无 session 行，图表会自动回退到本地最近可用的 35 天窗口，避免默认查询被全量历史数据放大（见页面说明）。

按日 listing 行数与 `SELECT DATE(al.open_date), COUNT(*) … WHERE al.asin IS NOT NULL AND DATE(al.open_date) BETWEEN …`
分组结果一致；过滤与分组均用 DATE(open_date)，避免 DATETIME 列仅用 `open_date BETWEEN 'd0' AND 'd1'` 时与按日历日统计的边界差异。

性能：本地矩阵一次 ``GROUP BY store_id, session_date, open_date``（列已为 DATE，避免 ``DATE(col)`` 以便走 ``ix_duad_session_open_store`` 等组合索引）再拆分全店/各店；cohort 表 30 日列一次 SQL 覆盖所有批次；
线上 KPI 单次查询合并 Total/Active；按日 listing 对 chart/cohort 区间合并为一次扫描；``open_date`` 条件尽量可索引（避免仅 ``DATE(open_date)`` 包在比较左侧）。按店铺并行独立连接。若已配置 ``online_db_host``，KPI 与 amazon_listing 按日统计均来自线上，不回退用本地行数冒充。
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import OrderedDict, defaultdict
import threading
from datetime import date, datetime, timedelta
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal, init_db
from app.logging_config import setup_logging
from app.online_engine import get_online_reporting_engine

logger = logging.getLogger(__name__)

TABLE = "daily_upload_asin_dates"
DEFAULT_LISTING_SINCE = date(2025, 5, 10)
DEFAULT_RECENT_WINDOW_DAYS = 35
COHORT_TRACK_DAYS = 30

# ----------------------------
# 本地矩阵 bulk 结果短缓存
# ----------------------------
# 目标：当先请求过 all 视图（bulk GROUP BY）后，单店切换 json_views=store 直接复用 bulk 的 by_store 切片，
# 避免再次对 daily_upload_asin_dates 做单店 GROUP BY，尽量把单店视图构建压到 <200ms（cache hit 情况下）。
_MATRIX_BULK_CACHE_TTL_SEC = 180
_MATRIX_BULK_CACHE_MAX_KEYS = 6
_MATRIX_BULK_CACHE_SCHEMA = "new-only-v1"
_matrix_bulk_cache_lock = threading.Lock()
# key=(d0.iso, d1.iso) -> (exp_mono, mat_all, mat_by_store)
_matrix_bulk_cache: "OrderedDict[tuple[str, str], tuple[float, dict, dict]]" = OrderedDict()

NewListingKey = tuple[str, int, int, date, date]


def _matrix_bulk_cache_get(d0: date, d1: date):
    k = (_MATRIX_BULK_CACHE_SCHEMA, d0.isoformat(), d1.isoformat())
    now = time.monotonic()
    with _matrix_bulk_cache_lock:
        item = _matrix_bulk_cache.get(k)
        if not item:
            return None
        exp_mono, mat_all, by_store = item
        if exp_mono < now:
            try:
                del _matrix_bulk_cache[k]
            except KeyError:
                pass
            return None
        _matrix_bulk_cache.move_to_end(k)
        return mat_all, by_store


def _matrix_bulk_cache_set(d0: date, d1: date, mat_all, by_store) -> None:
    k = (_MATRIX_BULK_CACHE_SCHEMA, d0.isoformat(), d1.isoformat())
    with _matrix_bulk_cache_lock:
        _matrix_bulk_cache[k] = (time.monotonic() + _MATRIX_BULK_CACHE_TTL_SEC, mat_all, by_store)
        _matrix_bulk_cache.move_to_end(k)
        while len(_matrix_bulk_cache) > _MATRIX_BULK_CACHE_MAX_KEYS:
            _matrix_bulk_cache.popitem(last=False)


def matrix_bulk_cache_wait_ready(d0: date, d1: date, *, timeout_sec: float = 2.0) -> bool:
    """
    等待本地 bulk 矩阵缓存就绪（通常 all_only/all 视图已生成后应立即为 True）。
    用于上层「后台预热 store payload」避免重复触发单店 GROUP BY。
    """
    deadline = time.monotonic() + max(0.0, float(timeout_sec or 0.0))
    while True:
        if _matrix_bulk_cache_get(d0, d1) is not None:
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.02)

# 与 draw_daily_session_change 堆叠图配色接近（tab20）
_COLORS = [
    "#1f77b4",
    "#ff7f0e",
    "#2ca02c",
    "#d62728",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#7f7f7f",
    "#bcbd22",
    "#17becf",
    "#aec7e8",
    "#ffbb78",
    "#98df8a",
    "#ff9896",
    "#c5b0d5",
    "#c49c94",
    "#f7b6d2",
    "#c7c7c7",
    "#dbdb8d",
    "#9edae5",
]


def _d(x) -> date:
    if x is None:
        return DEFAULT_LISTING_SINCE
    if isinstance(x, datetime):
        return x.date()
    return x


def _as_calendar_date(x) -> date:
    """矩阵/聚合用：统一转为日历日，避免驱动返回 datetime/str 时与 cohort_days 的 date 对不上导致 mat.get 全为 0。"""
    if x is None:
        raise ValueError("null date")
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    s = str(x).strip()
    if len(s) >= 10:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    raise ValueError(f"cannot parse calendar date from {x!r}")


def _coerce_listing_calendar_day(value) -> date | None:
    """listing 查询结果转日历日；NULL 或无法解析则跳过，绝不默认成 DEFAULT_LISTING_SINCE。"""
    if value is None:
        return None
    try:
        return _as_calendar_date(value)
    except (ValueError, TypeError):
        return None


def _fetch_store_ids_for_range(db: Session, d0: date, d1: date) -> list[int]:
    # 尽量避免 WHERE DATE(col) 以便索引可用；即便 col 为 DATETIME，该写法也能正确覆盖整天区间
    d1x = d1 + timedelta(days=1)
    q = text(
        f"SELECT DISTINCT store_id FROM {TABLE} "
        "WHERE session_date >= :d0 AND session_date < :d1x ORDER BY store_id"
    )
    rows = db.execute(q, {"d0": d0, "d1x": d1x}).fetchall()
    return [int(r[0]) for r in rows if r[0] is not None]


def _apply_cohort_session_window_to_matrix(
    mat: dict[tuple[date, date], int],
    *,
    listing_since: date,
    cohort_track_days: int,
    display_start: date,
    display_end: date,
) -> dict[tuple[date, date], int]:
    """
    仅保留「上新批次」口径：
    - open_date（cd）>= listing_since；
    - 每个批次只统计从上新日起连续 cohort_track_days 个日历日的 session（cd <= sd <= cd + days-1）；
    - 横轴展示区间夹紧到 [display_start, display_end]（与请求 session 区间一致）。
    """
    if cohort_track_days < 1:
        return {}
    max_sd = timedelta(days=cohort_track_days - 1)
    out: dict[tuple[date, date], int] = {}
    for (sd, cd), v in mat.items():
        if cd < listing_since:
            continue
        if sd < cd or sd > cd + max_sd:
            continue
        if sd < display_start or sd > display_end:
            continue
        out[(sd, cd)] = int(v or 0)
    return out


def _fetch_matrix_rows(
    db: Session,
    store_id: int | None,
    d0: date,
    d1: date,
    *,
    valid_listing_keys: set[NewListingKey] | None = None,
    open_date_start: date | None = None,
    open_date_end: date | None = None,
):
    # 列模型为 Date：SELECT/GROUP BY 直接用 session_date、open_date，便于走 ix_duad_store_session 等组合索引
    if valid_listing_keys is not None:
        params: dict[str, object] = {"d0": d0, "d1x": d1 + timedelta(days=1)}
        extra = ""
        if store_id is not None:
            extra += " AND store_id = :sid"
            params["sid"] = store_id
        if open_date_start is not None:
            extra += " AND open_date >= :od0"
            params["od0"] = open_date_start
        if open_date_end is not None:
            extra += " AND open_date <= :od1"
            params["od1"] = open_date_end
        q = text(
            f"""
            SELECT asin, COALESCE(pid, 0) AS pid_key, store_id, created_at, open_date,
                   session_date AS sd, SUM(COALESCE(sessions, 0)) AS s
            FROM {TABLE}
            WHERE open_date IS NOT NULL
              AND created_at IS NOT NULL
              AND session_date >= :d0 AND session_date < :d1x
              AND session_date >= open_date
              {extra}
            GROUP BY asin, COALESCE(pid, 0), store_id, created_at, open_date, session_date
            """
        )
        rows = db.execute(q, params).fetchall()
        mat: dict[tuple[date, date], int] = {}
        for r in rows:
            key = _local_listing_key(r[0], r[1], r[2], r[3], r[4])
            if key is None or key not in valid_listing_keys:
                continue
            try:
                sd = _as_calendar_date(r[5])
                cd = _as_calendar_date(r[4])
            except (ValueError, TypeError):
                continue
            mat[(sd, cd)] = mat.get((sd, cd), 0) + int(r[6] or 0)
        return mat

    d1x = d1 + timedelta(days=1)
    if store_id is not None:
        q = text(
            f"""
            SELECT session_date AS sd, open_date AS cd, SUM(COALESCE(sessions, 0)) AS s
            FROM {TABLE}
            WHERE store_id = :sid AND open_date IS NOT NULL
              AND session_date >= :d0 AND session_date < :d1x
              AND session_date >= open_date
            GROUP BY session_date, open_date
            """
        )
        rows = db.execute(q, {"sid": store_id, "d0": d0, "d1x": d1x}).fetchall()
    else:
        q = text(
            f"""
            SELECT session_date AS sd, open_date AS cd, SUM(COALESCE(sessions, 0)) AS s
            FROM {TABLE}
            WHERE open_date IS NOT NULL
              AND session_date >= :d0 AND session_date < :d1x
              AND session_date >= open_date
            GROUP BY session_date, open_date
            """
        )
        rows = db.execute(q, {"d0": d0, "d1x": d1x}).fetchall()
    mat: dict[tuple[date, date], int] = {}
    for r in rows:
        try:
            sd = _as_calendar_date(r[0])
            cd = _as_calendar_date(r[1])
        except (ValueError, TypeError):
            continue
        mat[(sd, cd)] = int(r[2] or 0)
    return mat


def _fetch_matrix_rows_bulk(
    db: Session,
    d0: date,
    d1: date,
    *,
    valid_listing_keys: set[NewListingKey] | None = None,
    open_date_start: date | None = None,
    open_date_end: date | None = None,
) -> tuple[dict[tuple[date, date], int], dict[int, dict[tuple[date, date], int]]]:
    """
    一次扫描 daily_upload_asin_dates，得到「全部店铺」矩阵与各店矩阵，避免对每个 store_id 重复 GROUP BY。
    """
    if valid_listing_keys is None:
        cached = _matrix_bulk_cache_get(d0, d1)
        if cached is not None:
            return cached
    d1x = d1 + timedelta(days=1)
    if valid_listing_keys is not None:
        params: dict[str, object] = {"d0": d0, "d1x": d1x}
        extra = ""
        if open_date_start is not None:
            extra += " AND open_date >= :od0"
            params["od0"] = open_date_start
        if open_date_end is not None:
            extra += " AND open_date <= :od1"
            params["od1"] = open_date_end
        q = text(
            f"""
            SELECT asin, COALESCE(pid, 0) AS pid_key, store_id, created_at, open_date,
                   session_date AS sd, SUM(COALESCE(sessions, 0)) AS s
            FROM {TABLE}
            WHERE open_date IS NOT NULL
              AND created_at IS NOT NULL
              AND session_date >= :d0 AND session_date < :d1x
              AND session_date >= open_date
              {extra}
            GROUP BY asin, COALESCE(pid, 0), store_id, created_at, open_date, session_date
            """
        )
        rows = db.execute(q, params).fetchall()
    else:
        q = text(
            f"""
            SELECT store_id, session_date AS sd, open_date AS cd,
                   SUM(COALESCE(sessions, 0)) AS s
            FROM {TABLE}
            WHERE open_date IS NOT NULL
              AND session_date >= :d0 AND session_date < :d1x
              AND session_date >= open_date
            GROUP BY store_id, session_date, open_date
            """
        )
        rows = db.execute(q, {"d0": d0, "d1x": d1x}).fetchall()
    mat_all: dict[tuple[date, date], int] = {}
    by_store: dict[int, dict[tuple[date, date], int]] = {}
    for r in rows:
        if valid_listing_keys is not None:
            key = _local_listing_key(r[0], r[1], r[2], r[3], r[4])
            if key is None or key not in valid_listing_keys:
                continue
            sid_raw = r[2]
            try:
                sid = int(sid_raw)
                sd = _as_calendar_date(r[5])
                cd = _as_calendar_date(r[4])
            except (TypeError, ValueError):
                continue
            s = int(r[6] or 0)
        else:
            sid_raw = r[0]
            if sid_raw is None:
                continue
            try:
                sid = int(sid_raw)
                sd = _as_calendar_date(r[1])
                cd = _as_calendar_date(r[2])
            except (TypeError, ValueError):
                continue
            s = int(r[3] or 0)
        mat_all[(sd, cd)] = mat_all.get((sd, cd), 0) + s
        by_store.setdefault(sid, {})[(sd, cd)] = by_store.setdefault(sid, {}).get((sd, cd), 0) + s
    if valid_listing_keys is None:
        _matrix_bulk_cache_set(d0, d1, mat_all, by_store)
    return mat_all, by_store


def _fetch_matrix_rows_online(
    conn: Connection,
    store_id: int | None,
    d0: date,
    d1: date,
    *,
    open_date_start: date | None = None,
    open_date_end: date | None = None,
) -> dict[tuple[date, date], int]:
    """
    线上按「纯上新 asin + store + open_date」聚合 session。

    本地 daily_upload_asin_dates 是按 created_at 窗口同步的，无法稳定覆盖按 open_date
    回看的纯上新 cohort，因此 online 可用时直接联查 amazon_listing/amazon_variation
    与 amazon_sales_and_traffic_daily。
    """
    params: dict[str, object] = {"d0": d0, "d1x": d1 + timedelta(days=1)}
    extra = ""
    if store_id is not None:
        extra += " AND al.store_id = :sid"
        params["sid"] = int(store_id)
    if open_date_start is not None:
        extra += " AND al.open_date >= :od0"
        params["od0"] = open_date_start
    if open_date_end is not None:
        extra += " AND al.open_date < :od1x"
        params["od1x"] = open_date_end + timedelta(days=1)
    q = text(
        f"""
        SELECT nl.open_day AS cd,
               td.session_day AS sd,
               SUM(td.sessions) AS s
        FROM (
            SELECT DISTINCT
                   TRIM(al.asin) AS asin,
                   al.store_id,
                   DATE(al.open_date) AS open_day
            FROM amazon_listing al
            INNER JOIN amazon_variation av
                ON av.id = al.variation_id
            WHERE al.asin IS NOT NULL
              AND TRIM(al.asin) <> ''
              AND al.store_id IS NOT NULL
              AND al.created_at IS NOT NULL
              AND al.open_date IS NOT NULL
              AND av.created_at IS NOT NULL
              AND DATE(al.created_at) = DATE(av.created_at)
              {extra}
        ) AS nl
        INNER JOIN (
            SELECT d.asin,
                   d.store_id,
                   DATE(d.`current_date`) AS session_day,
                   SUM(COALESCE(d.sessions, 0)) AS sessions
            FROM amazon_sales_and_traffic_daily AS d
            WHERE d.`current_date` >= :d0
              AND d.`current_date` < :d1x
            GROUP BY d.asin, d.store_id, DATE(d.`current_date`)
        ) AS td
            ON td.asin = nl.asin AND td.store_id = nl.store_id
        WHERE td.session_day >= nl.open_day
        GROUP BY nl.open_day, td.session_day
        """
    )
    rows = conn.execute(q, params).fetchall()
    mat: dict[tuple[date, date], int] = {}
    for r in rows:
        try:
            cd = _as_calendar_date(r[0])
            sd = _as_calendar_date(r[1])
        except (TypeError, ValueError):
            continue
        mat[(sd, cd)] = int(r[2] or 0)
    return mat


def _cohort_day_asin_breakdown_online(
    conn: Connection,
    store_id: int | None,
    cohort_dates: list[date],
    *,
    traffic_d0: date,
    traffic_d1: date,
    open_date_start: date | None,
    open_date_end: date | None,
) -> dict[tuple[date, date], list[dict]]:
    """
    与 ``_fetch_matrix_rows_online`` 同源（listing×variation 定批次 + traffic_daily 取 session）。
    cohort 表每日合计若来自线上矩阵，悬停明细必须走此函数；否则本地 ``daily_upload_asin_dates``
    与线上 traffic 不一致会导致「格子合计 ≠ 明细之和」。
    """
    if not cohort_dates:
        return {}
    uniq = sorted({d for d in cohort_dates if d is not None})
    if not uniq:
        return {}
    ph = ", ".join([f":cd{i}" for i in range(len(uniq))])
    params: dict[str, object] = {
        f"cd{i}": uniq[i] for i in range(len(uniq))
    }
    params["d0"] = traffic_d0
    params["d1x"] = traffic_d1 + timedelta(days=1)
    nd = max(0, COHORT_TRACK_DAYS - 1)
    extra_nl = ""
    if store_id is not None:
        extra_nl += " AND al.store_id = :sid"
        params["sid"] = int(store_id)
    if open_date_start is not None:
        extra_nl += " AND al.open_date >= :od0"
        params["od0"] = open_date_start
    if open_date_end is not None:
        extra_nl += " AND al.open_date < :od1x"
        params["od1x"] = open_date_end + timedelta(days=1)
    q = text(
        f"""
        SELECT nl.open_day AS cd,
               td.session_day AS sd,
               TRIM(nl.asin) AS asin_b,
               nl.store_id AS sid_raw,
               SUM(td.sessions) AS s
        FROM (
            SELECT DISTINCT
                   TRIM(al.asin) AS asin,
                   al.store_id,
                   DATE(al.open_date) AS open_day
            FROM amazon_listing al
            INNER JOIN amazon_variation av
                ON av.id = al.variation_id
            WHERE al.asin IS NOT NULL
              AND TRIM(al.asin) <> ''
              AND al.store_id IS NOT NULL
              AND al.created_at IS NOT NULL
              AND al.open_date IS NOT NULL
              AND av.created_at IS NOT NULL
              AND DATE(al.created_at) = DATE(av.created_at)
              AND DATE(al.open_date) IN ({ph})
              {extra_nl}
        ) AS nl
        INNER JOIN (
            SELECT d.asin,
                   d.store_id,
                   DATE(d.`current_date`) AS session_day,
                   SUM(COALESCE(d.sessions, 0)) AS sessions
            FROM amazon_sales_and_traffic_daily AS d
            WHERE d.`current_date` >= :d0
              AND d.`current_date` < :d1x
            GROUP BY d.asin, d.store_id, DATE(d.`current_date`)
        ) AS td
            ON td.asin = nl.asin AND td.store_id = nl.store_id
        WHERE td.session_day >= nl.open_day
          AND td.session_day <= DATE_ADD(nl.open_day, INTERVAL {nd} DAY)
        GROUP BY nl.open_day, td.session_day, nl.asin, nl.store_id
        HAVING SUM(td.sessions) > 0
        """
    )
    rows = conn.execute(q, params).fetchall()
    merged: dict[tuple[date, date], list[dict]] = {}
    for r in rows:
        try:
            cd = _as_calendar_date(r[0])
            sd = _as_calendar_date(r[1])
        except (TypeError, ValueError):
            continue
        asin_key = str(r[2] or "").strip()
        if not asin_key:
            continue
        try:
            sid = int(r[3])
        except (TypeError, ValueError):
            continue
        s = int(r[4] or 0)
        if s <= 0:
            continue
        pair = (cd, sd)
        if pair not in merged:
            merged[pair] = []
        merged[pair].append({"asin": asin_key, "storeId": sid, "sessions": s})
    out: dict[tuple[date, date], list[dict]] = {}
    for pair, lst in merged.items():
        lst.sort(key=lambda x: (-int(x["sessions"]), x["asin"], x["storeId"]))
        out[pair] = lst
    return out


def _fetch_matrix_rows_online_bulk(
    conn: Connection,
    d0: date,
    d1: date,
    *,
    open_date_start: date | None = None,
    open_date_end: date | None = None,
) -> tuple[dict[tuple[date, date], int], dict[int, dict[tuple[date, date], int]]]:
    params: dict[str, object] = {"d0": d0, "d1x": d1 + timedelta(days=1)}
    extra = ""
    if open_date_start is not None:
        extra += " AND al.open_date >= :od0"
        params["od0"] = open_date_start
    if open_date_end is not None:
        extra += " AND al.open_date < :od1x"
        params["od1x"] = open_date_end + timedelta(days=1)
    q = text(
        f"""
        SELECT nl.store_id,
               nl.open_day AS cd,
               td.session_day AS sd,
               SUM(td.sessions) AS s
        FROM (
            SELECT DISTINCT
                   TRIM(al.asin) AS asin,
                   al.store_id,
                   DATE(al.open_date) AS open_day
            FROM amazon_listing al
            INNER JOIN amazon_variation av
                ON av.id = al.variation_id
            WHERE al.asin IS NOT NULL
              AND TRIM(al.asin) <> ''
              AND al.store_id IS NOT NULL
              AND al.created_at IS NOT NULL
              AND al.open_date IS NOT NULL
              AND av.created_at IS NOT NULL
              AND DATE(al.created_at) = DATE(av.created_at)
              {extra}
        ) AS nl
        INNER JOIN (
            SELECT d.asin,
                   d.store_id,
                   DATE(d.`current_date`) AS session_day,
                   SUM(COALESCE(d.sessions, 0)) AS sessions
            FROM amazon_sales_and_traffic_daily AS d
            WHERE d.`current_date` >= :d0
              AND d.`current_date` < :d1x
            GROUP BY d.asin, d.store_id, DATE(d.`current_date`)
        ) AS td
            ON td.asin = nl.asin AND td.store_id = nl.store_id
        WHERE td.session_day >= nl.open_day
        GROUP BY nl.store_id, nl.open_day, td.session_day
        """
    )
    rows = conn.execute(q, params).fetchall()
    mat_all: dict[tuple[date, date], int] = {}
    by_store: dict[int, dict[tuple[date, date], int]] = {}
    for r in rows:
        sid_raw = r[0]
        if sid_raw is None:
            continue
        try:
            sid = int(sid_raw)
            cd = _as_calendar_date(r[1])
            sd = _as_calendar_date(r[2])
        except (TypeError, ValueError):
            continue
        s = int(r[3] or 0)
        mat_all[(sd, cd)] = mat_all.get((sd, cd), 0) + s
        store_mat = by_store.setdefault(sid, {})
        store_mat[(sd, cd)] = store_mat.get((sd, cd), 0) + s
    return mat_all, by_store


def _cohort_sessions_from_matrix(
    mat_raw: dict[tuple[date, date], int],
    cohort_dates: list[date],
) -> dict[tuple[date, date], int]:
    if not mat_raw or not cohort_dates:
        return {}
    cohort_set = set(cohort_dates)
    max_offset = max(0, COHORT_TRACK_DAYS - 1)
    out: dict[tuple[date, date], int] = {}
    for (sd, cd), v in mat_raw.items():
        if cd not in cohort_set:
            continue
        if sd < cd or sd > cd + timedelta(days=max_offset):
            continue
        out[(cd, sd)] = int(v or 0)
    return out


def _merge_daily_count_maps(*maps: dict[date, int] | None) -> dict[date, int]:
    out: dict[date, int] = {}
    for mp in maps:
        if not mp:
            continue
        for d, v in mp.items():
            out[d] = out.get(d, 0) + int(v or 0)
    return out


def _merge_daily_mix_maps(
    *maps: dict[date, dict[str, int]] | None,
) -> dict[date, dict[str, int]]:
    out: dict[date, dict[str, int]] = {}
    for mp in maps:
        if not mp:
            continue
        for d, vals in mp.items():
            slot = out.setdefault(d, {"new": 0, "refurbished": 0, "total": 0})
            slot["new"] += int(vals.get("new", 0) or 0)
            slot["refurbished"] += int(vals.get("refurbished", 0) or 0)
            slot["total"] += int(vals.get("total", 0) or 0)
    return out


def _merge_matrix_maps(
    *maps: dict[tuple[date, date], int] | None,
) -> dict[tuple[date, date], int]:
    out: dict[tuple[date, date], int] = {}
    for mp in maps:
        if not mp:
            continue
        for key, v in mp.items():
            out[key] = out.get(key, 0) + int(v or 0)
    return out


def _try_online_conn() -> Connection | None:
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        return None
    try:
        return get_online_reporting_engine().connect()
    except Exception as exc:
        logger.warning("Online DB 连接失败，KPI/当日上新将回退本地表: %s", exc)
        return None


def _fetch_listing_kpi_online(conn: Connection, since: date, store_id: int | None) -> tuple[int, int]:
    """
    amazon_listing KPI（与线上一致对账 SQL）：
    - Total：`COUNT(*)`，`DATE(open_date) > since`（严格大于 listing_since，不按 asin 过滤）
    - Active：同上且 `status = 'Active'`
    单店加 `store_id = :sid`；全店不加店铺条件。

    使用 ``open_date >= since + 1 日`` 代替 ``DATE(open_date) > since``，便于走 ``(store_id, open_date)`` 等索引。
    Total / Active 合并为一次查询，减少往返。
    """
    od_ge = since + timedelta(days=1)
    if store_id is not None:
        q = text(
            """
            SELECT COUNT(*) AS tot,
                   COALESCE(SUM(CASE WHEN al.status = 'Active' THEN 1 ELSE 0 END), 0) AS act
            FROM amazon_listing al
            WHERE al.open_date IS NOT NULL
              AND al.open_date >= :od_ge
              AND al.store_id = :sid
            """
        )
        row = conn.execute(q, {"od_ge": od_ge, "sid": store_id}).fetchone()
    else:
        q = text(
            """
            SELECT COUNT(*) AS tot,
                   COALESCE(SUM(CASE WHEN al.status = 'Active' THEN 1 ELSE 0 END), 0) AS act
            FROM amazon_listing al
            WHERE al.open_date IS NOT NULL
              AND al.open_date >= :od_ge
            """
        )
        row = conn.execute(q, {"od_ge": od_ge}).fetchone()
    if not row:
        return 0, 0
    return int(row[0] or 0), int(row[1] or 0)


def _fetch_listing_new_asin_by_day_online(
    conn: Connection, store_id: int | None, d0: date, d1: date
) -> dict[date, int]:
    """
    PST 报表：表格「按日上新行数」与 KPI 不同——此处仍按 asin 非空、日历日在 d0..d1（与堆叠批次一致）。
    KPI 全表行数见 _fetch_listing_kpi_online（open_date > listing_since、无 asin 条件）。

    用 ``open_date >= d0 AND open_date < d1+1 日`` 代替 ``DATE(open_date) BETWEEN``，便于索引范围扫描；
    SELECT 仍用 ``DATE(al.open_date)`` 分组，语义与原先一致。
    """
    d1_exclusive = d1 + timedelta(days=1)
    day_col = "DATE(al.open_date)"
    asin_ok = "al.asin IS NOT NULL "
    if store_id is not None:
        q = text(
            f"""
            SELECT {day_col} AS cd, COUNT(*) AS n
            FROM amazon_listing al
            WHERE {asin_ok}
              AND al.open_date IS NOT NULL
              AND al.open_date >= :d0 AND al.open_date < :d1x
              AND al.store_id = :sid
            GROUP BY {day_col}
            """
        )
        rows = conn.execute(
            q, {"d0": d0, "d1x": d1_exclusive, "sid": store_id}
        ).fetchall()
    else:
        q = text(
            f"""
            SELECT {day_col} AS cd, COUNT(*) AS n
            FROM amazon_listing al
            WHERE {asin_ok}
              AND al.open_date IS NOT NULL
              AND al.open_date >= :d0 AND al.open_date < :d1x
            GROUP BY {day_col}
            """
        )
        rows = conn.execute(q, {"d0": d0, "d1x": d1_exclusive}).fetchall()
    out: dict[date, int] = {}
    for r in rows:
        cd = _coerce_listing_calendar_day(r[0])
        if cd is None:
            continue
        out[cd] = int(r[1] or 0)
    return out


def _fetch_listing_new_refurb_by_day_online(
    conn: Connection, store_id: int | None, d0: date, d1: date
) -> dict[date, dict[str, int]]:
    """
    按 open_date 统计 amazon_listing 每日总数 / 上新数 / 尺寸补录数。

    判定规则：
    - 通过 ``amazon_listing.variation_id = amazon_variation.id`` 关联；
    - 若 ``DATE(amazon_listing.created_at) = DATE(amazon_variation.created_at)``，记为上新；
    - 若日期不一致（或任一侧为空 / 无关联记录），记为补录。
    """
    d1_exclusive = d1 + timedelta(days=1)
    day_col = "DATE(al.open_date)"
    asin_ok = "al.asin IS NOT NULL "
    if store_id is not None:
        q = text(
            f"""
            SELECT {day_col} AS cd,
                   COUNT(*) AS total_n,
                   COALESCE(SUM(CASE
                       WHEN al.created_at IS NOT NULL
                        AND av.created_at IS NOT NULL
                        AND DATE(al.created_at) = DATE(av.created_at)
                       THEN 1 ELSE 0 END), 0) AS new_n,
                   COALESCE(SUM(CASE
                       WHEN al.created_at IS NULL
                         OR av.created_at IS NULL
                         OR DATE(al.created_at) <> DATE(av.created_at)
                       THEN 1 ELSE 0 END), 0) AS refurb_n
            FROM amazon_listing al
            LEFT JOIN amazon_variation av ON av.id = al.variation_id
            WHERE {asin_ok}
              AND al.open_date IS NOT NULL
              AND al.open_date >= :d0 AND al.open_date < :d1x
              AND al.store_id = :sid
            GROUP BY {day_col}
            """
        )
        rows = conn.execute(q, {"d0": d0, "d1x": d1_exclusive, "sid": store_id}).fetchall()
    else:
        q = text(
            f"""
            SELECT {day_col} AS cd,
                   COUNT(*) AS total_n,
                   COALESCE(SUM(CASE
                       WHEN al.created_at IS NOT NULL
                        AND av.created_at IS NOT NULL
                        AND DATE(al.created_at) = DATE(av.created_at)
                       THEN 1 ELSE 0 END), 0) AS new_n,
                   COALESCE(SUM(CASE
                       WHEN al.created_at IS NULL
                         OR av.created_at IS NULL
                         OR DATE(al.created_at) <> DATE(av.created_at)
                       THEN 1 ELSE 0 END), 0) AS refurb_n
            FROM amazon_listing al
            LEFT JOIN amazon_variation av ON av.id = al.variation_id
            WHERE {asin_ok}
              AND al.open_date IS NOT NULL
              AND al.open_date >= :d0 AND al.open_date < :d1x
            GROUP BY {day_col}
            """
        )
        rows = conn.execute(q, {"d0": d0, "d1x": d1_exclusive}).fetchall()
    out: dict[date, dict[str, int]] = {}
    for r in rows:
        cd = _coerce_listing_calendar_day(r[0])
        if cd is None:
            continue
        out[cd] = {
            "total": int(r[1] or 0),
            "new": int(r[2] or 0),
            "refurbished": int(r[3] or 0),
        }
    return out


def _fetch_new_listing_keys_online(
    conn: Connection, store_id: int | None, d0: date, d1: date
) -> set[NewListingKey]:
    """
    返回当前 listing window 内「纯上新」amazon_listing 行的本地过滤键。

    键口径：
    - asin
    - pid（NULL 视为 0，便于与本地表 COALESCE(pid, 0) 对齐）
    - store_id
    - DATE(created_at)
    - DATE(open_date)
    """
    d1_exclusive = d1 + timedelta(days=1)
    base_sql = """
        SELECT DISTINCT
               TRIM(al.asin) AS asin,
               COALESCE(al.pid, 0) AS pid_key,
               al.store_id,
               DATE(al.created_at) AS created_day,
               DATE(al.open_date) AS open_day
        FROM amazon_listing al
        INNER JOIN amazon_variation av
            ON av.id = al.variation_id
        WHERE al.asin IS NOT NULL
          AND TRIM(al.asin) <> ''
          AND al.store_id IS NOT NULL
          AND al.created_at IS NOT NULL
          AND al.open_date IS NOT NULL
          AND av.created_at IS NOT NULL
          AND DATE(al.created_at) = DATE(av.created_at)
          AND al.open_date >= :d0 AND al.open_date < :d1x
    """
    params: dict[str, object] = {"d0": d0, "d1x": d1_exclusive}
    if store_id is not None:
        base_sql += " AND al.store_id = :sid"
        params["sid"] = int(store_id)
    rows = conn.execute(text(base_sql), params).fetchall()
    out: set[NewListingKey] = set()
    for r in rows:
        asin = str(r[0] or "").strip()
        if not asin:
            continue
        try:
            sid = int(r[2])
            created_day = _as_calendar_date(r[3])
            open_day = _as_calendar_date(r[4])
        except (TypeError, ValueError):
            continue
        pid_key = int(r[1] or 0)
        out.add((asin, pid_key, sid, created_day, open_day))
    return out


def _local_listing_key(
    asin,
    pid,
    store_id,
    created_at,
    open_date,
) -> NewListingKey | None:
    asin_key = str(asin or "").strip()
    if not asin_key or store_id is None or created_at is None or open_date is None:
        return None
    try:
        sid = int(store_id)
        pid_key = int(pid or 0)
        created_day = _as_calendar_date(created_at)
        open_day = _as_calendar_date(open_date)
    except (TypeError, ValueError):
        return None
    return (asin_key, pid_key, sid, created_day, open_day)


def _fetch_listing_asin_by_cohort_dates_online(
    conn: Connection, store_id: int | None, cohort_dates: list[date]
) -> dict[date, int]:
    """各上新日在 amazon_listing 的 listing 行数；口径同 _fetch_listing_new_asin_by_day_online。"""
    uniq = sorted({d for d in cohort_dates if d is not None})
    if not uniq:
        return {}
    ph = ", ".join([f":d{i}" for i in range(len(uniq))])
    params: dict = {f"d{i}": uniq[i] for i in range(len(uniq))}
    od_min = uniq[0]
    od_max_x = uniq[-1] + timedelta(days=1)
    params["od_min"] = od_min
    params["od_max_x"] = od_max_x
    day_col = "DATE(al.open_date)"
    asin_ok = "al.asin IS NOT NULL AND al.asin <> ''"
    if store_id is not None:
        params["sid"] = store_id
        q = text(
            f"""
            SELECT {day_col} AS cd, COUNT(*) AS n
            FROM amazon_listing al
            WHERE {asin_ok}
              AND al.open_date IS NOT NULL
              AND al.open_date >= :od_min AND al.open_date < :od_max_x
              AND {day_col} IN ({ph})
              AND al.store_id = :sid
            GROUP BY {day_col}
            """
        )
        rows = conn.execute(q, params).fetchall()
    else:
        q = text(
            f"""
            SELECT {day_col} AS cd, COUNT(*) AS n
            FROM amazon_listing al
            WHERE {asin_ok}
              AND al.open_date IS NOT NULL
              AND al.open_date >= :od_min AND al.open_date < :od_max_x
              AND {day_col} IN ({ph})
            GROUP BY {day_col}
            """
        )
        rows = conn.execute(q, params).fetchall()
    out: dict[date, int] = {}
    for r in rows:
        cd = _coerce_listing_calendar_day(r[0])
        if cd is None:
            continue
        out[cd] = int(r[1] or 0)
    return out


def _fetch_local_asin_by_cohort_dates(
    db: Session, store_id: int | None, cohort_dates: list[date]
) -> dict[date, int]:
    """Online 不可用时：本地表按 open_date(PST) 日行数 COUNT(*)。"""
    uniq = sorted({d for d in cohort_dates if d is not None})
    if not uniq:
        return {}
    ph = ", ".join([f":d{i}" for i in range(len(uniq))])
    params: dict = {f"d{i}": uniq[i] for i in range(len(uniq))}
    if store_id is not None:
        params["sid"] = store_id
        q = text(
            f"""
            SELECT open_date, COUNT(*) AS n
            FROM {TABLE}
            WHERE open_date IN ({ph}) AND store_id = :sid
            GROUP BY open_date
            """
        )
        rows = db.execute(q, params).fetchall()
    else:
        q = text(
            f"""
            SELECT open_date, COUNT(*) AS n
            FROM {TABLE}
            WHERE open_date IN ({ph})
            GROUP BY open_date
            """
        )
        rows = db.execute(q, params).fetchall()
    out: dict[date, int] = {}
    for r in rows:
        cd = _coerce_listing_calendar_day(r[0])
        if cd is None:
            continue
        out[cd] = int(r[1] or 0)
    return out


def _cohort_listing_asin_map_json(
    db: Session,
    online: Connection | None,
    store_id: int | None,
    cohort_days: list[date],
) -> dict[str, int]:
    if not cohort_days:
        return {}
    if online is not None:
        raw = _fetch_listing_asin_by_cohort_dates_online(online, store_id, cohort_days)
    else:
        raw = _fetch_local_asin_by_cohort_dates(db, store_id, cohort_days)
    return {cd.isoformat(): int(raw.get(cd, 0)) for cd in cohort_days}


def _fetch_new_asin_by_day(db: Session, store_id: int | None, d0: date, d1: date) -> dict[date, int]:
    """本地按 open_date(PST) 日行数（与 PST 报表口径一致）。"""
    if store_id is not None:
        q = text(
            f"""
            SELECT open_date, COUNT(*) AS n
            FROM {TABLE}
            WHERE store_id = :sid AND open_date IS NOT NULL
              AND open_date >= :d0 AND open_date <= :d1
            GROUP BY open_date
            """
        )
        rows = db.execute(q, {"sid": store_id, "d0": d0, "d1": d1}).fetchall()
    else:
        q = text(
            f"""
            SELECT open_date, COUNT(*) AS n
            FROM {TABLE}
            WHERE open_date IS NOT NULL
              AND open_date >= :d0 AND open_date <= :d1
            GROUP BY open_date
            """
        )
        rows = db.execute(q, {"d0": d0, "d1": d1}).fetchall()
    out: dict[date, int] = {}
    for r in rows:
        cd = _coerce_listing_calendar_day(r[0])
        if cd is None:
            continue
        out[cd] = int(r[1] or 0)
    return out


def _fetch_total_asin_since(db: Session, store_id: int | None, since: date) -> int:
    """回退口径：本地表按 open_date ≥ since 且非空计数行（与 online KPI、堆叠批次一致）。"""
    if store_id is not None:
        q = text(
            f"""
            SELECT COUNT(*) FROM {TABLE}
            WHERE open_date IS NOT NULL AND open_date >= :since AND store_id = :sid
            """
        )
        r = db.execute(q, {"since": since, "sid": store_id}).scalar()
    else:
        q = text(
            f"""
            SELECT COUNT(*) FROM {TABLE}
            WHERE open_date IS NOT NULL AND open_date >= :since
            """
        )
        r = db.execute(q, {"since": since}).scalar()
    return int(r or 0)


def _fetch_active_asin_since(db: Session, store_id: int | None, since: date) -> int:
    if store_id is not None:
        q = text(
            f"""
            SELECT COUNT(*) FROM (
              SELECT store_id, asin FROM {TABLE}
              WHERE open_date IS NOT NULL AND open_date >= :since AND store_id = :sid
              GROUP BY store_id, asin
              HAVING SUM(CASE WHEN LOWER(TRIM(COALESCE(status, ''))) = 'active' THEN 1 ELSE 0 END) > 0
            ) t
            """
        )
        r = db.execute(q, {"since": since, "sid": store_id}).scalar()
    else:
        q = text(
            f"""
            SELECT COUNT(*) FROM (
              SELECT store_id, asin FROM {TABLE}
              WHERE open_date IS NOT NULL AND open_date >= :since
              GROUP BY store_id, asin
              HAVING SUM(CASE WHEN LOWER(TRIM(COALESCE(status, ''))) = 'active' THEN 1 ELSE 0 END) > 0
            ) t
            """
        )
        r = db.execute(q, {"since": since}).scalar()
    return int(r or 0)


def _sum_sessions_by_cohort_local(
    db: Session,
    store_id: int | None,
    cohort_date: date,
) -> dict[date, int]:
    """
    本地 daily_upload：该批次日 open_date(PST) 下，按 session_date 汇总 sessions（同步数据已来自 listing，
    避免对数十万 (store,asin) 做 IN 分块导致全 0 或超时）。
    """
    sd_max = cohort_date + timedelta(days=COHORT_TRACK_DAYS - 1)
    if store_id is not None:
        q = text(
            f"""
            SELECT session_date AS sd, SUM(COALESCE(sessions, 0)) AS s
            FROM {TABLE}
            WHERE open_date = :cd AND store_id = :sid
              AND session_date >= :cd AND session_date <= :sdmax
            GROUP BY session_date
            """
        )
        rows = db.execute(
            q, {"cd": cohort_date, "sid": store_id, "sdmax": sd_max}
        ).fetchall()
    else:
        q = text(
            f"""
            SELECT session_date AS sd, SUM(COALESCE(sessions, 0)) AS s
            FROM {TABLE}
            WHERE open_date = :cd
              AND session_date >= :cd AND session_date <= :sdmax
            GROUP BY session_date
            """
        )
        rows = db.execute(q, {"cd": cohort_date, "sdmax": sd_max}).fetchall()
    out: dict[date, int] = {}
    for r in rows:
        try:
            sd = _as_calendar_date(r[0])
        except (ValueError, TypeError):
            continue
        out[sd] = int(r[1] or 0)
    return out


def _sum_sessions_by_cohorts_local_batch(
    db: Session,
    store_id: int | None,
    cohort_dates: list[date],
    *,
    valid_listing_keys: set[NewListingKey] | None = None,
) -> dict[tuple[date, date], int]:
    """
    一次查询多个 cohort（open_date）在各自 30 日窗口内的 (cd, session_date) → sessions，
    替代对每个 cd 调用 _sum_sessions_by_cohort_local。
    """
    if not cohort_dates:
        return {}
    uniq = sorted({d for d in cohort_dates if d is not None})
    if not uniq:
        return {}
    ph = ", ".join([f":cd{i}" for i in range(len(uniq))])
    params: dict = {f"cd{i}": uniq[i] for i in range(len(uniq))}
    nd = max(0, COHORT_TRACK_DAYS - 1)
    extra = " AND store_id = :sid" if store_id is not None else ""
    if store_id is not None:
        params["sid"] = store_id
    if valid_listing_keys is not None:
        q = text(
            f"""
            SELECT asin, COALESCE(pid, 0) AS pid_key, store_id, created_at, open_date AS cd,
                   session_date AS sd, SUM(COALESCE(sessions, 0)) AS s
            FROM {TABLE}
            WHERE open_date IS NOT NULL
              AND created_at IS NOT NULL
              AND open_date IN ({ph})
              AND session_date >= open_date
              AND session_date <= DATE_ADD(open_date, INTERVAL {nd} DAY)
              {extra}
            GROUP BY asin, COALESCE(pid, 0), store_id, created_at, open_date, session_date
            """
        )
    else:
        q = text(
            f"""
            SELECT open_date AS cd, session_date AS sd,
                   SUM(COALESCE(sessions, 0)) AS s
            FROM {TABLE}
            WHERE open_date IS NOT NULL
              AND open_date IN ({ph})
              AND session_date >= open_date
              AND session_date <= DATE_ADD(open_date, INTERVAL {nd} DAY)
              {extra}
            GROUP BY open_date, session_date
            """
        )
    rows = db.execute(q, params).fetchall()
    out: dict[tuple[date, date], int] = {}
    for r in rows:
        if valid_listing_keys is not None:
            key = _local_listing_key(r[0], r[1], r[2], r[3], r[4])
            if key is None or key not in valid_listing_keys:
                continue
            try:
                cd = _as_calendar_date(r[4])
                sd = _as_calendar_date(r[5])
            except (ValueError, TypeError):
                continue
            out[(cd, sd)] = out.get((cd, sd), 0) + int(r[6] or 0)
        else:
            try:
                cd = _as_calendar_date(r[0])
                sd = _as_calendar_date(r[1])
            except (ValueError, TypeError):
                continue
            out[(cd, sd)] = int(r[2] or 0)
    return out


def _cohort_day_asin_breakdown_batch(
    db: Session,
    store_id: int | None,
    cohort_dates: list[date],
    *,
    valid_listing_keys: set[NewListingKey] | None = None,
) -> dict[tuple[date, date], list[dict]]:
    """
    (cohort open_date, session_date) → 当日 sessions>0 的 ASIN 明细（asin + store_id + sessions），
    按 sessions 降序；同一 ASIN+store 合并多条 listing 粒度行。
    """
    if not cohort_dates:
        return {}
    uniq = sorted({d for d in cohort_dates if d is not None})
    if not uniq:
        return {}
    ph = ", ".join([f":cd{i}" for i in range(len(uniq))])
    params: dict = {f"cd{i}": uniq[i] for i in range(len(uniq))}
    nd = max(0, COHORT_TRACK_DAYS - 1)
    extra = " AND store_id = :sid" if store_id is not None else ""
    if store_id is not None:
        params["sid"] = store_id

    merged: dict[tuple[date, date], dict[tuple[str, int], int]] = defaultdict(
        lambda: defaultdict(int)
    )

    if valid_listing_keys is not None:
        q = text(
            f"""
            SELECT asin, COALESCE(pid, 0) AS pid_key, store_id, created_at, open_date AS cd,
                   session_date AS sd, SUM(COALESCE(sessions, 0)) AS s
            FROM {TABLE}
            WHERE open_date IS NOT NULL
              AND created_at IS NOT NULL
              AND open_date IN ({ph})
              AND session_date >= open_date
              AND session_date <= DATE_ADD(open_date, INTERVAL {nd} DAY)
              {extra}
            GROUP BY asin, COALESCE(pid, 0), store_id, created_at, open_date, session_date
            """
        )
        rows = db.execute(q, params).fetchall()
        for r in rows:
            lk = _local_listing_key(r[0], r[1], r[2], r[3], r[4])
            if lk is None or lk not in valid_listing_keys:
                continue
            try:
                cd = _as_calendar_date(r[4])
                sd = _as_calendar_date(r[5])
            except (ValueError, TypeError):
                continue
            asin_key = str(r[0] or "").strip()
            if not asin_key:
                continue
            try:
                sid = int(r[2])
            except (TypeError, ValueError):
                continue
            s = int(r[6] or 0)
            if s <= 0:
                continue
            merged[(cd, sd)][(asin_key, sid)] += s
    else:
        q = text(
            f"""
            SELECT TRIM(asin) AS asin_b, store_id, open_date AS cd, session_date AS sd,
                   SUM(COALESCE(sessions, 0)) AS s
            FROM {TABLE}
            WHERE open_date IS NOT NULL
              AND asin IS NOT NULL
              AND TRIM(asin) <> ''
              AND store_id IS NOT NULL
              AND open_date IN ({ph})
              AND session_date >= open_date
              AND session_date <= DATE_ADD(open_date, INTERVAL {nd} DAY)
              {extra}
            GROUP BY TRIM(asin), store_id, open_date, session_date
            HAVING SUM(COALESCE(sessions, 0)) > 0
            """
        )
        rows = db.execute(q, params).fetchall()
        for r in rows:
            try:
                cd = _as_calendar_date(r[2])
                sd = _as_calendar_date(r[3])
            except (ValueError, TypeError):
                continue
            asin_key = str(r[0] or "").strip()
            if not asin_key:
                continue
            try:
                sid = int(r[1])
            except (TypeError, ValueError):
                continue
            s = int(r[4] or 0)
            if s <= 0:
                continue
            merged[(cd, sd)][(asin_key, sid)] += s

    out: dict[tuple[date, date], list[dict]] = {}
    for pair, amap in merged.items():
        items = [
            {"asin": a, "storeId": sid, "sessions": int(t)}
            for (a, sid), t in amap.items()
            if int(t or 0) > 0
        ]
        items.sort(key=lambda x: (-int(x["sessions"]), x["asin"], x["storeId"]))
        out[pair] = items
    return out


def _fetch_local_session_bounds(db: Session) -> tuple[date | None, date | None]:
    row = db.execute(
        text(f"SELECT MIN(session_date), MAX(session_date) FROM {TABLE}")
    ).fetchone()
    if not row or row[0] is None:
        return None, None
    return _d(row[0]), _d(row[1])


def _fallback_chart_range_from_local_bounds(
    gmin: date | None,
    gmax: date | None,
    *,
    preferred_days: int,
) -> tuple[date, date] | None:
    """
    请求区间无本地 session 时，回退到「本地最近可用窗口」而不是全表 min~max，
    避免 New Listing 首屏把矩阵/online 查询扩大到整段历史。
    """
    if gmin is None or gmax is None or gmin > gmax:
        return None
    days = max(1, int(preferred_days or 1))
    chart_end = gmax
    chart_start = max(gmin, chart_end - timedelta(days=days - 1))
    return chart_start, chart_end


def _has_session_rows_in_range(db: Session, d0: date, d1: date) -> bool:
    r = db.execute(
        text(
            f"SELECT 1 FROM {TABLE} WHERE session_date >= :d0 AND session_date <= :d1 LIMIT 1"
        ),
        {"d0": d0, "d1": d1},
    ).fetchone()
    return r is not None


def _build_cohort_table_rows(
    db: Session,
    online: Connection | None,
    listing_since: date,
    listing_through: date,
    store_id: int | None,
    *,
    prefetched_new_by_day: dict[date, int] | None = None,
    prefetched_mix_by_day: dict[date, dict[str, int]] | None = None,
    prefetched_session_mat_raw: dict[tuple[date, date], int] | None = None,
    valid_listing_keys: set[NewListingKey] | None = None,
    asin_breakdown_online_conn: Connection | None = None,
    matrix_session_start: date | None = None,
    matrix_session_end: date | None = None,
) -> list[dict]:
    """
    一行 = 一个上新日：优先 amazon_listing 当日「纯上新」ASIN 数；
    第 1～30 列 = 本地 daily_upload_asin_dates 中该 open_date 批次的各 session_date sessions。
    """
    mix_by_day: dict[date, dict[str, int]] = {}
    if prefetched_new_by_day is not None:
        new_by_day = prefetched_new_by_day
        if prefetched_mix_by_day is not None:
            mix_by_day = prefetched_mix_by_day
    elif online is not None:
        merged = _fetch_listing_new_refurb_by_day_online(
            online, store_id, listing_since, listing_through
        )
        new_by_day = {cd: int(v.get("new", 0)) for cd, v in merged.items()}
        mix_by_day = merged
    else:
        new_by_day = _fetch_new_asin_by_day(db, store_id, listing_since, listing_through)
    cohort_dates = sorted(
        cd for cd in new_by_day if listing_since <= cd <= listing_through
    )

    if prefetched_session_mat_raw is not None:
        batch_sess = _cohort_sessions_from_matrix(prefetched_session_mat_raw, cohort_dates)
    else:
        batch_sess = _sum_sessions_by_cohorts_local_batch(
            db,
            store_id,
            cohort_dates,
            valid_listing_keys=valid_listing_keys,
        )

    if (
        asin_breakdown_online_conn is not None
        and matrix_session_start is not None
        and matrix_session_end is not None
    ):
        breakdown_by_pair = _cohort_day_asin_breakdown_online(
            asin_breakdown_online_conn,
            store_id,
            cohort_dates,
            traffic_d0=matrix_session_start,
            traffic_d1=matrix_session_end,
            open_date_start=listing_since,
            open_date_end=listing_through,
        )
    else:
        breakdown_by_pair = _cohort_day_asin_breakdown_batch(
            db, store_id, cohort_dates, valid_listing_keys=valid_listing_keys
        )

    rows: list[dict] = []
    for cd in cohort_dates:
        n_new = int(new_by_day[cd])
        mix = mix_by_day.get(cd, {})
        day_sessions: list[int] = []
        day_session_asins: list[list[dict]] = []
        for k in range(COHORT_TRACK_DAYS):
            sd = cd + timedelta(days=k)
            day_sessions.append(int(batch_sess.get((cd, sd), 0)))
            day_session_asins.append(breakdown_by_pair.get((cd, sd), []))
        rows.append(
            {
                "cohortDate": cd.isoformat(),
                "newAsin": n_new,
                "listingNewCount": int(mix.get("new", 0)) if mix else None,
                "listingRefurbishedCount": int(mix.get("refurbished", 0)) if mix else None,
                "daySessions": day_sessions,
                "daySessionAsins": day_session_asins,
            }
        )
    return rows


def _build_view_payload(
    db: Session,
    store_id: int | None,
    listing_since: date,
    chart_cohort_start: date,
    session_start: date,
    session_end: date,
    *,
    matrix_session_start: date,
    matrix_session_end: date,
    total_asin: int,
    active_asin: int,
    listing_new_count: int | None,
    listing_refurbished_count: int | None,
    new_asin_by_day: dict[date, int],
    cohort_table: list[dict],
    online: Connection | None,
    mat_raw: dict[tuple[date, date], int] | None = None,
    cohort_listing_by_day: dict[date, int] | None = None,
    valid_listing_keys: set[NewListingKey] | None = None,
) -> dict:
    if mat_raw is not None:
        mat_raw_use = mat_raw
    else:
        mat_raw_use = _fetch_matrix_rows(
            db,
            store_id,
            matrix_session_start,
            matrix_session_end,
            valid_listing_keys=valid_listing_keys,
            open_date_start=listing_since,
            open_date_end=max(session_end, listing_since),
        )
    mat = _apply_cohort_session_window_to_matrix(
        mat_raw_use,
        listing_since=chart_cohort_start,
        cohort_track_days=COHORT_TRACK_DAYS,
        display_start=session_start,
        display_end=session_end,
    )
    session_days = sorted({k[0] for k in mat})
    cohort_days = sorted({k[1] for k in mat})
    labels = [d.isoformat() for d in session_days]
    cohort_labels = [d.isoformat() for d in cohort_days]
    if cohort_listing_by_day is not None:
        cohort_listing_asin = {
            cd.isoformat(): int(cohort_listing_by_day.get(cd, 0)) for cd in cohort_days
        }
    else:
        cohort_listing_asin = _cohort_listing_asin_map_json(
            db, online, store_id, cohort_days
        )

    by_day: list[dict] = []
    chart_datasets: list[dict] = []
    for j, cd in enumerate(cohort_days):
        series = [mat.get((sd, cd), 0) for sd in session_days]
        chart_datasets.append(
            {
                "type": "bar",
                "label": f"批次 {cd.isoformat()}",
                "data": series,
                "backgroundColor": _COLORS[j % len(_COLORS)],
                "borderWidth": 0,
                "stack": "sess",
                "yAxisID": "y",
            }
        )

    line_total: list[int] = []
    for sd in session_days:
        t = sum(mat.get((sd, cd), 0) for cd in cohort_days)
        line_total.append(int(t))
        parts = [
            {"cohort": cd.isoformat(), "sessions": mat.get((sd, cd), 0)}
            for cd in cohort_days
            if mat.get((sd, cd), 0) > 0
        ]
        parts.sort(key=lambda x: -x["sessions"])
        by_day.append(
            {
                "sessionDate": sd.isoformat(),
                "totalSessions": t,
                "newAsinCount": int(new_asin_by_day.get(sd, 0)),
                "cohortParts": parts,
            }
        )

    key = "all" if store_id is None else str(store_id)
    return {
        "key": key,
        "storeId": store_id,
        "labels": labels,
        "cohortLabels": cohort_labels,
        "datasets": chart_datasets,
        "lineTotal": line_total,
        "byDay": by_day,
        "kpi": {
            "totalAsin": total_asin,
            "activeAsin": active_asin,
            "listingNewCount": listing_new_count,
            "listingRefurbishedCount": listing_refurbished_count,
            "listingSince": listing_since.isoformat(),
        },
        "cohortTable": cohort_table,
        "cohortListingAsin": cohort_listing_asin,
    }


def build_report_payload(
    db: Session,
    listing_since: date,
    session_start: date,
    session_end: date,
    *,
    prefer_online: bool = True,
    prefer_listing_online: bool = True,
    profile: bool = False,
    json_views_mode: str = "full",
    single_store_id: int | None = None,
) -> dict:
    """
    生成 PST New Listing 报表 JSON/HTML 用 payload。

    - ``json_views_mode="full"``：全部店铺视图 + 各店视图（HTML/CLI 默认）。
    - ``json_views_mode="all_only"``：仅 ``views["all"]``，单店按需另请求。
    - ``json_views_mode="store"``：仅 ``views[str(single_store_id)]``，需 ``single_store_id`` 合法 int。
    """
    if json_views_mode not in ("full", "all_only", "store"):
        json_views_mode = "full"
    if json_views_mode == "store" and single_store_id is None:
        json_views_mode = "full"

    t0 = time.perf_counter()
    timings: dict[str, float] = {}

    def _t(name: str, t_start: float) -> None:
        if profile:
            timings[name] = round(time.perf_counter() - t_start, 4)

    t_phase = time.perf_counter()
    # cohort / 上新统计起点不早于 DEFAULT_LISTING_SINCE（2026-02-20）
    listing_since = max(listing_since, DEFAULT_LISTING_SINCE)
    if session_start < listing_since:
        session_start = listing_since
    online = _try_online_conn() if prefer_online else None
    listing_conn = online
    listing_conn_owned_separately = False
    if listing_conn is None and prefer_listing_online:
        c = _try_online_conn()
        if c is not None:
            listing_conn = c
            listing_conn_owned_separately = True

    _online_db_configured = bool(settings.ONLINE_DB_HOST and settings.ONLINE_DB_USER)
    if online is not None:
        kpi_source = "amazon_listing"
    elif listing_conn is not None and prefer_listing_online:
        kpi_source = "amazon_listing"
    elif _online_db_configured and prefer_listing_online:
        kpi_source = "amazon_listing_unreachable"
    else:
        kpi_source = "daily_upload_asin_dates_fallback"
    if prefer_listing_online and listing_conn is None:
        logger.warning(
            "[PST] online_db 不可用，cohort 表「上新日/上新 listing 行数」回退本地 daily_upload_asin_dates，与 amazon_listing 可能不一致"
        )
    # PST 报表：上新日统计区间跟随本次报表的 session_end（便于与对账 SQL open_date BETWEEN 起止一致）
    listing_through = session_end
    _t("phase.init_and_online_conn", t_phase)
    t_phase = time.perf_counter()
    gmin, gmax = _fetch_local_session_bounds(db)
    chart_start, chart_end = session_start, session_end
    chart_auto = False
    if not _has_session_rows_in_range(db, session_start, session_end):
        fallback_range = _fallback_chart_range_from_local_bounds(
            gmin,
            gmax,
            preferred_days=DEFAULT_RECENT_WINDOW_DAYS,
        )
        if fallback_range is not None:
            chart_start, chart_end = fallback_range
            chart_auto = True
    _t("phase.local_bounds_and_range", t_phase)

    matrix_session_end = max(
        chart_end,
        listing_through + timedelta(days=COHORT_TRACK_DAYS - 1),
    )
    matrix_session_start = min(chart_start, listing_since)
    chart_cohort_start = max(
        DEFAULT_LISTING_SINCE,
        chart_start - timedelta(days=COHORT_TRACK_DAYS - 1),
    )

    store_ids = _fetch_store_ids_for_range(db, matrix_session_start, matrix_session_end)
    try:
        kpi_by: dict[int | None, tuple[int, int]] = {}
        listing_mix_by: dict[int | None, tuple[int | None, int | None]] = {}
        listing_mix_cohort_by: dict[int | None, dict[date, dict[str, int]]] = {}
        new_chart_by: dict[int | None, dict[date, int]] = {}
        new_cohort_by: dict[int | None, dict[date, int]] = {}
        valid_new_keys_by: dict[int | None, set[NewListingKey]] = {}

        if json_views_mode == "store" and single_store_id is not None:
            t_phase = time.perf_counter()
            mat_all_raw: dict = {}
            mat_by_store_raw: dict[int, dict] = {}
            cached = _matrix_bulk_cache_get(matrix_session_start, matrix_session_end)
            if cached is not None:
                _, by_store_cached = cached
                chart_mat_one = by_store_cached.get(int(single_store_id), {}) or {}
            else:
                chart_mat_one = _fetch_matrix_rows(
                    db, single_store_id, matrix_session_start, matrix_session_end
                )
            mat_one = chart_mat_one
            _t("phase.local_matrix_single_store", t_phase)
            target_sids = [single_store_id]
            online_prefetch_store_ids = [single_store_id]
        else:
            t_phase = time.perf_counter()
            chart_mat_all_raw, chart_mat_by_store_raw = _fetch_matrix_rows_bulk(
                db, matrix_session_start, matrix_session_end
            )
            mat_all_raw, mat_by_store_raw = chart_mat_all_raw, chart_mat_by_store_raw
            _t("phase.local_matrix_bulk", t_phase)
            target_sids = [None, *store_ids] if json_views_mode == "full" else [None]
            online_prefetch_store_ids = list(store_ids)

        def _amazon_listing_prefetch_on_conn(conn: Connection, sid: int | None):
            """KPI + 按日上新数均查 online ``amazon_listing``，不回退本地 daily_upload_asin_dates。"""
            kpi = _fetch_listing_kpi_online(conn, listing_since, sid)
            d0_u = min(chart_cohort_start, listing_since)
            d1_u = max(chart_end, listing_through)
            merged = _fetch_listing_new_refurb_by_day_online(conn, sid, d0_u, d1_u)
            valid_keys = _fetch_new_listing_keys_online(conn, sid, listing_since, listing_through)
            nc = {
                k: int(v.get("new", 0)) for k, v in merged.items() if chart_cohort_start <= k <= chart_end
            }
            nh = {
                k: int(v.get("new", 0)) for k, v in merged.items() if listing_since <= k <= listing_through
            }
            cohort_mix = {
                k: {
                    "new": int(v.get("new", 0)),
                    "refurbished": int(v.get("refurbished", 0)),
                    "total": int(v.get("total", 0)),
                }
                for k, v in merged.items()
                if listing_since <= k <= listing_through
            }
            mix_new = 0
            mix_refurb = 0
            for k, v in merged.items():
                if listing_since <= k <= listing_through:
                    mix_new += int(v.get("new", 0))
                    mix_refurb += int(v.get("refurbished", 0))
            return sid, kpi, nc, nh, (mix_new, mix_refurb), cohort_mix, valid_keys

        def _prefetch_worker(sid: int | None):
            eng = get_online_reporting_engine()
            with eng.connect() as c:
                return _amazon_listing_prefetch_on_conn(c, sid)

        def _aggregate_all_scope_from_store_prefetch() -> None:
            if json_views_mode == "store" and single_store_id is not None:
                return
            if not online_prefetch_store_ids:
                kpi_by[None] = (0, 0)
                listing_mix_by[None] = (0, 0)
                listing_mix_cohort_by[None] = {}
                valid_new_keys_by[None] = set()
                new_chart_by[None] = {}
                new_cohort_by[None] = {}
                return
            kpi_by[None] = (
                sum(int(kpi_by.get(sid, (0, 0))[0] or 0) for sid in online_prefetch_store_ids),
                sum(int(kpi_by.get(sid, (0, 0))[1] or 0) for sid in online_prefetch_store_ids),
            )
            listing_mix_by[None] = (
                sum(int(listing_mix_by.get(sid, (0, 0))[0] or 0) for sid in online_prefetch_store_ids),
                sum(int(listing_mix_by.get(sid, (0, 0))[1] or 0) for sid in online_prefetch_store_ids),
            )
            listing_mix_cohort_by[None] = _merge_daily_mix_maps(
                *(listing_mix_cohort_by.get(sid) for sid in online_prefetch_store_ids)
            )
            valid_new_keys_by[None] = set().union(
                *(valid_new_keys_by.get(sid, set()) for sid in online_prefetch_store_ids)
            )
            new_chart_by[None] = _merge_daily_count_maps(
                *(new_chart_by.get(sid) for sid in online_prefetch_store_ids)
            )
            new_cohort_by[None] = _merge_daily_count_maps(
                *(new_cohort_by.get(sid) for sid in online_prefetch_store_ids)
            )

        use_amazon_listing = (online is not None) or (
            listing_conn is not None and prefer_listing_online
        )
        if use_amazon_listing:
            t_phase = time.perf_counter()
            if listing_conn is None:
                logger.warning("[PST] use_amazon_listing but listing_conn is None; listing prefetch skipped")
                for sid in target_sids:
                    kpi_by[sid] = (0, 0)
                    listing_mix_by[sid] = (None, None)
                    listing_mix_cohort_by[sid] = {}
                    valid_new_keys_by[sid] = set()
                    new_chart_by[sid] = _fetch_new_asin_by_day(
                        db, sid, chart_cohort_start, chart_end
                    )
                    new_cohort_by[sid] = _fetch_new_asin_by_day(db, sid, listing_since, listing_through)
            else:
                prefetch_sids = (
                    [single_store_id]
                    if json_views_mode == "store" and single_store_id is not None
                    else online_prefetch_store_ids
                )
                if len(prefetch_sids) > 1:
                    pool_cap = max(
                        1,
                        int(settings.ONLINE_REPORT_POOL_SIZE)
                        + int(settings.ONLINE_REPORT_POOL_OVERFLOW)
                        - 1,
                    )
                    max_workers = min(len(prefetch_sids), pool_cap)
                    fmap = {}
                    with ThreadPoolExecutor(max_workers=max_workers) as ex:
                        for sid in prefetch_sids:
                            fmap[ex.submit(_prefetch_worker, sid)] = sid
                        for fut in as_completed(fmap):
                            sid = fmap[fut]
                            try:
                                sid2, kpi, nc, nh, mix, cohort_mix, valid_keys = fut.result()
                                kpi_by[sid2] = kpi
                                listing_mix_by[sid2] = mix
                                listing_mix_cohort_by[sid2] = cohort_mix
                                valid_new_keys_by[sid2] = valid_keys
                                new_chart_by[sid2] = nc
                                new_cohort_by[sid2] = nh
                            except Exception as exc:
                                logger.warning(
                                    "[PST] amazon_listing prefetch parallel store_id=%s failed (degraded): %s",
                                    sid,
                                    exc,
                                )
                                kpi_by[sid] = (0, 0)
                                listing_mix_by[sid] = (None, None)
                                listing_mix_cohort_by[sid] = {}
                                valid_new_keys_by[sid] = set()
                                new_chart_by[sid] = _fetch_new_asin_by_day(
                                    db, sid, chart_cohort_start, chart_end
                                )
                                new_cohort_by[sid] = _fetch_new_asin_by_day(
                                    db, sid, listing_since, listing_through
                                )
                else:
                    for sid in prefetch_sids:
                        try:
                            sid2, kpi, nc, nh, mix, cohort_mix, valid_keys = _amazon_listing_prefetch_on_conn(
                                listing_conn, sid
                            )
                            kpi_by[sid2] = kpi
                            listing_mix_by[sid2] = mix
                            listing_mix_cohort_by[sid2] = cohort_mix
                            valid_new_keys_by[sid2] = valid_keys
                            new_chart_by[sid2] = nc
                            new_cohort_by[sid2] = nh
                        except Exception as exc:
                            logger.warning(
                                "[PST] amazon_listing prefetch store_id=%s failed (degraded): %s", sid, exc
                            )
                            kpi_by[sid] = (0, 0)
                            listing_mix_by[sid] = (None, None)
                            listing_mix_cohort_by[sid] = {}
                            valid_new_keys_by[sid] = set()
                            new_chart_by[sid] = _fetch_new_asin_by_day(
                                db, sid, chart_cohort_start, chart_end
                            )
                            new_cohort_by[sid] = _fetch_new_asin_by_day(
                                db, sid, listing_since, listing_through
                            )
                _aggregate_all_scope_from_store_prefetch()
            _t("phase.online_listing_prefetch", t_phase)
        elif _online_db_configured and prefer_listing_online:
            t_phase = time.perf_counter()
            logger.warning(
                "[PST] online_db_host 已配置但当前无法建立连接，KPI 不回退本地表，置 0；上新按日数据用本地"
            )
            for sid in target_sids:
                kpi_by[sid] = (0, 0)
                listing_mix_by[sid] = (None, None)
                listing_mix_cohort_by[sid] = {}
            for sid in target_sids:
                new_chart_by[sid] = _fetch_new_asin_by_day(
                    db, sid, chart_cohort_start, chart_end
                )
                new_cohort_by[sid] = _fetch_new_asin_by_day(
                    db, sid, listing_since, listing_through
                )
            _t("phase.local_fallback_kpi_and_new_by_day", t_phase)
        else:
            t_phase = time.perf_counter()
            for sid in target_sids:
                kpi_by[sid] = (
                    _fetch_total_asin_since(db, sid, listing_since),
                    _fetch_active_asin_since(db, sid, listing_since),
                )
                listing_mix_by[sid] = (None, None)
                listing_mix_cohort_by[sid] = {}
            for sid in target_sids:
                new_chart_by[sid] = _fetch_new_asin_by_day(
                    db, sid, chart_cohort_start, chart_end
                )
                new_cohort_by[sid] = _fetch_new_asin_by_day(
                    db, sid, listing_since, listing_through
                )
            _t("phase.local_fallback_all", t_phase)

        if use_amazon_listing:
            t_phase = time.perf_counter()
            if listing_conn is not None:
                if json_views_mode == "store" and single_store_id is not None:
                    table_mat_one = _fetch_matrix_rows_online(
                        listing_conn,
                        single_store_id,
                        matrix_session_start,
                        matrix_session_end,
                        open_date_start=listing_since,
                        open_date_end=listing_through,
                    )
                    mat_one = table_mat_one
                else:
                    table_mat_by_store_raw: dict[int, dict[tuple[date, date], int]] = {}

                    def _matrix_worker(sid: int):
                        eng = get_online_reporting_engine()
                        with eng.connect() as c:
                            return (
                                sid,
                                _fetch_matrix_rows_online(
                                    c,
                                    sid,
                                    matrix_session_start,
                                    matrix_session_end,
                                    open_date_start=listing_since,
                                    open_date_end=listing_through,
                                ),
                            )

                    if len(online_prefetch_store_ids) > 1:
                        pool_cap = max(
                            1,
                            int(settings.ONLINE_REPORT_POOL_SIZE)
                            + int(settings.ONLINE_REPORT_POOL_OVERFLOW)
                            - 1,
                        )
                        max_workers = min(len(online_prefetch_store_ids), pool_cap)
                        fmap = {}
                        with ThreadPoolExecutor(max_workers=max_workers) as ex:
                            for sid in online_prefetch_store_ids:
                                fmap[ex.submit(_matrix_worker, sid)] = sid
                            for fut in as_completed(fmap):
                                sid = fmap[fut]
                                try:
                                    sid2, mat_s = fut.result()
                                    table_mat_by_store_raw[sid2] = mat_s
                                except Exception as exc:
                                    logger.warning(
                                        "[PST] online matrix store_id=%s failed (degraded to empty): %s",
                                        sid,
                                        exc,
                                    )
                                    table_mat_by_store_raw[sid] = {}
                    else:
                        for sid in online_prefetch_store_ids:
                            table_mat_by_store_raw[sid] = _fetch_matrix_rows_online(
                                listing_conn,
                                sid,
                                matrix_session_start,
                                matrix_session_end,
                                open_date_start=listing_since,
                                open_date_end=listing_through,
                            )
                    table_mat_all_raw = _merge_matrix_maps(
                        *(table_mat_by_store_raw.get(sid) for sid in online_prefetch_store_ids)
                    )
                    mat_all_raw, mat_by_store_raw = table_mat_all_raw, table_mat_by_store_raw
            else:
                if json_views_mode == "store" and single_store_id is not None:
                    table_mat_one = _fetch_matrix_rows(
                        db,
                        single_store_id,
                        matrix_session_start,
                        matrix_session_end,
                        valid_listing_keys=valid_new_keys_by.get(single_store_id),
                        open_date_start=listing_since,
                        open_date_end=listing_through,
                    )
                    mat_one = table_mat_one
                else:
                    table_mat_all_raw, table_mat_by_store_raw = _fetch_matrix_rows_bulk(
                        db,
                        matrix_session_start,
                        matrix_session_end,
                        valid_listing_keys=valid_new_keys_by.get(None),
                        open_date_start=listing_since,
                        open_date_end=listing_through,
                    )
                    mat_all_raw, mat_by_store_raw = table_mat_all_raw, table_mat_by_store_raw
            _t("phase.local_matrix_filter_new_only", t_phase)

        # cohort 表格子若走线上矩阵，悬停 ASIN 明细必须与同一数据源（见 _cohort_day_asin_breakdown_online）
        asin_bd_online = listing_conn if use_amazon_listing and listing_conn is not None else None

        views: dict[str, dict] = {}
        if json_views_mode == "store" and single_store_id is not None:
            sid = single_store_id
            t, a = kpi_by[sid]
            mix_new, mix_refurb = listing_mix_by.get(sid, (None, None))
            t_phase = time.perf_counter()
            ct = _build_cohort_table_rows(
                db,
                listing_conn,
                listing_since,
                listing_through,
                sid,
                prefetched_new_by_day=new_cohort_by[sid],
                prefetched_mix_by_day=listing_mix_cohort_by.get(sid),
                prefetched_session_mat_raw=mat_one,
                valid_listing_keys=valid_new_keys_by.get(sid),
                asin_breakdown_online_conn=asin_bd_online,
                matrix_session_start=matrix_session_start,
                matrix_session_end=matrix_session_end,
            )
            _t("phase.cohort_single_store", t_phase)
            t_phase2 = time.perf_counter()
            views[str(sid)] = _build_view_payload(
                db,
                sid,
                listing_since,
                chart_cohort_start,
                chart_start,
                chart_end,
                matrix_session_start=matrix_session_start,
                matrix_session_end=matrix_session_end,
                total_asin=t,
                active_asin=a,
                listing_new_count=mix_new,
                listing_refurbished_count=mix_refurb,
                new_asin_by_day=new_chart_by[sid],
                cohort_table=ct,
                online=listing_conn,
                mat_raw=chart_mat_one if use_amazon_listing else mat_one,
                cohort_listing_by_day=new_cohort_by[sid],
                valid_listing_keys=valid_new_keys_by.get(sid),
            )
            if profile:
                timings["phase.views_single_store"] = round(time.perf_counter() - t_phase2, 4)
        else:
            all_tot, all_act = kpi_by[None]
            all_mix_new, all_mix_refurb = listing_mix_by.get(None, (None, None))
            t_phase = time.perf_counter()
            all_cohort = _build_cohort_table_rows(
                db,
                listing_conn,
                listing_since,
                listing_through,
                None,
                prefetched_new_by_day=new_cohort_by[None],
                prefetched_mix_by_day=listing_mix_cohort_by.get(None),
                prefetched_session_mat_raw=mat_all_raw,
                valid_listing_keys=valid_new_keys_by.get(None),
                asin_breakdown_online_conn=asin_bd_online,
                matrix_session_start=matrix_session_start,
                matrix_session_end=matrix_session_end,
            )
            _t("phase.cohort_all", t_phase)
            t_phase_va = time.perf_counter()
            views["all"] = _build_view_payload(
                db,
                None,
                listing_since,
                chart_cohort_start,
                chart_start,
                chart_end,
                matrix_session_start=matrix_session_start,
                matrix_session_end=matrix_session_end,
                total_asin=all_tot,
                active_asin=all_act,
                listing_new_count=all_mix_new,
                listing_refurbished_count=all_mix_refurb,
                new_asin_by_day=new_chart_by[None],
                cohort_table=all_cohort,
                online=listing_conn,
                mat_raw=chart_mat_all_raw if use_amazon_listing else mat_all_raw,
                cohort_listing_by_day=new_cohort_by[None],
                valid_listing_keys=valid_new_keys_by.get(None),
            )
            if profile:
                timings["phase.views_all"] = round(time.perf_counter() - t_phase_va, 4)
            if json_views_mode == "full":
                for sid in store_ids:
                    t, a = kpi_by[sid]
                    mix_new, mix_refurb = listing_mix_by.get(sid, (None, None))
                    mat_s = mat_by_store_raw.get(sid)
                    chart_mat_s = chart_mat_by_store_raw.get(sid) if use_amazon_listing else mat_s
                    t_phase = time.perf_counter()
                    ct = _build_cohort_table_rows(
                        db,
                        listing_conn,
                        listing_since,
                        listing_through,
                        sid,
                        prefetched_new_by_day=new_cohort_by[sid],
                        prefetched_mix_by_day=listing_mix_cohort_by.get(sid),
                        prefetched_session_mat_raw=mat_s if mat_s is not None else {},
                        valid_listing_keys=valid_new_keys_by.get(sid),
                        asin_breakdown_online_conn=asin_bd_online,
                        matrix_session_start=matrix_session_start,
                        matrix_session_end=matrix_session_end,
                    )
                    if profile:
                        timings.setdefault("phase.cohort_per_store_total", 0.0)
                        timings["phase.cohort_per_store_total"] = round(
                            float(timings["phase.cohort_per_store_total"])
                            + (time.perf_counter() - t_phase),
                            4,
                        )
                    t_phase2 = time.perf_counter()
                    views[str(sid)] = _build_view_payload(
                        db,
                        sid,
                        listing_since,
                        chart_cohort_start,
                        chart_start,
                        chart_end,
                        matrix_session_start=matrix_session_start,
                        matrix_session_end=matrix_session_end,
                        total_asin=t,
                        active_asin=a,
                        listing_new_count=mix_new,
                        listing_refurbished_count=mix_refurb,
                        new_asin_by_day=new_chart_by[sid],
                        cohort_table=ct,
                        online=listing_conn,
                        mat_raw=chart_mat_s if chart_mat_s is not None else {},
                        cohort_listing_by_day=new_cohort_by[sid],
                        valid_listing_keys=valid_new_keys_by.get(sid),
                    )
                    if profile:
                        timings.setdefault("phase.views_per_store_total", 0.0)
                        timings["phase.views_per_store_total"] = round(
                            float(timings["phase.views_per_store_total"])
                            + (time.perf_counter() - t_phase2),
                            4,
                        )

        payload = {
            "generatedAt": date.today().isoformat(),
            "listingSince": listing_since.isoformat(),
            "listingThrough": listing_through.isoformat(),
            "sessionRequestedStart": session_start.isoformat(),
            "sessionRequestedEnd": session_end.isoformat(),
            "sessionChartStart": chart_start.isoformat(),
            "sessionChartEnd": chart_end.isoformat(),
            "matrixSessionFetchStart": matrix_session_start.isoformat(),
            "matrixSessionFetchEnd": matrix_session_end.isoformat(),
            "chartRangeAutoExpanded": chart_auto,
            "localSessionMin": gmin.isoformat() if gmin else None,
            "localSessionMax": gmax.isoformat() if gmax else None,
            "storeIds": store_ids,
            "views": views,
            "kpiSource": kpi_source,
            "cohortTrackDays": COHORT_TRACK_DAYS,
            "jsonViewsMode": json_views_mode,
            "viewsPartial": json_views_mode in ("all_only", "store"),
        }
        if json_views_mode == "store" and single_store_id is not None:
            payload["requestedStoreId"] = int(single_store_id)
        if profile:
            payload["profileTimingsSec"] = {**timings, "total": round(time.perf_counter() - t0, 4)}
        logger.info(
            "[PST] build_report_payload done: mode=%s prefer_online=%s online=%s listing_conn=%s stores=%s matrix_range=%s..%s chart_range=%s..%s elapsed_sec=%.2f",
            json_views_mode,
            prefer_online,
            online is not None,
            listing_conn is not None,
            len(store_ids),
            matrix_session_start,
            matrix_session_end,
            chart_start,
            chart_end,
            time.perf_counter() - t0,
        )
        if profile:
            logger.info("[PST] build_report_payload profile timings (sec): %s", payload.get("profileTimingsSec"))
        return payload
    finally:
        if online is not None:
            try:
                online.close()
            except Exception:
                pass
        elif listing_conn_owned_separately and listing_conn is not None:
            try:
                listing_conn.close()
            except Exception:
                pass


def render_html(payload: dict) -> str:
    json_str = json.dumps(payload, ensure_ascii=False)
    return _HTML_TEMPLATE.replace("__PAYLOAD_JSON__", json_str)


_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
  <title>每日上新 Session 报表</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
    *, *::before, *::after { box-sizing: border-box; }
    html { -webkit-text-size-adjust: 100%; }
    :root { font-family: system-ui, "PingFang SC", "Microsoft YaHei", sans-serif; }
    body {
      margin: 0;
      padding: clamp(10px, 2.5vw, 24px);
      padding-bottom: max(clamp(10px, 2.5vw, 24px), env(safe-area-inset-bottom));
      background: #f4f4f5;
      color: #18181b;
      min-height: 100dvh;
      max-width: 100vw;
      overflow-x: hidden;
    }
    .page {
      width: 100%;
      max-width: min(1280px, 100%);
      margin: 0 auto;
    }
    h1 { font-size: clamp(1.05rem, 3.5vw, 1.35rem); margin: 0 0 10px; line-height: 1.3; }
    .row {
      display: flex;
      flex-wrap: wrap;
      gap: clamp(10px, 2vw, 16px);
      align-items: flex-start;
      margin-bottom: 16px;
    }
    .row > div:first-child { flex: 0 1 auto; }
    #rangeHint {
      flex: 1 1 200px;
      min-width: 0;
      word-break: break-word;
    }
    label { font-size: clamp(0.8rem, 2.2vw, 0.875rem); color: #52525b; }
    select {
      padding: 8px 12px;
      border-radius: 8px;
      border: 1px solid #d4d4d8;
      min-width: 0;
      width: 100%;
      max-width: min(360px, 100%);
      font-size: max(16px, 0.9rem);
    }
    .kpi {
      display: flex;
      flex-wrap: wrap;
      gap: clamp(12px, 2vw, 20px);
      margin-bottom: 20px;
    }
    .kpi-card {
      background: #fff;
      border-radius: 12px;
      padding: clamp(12px, 2.5vw, 18px) clamp(14px, 3vw, 20px);
      box-shadow: 0 1px 3px rgb(0 0 0 / 0.08);
      flex: 1 1 240px;
      min-width: min(100%, 200px);
      max-width: 100%;
    }
    .kpi-card strong {
      display: block;
      font-size: clamp(1.35rem, 5vw, 1.75rem);
      margin-top: 4px;
      word-break: break-all;
    }
    .muted {
      font-size: clamp(0.72rem, 2vw, 0.8rem);
      color: #71717a;
      margin-top: 4px;
      line-height: 1.45;
    }
    .chart-wrap {
      background: #fff;
      border-radius: 12px;
      padding: clamp(12px, 2.5vw, 18px);
      box-shadow: 0 1px 3px rgb(0 0 0 / 0.08);
      width: 100%;
      max-width: 100%;
    }
    .chart-canvas-box {
      position: relative;
      width: 100%;
      height: clamp(220px, 38vh, 520px);
      min-height: 200px;
      max-height: 70vh;
    }
    .chart-canvas-box canvas {
      display: block;
      width: 100% !important;
      height: 100% !important;
    }
    .table-wrap {
      margin-top: clamp(14px, 3vw, 22px);
      width: 100%;
      max-width: 100%;
      border-radius: 12px;
      box-shadow: 0 1px 3px rgb(0 0 0 / 0.08);
      background: #fff;
      padding: clamp(12px, 2.5vw, 18px);
    }
    .table-wrap h2 {
      font-size: clamp(0.88rem, 2.8vw, 1rem);
      margin: 0 0 12px;
      font-weight: 600;
      line-height: 1.35;
    }
    .table-scroll {
      overflow-x: auto;
      overflow-y: visible;
      max-width: 100%;
      -webkit-overflow-scrolling: touch;
      margin: 0 -4px;
      padding: 0 4px;
    }
    #cohortTbl { border-collapse: collapse; font-size: clamp(9px, 2.1vw, 11px); width: max-content; min-width: 100%; }
    #cohortTbl th, #cohortTbl td {
      border: 1px solid #e4e4e7;
      padding: 3px clamp(4px, 1.2vw, 8px);
      text-align: right;
      white-space: nowrap;
    }
    #cohortTbl th:nth-child(1), #cohortTbl td:nth-child(1),
    #cohortTbl th:nth-child(2), #cohortTbl td:nth-child(2) { text-align: left; }
    #cohortTbl thead th { background: #f4f4f5; position: sticky; top: 0; z-index: 1; }
    #cohortTbl tbody tr:nth-child(even) { background: #fafafa; }
    @media (max-width: 480px) {
      .kpi-card { flex-basis: 100%; }
    }
    .chart-tooltip-external {
      position: fixed;
      z-index: 10050;
      max-width: min(92vw, 440px);
      overflow: hidden;
      display: flex;
      flex-direction: column;
      background: rgba(28, 28, 30, 0.97);
      color: #fafafa;
      border-radius: 10px;
      box-shadow: 0 8px 32px rgba(0, 0, 0, 0.38);
      font-size: 12px;
      line-height: 1.45;
      pointer-events: auto;
    }
    .chart-tooltip-external-title {
      font-weight: 600;
      padding: 10px 12px 8px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.12);
      flex-shrink: 0;
    }
    .chart-tooltip-external-scroll {
      overflow-y: auto;
      overflow-x: hidden;
      -webkit-overflow-scrolling: touch;
      padding: 8px 12px 12px;
      max-height: min(62vh, 500px);
    }
    .chart-tooltip-external-row {
      display: flex;
      align-items: flex-start;
      gap: 8px;
      margin: 5px 0;
    }
    .chart-tooltip-external-row .swatch {
      width: 12px;
      height: 12px;
      border-radius: 2px;
      flex-shrink: 0;
      margin-top: 3px;
      border: 1px solid rgba(255, 255, 255, 0.15);
    }
    .chart-tooltip-external-row .txt {
      flex: 1;
      min-width: 0;
      word-break: break-word;
    }
    .chart-tooltip-external-sep { opacity: 0.55; margin: 10px 0 6px; font-size: 11px; }
    .chart-tooltip-external-footer { font-size: 11px; }
    .chart-tooltip-external-sub { margin-top: 3px; opacity: 0.92; }
  </style>
</head>
<body>
<div class="page">
  <h1>每日上新 Session（按批次堆叠）</h1>
  <div class="row">
    <div>
      <label for="storeSel">店铺</label><br/>
      <select id="storeSel"></select>
    </div>
    <div class="muted" id="rangeHint"></div>
  </div>
  <div class="kpi">
    <div class="kpi-card">
      <span>Total Asins</span>
      <strong id="kpiTotal">—</strong>
      <div class="muted">amazon_listing <code>COUNT(*)</code>，<code>DATE(open_date) &gt; listing_since</code>（全表行，含 asin 为空）</div>
    </div>
    <div class="kpi-card">
      <span>Active Asins</span>
      <strong id="kpiActive">—</strong>
      <div class="muted">同上且 <code>status = 'Active'</code></div>
    </div>
  </div>
  <p class="muted" id="kpiSourceHint" style="margin-bottom:16px"></p>
  <div class="chart-wrap">
    <p class="muted" style="margin-top:0">柱形为各上新批次贡献的 sessions 堆叠；黑色折线为每日 sessions 合计。悬停主列表中各批次数字为 amazon_listing 该批次日的纯上新 ASIN 数；底部为当日 session 明细、上新 ASIN 数与占比。</p>
    <p id="noData" class="muted" style="display:none"></p>
    <div class="chart-canvas-box"><canvas id="ch"></canvas></div>
  </div>

  <div class="table-wrap" id="cohortTableWrap">
    <h2>批次上新 ASIN 的 30 日 session 变化（PST：open_date 为上新日）</h2>
    <p class="muted" style="margin:0 0 10px" id="cohortTableHint"></p>
    <div class="table-scroll">
      <table id="cohortTbl"></table>
    </div>
  </div>

  <script type="application/json" id="payload">__PAYLOAD_JSON__</script>
  <script>
  const P = JSON.parse(document.getElementById('payload').textContent);
  const sel = document.getElementById('storeSel');
  const sinceLbl = document.getElementById('sinceLbl');
  const rangeHint = document.getElementById('rangeHint');
  const kpiTotal = document.getElementById('kpiTotal');
  const kpiActive = document.getElementById('kpiActive');

  sinceLbl.textContent = P.listingSince;
  (function setRangeHint() {
    var rq0 = P.sessionRequestedStart || P.sessionChartStart;
    var rq1 = P.sessionRequestedEnd || P.sessionChartEnd;
    var h = '请求 session 区间：' + rq0 + ' ~ ' + rq1;
    if (P.sessionChartStart && (P.chartRangeAutoExpanded || P.sessionChartStart !== rq0 || P.sessionChartEnd !== rq1)) {
      h += ' · 图表使用：' + P.sessionChartStart + ' ~ ' + P.sessionChartEnd;
    }
    if (P.chartRangeAutoExpanded) {
      h += '（本地在请求区间内无数据，已自动回退到最近可用 session 窗口）';
    }
    h += ' · 生成日 ' + P.generatedAt;
    rangeHint.textContent = h;
  })();
  (function setNoDataText() {
    var el = document.getElementById('noData');
    var parts = ['当前视图下仍无 session 数据（本地 daily_upload_asin_dates 在该区间为空）。'];
    if (P.localSessionMin && P.localSessionMax) {
      parts.push('表中已有 session_date 约在 ' + P.localSessionMin + ' ~ ' + P.localSessionMax + '，请检查店铺筛选或先执行 daily_upload 同步。');
    } else {
      parts.push('本地表尚无数据，请在 backend 目录运行：python3.11 -m app.services.daily_upload_asin_data --start-date … --end-date …');
    }
    el.textContent = parts.join('');
  })();
  var kpiHint = document.getElementById('kpiSourceHint');
  if (P.kpiSource === 'amazon_listing') {
    kpiHint.textContent = \"说明：顶部 KPI = online_db.amazon_listing：Total 为 DATE(open_date) > listing_since 的 COUNT(*)；Active 另加 status = 'Active'。与下方表格第二列（按日、asin 非空）口径不同。堆叠图按 open_date 批次；第1～30天为本地 session。\";
  } else if (P.kpiSource === 'amazon_listing_unreachable') {
    kpiHint.textContent = '说明：已配置 online_db 但当前无法连接，KPI 为 0（不回退本地 daily_upload 冒充 listing 行数）。请检查 online_db_host / 网络 / 权限。';
  } else {
    kpiHint.textContent = '说明：未配置 online_db 时 KPI 来自本地 daily_upload_asin_dates；配置后 KPI 仅来自 amazon_listing。';
  }
  document.getElementById('cohortTableHint').textContent =
    '上新统计区间：' + P.listingSince + ' ～ ' + P.listingThrough + '（与图表 session 区间无关；listing_through 默认等于本次 session_end）。第二列「上新 ASIN 数」= online amazon_listing 中按 DATE(open_date) 统计的纯上新数量（DATE(amazon_listing.created_at)=DATE(amazon_variation.created_at)）。「第 k 天」= 该批次 sessions 合计；括号内百分比 = sessions / 该批次上新 ASIN 数。图表折线按 session_date 把各批次堆叠相加，与表格某一行的「第几天」口径不同。Online 不可用时第二列回退本地口径估算。';

  sel.innerHTML = '';
  const optAll = document.createElement('option');
  optAll.value = 'all';
  optAll.textContent = '全部店铺（聚合）';
  sel.appendChild(optAll);
  (P.storeIds || []).forEach(function (id) {
    const o = document.createElement('option');
    o.value = String(id);
    o.textContent = '店铺 ' + id;
    sel.appendChild(o);
  });

  let chart = null;
  var currentTooltipView = null;

  function escapeHtml(s) {
    if (s == null) return '';
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function formatSessionSharePercent(numerator, denominator) {
    var n = Number(numerator || 0);
    var d = Number(denominator || 0);
    if (!Number.isFinite(n) || !Number.isFinite(d) || d <= 0) return '—';
    var pct = n / d * 100;
    var digits = pct !== 0 && Math.abs(pct) < 0.1 ? 4 : 2;
    return pct.toFixed(digits) + '%';
  }

  var NL_TOOLTIP_COHORT_LOOKBACK_DAYS = 35;
  function nlMinCohortYmdForSession(sessionYmd, daysBack) {
    var p = String(sessionYmd || '').slice(0, 10).split('-');
    if (p.length !== 3) return null;
    var d = new Date(Date.UTC(parseInt(p[0], 10), parseInt(p[1], 10) - 1, parseInt(p[2], 10)));
    d.setUTCDate(d.getUTCDate() - daysBack);
    var yy = d.getUTCFullYear();
    var mm = ('0' + (d.getUTCMonth() + 1)).slice(-2);
    var dd = ('0' + d.getUTCDate()).slice(-2);
    return yy + '-' + mm + '-' + dd;
  }
  function nlCohortVisibleInTooltip(sessionYmd, cohortYmd) {
    var minY = nlMinCohortYmdForSession(sessionYmd, NL_TOOLTIP_COHORT_LOOKBACK_DAYS);
    if (!minY) return true;
    var c = String(cohortYmd || '').slice(0, 10);
    var s = String(sessionYmd || '').slice(0, 10);
    return c >= minY && c <= s;
  }

  function hideChartTooltipEl() {
    var el = document.getElementById('daily-report-chart-tooltip');
    if (el) {
      el.style.opacity = '0';
      el.style.visibility = 'hidden';
      el.style.pointerEvents = 'none';
    }
  }

  function ensureChartTooltipEl() {
    var el = document.getElementById('daily-report-chart-tooltip');
    if (!el) {
      el = document.createElement('div');
      el.id = 'daily-report-chart-tooltip';
      el.className = 'chart-tooltip-external';
      el.setAttribute('role', 'tooltip');
      document.body.appendChild(el);
    }
    return el;
  }

  function positionChartTooltipEl(el, context) {
    var tooltip = context.tooltip;
    var canvas = context.chart.canvas;
    var rect = canvas.getBoundingClientRect();
    var pad = 8;
    el.style.transform = 'translateX(-50%)';
    el.style.left = (rect.left + tooltip.caretX) + 'px';
    el.style.top = (rect.top + tooltip.caretY + 12) + 'px';
    var br = el.getBoundingClientRect();
    var left = br.left;
    if (br.right > window.innerWidth - pad) {
      left = window.innerWidth - pad - br.width;
    }
    if (left < pad) left = pad;
    el.style.left = left + 'px';
    el.style.transform = 'none';
    br = el.getBoundingClientRect();
    if (br.bottom > window.innerHeight - pad) {
      el.style.top = Math.max(pad, rect.top + tooltip.caretY - el.offsetHeight - 12) + 'px';
    }
  }

  function renderExternalTooltip(context) {
    var tooltip = context.tooltip;
    var el = ensureChartTooltipEl();
    var v = currentTooltipView;

    if (tooltip.opacity === 0 || !v || !tooltip.dataPoints || !tooltip.dataPoints.length) {
      hideChartTooltipEl();
      return;
    }

    var idx = tooltip.dataPoints[0].dataIndex;
    var day = v.byDay[idx];
    if (!day) {
      hideChartTooltipEl();
      return;
    }

    var html = '<div class="chart-tooltip-external-title">' + escapeHtml(day.sessionDate) + '</div>';
    html += '<div class="chart-tooltip-external-scroll">';

    tooltip.dataPoints.forEach(function (dp) {
      var ds = dp.dataset;
      var dsi = dp.datasetIndex;
      var color = ds.backgroundColor || ds.borderColor || '#888';
      if (ds.type === 'line') {
        var yv = dp.parsed.y;
        html += '<div class="chart-tooltip-external-row"><span class="swatch" style="background:' + escapeHtml(color) + '"></span><span class="txt">' +
          escapeHtml(ds.label) + ': ' + Number(yv).toLocaleString() + '</span></div>';
      } else {
        var cd = v.cohortLabels[dsi];
        if (!nlCohortVisibleInTooltip(day.sessionDate, cd)) return;
        var m = v.cohortListingAsin || {};
        var na = cd != null ? m[cd] : null;
        var ns = na != null && na !== undefined ? Number(na).toLocaleString() : '—';
        html += '<div class="chart-tooltip-external-row"><span class="swatch" style="background:' + escapeHtml(color) + '"></span><span class="txt">' +
          escapeHtml(ds.label) + ': ' + ns + '（上新 ASIN 数）</span></div>';
      }
    });

    html += '<div class="chart-tooltip-external-sep">────────</div><div class="chart-tooltip-external-footer">';
    html += '<div>当日 sessions 合计: ' + day.totalSessions.toLocaleString() + '</div>';
    html += '<div>当日上新 ASIN 数: ' + day.newAsinCount.toLocaleString() + '</div>';
    html += '<div>sessions / 上新 ASIN 数: ' + formatSessionSharePercent(day.totalSessions, day.newAsinCount) + '</div>';
    if (day.cohortParts && day.cohortParts.length) {
      html += '<div class="chart-tooltip-external-sub">各批次 sessions（仅横轴日前 ' + NL_TOOLTIP_COHORT_LOOKBACK_DAYS + ' 天内）:</div>';
      day.cohortParts.forEach(function (p) {
        if (!nlCohortVisibleInTooltip(day.sessionDate, p.cohort)) return;
        html += '<div class="chart-tooltip-external-sub">  · 批次 ' + escapeHtml(p.cohort) + ': ' +
          Number(p.sessions).toLocaleString() + '</div>';
      });
    }
    html += '</div></div>';

    el.innerHTML = html;
    el.style.opacity = '1';
    el.style.visibility = 'visible';
    el.style.pointerEvents = 'auto';
    el.style.position = 'fixed';
    positionChartTooltipEl(el, context);
  }

  function viewForKey(k) {
    return P.views[k] || P.views.all;
  }

  function renderCohortTable(v) {
    var tbl = document.getElementById('cohortTbl');
    var nd = P.cohortTrackDays || 30;
    if (!v.cohortTable || !v.cohortTable.length) {
      tbl.innerHTML = '<tbody><tr><td class="muted" colspan="' + (2 + nd) +
        '">暂无行：该店铺在 listing 区间内无上新记录，或 online 不可用时本地亦无对应 open_date。</td></tr></tbody>';
      return;
    }
    var hr = '<tr><th>上新日（PST）</th><th>上新 ASIN 数</th>';
    for (var i = 1; i <= nd; i++) {
      hr += '<th title="该批上新 ASIN 在上新日起第 ' + i + ' 个日历日的 sessions 合计；括号内为 sessions / 上新 ASIN 数">第' + i + '天</th>';
    }
    hr += '</tr>';
    var body = '';
    v.cohortTable.forEach(function (row) {
      body += '<tr><td>' + row.cohortDate + '</td><td>' + row.newAsin.toLocaleString() + '</td>';
      for (var j = 0; j < nd; j++) {
        var val = (row.daySessions && row.daySessions[j] !== undefined) ? row.daySessions[j] : 0;
        body += '<td>' + Number(val).toLocaleString() + ' (' + formatSessionSharePercent(val, row.newAsin) + ')</td>';
      }
      body += '</tr>';
    });
    tbl.innerHTML = '<thead>' + hr + '</thead><tbody>' + body + '</tbody>';
  }

    function applyView(k) {
    const v = viewForKey(k);
    currentTooltipView = v;
    const noData = document.getElementById('noData');
    const ctx = document.getElementById('ch');
    const chartBox = document.querySelector('.chart-canvas-box');
    kpiTotal.textContent = v.kpi.totalAsin.toLocaleString();
    kpiActive.textContent = v.kpi.activeAsin.toLocaleString();
    renderCohortTable(v);

    if (!v.labels.length || !v.datasets.length) {
      noData.style.display = 'block';
      if (chartBox) chartBox.style.display = 'none';
      ctx.style.display = 'none';
      if (chart) {
        hideChartTooltipEl();
        chart.destroy();
        chart = null;
      }
      return;
    }
    noData.style.display = 'none';
    if (chartBox) chartBox.style.display = '';
    ctx.style.display = 'block';

    var lt = (v.lineTotal && v.lineTotal.length) ? v.lineTotal.map(Number) : v.byDay.map(function (d) { return d.totalSessions; });
    var maxY = 0;
    for (var i = 0; i < lt.length; i++) { if (lt[i] > maxY) maxY = lt[i]; }
    maxY = maxY > 0 ? maxY : 1;
    var yMax = maxY * 1.12;

    var lineDs = {
      type: 'line',
      label: '当日 sessions 合计',
      data: lt,
      borderColor: '#111827',
      backgroundColor: 'transparent',
      borderWidth: 2.5,
      pointRadius: 4,
      pointBackgroundColor: '#111827',
      tension: 0.2,
      order: 100,
      yAxisID: 'y1',
    };

    const cfg = {
      type: 'bar',
      data: {
        labels: v.labels,
        datasets: v.datasets.concat([lineDs]),
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        scales: {
          x: {
            stacked: true,
            ticks: {
              maxRotation: typeof window !== 'undefined' && window.matchMedia && window.matchMedia('(max-width: 520px)').matches ? 65 : 45,
              minRotation: 0,
              font: { size: typeof window !== 'undefined' && window.matchMedia && window.matchMedia('(max-width: 520px)').matches ? 9 : 11 },
            },
          },
          y: {
            stacked: true,
            beginAtZero: true,
            max: yMax,
            title: { display: true, text: 'Sessions（堆叠）' },
          },
          y1: {
            stacked: false,
            beginAtZero: true,
            max: yMax,
            position: 'right',
            grid: { drawOnChartArea: false },
            title: { display: true, text: '合计（折线）' },
          },
        },
        plugins: {
          legend: {
            position: 'top',
            labels: {
              boxWidth: 10,
              font: { size: typeof window !== 'undefined' && window.matchMedia && window.matchMedia('(max-width: 520px)').matches ? 8 : 10 },
            },
          },
          tooltip: {
            enabled: false,
            external: renderExternalTooltip,
          },
        },
      },
    };

    if (chart) {
      hideChartTooltipEl();
      chart.destroy();
    }
    chart = new Chart(ctx, cfg);
  }

  sel.addEventListener('change', function () {
    applyView(sel.value);
  });
  applyView('all');

  var _resizeTimer;
  window.addEventListener('resize', function () {
    clearTimeout(_resizeTimer);
    _resizeTimer = setTimeout(function () {
      if (chart) chart.resize();
    }, 120);
  });
  </script>
</div>
</body>
</html>
"""


def write_daily_upload_session_report_file(
    out: str | Path,
    *,
    listing_since: date | None = None,
    session_start: date | None = None,
    session_end: date | None = None,
) -> Path:
    """
    写入 HTML 报表。未传 listing_since / session_* 时与 CLI 默认一致（listing_since～今天）。
    供定时任务与 CLI 共用。
    """
    ls = listing_since if listing_since is not None else DEFAULT_LISTING_SINCE
    end_d = session_end if session_end is not None else date.today()
    start_d = session_start if session_start is not None else ls
    init_db()
    db = SessionLocal()
    try:
        payload = build_report_payload(db, ls, start_d, end_d)
        out_path = Path(out).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(render_html(payload), encoding="utf-8")
        logger.info("已写入 %s", out_path)
        return out_path
    finally:
        db.close()


def main(argv: list[str]) -> int:
    setup_logging(level=logging.INFO)
    p = argparse.ArgumentParser(description="导出每日上新 session HTML 报表")
    p.add_argument("--out", type=str, required=True, help="输出 .html 路径")
    p.add_argument(
        "--listing-since",
        type=str,
        default=DEFAULT_LISTING_SINCE.isoformat(),
        help="KPI 统计起点 YYYY-MM-DD（默认 2026-02-20）",
    )
    p.add_argument("--session-start", type=str, default="", help="图表 session_date 起始，默认与 listing-since 相同")
    p.add_argument("--session-end", type=str, default="", help="图表 session_date 结束，默认今天")
    args = p.parse_args(argv)
    listing_since = datetime.strptime(args.listing_since.strip(), "%Y-%m-%d").date()
    today = date.today()
    session_end = today
    if args.session_end.strip():
        session_end = datetime.strptime(args.session_end.strip(), "%Y-%m-%d").date()
    session_start = listing_since
    if args.session_start.strip():
        session_start = datetime.strptime(args.session_start.strip(), "%Y-%m-%d").date()
    if session_start > session_end:
        p.error("session-start 不能晚于 session-end")

    write_daily_upload_session_report_file(
        args.out,
        listing_since=listing_since,
        session_start=session_start,
        session_end=session_end,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
