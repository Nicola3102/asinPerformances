"""
线上 amazon_sales_and_traffic_daily 按日总 sessions
+ ``amazon_search`` 按 ``week_no`` 汇总**整周** ``SUM(impression_count)``
+ amazon_ads_ad_group_ad_report 按日全店汇总广告 clicks / impressions，多 Y 轴折线图 HTML。
+ 每天session数据去重3C业务的session值
- Sessions：amazon_sales_and_traffic_daily，按 store_id、DATE(current_date) 汇总 SUM(sessions)；
  「全部店铺」为各店按日加总。
- 广告：amazon_ads_ad_group_ad_report，SUM(clicks)、SUM(impressions)，store_id, DATE(current_date)；
  起止与命令行 / API 日期区间一致。
- 自然周 impressions：候选 ``week_no`` 由 ``amazon_search_data`` 中 ``DATE(start_date)`` 落在报表区间内的行去重得到（与线上一致）；
  再对 ``amazon_search`` 同 ``week_no`` 做**整表** ``SUM(impression_count)``（整周全量）。
  页面与 JSON 中的 ``week_no`` 为**数据表原样**；横轴周三由本模块 ``_week_no_to_week_range`` 按线上 ``week_no`` 编码解析（与 groupA 周定义不同）。

用法（backend 目录）：
  python3.11 -m app.services.weekly_upload_asin_date_add_impression_add_ads \\
    --out ./charts/traffic_impression_ads.html \\
    --start-date 2026-02-22 --end-date 2026-04-01
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import text

from app.config import settings
from app.logging_config import setup_logging
from app.online_engine import get_online_engine

logger = logging.getLogger(__name__)

# 广告报表中 store_id 为 NULL 或无法解析时，单店序列用该占位 id（与真实店铺 id 区分）
ADS_UNASSIGNED_STORE_ID = -1

# 页面展示「数据查询截止时间」用固定 UTC+8（与北京时间一致，无夏令时）
_UTC_PLUS_8 = timezone(timedelta(hours=8))


def _query_cutoff_display_utc8() -> str:
    """年-月-日 时-分-秒（UTC+8）。"""
    return datetime.now(_UTC_PLUS_8).strftime("%Y-%m-%d %H-%M-%S")


def _parse_ymd(s: str) -> date:
    return datetime.strptime(str(s).strip(), "%Y-%m-%d").date()


def _cell_date(v) -> date:
    if v is None:
        raise ValueError("null date")
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return _parse_ymd(str(v)[:10])


def _parse_store_id_or_unassigned(v) -> int | None:
    """
    能解析为 int 则返回店铺 id；否则返回 None（表示该行应计入「未分配店铺」桶，避免丢弃 clicks）。
    """
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    s = str(v).strip()
    if not s:
        return None
    try:
        return int(s, 10)
    except ValueError:
        return None


def _iter_dates(start: date, end: date) -> list[date]:
    if start > end:
        return []
    out, cur = [], start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _week_no_to_week_start(week_no: str) -> date:
    """
    与 groupA / ``weekly_upload_asin_date_add_impression`` 一致：周日为一周开始。
    ``amazon_search_data.week_no`` 相对 groupA 周序号有 +1 偏移，故 groupA_week_num = week_num - 1。
    """
    wn = str(week_no).strip()
    if not wn.isdigit() or len(wn) < 6:
        raise ValueError(f"Invalid week_no: {week_no!r}")
    year = int(wn[:4])
    week_num = int(wn[4:])
    groupa_week_num = week_num - 1

    def _groupa_first_sunday(y: int) -> date:
        jan1 = date(y, 1, 1)
        return jan1 + timedelta(days=(6 - jan1.weekday()) % 7)

    def _groupa_last_week_num(y: int) -> int:
        first = _groupa_first_sunday(y)
        first_next = _groupa_first_sunday(y + 1)
        last_week_start = first_next - timedelta(days=7)
        return (last_week_start - first).days // 7 + 1

    if groupa_week_num <= 0:
        prev_year = year - 1
        groupa_week_num = _groupa_last_week_num(prev_year)
        year = prev_year

    first_sunday = _groupa_first_sunday(year)
    return first_sunday + timedelta(weeks=groupa_week_num - 1)


def _week_no_to_week_range(week_no: str) -> tuple[date, date, date]:
    """返回 (周日, 周六, 周三展示日)；与 202609 → 周含 2026-02-25 一致。"""
    ws = _week_no_to_week_start(week_no)
    we = ws + timedelta(days=6)
    mid = ws + timedelta(days=3)
    return ws, we, mid


def _label_index_of_date(date_to_idx: dict[date, int], labels: list[date], target: date) -> int:
    """
    首选精确匹配；若标签中不存在该日期，则退化到最近点。
    这样可避免同一横坐标聚合到多个 week_no（你遇到的 202610/202611 点在一起）。
    """
    if target in date_to_idx:
        return date_to_idx[target]
    if not labels:
        return 0
    best_i = 0
    best_d = abs((labels[0] - target).days)
    for i, d in enumerate(labels):
        dist = abs((d - target).days)
        if dist < best_d:
            best_d = dist
            best_i = i
    return best_i


def fetch_traffic_daily_by_store(
    start_date: date | None,
    end_date: date | None,
) -> tuple[list[date], dict[int | None, dict[date, int]], list[int]]:
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        raise ValueError("Online DB 未配置：需设置 online_db_host, online_db_user 等")

    where_parts = [
        # 排除黑名单 ASIN（按 asin + store_id 精确匹配），避免其 sessions 进入日汇总
        "NOT EXISTS ("
        "  SELECT 1 FROM black_asin b"
        "  WHERE b.asin = asatd.asin AND b.store_id = asatd.store_id"
        ")"
    ]
    params: dict = {}
    if start_date is not None:
        where_parts.append("DATE(asatd.`current_date`) >= :d0")
        params["d0"] = start_date.strftime("%Y-%m-%d")
    if end_date is not None:
        where_parts.append("DATE(asatd.`current_date`) <= :d1")
        params["d1"] = end_date.strftime("%Y-%m-%d")
    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

    sql = text(
        f"""
        SELECT asatd.store_id,
               SUM(COALESCE(asatd.sessions, 0)) AS total_sessions,
               DATE(asatd.`current_date`) AS d
        FROM amazon_sales_and_traffic_daily AS asatd
        {where_sql}
        GROUP BY asatd.store_id, DATE(asatd.`current_date`)
        ORDER BY d ASC, asatd.store_id ASC
        """
    )

    per_store: dict[int, dict[date, int]] = {}
    all_dates: set[date] = set()

    with get_online_engine().connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    for r in rows:
        sid = int(r[0]) if r[0] is not None else None
        if sid is None:
            continue
        total = int(r[1] or 0)
        d = _cell_date(r[2])
        all_dates.add(d)
        per_store.setdefault(sid, {})[d] = total

    sorted_dates = sorted(all_dates)
    store_ids = sorted(per_store.keys())

    totals: dict[date, int] = {d: 0 for d in sorted_dates}
    for sid in store_ids:
        for d, v in per_store[sid].items():
            totals[d] = totals.get(d, 0) + v

    series_map: dict[int | None, dict[date, int]] = {None: totals}
    for sid in store_ids:
        series_map[sid] = per_store[sid]

    return sorted_dates, series_map, store_ids


def fetch_ads_daily_metrics_by_store(
    start_date: date | None,
    end_date: date | None,
) -> tuple[
    list[date],
    dict[int | None, dict[date, int]],
    dict[int | None, dict[date, int]],
    list[int],
]:
    """
    amazon_ads_ad_group_ad_report：按店铺、日历日汇总 ``SUM(clicks)``、``SUM(impressions)``（字段名 impressions）。
    store_id 为 NULL 或无法解析的行单独计入 ``ADS_UNASSIGNED_STORE_ID``（-1），并包含在「全部店铺」合计中。
    """
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        raise ValueError("Online DB 未配置：需设置 online_db_host, online_db_user 等")

    where_parts = []
    params: dict = {}
    if start_date is not None:
        where_parts.append("DATE(aaagar.`current_date`) >= :d0")
        params["d0"] = start_date.strftime("%Y-%m-%d")
    if end_date is not None:
        where_parts.append("DATE(aaagar.`current_date`) <= :d1")
        params["d1"] = end_date.strftime("%Y-%m-%d")
    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

    sql = text(
        f"""
        SELECT aaagar.store_id,
               SUM(COALESCE(aaagar.clicks, 0)) AS total_clicks,
               SUM(COALESCE(aaagar.impressions, 0)) AS total_impressions,
               DATE(aaagar.`current_date`) AS d
        FROM amazon_ads_ad_group_ad_report AS aaagar
        {where_sql}
        GROUP BY aaagar.store_id, DATE(aaagar.`current_date`)
        ORDER BY d ASC, aaagar.store_id ASC
        """
    )

    per_clicks: dict[int, dict[date, int]] = {}
    per_imp: dict[int, dict[date, int]] = {}
    unassigned_clicks: dict[date, int] = {}
    unassigned_imp: dict[date, int] = {}
    all_dates: set[date] = set()

    with get_online_engine().connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    for r in rows:
        clicks = int(r[1] or 0)
        imps = int(r[2] or 0)
        d = _cell_date(r[3])
        all_dates.add(d)
        sid = _parse_store_id_or_unassigned(r[0])
        if sid is None:
            unassigned_clicks[d] = unassigned_clicks.get(d, 0) + clicks
            unassigned_imp[d] = unassigned_imp.get(d, 0) + imps
            continue
        per_clicks.setdefault(sid, {})[d] = clicks
        per_imp.setdefault(sid, {})[d] = imps

    sorted_dates = sorted(all_dates)
    store_ids = sorted(per_clicks.keys())
    if unassigned_clicks or unassigned_imp:
        store_ids = sorted(store_ids + [ADS_UNASSIGNED_STORE_ID])

    def _build_totals(
        per: dict[int, dict[date, int]], unassigned: dict[date, int]
    ) -> dict[date, int]:
        totals: dict[date, int] = {d: 0 for d in sorted_dates}
        for sid in per:
            for d, v in per[sid].items():
                totals[d] = totals.get(d, 0) + v
        for d, v in unassigned.items():
            totals[d] = totals.get(d, 0) + v
        return totals

    totals_clicks = _build_totals(per_clicks, unassigned_clicks)
    totals_imp = _build_totals(per_imp, unassigned_imp)

    clicks_map: dict[int | None, dict[date, int]] = {None: totals_clicks}
    imp_map: dict[int | None, dict[date, int]] = {None: totals_imp}
    for sid in per_clicks:
        clicks_map[sid] = per_clicks[sid]
        imp_map[sid] = per_imp.get(sid, {})
    if unassigned_clicks or unassigned_imp:
        clicks_map[ADS_UNASSIGNED_STORE_ID] = unassigned_clicks
        imp_map[ADS_UNASSIGNED_STORE_ID] = unassigned_imp

    logger.info(
        "[AdsDailyMetrics] range=%s..%s store_groups=%s days=%s unassigned_days=%s",
        params.get("d0"),
        params.get("d1"),
        len(per_clicks),
        len(sorted_dates),
        max(len(unassigned_clicks), len(unassigned_imp)),
    )
    return sorted_dates, clicks_map, imp_map, store_ids


def fetch_impression_weekly(
    start_date: date,
    end_date: date,
) -> tuple[dict[int, list[dict]], list[dict]]:
    """
    候选 ``week_no``：``amazon_search_data`` 在报表区间内 ``DATE(start_date)`` 出现过的值（子查询，与线上一致）。
    汇总：``amazon_search`` 上同 ``week_no`` **全表** ``SUM(impression_count)``。
    返回的 ``week_no`` 字符串为查询结果原样（表内存储）；``d_min``/``mid``/``d_max`` 由 ``_week_no_to_week_range`` 解析该编码（非 groupA）。

    返回 (per_store_weeks, all_stores_weeks)。
    每项: week_no, impressions, d_min, d_max, mid (iso), store_id(单店时)。
    """
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        raise ValueError("Online DB 未配置")

    params = {
        "d0": start_date.strftime("%Y-%m-%d"),
        "d1": end_date.strftime("%Y-%m-%d"),
    }

    sql_by_store = text(
        """
        SELECT s.store_id,
               s.week_no,
               SUM(COALESCE(s.impression_count, 0)) AS total_impression
        FROM amazon_search AS s
        WHERE s.week_no IN (
            SELECT DISTINCT asd2.week_no
            FROM amazon_search_data AS asd2
            WHERE asd2.week_no IS NOT NULL
              AND DATE(asd2.start_date) >= :d0 AND DATE(asd2.start_date) <= :d1
        )
          AND s.week_no IS NOT NULL
        GROUP BY s.store_id, s.week_no
        ORDER BY s.week_no ASC, s.store_id ASC
        """
    )

    per_store: dict[int, list[dict]] = {}
    all_weeks: list[dict] = []
    # 全店按周合计：由单次 GROUP BY store_id, week_no 的结果累加，避免对同一批 week_no 再扫全表
    week_totals: dict[str, int] = defaultdict(int)

    with get_online_engine().connect() as conn:
        rows = conn.execute(sql_by_store, params).fetchall()

    def _wn_str(v) -> str:
        if v is None:
            return ""
        if isinstance(v, int):
            return str(v)
        return str(v).strip()

    for r in rows:
        sid = int(r[0]) if r[0] is not None else None
        wn = _wn_str(r[1])
        if not wn:
            continue
        imp = int(r[2] or 0)
        week_totals[wn] += imp
        if sid is None:
            continue
        try:
            d_min, d_max, mid = _week_no_to_week_range(wn)
        except ValueError:
            logger.warning("[ImpressionWeekly] skip invalid week_no=%r", wn)
            continue
        item = {
            "week_no": wn,
            "impressions": imp,
            "store_id": sid,
            "d_min": d_min.isoformat(),
            "d_max": d_max.isoformat(),
            "mid": mid.isoformat(),
        }
        per_store.setdefault(sid, []).append(item)

    for wn in sorted(week_totals.keys()):
        imp_total = int(week_totals[wn])
        try:
            d_min, d_max, mid = _week_no_to_week_range(wn)
        except ValueError:
            logger.warning("[ImpressionWeekly] skip invalid week_no=%r", wn)
            continue
        all_weeks.append(
            {
                "week_no": wn,
                "impressions": imp_total,
                "d_min": d_min.isoformat(),
                "d_max": d_max.isoformat(),
                "mid": mid.isoformat(),
            }
        )

    logger.info(
        "[ImpressionWeekly] source=amazon_search sum, week_filter=amazon_search_data.start_date range=%s..%s store_groups=%s aggregate_weeks=%s",
        params["d0"],
        params["d1"],
        len(per_store),
        len(all_weeks),
    )
    return per_store, all_weeks


def fetch_ads_impression_weekly_filtered(
    start_date: date,
    end_date: date,
) -> tuple[dict[int, list[dict]], list[dict]]:
    """
    按与 total impressions 相同的 week_no 候选集合，统计「广告 impressions（投放位>0）」的整周值。

    广告来源：
    - amazon_ads_ad_group_ad_report aaagar.impressions
    - INNER JOIN amazon_ads_campaign aac
    - 条件：aac.rest_of_search > 0 OR aac.top_of_search > 0

    周口径：
    - 先取报表区间内 amazon_search_data.start_date 出现过的 week_no
    - week_no -> (周日~周六) 后，把广告日数据映射回该 week_no 汇总
    """
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        raise ValueError("Online DB 未配置")

    params = {
        "d0": start_date.strftime("%Y-%m-%d"),
        "d1": end_date.strftime("%Y-%m-%d"),
    }
    sql_week_no = text(
        """
        SELECT DISTINCT asd2.week_no
        FROM amazon_search_data AS asd2
        WHERE asd2.week_no IS NOT NULL
          AND DATE(asd2.start_date) >= :d0
          AND DATE(asd2.start_date) <= :d1
        ORDER BY asd2.week_no ASC
        """
    )
    with get_online_engine().connect() as conn:
        wn_rows = conn.execute(sql_week_no, params).fetchall()

    week_meta: dict[str, dict] = {}
    day_to_week: dict[date, str] = {}
    for r in wn_rows:
        wn = str(r[0]).strip() if r and r[0] is not None else ""
        if not wn:
            continue
        try:
            d_min, d_max, mid = _week_no_to_week_range(wn)
        except ValueError:
            logger.warning("[AdsImpressionWeekly] skip invalid week_no=%r", wn)
            continue
        week_meta[wn] = {
            "week_no": wn,
            "d_min": d_min.isoformat(),
            "d_max": d_max.isoformat(),
            "mid": mid.isoformat(),
        }
        cur = d_min
        while cur <= d_max:
            day_to_week[cur] = wn
            cur += timedelta(days=1)

    if not week_meta:
        return {}, []

    min_day = min(_parse_ymd(x["d_min"]) for x in week_meta.values())
    max_day = max(_parse_ymd(x["d_max"]) for x in week_meta.values())
    sql_ads_daily = text(
        """
        SELECT aaagar.store_id,
               DATE(aaagar.`current_date`) AS d,
               SUM(COALESCE(aaagar.impressions, 0)) AS imp
        FROM amazon_ads_ad_group_ad_report AS aaagar
        INNER JOIN amazon_ads_campaign AS aac
            ON aac.campaign_id = aaagar.campaign_id
        WHERE DATE(aaagar.`current_date`) >= :d0
          AND DATE(aaagar.`current_date`) <= :d1
          AND (COALESCE(aac.rest_of_search, 0) > 0 OR COALESCE(aac.top_of_search, 0) > 0)
        GROUP BY aaagar.store_id, DATE(aaagar.`current_date`)
        ORDER BY d ASC, aaagar.store_id ASC
        """
    )
    with get_online_engine().connect() as conn:
        daily_rows = conn.execute(
            sql_ads_daily,
            {"d0": min_day.strftime("%Y-%m-%d"), "d1": max_day.strftime("%Y-%m-%d")},
        ).fetchall()

    per_store_sum: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    all_sum: dict[str, int] = defaultdict(int)
    for r in daily_rows:
        sid = _parse_store_id_or_unassigned(r[0])
        sid_key = ADS_UNASSIGNED_STORE_ID if sid is None else int(sid)
        d = _cell_date(r[1])
        wn = day_to_week.get(d)
        if wn is None:
            continue
        imp = int(r[2] or 0)
        per_store_sum[sid_key][wn] += imp
        all_sum[wn] += imp

    per_store: dict[int, list[dict]] = {}
    for sid, by_week in per_store_sum.items():
        arr: list[dict] = []
        for wn in sorted(by_week.keys()):
            m = week_meta.get(wn)
            if not m:
                continue
            arr.append(
                {
                    "week_no": wn,
                    "impressions": int(by_week[wn]),
                    "store_id": sid,
                    "d_min": m["d_min"],
                    "d_max": m["d_max"],
                    "mid": m["mid"],
                }
            )
        per_store[sid] = arr

    all_weeks: list[dict] = []
    for wn in sorted(all_sum.keys()):
        m = week_meta.get(wn)
        if not m:
            continue
        all_weeks.append(
            {
                "week_no": wn,
                "impressions": int(all_sum[wn]),
                "d_min": m["d_min"],
                "d_max": m["d_max"],
                "mid": m["mid"],
            }
        )

    logger.info(
        "[AdsImpressionWeekly] week_filter=amazon_search_data.start_date range=%s..%s store_groups=%s aggregate_weeks=%s",
        params["d0"],
        params["d1"],
        len(per_store),
        len(all_weeks),
    )
    return per_store, all_weeks


def _subtract_weekly_impressions(
    total_per_store: dict[int, list[dict]],
    total_all: list[dict],
    ads_per_store: dict[int, list[dict]],
    ads_all: list[dict],
) -> tuple[dict[int, list[dict]], list[dict]]:
    """按 week_no 计算去广告 impressions：max(total - ads, 0)。"""
    ads_all_map = {str(w["week_no"]): int(w.get("impressions") or 0) for w in ads_all}
    out_all: list[dict] = []
    for w in total_all:
        wn = str(w["week_no"])
        total_imp = int(w.get("impressions") or 0)
        ad_imp = int(ads_all_map.get(wn, 0))
        out_all.append({**w, "impressions": max(total_imp - ad_imp, 0)})

    out_store: dict[int, list[dict]] = {}
    for sid, weeks in total_per_store.items():
        ads_map = {
            str(w["week_no"]): int(w.get("impressions") or 0)
            for w in ads_per_store.get(sid, [])
        }
        arr: list[dict] = []
        for w in weeks:
            wn = str(w["week_no"])
            total_imp = int(w.get("impressions") or 0)
            ad_imp = int(ads_map.get(wn, 0))
            arr.append({**w, "impressions": max(total_imp - ad_imp, 0)})
        out_store[sid] = arr
    return out_store, out_all


def _merge_label_dates(
    traffic_dates: list[date],
    imp_weeks_all: list[dict],
    range_start: date | None,
    range_end: date | None,
    extra_day_lists: tuple[list[date], ...] = (),
) -> list[date]:
    s: set[date] = set(traffic_dates)
    for w in imp_weeks_all:
        s.add(_parse_ymd(w["mid"]))
        s.add(_parse_ymd(w["d_min"]))
        s.add(_parse_ymd(w["d_max"]))
    if range_start is not None and range_end is not None:
        s.update(_iter_dates(range_start, range_end))
    for dl in extra_day_lists:
        s.update(dl)
    return sorted(s)


def _series_for_labels(m: dict[date, int], labels: list[date]) -> list[int | None]:
    """
    labels 中若存在但该序列没有数据，则返回 `None`，让 Chart.js 不绘制该点/线段。
    这样可避免 traffic 表缺少末尾 1-2 天时，折线被错误补成 0。
    """
    out: list[int | None] = []
    for d in labels:
        v = m.get(d)
        out.append(int(v) if v is not None else None)
    return out


def _impression_line_for_labels(
    weeks: list[dict],
    labels: list[date],
) -> tuple[list[int | None], list[dict | None]]:
    """与 labels 对齐的 impression 值与点击用 meta（同 index 可合并多周）。"""
    vals: list[int | None] = [None] * len(labels)
    metas: list[dict | None] = [None] * len(labels)
    date_to_idx = {d: i for i, d in enumerate(labels)}
    for w in weeks:
        mid_d = _parse_ymd(w["mid"])
        idx = _label_index_of_date(date_to_idx, labels, mid_d)
        imp = int(w["impressions"])
        vals[idx] = (vals[idx] or 0) + imp
        entry = {
            "week_no": w["week_no"],
            "impressions": imp,
            "d_min": w["d_min"],
            "d_max": w["d_max"],
            "mid": w["mid"],
        }
        if w.get("store_id") is not None:
            entry["store_id"] = w["store_id"]
        if metas[idx] is None:
            metas[idx] = {"weeks": [entry]}
        else:
            metas[idx]["weeks"].append(entry)
    return vals, metas


def build_chart_payload(
    labels: list[date],
    series_map: dict[int | None, dict[date, int]],
    store_ids: list[int],
    impression_per_store: dict[int, list[dict]],
    impression_all: list[dict],
    impression_enabled: bool,
    non_ad_impression_per_store: dict[int, list[dict]],
    non_ad_impression_all: list[dict],
    non_ad_impression_enabled: bool,
    ads_series_map: dict[int | None, dict[date, int]],
    ads_impressions_map: dict[int | None, dict[date, int]],
    ads_enabled: bool,
) -> dict:
    lab_iso = [d.isoformat() for d in labels]

    def sessions_series(key: int | None) -> list[int | None]:
        return _series_for_labels(series_map.get(key, {}), labels)

    def ads_series(key: int | None) -> list[int | None]:
        return _series_for_labels(ads_series_map.get(key, {}), labels)

    def ads_impr_series(key: int | None) -> list[int | None]:
        return _series_for_labels(ads_impressions_map.get(key, {}), labels)

    imp_all_vals, imp_all_meta = _impression_line_for_labels(impression_all, labels)
    non_ad_imp_all_vals, non_ad_imp_all_meta = _impression_line_for_labels(
        non_ad_impression_all, labels
    )
    by_store_imp: dict[str, list[int | None]] = {}
    by_store_meta: dict[str, list[dict | None]] = {}
    by_store_non_ad_imp: dict[str, list[int | None]] = {}
    by_store_non_ad_meta: dict[str, list[dict | None]] = {}
    for sid in store_ids:
        wks = impression_per_store.get(sid, [])
        v, m = _impression_line_for_labels(wks, labels)
        by_store_imp[str(sid)] = v
        by_store_meta[str(sid)] = m
        wks_non_ad = non_ad_impression_per_store.get(sid, [])
        v2, m2 = _impression_line_for_labels(wks_non_ad, labels)
        by_store_non_ad_imp[str(sid)] = v2
        by_store_non_ad_meta[str(sid)] = m2

    return {
        "labels": lab_iso,
        "store_ids": store_ids,
        "all_data": sessions_series(None),
        "by_store": {str(sid): sessions_series(sid) for sid in store_ids},
        "impressionEnabled": impression_enabled,
        "impression_all": imp_all_vals,
        "impression_all_meta": imp_all_meta,
        "impression_by_store": by_store_imp,
        "impression_meta_by_store": by_store_meta,
        "nonAdImpressionEnabled": non_ad_impression_enabled,
        "non_ad_impression_all": non_ad_imp_all_vals,
        "non_ad_impression_all_meta": non_ad_imp_all_meta,
        "non_ad_impression_by_store": by_store_non_ad_imp,
        "non_ad_impression_meta_by_store": by_store_non_ad_meta,
        "adsEnabled": ads_enabled,
        "ads_all_data": ads_series(None),
        "ads_by_store": {str(sid): ads_series(sid) for sid in store_ids},
        "ads_impressions_all_data": ads_impr_series(None),
        "ads_impressions_by_store": {
            str(sid): ads_impr_series(sid) for sid in store_ids
        },
    }


def render_html(payload: dict) -> str:
    data_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Sessions + amazon_search 周 Impression + 广告 Clicks / 日 Impressions</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
    :root {{
      --bg: #0f1419;
      --panel: #1a2332;
      --text: #e8ecf1;
      --muted: #8b9cb3;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      font-family: "IBM Plex Sans", "Segoe UI", system-ui, sans-serif;
      background: var(--bg);
      color: var(--text);
    }}
    .wrap {{
      max-width: 1100px;
      margin: 0 auto;
      padding: 1.75rem 1.25rem 2.5rem;
    }}
    h1 {{
      font-weight: 600;
      font-size: 1.35rem;
      margin: 0 0 0.35rem;
    }}
    .sub {{ color: var(--muted); font-size: 0.875rem; margin-bottom: 1rem; line-height: 1.5; }}
    .query-cutoff {{
      font-size: 0.8125rem;
      color: #a8b8cc;
      margin: 0 0 0.65rem;
      letter-spacing: 0.02em;
    }}
    .toolbar {{
      display: flex;
      flex-wrap: wrap;
      gap: 0.75rem 1.25rem;
      margin-bottom: 1rem;
      padding: 0.85rem 1rem;
      background: var(--panel);
      border-radius: 10px;
      border: 1px solid rgba(125, 211, 192, 0.12);
    }}
    label {{ font-size: 0.8rem; color: var(--muted); }}
    select {{
      background: #243044;
      color: var(--text);
      border: 1px solid rgba(61, 157, 255, 0.35);
      border-radius: 6px;
      padding: 0.45rem 0.65rem;
      font-size: 0.9rem;
      min-width: 200px;
    }}
    .chart-box {{
      background: var(--panel);
      border-radius: 12px;
      padding: 1rem 0.75rem 1.25rem;
      border: 1px solid rgba(61, 157, 255, 0.1);
    }}
    .series-toggles {{
      display: flex;
      flex-wrap: wrap;
      gap: 0.55rem 1rem;
      align-items: center;
      margin-bottom: 0.75rem;
      padding: 0.5rem 0.15rem 0.15rem;
    }}
    .series-toggle {{
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      cursor: pointer;
      font-size: 0.78rem;
      color: var(--text);
      user-select: none;
      max-width: 100%;
    }}
    .series-toggle input {{
      width: 0.95rem;
      height: 0.95rem;
      accent-color: #5eb8ff;
      flex-shrink: 0;
    }}
    .series-swatch {{
      width: 11px;
      height: 11px;
      border-radius: 3px;
      flex-shrink: 0;
      border: 1px solid rgba(255,255,255,0.15);
    }}
    canvas {{ max-height: 440px; }}
    #detailPanel {{
      display: none;
      margin-top: 1rem;
      padding: 1rem 1.1rem;
      background: #243044;
      border-radius: 10px;
      border: 1px solid rgba(255, 179, 71, 0.35);
      font-size: 0.9rem;
      line-height: 1.55;
    }}
    #detailPanel h3 {{ margin: 0 0 0.5rem; font-size: 1rem; color: #ffb347; }}
    #detailPanel .row {{ margin: 0.35rem 0; }}
    #pointValuePanel {{
      display: none;
      margin-top: 1rem;
      padding: 0.85rem 1rem;
      background: #1e2a3d;
      border-radius: 10px;
      border: 1px solid rgba(94, 184, 255, 0.35);
      font-size: 0.9rem;
      line-height: 1.55;
    }}
    #pointValuePanel h3 {{
      margin: 0 0 0.45rem;
      font-size: 0.95rem;
      color: #7dd3c0;
    }}
    #pointValuePanel .pv-row {{ margin: 0.3rem 0; }}
    #pointValuePanel .pv-num {{ font-weight: 600; color: #e8ecf1; }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Traffic daily + amazon_search 周 impression + 广告 Clicks / 日 Impressions</h1>
    <p class="query-cutoff" id="queryCutoffUtc8Line"></p>
    <p class="sub">
      <strong>左轴（0～500～1000…，步长 500）</strong>：<code>amazon_sales_and_traffic_daily</code> 按日 <code>SUM(sessions)</code>（绿）与 <code>amazon_ads_ad_group_ad_report</code> 按日 <code>SUM(clicks)</code>（紫）共用。<br />
      <strong>右轴（0～1万～2万…，步长 1 万）</strong>：<code>amazon_search</code> 按该 <code>week_no</code> 汇总<strong>整周</strong>
      <code>SUM(impression_count)</code>（橙点）、
      去广告 impressions（蓝点 = 周 total impressions - 该周广告 impressions）、
      以及 <code>广告日 impressions</code>（黄线）共用；周点横坐标均为周三。<br />
    </p>
    <div class="toolbar">
      <div>
        <label for="storeSel">店铺</label><br />
        <select id="storeSel">
          <option value="">全部店铺（合计）</option>
        </select>
      </div>
    </div>
    <div class="chart-box">
      <div id="seriesToggles" class="series-toggles" aria-label="折线显示"></div>
      <canvas id="c" height="120"></canvas>
    </div>
    <div id="pointValuePanel" aria-live="polite">
      <h3>选中数据点</h3>
      <div id="pointValueBody"></div>
    </div>
    <div id="detailPanel"><h3>amazon_search 整周 impression（week_no 为表内原值）</h3><div id="detailBody"></div></div>
  </div>
  <script>
    var payload = {data_json};
    (function fillQueryCutoff() {{
      var el = document.getElementById('queryCutoffUtc8Line');
      var t = payload.queryCutoffUtc8;
      if (el && t) {{
        el.textContent = '数据查询截止时间（UTC+8）：' + t;
      }}
    }})();
    var labels = payload.labels;
    var allData = payload.all_data;
    var byStore = payload.by_store;
    var storeIds = payload.store_ids || [];
    var impOn = payload.impressionEnabled;
    var impAll = payload.impression_all || [];
    var impAllMeta = payload.impression_all_meta || [];
    var impByStore = payload.impression_by_store || {{}};
    var impMetaByStore = payload.impression_meta_by_store || {{}};
    var nonAdImpOn = payload.nonAdImpressionEnabled;
    var nonAdImpAll = payload.non_ad_impression_all || [];
    var nonAdImpAllMeta = payload.non_ad_impression_all_meta || [];
    var nonAdImpByStore = payload.non_ad_impression_by_store || {{}};
    var nonAdImpMetaByStore = payload.non_ad_impression_meta_by_store || {{}};
    var adsOn = payload.adsEnabled;
    var adsAll = payload.ads_all_data || [];
    var adsByStore = payload.ads_by_store || {{}};
    var adsImpAll = payload.ads_impressions_all_data || [];
    var adsImpByStore = payload.ads_impressions_by_store || {{}};

    var idxSessions = 0;
    var _idx = 1;
    var idxImp = impOn ? _idx++ : -1;
    var idxNonAdImp = nonAdImpOn ? _idx++ : -1;
    var idxAdsClicks = adsOn ? _idx++ : -1;
    var idxAdsImp = adsOn ? _idx++ : -1;

    var sel = document.getElementById('storeSel');
    storeIds.forEach(function (sid) {{
      var o = document.createElement('option');
      o.value = String(sid);
      o.textContent = (sid === -1 || sid === '-1')
        ? 'store_id 为空（仅广告）'
        : ('store_id = ' + sid);
      sel.appendChild(o);
    }});

    var panel = document.getElementById('detailPanel');
    var detailBody = document.getElementById('detailBody');
    var pvPanel = document.getElementById('pointValuePanel');
    var pvBody = document.getElementById('pointValueBody');

    function hidePointValue() {{
      if (pvPanel) pvPanel.style.display = 'none';
      if (pvBody) pvBody.innerHTML = '';
    }}

    function showPointValue(dateStr, seriesLabel, rawVal, axisHint) {{
      if (!pvPanel || !pvBody) return;
      var numStr = '（无数据）';
      if (rawVal != null && rawVal !== '' && !isNaN(Number(rawVal))) {{
        numStr = Number(rawVal).toLocaleString();
      }}
      var axis = axisHint || '';
      pvBody.innerHTML =
        '<div class="pv-row">日期：<span class="pv-num">' + escapeHtml(String(dateStr)) + '</span></div>' +
        '<div class="pv-row">序列：' + escapeHtml(String(seriesLabel || '')) +
        (axis ? ' <span style="color:#8b9cb3;font-size:0.82rem;">' + escapeHtml(axis) + '</span>' : '') +
        '</div>' +
        '<div class="pv-row">数值：<span class="pv-num">' + escapeHtml(numStr) + '</span></div>';
      pvPanel.style.display = 'block';
    }}

    function currentImpressionSeries() {{
      var v = sel.value;
      if (!impOn) return {{ data: [], meta: [] }};
      if (!v) return {{ data: impAll, meta: impAllMeta }};
      return {{
        data: impByStore[v] || labels.map(function () {{ return null; }}),
        meta: impMetaByStore[v] || labels.map(function () {{ return null; }})
      }};
    }}

    function currentNonAdImpressionSeries() {{
      var v = sel.value;
      if (!nonAdImpOn) return {{ data: [], meta: [] }};
      if (!v) return {{ data: nonAdImpAll, meta: nonAdImpAllMeta }};
      return {{
        data: nonAdImpByStore[v] || labels.map(function () {{ return null; }}),
        meta: nonAdImpMetaByStore[v] || labels.map(function () {{ return null; }})
      }};
    }}

    function hideDetail() {{
      panel.style.display = 'none';
      detailBody.innerHTML = '';
    }}

    function showDetail(meta) {{
      if (!meta || !meta.weeks || !meta.weeks.length) return;
      var html = '';
      meta.weeks.forEach(function (w) {{
        html += '<div class="row"><strong>week_no</strong> ' + escapeHtml(String(w.week_no));
        if (w.store_id != null) html += ' · store_id ' + escapeHtml(String(w.store_id));
        html += '</div>';
        html += '<div class="row">impressions: <strong>' + Number(w.impressions).toLocaleString() + '</strong></div>';
        html += '<hr style="border:none;border-top:1px solid rgba(255,255,255,0.12);margin:0.6rem 0"/>';
      }});
      detailBody.innerHTML = html;
      panel.style.display = 'block';
    }}

    function escapeHtml(s) {{
      if (s == null) return '';
      return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    }}

    var impSeries = currentImpressionSeries();
    var ctx = document.getElementById('c').getContext('2d');
    var chart = new Chart(ctx, {{
      type: 'line',
      data: {{
        labels: labels,
        datasets: [
          {{
            label: 'Sessions（左轴）',
            data: allData,
            yAxisID: 'y',
            borderColor: '#7dd3c0',
            backgroundColor: 'rgba(125, 211, 192, 0.12)',
            fill: true,
            spanGaps: false,
            tension: 0.22,
            pointRadius: 2,
            pointHoverRadius: 5,
            borderWidth: 2,
            order: 2,
          }},
        ],
      }},
      options: {{
        responsive: true,
        maintainAspectRatio: true,
        interaction: {{ mode: 'nearest', intersect: true }},
        onClick: function (ev, els, ch) {{
          if (!els.length) {{
            hidePointValue();
            return;
          }}
          var el = els[0];
          var dsIdx = el.datasetIndex;
          var ix = el.index;
          if (!ch.isDatasetVisible(dsIdx)) return;
          var ds = ch.data.datasets[dsIdx];
          if (!ds) return;
          var xLab = (labels && labels[ix] != null) ? labels[ix] : String(ix);
          var yRaw = ds.data[ix];
          var axisHint = ds.yAxisID === 'y1' ? '右轴' : '左轴';
          showPointValue(xLab, ds.label || '', yRaw, axisHint);
          if ((dsIdx === idxImp && idxImp >= 0) || (dsIdx === idxNonAdImp && idxNonAdImp >= 0)) {{
            var meta = null;
            try {{
              var is = (dsIdx === idxNonAdImp) ? currentNonAdImpressionSeries() : currentImpressionSeries();
              meta = (is && is.meta && is.meta[ix]) ? is.meta[ix] : null;
            }} catch (e2) {{
              meta = null;
            }}
            if (meta && meta.weeks && meta.weeks.length) showDetail(meta);
            else hideDetail();
          }} else {{
            hideDetail();
          }}
        }},
        plugins: {{
          legend: {{ display: false }},
          tooltip: {{
            callbacks: {{
              label: function (ctx) {{
                var v = ctx.parsed.y;
                if (v == null) return '';
                if (ctx.dataset.yAxisID === 'y1') {{
                  if (idxAdsImp >= 0 && ctx.datasetIndex === idxAdsImp)
                    return ' 广告 impressions（日）: ' + Number(v).toLocaleString();
                  if (idxNonAdImp >= 0 && ctx.datasetIndex === idxNonAdImp)
                    return ' 去广告 impressions（week_no）: ' + Number(v).toLocaleString();
                  var line = ' total impressions（week_no）: ' + Number(v).toLocaleString();
                  if (idxImp >= 0 && ctx.datasetIndex === idxImp && ctx.dataset._impressionMeta) {{
                    var m = ctx.dataset._impressionMeta[ctx.dataIndex];
                    if (m && m.weeks && m.weeks.length)
                      line += ' [' + m.weeks.map(function (w) {{ return w.week_no; }}).join(', ') + ']';
                  }}
                  return line;
                }}
                if (ctx.dataset.yAxisID === 'y') {{
                  if (idxAdsClicks >= 0 && ctx.datasetIndex === idxAdsClicks)
                    return ' 广告 sessions: ' + Number(v).toLocaleString();
                  return ' sessions: ' + Number(v).toLocaleString();
                }}
                return '';
              }},
            }},
          }},
        }},
        scales: {{
          x: {{
            ticks: {{ color: '#8b9cb3', maxRotation: 45 }},
            grid: {{ color: 'rgba(139, 156, 179, 0.08)' }},
          }},
          y: {{
            id: 'y',
            type: 'linear',
            position: 'left',
            beginAtZero: true,
            title: {{ display: true, text: 'Sessions + 广告 Sessions', color: '#8b9cb3' }},
            ticks: {{
              color: '#8b9cb3',
              stepSize: 500,
              callback: function (v) {{ return Number(v).toLocaleString(); }},
            }},
            grid: {{ color: 'rgba(139, 156, 179, 0.1)' }},
          }},
          y1: {{
            id: 'y1',
            type: 'linear',
            position: 'right',
            beginAtZero: true,
            display: true,
            title: {{ display: true, text: '周 + 广告日 Impressions（步长 1 万）', color: '#c9b896' }},
            ticks: {{
              color: '#c9b896',
              stepSize: 10000,
              callback: function (v) {{ return Number(v).toLocaleString(); }},
            }},
            grid: {{ drawOnChartArea: false }},
          }},
        }},
      }},
    }});

    if (impOn) {{
      var dsImp = {{
        label: 'total impressions · week_no（右轴，点=周三）',
        data: impSeries.data,
        yAxisID: 'y1',
        borderColor: '#ffb347',
        backgroundColor: 'rgba(255, 179, 71, 0.15)',
        fill: false,
        tension: 0.2,
        spanGaps: false,
        pointRadius: 6,
        pointHoverRadius: 9,
        pointBackgroundColor: '#ffb347',
        pointBorderColor: '#1a2332',
        pointBorderWidth: 2,
        borderWidth: 2,
        order: 1,
      }};
      dsImp._impressionMeta = impSeries.meta;
      chart.data.datasets.push(dsImp);
    }}
    if (nonAdImpOn) {{
      var nonAdSeries = currentNonAdImpressionSeries();
      var dsNonAdImp = {{
        label: '去广告 impressions · week_no（右轴，点=周三）',
        data: nonAdSeries.data,
        yAxisID: 'y1',
        borderColor: '#60a5fa',
        backgroundColor: 'rgba(96, 165, 250, 0.15)',
        fill: false,
        tension: 0.2,
        spanGaps: false,
        pointRadius: 6,
        pointHoverRadius: 9,
        pointBackgroundColor: '#60a5fa',
        pointBorderColor: '#1a2332',
        pointBorderWidth: 2,
        borderWidth: 2,
        order: 1,
      }};
      dsNonAdImp._impressionMeta = nonAdSeries.meta;
      chart.data.datasets.push(dsNonAdImp);
    }}
    if (adsOn) {{
      chart.data.datasets.push({{
        label: '广告 sessions（全部店铺合计，左轴）',
        data: adsAll,
        yAxisID: 'y',
        borderColor: '#e879f9',
        backgroundColor: 'rgba(232, 121, 249, 0.08)',
        fill: false,
        tension: 0.22,
          spanGaps: false,
        pointRadius: 3,
        pointHoverRadius: 6,
        pointBackgroundColor: '#e879f9',
        pointBorderColor: '#1a2332',
        pointBorderWidth: 1,
        borderWidth: 2,
        order: 2,
      }});
      chart.data.datasets.push({{
        label: '广告 impressions 日汇总（全部店铺，右轴）',
        data: adsImpAll,
        yAxisID: 'y1',
        borderColor: '#f5d547',
        backgroundColor: 'rgba(245, 213, 71, 0.06)',
        fill: false,
        tension: 0.22,
        spanGaps: false,
        pointRadius: 3,
        pointHoverRadius: 6,
        pointBackgroundColor: '#f5d547',
        pointBorderColor: '#1a2332',
        pointBorderWidth: 1,
        borderWidth: 2,
        order: 3,
      }});
    }}
    chart.update();

    function syncY1AxisDisplay() {{
      var show = false;
      for (var i = 0; i < chart.data.datasets.length; i++) {{
        if (chart.data.datasets[i].yAxisID === 'y1' && chart.isDatasetVisible(i)) {{
          show = true;
          break;
        }}
      }}
      chart.options.scales.y1.display = show;
    }}

    function buildSeriesToggles() {{
      var host = document.getElementById('seriesToggles');
      if (!host) return;
      host.innerHTML = '';
      var specs = [
        {{ key: 'sess', idx: idxSessions, shortLabel: 'Sessions（左轴）', color: '#7dd3c0' }},
      ];
      if (impOn && idxImp >= 0) {{
        specs.push({{ key: 'imp', idx: idxImp, shortLabel: 'total impressions · week_no（右轴·周三）', color: '#ffb347' }});
      }}
      if (nonAdImpOn && idxNonAdImp >= 0) {{
        specs.push({{ key: 'imp_no_ad', idx: idxNonAdImp, shortLabel: '去广告 impressions · week_no（右轴·周三）', color: '#60a5fa' }});
      }}
      if (adsOn && idxAdsClicks >= 0) {{
        specs.push({{ key: 'adclk', idx: idxAdsClicks, shortLabel: '广告 sessions（左轴）', color: '#e879f9' }});
      }}
      if (adsOn && idxAdsImp >= 0) {{
        specs.push({{ key: 'adimp', idx: idxAdsImp, shortLabel: '广告 impressions 日汇总（右轴）', color: '#f5d547' }});
      }}
      specs.forEach(function (s) {{
        var lab = document.createElement('label');
        lab.className = 'series-toggle';
        var cb = document.createElement('input');
        cb.type = 'checkbox';
        // 默认全选：未勾选时隐藏对应折线
        cb.checked = true;
        cb.setAttribute('data-dataset-index', String(s.idx));
        var sw = document.createElement('span');
        sw.className = 'series-swatch';
        sw.style.background = s.color;
        var tx = document.createElement('span');
        tx.textContent = s.shortLabel;
        lab.appendChild(cb);
        lab.appendChild(sw);
        lab.appendChild(tx);
        cb.addEventListener('change', function () {{
          chart.setDatasetVisibility(s.idx, cb.checked);
          if (!cb.checked && s.idx === idxImp) hideDetail();
          hidePointValue();
          syncY1AxisDisplay();
          chart.update();
        }});
        host.appendChild(lab);
      }});
    }}

    buildSeriesToggles();
    syncY1AxisDisplay();
    chart.update();

    function applyStore() {{
      hideDetail();
      hidePointValue();
      var v = sel.value;
      var ds0 = chart.data.datasets[idxSessions];
      if (!v) {{
        ds0.label = 'Sessions（全部店铺合计）';
        ds0.data = allData;
        ds0.borderColor = '#7dd3c0';
        ds0.backgroundColor = 'rgba(125, 211, 192, 0.12)';
      }} else {{
        ds0.label = (v === '-1')
          ? 'store_id 为空（sessions 无此桶，折线为 0）'
          : ('store_id ' + v + ' sessions');
        ds0.data = byStore[v] || labels.map(function () {{ return null; }});
        ds0.borderColor = '#3d9dff';
        ds0.backgroundColor = 'rgba(61, 157, 255, 0.12)';
      }}
      if (impOn && idxImp >= 0) {{
        var is = currentImpressionSeries();
        chart.data.datasets[idxImp].data = is.data;
        chart.data.datasets[idxImp]._impressionMeta = is.meta;
      }}
      if (nonAdImpOn && idxNonAdImp >= 0) {{
        var nis = currentNonAdImpressionSeries();
        chart.data.datasets[idxNonAdImp].data = nis.data;
        chart.data.datasets[idxNonAdImp]._impressionMeta = nis.meta;
      }}
      if (adsOn && idxAdsClicks >= 0) {{
        var adData = !v ? adsAll : (adsByStore[v] || labels.map(function () {{ return null; }}));
        var adsDs = chart.data.datasets[idxAdsClicks];
        adsDs.data = adData;
        adsDs.label = !v
          ? '广告 sessions'
          : ((v === '-1')
            ? '广告 sessions'
            : ('store_id ' + v + ' 广告 sessions'));
      }}
      if (adsOn && idxAdsImp >= 0) {{
        var impD = !v ? adsImpAll : (adsImpByStore[v] || labels.map(function () {{ return null; }}));
        var impDs = chart.data.datasets[idxAdsImp];
        impDs.data = impD;
        impDs.label = !v
          ? '广告 impressions 日汇总（全部店铺，右轴）'
          : ((v === '-1')
            ? '广告 impressions（store_id 为空，右轴）'
            : ('store_id ' + v + ' 广告 impressions 日（右轴）'));
      }}
      syncY1AxisDisplay();
      chart.update();
    }}
    sel.addEventListener('change', applyStore);
  </script>
</body>
</html>
"""

# API / 定时报表默认起始日（与产品约定一致时可改配置）
DEFAULT_TRAFFIC_IMPRESSION_ADS_START = date(2026, 2, 22)


def build_report_html_for_range(start_date: date | None, end_date: date | None) -> str:
    """
    与 write_report 相同的数据与 HTML，不写文件；供 FastAPI 等直接返回 HTMLResponse。
    起止日齐全时 traffic / 广告 / 周 impression 三路在线查询并行，缩短总耗时。
    """
    range_ok = start_date is not None and end_date is not None
    impression_enabled = range_ok
    impression_per_store: dict[int, list[dict]] = {}
    impression_all: list[dict] = []
    non_ad_impression_enabled = range_ok
    non_ad_impression_per_store: dict[int, list[dict]] = {}
    non_ad_impression_all: list[dict] = []
    ads_enabled = range_ok
    ads_dates: list[date] = []
    ads_series_map: dict[int | None, dict[date, int]] = {None: {}}
    ads_impressions_map: dict[int | None, dict[date, int]] = {None: {}}
    ads_store_ids: list[int] = []

    if range_ok:
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=4, thread_name_prefix="traffic-report") as ex:
            fut_traffic = ex.submit(fetch_traffic_daily_by_store, start_date, end_date)
            fut_ads = ex.submit(fetch_ads_daily_metrics_by_store, start_date, end_date)
            fut_imp = ex.submit(fetch_impression_weekly, start_date, end_date)
            fut_ads_imp_weekly = ex.submit(fetch_ads_impression_weekly_filtered, start_date, end_date)
            sorted_dates, series_map, store_ids = fut_traffic.result()
            ads_dates, ads_series_map, ads_impressions_map, ads_store_ids = fut_ads.result()
            impression_per_store, impression_all = fut_imp.result()
            ads_imp_weekly_per_store, ads_imp_weekly_all = fut_ads_imp_weekly.result()
            non_ad_impression_per_store, non_ad_impression_all = _subtract_weekly_impressions(
                impression_per_store,
                impression_all,
                ads_imp_weekly_per_store,
                ads_imp_weekly_all,
            )
        logger.info(
            "[Traffic+Impression+Ads] parallel fetch range=%s..%s elapsed_sec=%.2f",
            start_date,
            end_date,
            time.time() - t0,
        )
    else:
        sorted_dates, series_map, store_ids = fetch_traffic_daily_by_store(start_date, end_date)

    merged_ids = sorted(set(store_ids) | set(ads_store_ids))
    for sid in merged_ids:
        series_map.setdefault(sid, {})
        ads_series_map.setdefault(sid, {})
        ads_impressions_map.setdefault(sid, {})

    labels = _merge_label_dates(
        sorted_dates, impression_all, start_date, end_date, (ads_dates,)
    )
    if not labels:
        labels = sorted_dates

    payload = build_chart_payload(
        labels,
        series_map,
        merged_ids,
        impression_per_store,
        impression_all,
        impression_enabled,
        non_ad_impression_per_store,
        non_ad_impression_all,
        non_ad_impression_enabled,
        ads_series_map,
        ads_impressions_map,
        ads_enabled,
    )
    payload["queryCutoffUtc8"] = _query_cutoff_display_utc8()
    return render_html(payload)


def write_report(out: str | Path, start_date: date | None, end_date: date | None) -> Path:
    html = build_report_html_for_range(start_date, end_date)
    out_path = Path(out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    logger.info("[Traffic+Impression+Ads] wrote path=%s", out_path)
    return out_path


def main(argv: list[str]) -> int:
    setup_logging(level=logging.INFO)
    p = argparse.ArgumentParser(
        description="线上 daily sessions + 周 impression + 广告表日 clicks/impressions，多轴折线图 HTML"
    )
    p.add_argument("--out", type=str, required=True, help="输出 .html 路径")
    p.add_argument(
        "--start-date",
        type=str,
        default="",
        help="traffic 的 current_date 下限（含）；与 --end-date 同时传时查 amazon_search 周 impression（整周汇总）",
    )
    p.add_argument(
        "--end-date",
        type=str,
        default="",
        help="traffic / 广告 / 报表区间的日历上限（含）；total impressions 用区间内 start_date 出现过的 week_no 再整表汇总",
    )
    args = p.parse_args(argv)
    start_d = _parse_ymd(args.start_date) if args.start_date.strip() else None
    end_d = _parse_ymd(args.end_date) if args.end_date.strip() else None
    if start_d and end_d and start_d > end_d:
        p.error("start-date 不能晚于 end-date")
    try:
        write_report(args.out, start_d, end_d)
        return 0
    except Exception as e:
        logger.exception("[Traffic+Impression+Ads] failed: %s", e)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
