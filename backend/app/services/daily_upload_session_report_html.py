"""
每日上新 ASIN session 堆叠图 — HTML 交互报表（Chart.js）。

- 默认「全部店铺」聚合；可按 store_id 切换。
- 堆叠柱 + 折线：折线为每日 sessions 合计（与柱顶一致），双 Y 轴同刻度对齐。
- 悬停：各批次 sessions、当日合计、当日上新 ASIN 数（均可在 tooltip 查看）。
- KPI「上新 ASIN 总数」为线上 **amazon_listing** 满足条件的**行数**（不按 ASIN/店铺去重）；其它指标与
  「当日上新 ASIN 数」仍以 listing 日期口径为准。Online 不可用时回退本地 daily_upload_asin_dates。

本地库表（仅 session 堆叠与回退）：daily_upload_asin_dates。
图表下表格：第二列为 amazon_listing 的 COUNT(*), DATE(created_at)；第 1～30 列为本地 daily_upload_asin_dates
中 created_at=该上新日 的 session_date 汇总（与同步脚本写入口径一致）。

静态导出（backend 目录）：
  python3.11 -m app.services.daily_upload_session_report_html --out ./charts/report.html

或启动 API 后访问：
  GET /reports/daily-upload-sessions?listing_since=2026-02-20&session_start=...&session_end=...

若请求区间内本地表无 session 行，图表会自动改用本地 session_date 的全局 min~max 区间（见页面说明）。

按日 listing 行数与 `SELECT COUNT(*), DATE(created_at) … GROUP BY DATE(created_at)` 一致（含 asin 为 NULL/空白的行）。
若你在 SQL 里用 `created_at BETWEEN 'a' AND 'b'` 且列为 DATETIME，MySQL 会把上界当作当日 00:00:00，
与「按日历日 DATE(created_at)」统计时，边界日可能差少量行。
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal, init_db
from app.logging_config import setup_logging
from app.online_engine import get_online_engine

logger = logging.getLogger(__name__)

TABLE = "daily_upload_asin_dates"
DEFAULT_LISTING_SINCE = date(2026, 2, 20)
COHORT_TRACK_DAYS = 30

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


def _fetch_store_ids_for_range(db: Session, d0: date, d1: date) -> list[int]:
    q = text(
        f"SELECT DISTINCT store_id FROM {TABLE} "
        "WHERE DATE(session_date) >= :d0 AND DATE(session_date) <= :d1 ORDER BY store_id"
    )
    rows = db.execute(q, {"d0": d0, "d1": d1}).fetchall()
    return [int(r[0]) for r in rows if r[0] is not None]


def _fetch_matrix_rows(db: Session, store_id: int | None, d0: date, d1: date):
    # 必须用 DATE(...) GROUP BY：若列实际为 DATETIME，按「原值」分组会与 Python 侧 date  cohort 不一致，堆叠与折线合计会为 0。
    if store_id is not None:
        q = text(
            f"""
            SELECT DATE(session_date) AS sd, DATE(created_at) AS cd, SUM(COALESCE(sessions, 0)) AS s
            FROM {TABLE}
            WHERE store_id = :sid AND created_at IS NOT NULL
              AND DATE(session_date) >= :d0 AND DATE(session_date) <= :d1
            GROUP BY DATE(session_date), DATE(created_at)
            """
        )
        rows = db.execute(q, {"sid": store_id, "d0": d0, "d1": d1}).fetchall()
    else:
        q = text(
            f"""
            SELECT DATE(session_date) AS sd, DATE(created_at) AS cd, SUM(COALESCE(sessions, 0)) AS s
            FROM {TABLE}
            WHERE created_at IS NOT NULL
              AND DATE(session_date) >= :d0 AND DATE(session_date) <= :d1
            GROUP BY DATE(session_date), DATE(created_at)
            """
        )
        rows = db.execute(q, {"d0": d0, "d1": d1}).fetchall()
    mat: dict[tuple[date, date], int] = {}
    for r in rows:
        try:
            sd = _as_calendar_date(r[0])
            cd = _as_calendar_date(r[1])
        except (ValueError, TypeError):
            continue
        mat[(sd, cd)] = int(r[2] or 0)
    return mat


def _try_online_conn() -> Connection | None:
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        return None
    try:
        return get_online_engine().connect()
    except Exception as exc:
        logger.warning("Online DB 连接失败，KPI/当日上新将回退本地表: %s", exc)
        return None


def _fetch_listing_kpi_online(conn: Connection, since: date, store_id: int | None) -> tuple[int, int]:
    """amazon_listing：自 since 起 listing 全表行数 COUNT(*)（与常见 SQL 一致，含 asin 为空行）；active 仅统计有 asin 的 (店铺,asin)。"""
    if store_id is not None:
        total_sql = text(
            """
            SELECT COUNT(*) FROM amazon_listing
            WHERE DATE(created_at) >= :since
              AND store_id = :sid
            """
        )
        active_sql = text(
            """
            SELECT COUNT(*) FROM (
              SELECT store_id, asin FROM amazon_listing
              WHERE asin IS NOT NULL AND TRIM(asin) <> ''
                AND DATE(created_at) >= :since
                AND store_id = :sid
              GROUP BY store_id, asin
              HAVING SUM(CASE WHEN LOWER(TRIM(COALESCE(status, ''))) = 'active' THEN 1 ELSE 0 END) > 0
            ) t
            """
        )
        params = {"since": since, "sid": store_id}
    else:
        total_sql = text(
            """
            SELECT COUNT(*) FROM amazon_listing
            WHERE DATE(created_at) >= :since
            """
        )
        active_sql = text(
            """
            SELECT COUNT(*) FROM (
              SELECT store_id, asin FROM amazon_listing
              WHERE asin IS NOT NULL AND TRIM(asin) <> ''
                AND DATE(created_at) >= :since
              GROUP BY store_id, asin
              HAVING SUM(CASE WHEN LOWER(TRIM(COALESCE(status, ''))) = 'active' THEN 1 ELSE 0 END) > 0
            ) t
            """
        )
        params = {"since": since}
    tot = int(conn.execute(total_sql, params).scalar() or 0)
    act = int(conn.execute(active_sql, params).scalar() or 0)
    return tot, act


def _fetch_listing_new_asin_by_day_online(
    conn: Connection, store_id: int | None, d0: date, d1: date
) -> dict[date, int]:
    """与 ad-hoc SQL 一致：COUNT(*), DATE(created_at) GROUP BY DATE(created_at)，不筛 asin（含 NULL/空白）。"""
    if store_id is not None:
        q = text(
            """
            SELECT DATE(created_at) AS cd, COUNT(*) AS n
            FROM amazon_listing
            WHERE DATE(created_at) >= :d0 AND DATE(created_at) <= :d1
              AND store_id = :sid
            GROUP BY DATE(created_at)
            """
        )
        rows = conn.execute(q, {"d0": d0, "d1": d1, "sid": store_id}).fetchall()
    else:
        q = text(
            """
            SELECT DATE(created_at) AS cd, COUNT(*) AS n
            FROM amazon_listing
            WHERE DATE(created_at) >= :d0 AND DATE(created_at) <= :d1
            GROUP BY DATE(created_at)
            """
        )
        rows = conn.execute(q, {"d0": d0, "d1": d1}).fetchall()
    out: dict[date, int] = {}
    for r in rows:
        cd = _d(r[0])
        out[cd] = int(r[1] or 0)
    return out


def _fetch_listing_asin_by_cohort_dates_online(
    conn: Connection, store_id: int | None, cohort_dates: list[date]
) -> dict[date, int]:
    """各批次日在 amazon_listing 中的行数 COUNT(*)（与图表 tooltip / 表格第二列口径一致）。"""
    uniq = sorted({d for d in cohort_dates if d is not None})
    if not uniq:
        return {}
    ph = ", ".join([f":d{i}" for i in range(len(uniq))])
    params: dict = {f"d{i}": uniq[i] for i in range(len(uniq))}
    if store_id is not None:
        params["sid"] = store_id
        q = text(
            f"""
            SELECT DATE(created_at) AS cd, COUNT(*) AS n
            FROM amazon_listing
            WHERE DATE(created_at) IN ({ph})
              AND store_id = :sid
            GROUP BY DATE(created_at)
            """
        )
        rows = conn.execute(q, params).fetchall()
    else:
        q = text(
            f"""
            SELECT DATE(created_at) AS cd, COUNT(*) AS n
            FROM amazon_listing
            WHERE DATE(created_at) IN ({ph})
            GROUP BY DATE(created_at)
            """
        )
        rows = conn.execute(q, params).fetchall()
    out: dict[date, int] = {}
    for r in rows:
        out[_d(r[0])] = int(r[1] or 0)
    return out


def _fetch_local_asin_by_cohort_dates(
    db: Session, store_id: int | None, cohort_dates: list[date]
) -> dict[date, int]:
    """Online 不可用时：本地表按 created_at 日行数 COUNT(*)（与 listing 行数口径类比）。"""
    uniq = sorted({d for d in cohort_dates if d is not None})
    if not uniq:
        return {}
    ph = ", ".join([f":d{i}" for i in range(len(uniq))])
    params: dict = {f"d{i}": uniq[i] for i in range(len(uniq))}
    if store_id is not None:
        params["sid"] = store_id
        q = text(
            f"""
            SELECT created_at, COUNT(*) AS n
            FROM {TABLE}
            WHERE created_at IN ({ph}) AND store_id = :sid
            GROUP BY created_at
            """
        )
        rows = db.execute(q, params).fetchall()
    else:
        q = text(
            f"""
            SELECT created_at, COUNT(*) AS n
            FROM {TABLE}
            WHERE created_at IN ({ph})
            GROUP BY created_at
            """
        )
        rows = db.execute(q, params).fetchall()
    out: dict[date, int] = {}
    for r in rows:
        out[_d(r[0])] = int(r[1] or 0)
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
    """本地按 created_at 日行数（与 online COUNT(*) 类比）。"""
    if store_id is not None:
        q = text(
            f"""
            SELECT created_at, COUNT(*) AS n
            FROM {TABLE}
            WHERE store_id = :sid AND created_at IS NOT NULL
              AND created_at >= :d0 AND created_at <= :d1
            GROUP BY created_at
            """
        )
        rows = db.execute(q, {"sid": store_id, "d0": d0, "d1": d1}).fetchall()
    else:
        q = text(
            f"""
            SELECT created_at, COUNT(*) AS n
            FROM {TABLE}
            WHERE created_at IS NOT NULL
              AND created_at >= :d0 AND created_at <= :d1
            GROUP BY created_at
            """
        )
        rows = db.execute(q, {"d0": d0, "d1": d1}).fetchall()
    out: dict[date, int] = {}
    for r in rows:
        cd = _d(r[0])
        out[cd] = int(r[1] or 0)
    return out


def _fetch_total_asin_since(db: Session, store_id: int | None, since: date) -> int:
    """回退口径：本地同步表行数，不按 (店铺, ASIN) 去重（与 online 总行数口径对齐）。"""
    if store_id is not None:
        q = text(
            f"""
            SELECT COUNT(*) FROM {TABLE}
            WHERE created_at >= :since AND store_id = :sid
            """
        )
        r = db.execute(q, {"since": since, "sid": store_id}).scalar()
    else:
        q = text(
            f"""
            SELECT COUNT(*) FROM {TABLE}
            WHERE created_at >= :since
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
              WHERE created_at >= :since AND store_id = :sid
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
              WHERE created_at >= :since
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
    本地 daily_upload：该批次日 created_at 下，按 session_date 汇总 sessions（同步数据已来自 listing，
    避免对数十万 (store,asin) 做 IN 分块导致全 0 或超时）。
    """
    sd_max = cohort_date + timedelta(days=COHORT_TRACK_DAYS - 1)
    if store_id is not None:
        q = text(
            f"""
            SELECT DATE(session_date) AS sd, SUM(COALESCE(sessions, 0)) AS s
            FROM {TABLE}
            WHERE DATE(created_at) = :cd AND store_id = :sid
              AND DATE(session_date) >= :cd AND DATE(session_date) <= :sdmax
            GROUP BY DATE(session_date)
            """
        )
        rows = db.execute(
            q, {"cd": cohort_date, "sid": store_id, "sdmax": sd_max}
        ).fetchall()
    else:
        q = text(
            f"""
            SELECT DATE(session_date) AS sd, SUM(COALESCE(sessions, 0)) AS s
            FROM {TABLE}
            WHERE DATE(created_at) = :cd
              AND DATE(session_date) >= :cd AND DATE(session_date) <= :sdmax
            GROUP BY DATE(session_date)
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


def _fetch_local_session_bounds(db: Session) -> tuple[date | None, date | None]:
    row = db.execute(
        text(f"SELECT MIN(DATE(session_date)), MAX(DATE(session_date)) FROM {TABLE}")
    ).fetchone()
    if not row or row[0] is None:
        return None, None
    return _d(row[0]), _d(row[1])


def _has_session_rows_in_range(db: Session, d0: date, d1: date) -> bool:
    r = db.execute(
        text(
            f"SELECT 1 FROM {TABLE} WHERE DATE(session_date) >= :d0 AND DATE(session_date) <= :d1 LIMIT 1"
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
) -> list[dict]:
    """
    一行 = 一个上新日：amazon_listing 当日行数 COUNT(*)；
    第 1～30 列 = 本地表中 created_at=该上新日 的各 session_date 的 sessions 合计（与 daily_upload 同步口径一致）。
    """
    if online is not None:
        new_by_day = _fetch_listing_new_asin_by_day_online(
            online, store_id, listing_since, listing_through
        )
    else:
        new_by_day = _fetch_new_asin_by_day(db, store_id, listing_since, listing_through)
    cohort_dates = sorted(
        cd for cd in new_by_day if listing_since <= cd <= listing_through
    )

    rows: list[dict] = []
    for cd in cohort_dates:
        n_new = int(new_by_day[cd])
        sess_by_sd = _sum_sessions_by_cohort_local(db, store_id, cd)
        day_sessions: list[int] = []
        for k in range(COHORT_TRACK_DAYS):
            sd = cd + timedelta(days=k)
            day_sessions.append(int(sess_by_sd.get(sd, 0)))
        rows.append(
            {
                "cohortDate": cd.isoformat(),
                "newAsin": n_new,
                "daySessions": day_sessions,
            }
        )
    return rows


def _build_view_payload(
    db: Session,
    store_id: int | None,
    listing_since: date,
    session_start: date,
    session_end: date,
    *,
    total_asin: int,
    active_asin: int,
    new_asin_by_day: dict[date, int],
    cohort_table: list[dict],
    online: Connection | None,
) -> dict:
    mat = _fetch_matrix_rows(db, store_id, session_start, session_end)
    session_days = sorted({k[0] for k in mat})
    cohort_days = sorted({k[1] for k in mat})
    labels = [d.isoformat() for d in session_days]
    cohort_labels = [d.isoformat() for d in cohort_days]
    cohort_listing_asin = _cohort_listing_asin_map_json(db, online, store_id, cohort_days)

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
) -> dict:
    """含全部店铺视图 + 各 store 视图，供前端切换。"""
    online = _try_online_conn()
    kpi_source = "amazon_listing" if online is not None else "daily_upload_asin_dates_fallback"
    listing_through = date.today()
    gmin, gmax = _fetch_local_session_bounds(db)
    chart_start, chart_end = session_start, session_end
    chart_auto = False
    if not _has_session_rows_in_range(db, session_start, session_end):
        if gmin is not None and gmax is not None and gmin <= gmax:
            chart_start, chart_end = gmin, gmax
            chart_auto = True

    store_ids = _fetch_store_ids_for_range(db, chart_start, chart_end)
    try:

        def listing_kpi(sid: int | None) -> tuple[int, int]:
            if online is not None:
                return _fetch_listing_kpi_online(online, listing_since, sid)
            return _fetch_total_asin_since(db, sid), _fetch_active_asin_since(db, sid)

        def listing_new_per_day_for_chart(sid: int | None) -> dict[date, int]:
            if online is not None:
                return _fetch_listing_new_asin_by_day_online(
                    online, sid, chart_start, chart_end
                )
            return _fetch_new_asin_by_day(db, sid, chart_start, chart_end)

        all_tot, all_act = listing_kpi(None)
        all_new = listing_new_per_day_for_chart(None)
        all_cohort = _build_cohort_table_rows(db, online, listing_since, listing_through, None)
        views: dict[str, dict] = {
            "all": _build_view_payload(
                db,
                None,
                listing_since,
                chart_start,
                chart_end,
                total_asin=all_tot,
                active_asin=all_act,
                new_asin_by_day=all_new,
                cohort_table=all_cohort,
                online=online,
            ),
        }
        for sid in store_ids:
            t, a = listing_kpi(sid)
            nw = listing_new_per_day_for_chart(sid)
            ct = _build_cohort_table_rows(db, online, listing_since, listing_through, sid)
            views[str(sid)] = _build_view_payload(
                db,
                sid,
                listing_since,
                chart_start,
                chart_end,
                total_asin=t,
                active_asin=a,
                new_asin_by_day=nw,
                cohort_table=ct,
                online=online,
            )
        return {
            "generatedAt": date.today().isoformat(),
            "listingSince": listing_since.isoformat(),
            "listingThrough": listing_through.isoformat(),
            "sessionRequestedStart": session_start.isoformat(),
            "sessionRequestedEnd": session_end.isoformat(),
            "sessionChartStart": chart_start.isoformat(),
            "sessionChartEnd": chart_end.isoformat(),
            "chartRangeAutoExpanded": chart_auto,
            "localSessionMin": gmin.isoformat() if gmin else None,
            "localSessionMax": gmax.isoformat() if gmax else None,
            "storeIds": store_ids,
            "views": views,
            "kpiSource": kpi_source,
            "cohortTrackDays": COHORT_TRACK_DAYS,
        }
    finally:
        if online is not None:
            try:
                online.close()
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
      <span>自 <span id="sinceLbl"></span> 起上新 ASIN 总数</span>
      <strong id="kpiTotal">—</strong>
      <div class="muted">amazon_listing 行数，不按 ASIN/店铺去重 · 详见下方说明</div>
    </div>
    <div class="kpi-card">
      <span>其中 Active（listing status=active）</span>
      <strong id="kpiActive">—</strong>
      <div class="muted">该 ASIN 在 listing 中至少一条为 active</div>
    </div>
  </div>
  <p class="muted" id="kpiSourceHint" style="margin-bottom:16px"></p>
  <div class="chart-wrap">
    <p class="muted" style="margin-top:0">柱形为各上新批次贡献的 sessions 堆叠；黑色折线为每日 sessions 合计。悬停主列表中各批次数字为 amazon_listing 该批次日的 listing 行数 COUNT(*)；底部为当日 session 明细与合计。</p>
    <p id="noData" class="muted" style="display:none"></p>
    <div class="chart-canvas-box"><canvas id="ch"></canvas></div>
  </div>

  <div class="table-wrap" id="cohortTableWrap">
    <h2>批次上新 ASIN 的 30 日 session 变化（listing 定 ASIN，本地表取 session）</h2>
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
      h += '（本地在请求区间内无数据，已自动扩展到表内 session_date 范围）';
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
    kpiHint.textContent = '说明：KPI 与表格第二列为 amazon_listing 全表行数 COUNT(*)（含 asin 为空），与 `COUNT(*), DATE(created_at) GROUP BY DATE(created_at)` 一致；第1～30天为本地表按 created_at 批次的 session。若请求区间内无 session，图表会按本地 session_date 全局范围扩展。';
  } else {
    kpiHint.textContent = '说明：线上库未配置或不可用，上新相关数字回退为本地 daily_upload_asin_dates（与 listing 口径可能不一致）。';
  }
  document.getElementById('cohortTableHint').textContent =
    '上新统计区间：' + P.listingSince + ' ～ ' + P.listingThrough + '（与图表 session 区间无关）。「listing 当日行数」= online 全表 COUNT(*), DATE(created_at)（含 asin 为空，与 ad-hoc SQL 一致）。「第 k 天」= 本地 DATE(created_at)=该上新日 且 DATE(session_date)=上新日+(k-1) 的 sessions；若 session 行的上新日早于该行「上新日」，则数字出现在更早的上新日行，不会出现在本行。图表折线按 session_date 把各批次堆叠相加，与表格某一行的「第几天」口径不同。Online 不可用时第二列由本地表按 created_at 估算。';

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
        var m = v.cohortListingAsin || {};
        var na = cd != null ? m[cd] : null;
        var ns = na != null && na !== undefined ? Number(na).toLocaleString() : '—';
        html += '<div class="chart-tooltip-external-row"><span class="swatch" style="background:' + escapeHtml(color) + '"></span><span class="txt">' +
          escapeHtml(ds.label) + ': ' + ns + '（listing 行数）</span></div>';
      }
    });

    html += '<div class="chart-tooltip-external-sep">────────</div><div class="chart-tooltip-external-footer">';
    html += '<div>当日 sessions 合计: ' + day.totalSessions.toLocaleString() + '</div>';
    html += '<div>当日上新 ASIN 数: ' + day.newAsinCount.toLocaleString() + '</div>';
    if (day.cohortParts && day.cohortParts.length) {
      html += '<div class="chart-tooltip-external-sub">各批次 sessions:</div>';
      day.cohortParts.forEach(function (p) {
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
        '">暂无行：该店铺在 listing 区间内无上新记录，或 online 不可用时本地亦无对应 created_at。</td></tr></tbody>';
      return;
    }
    var hr = '<tr><th>上新日</th><th>listing 当日行数</th>';
    for (var i = 1; i <= nd; i++) {
      hr += '<th title="该批 listing ASIN 在上新日起第 ' + i + ' 个日历日的 sessions 合计">第' + i + '天</th>';
    }
    hr += '</tr>';
    var body = '';
    v.cohortTable.forEach(function (row) {
      body += '<tr><td>' + row.cohortDate + '</td><td>' + row.newAsin.toLocaleString() + '</td>';
      for (var j = 0; j < nd; j++) {
        var val = (row.daySessions && row.daySessions[j] !== undefined) ? row.daySessions[j] : 0;
        body += '<td>' + Number(val).toLocaleString() + '</td>';
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
