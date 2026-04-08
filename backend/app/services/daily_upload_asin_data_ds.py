"""
从线上库拉取每日上新 ASIN，并写入本地 daily_upload_asin_dates。

**并非**把线上 amazon_listing 全表搬进本地：只处理命令行/默认区间内的
``DATE(created_at)`` 日历日；每条 listing 会展开为多行（不同 ``session_date``）。

若本地 ``open_date`` 为 NULL 而线上 ``amazon_listing.open_date`` 已有值，常见原因：
1) **未再同步到该 listing 的 created_at 日**：默认起始日来自本地 ``MAX(session_date)-1``，
   与 ``created_at`` 无关，历史上新日不会被重复扫描，已写入行的 ``open_date`` 不会自动更新。
2) **曾同步时线上 open_date 尚为空**，后来线上补全——需对该 ``created_at`` 日重新跑同步区间，
   或 CLI 使用 ``--backfill-open-date`` 按 (asin, store_id, created_at, pid) 向线上补拉。
3) 驱动/MySQL 对非法日期、零日期返回 NULL：本模块对 ``open_date`` 做规范化读取。

数据来源（online）：
1) amazon_listing: 按 created_at（DATE）筛选每日上新记录
2) amazon_variation: 通过 listing.variation_id -> variation.asin 得到 parent_asin（写入 paren_asin 字段）
3) sessions：
   - 默认从 amazon_sales_and_traffic_daily 拉取列 `current_date`（非 MySQL 的 CURRENT_DATE 函数）；
   - **重要**：`--start-date` / `--end-date` 只约束 **amazon_listing 的上新日历日**（DATE(created_at)），不是 traffic 的 current_date。
     默认对每个上新日拉取 **[上新日-1, 上新日+30]** 共 32 个日历日的 daily 行（避免仅 4/1 上新时永远查不到 3/31）。
   - 若命令行指定 --session-date（可多个），则仅同步这些 current_date（缺失则 sessions=0）；
   - 若 daily 表不可用，则回退尝试 amazon_sales_and_traffic(session_date=...)，再回退 amazon_sales_traffic(week_no)（周粒度）。

写入（local）：
表 daily_upload_asin_dates，唯一键 (asin, created_at, pid, store_id, session_date)（见模型）；
如已存在则仅当 sessions 变化时更新。

定时任务（后端 ENABLE_SCHEDULED_SYNC=true 且 enable_daily_upload_ds_schedule=true）：
  - 每 ``daily_upload_ds_interval_hours`` 小时（默认 2）在 ``sync_timezone``（默认 Asia/Shanghai）整点执行；
  - ``--start-date`` 等价配置：``.env`` 中 ``daily_upload_ds_start_date``（默认 2026-02-20）；
  - 结束日为东八区（与 ``SYNC_TIMEZONE`` 一致）当前日历日（含）。

用法（在 backend 目录下）：
  python3.11 -m app.services.daily_upload_asin_data_ds
      # 省略起止日时：按东八区日期默认同步最近 35 天（start=今天-35 天，end=今天，含端点）
  python3.11 -m app.services.daily_upload_asin_data_ds --start-date 2026-02-20 --end-date 2026-03-31
  python3.11 -m app.services.daily_upload_asin_data_ds --date 2026-03-31
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.dialects.mysql import insert as mysql_insert

from app.config import settings
from app.database import SessionLocal, init_db
from app.logging_config import setup_logging
from app.models.daily_upload_asin_data import DailyUploadAsinData
from app.online_engine import get_online_engine
from app.sync_run_record import now_asia, record_daily_upload_ds_run, should_run_daily_upload_ds_sync

logger = logging.getLogger(__name__)

# 定时任务 listing 下界默认值（与 Settings.daily_upload_ds_start_date 一致）
_DEFAULT_SCHEDULED_LISTING_START = date(2026, 2, 20)

# 相对「上新日历日」拉取 amazon_sales_and_traffic_daily.current_date 的区间（含端点）
_DEFAULT_SESSION_OFFSET_START = -1
_DEFAULT_SESSION_OFFSET_END = 30


def _parse_ymd(s: str) -> date:
    s = str(s or "").strip()
    return datetime.strptime(s, "%Y-%m-%d").date()


def _coerce_sql_date(val, *, field: str = "date") -> date | None:
    """
    将 MySQL/SQLAlchemy 返回的列值规范为 datetime.date 或 None。
    处理 datetime、date、字符串、零日期（部分驱动会读成 None 或异常字符串）。
    """
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    s = str(val).strip()
    if not s:
        return None
    head = s[:10]
    if head in ("0000-00-00", "0000-00-0"):
        return None
    try:
        return _parse_ymd(head)
    except ValueError:
        logger.warning("[DailyUploadDS] skip invalid %s value=%r", field, val)
        return None


def _same_calendar_date(a, b) -> bool:
    return _coerce_sql_date(a) == _coerce_sql_date(b)


def default_sync_listing_date_bounds(local_db) -> tuple[date, date]:
    """
    默认 listing 日区间（created_at 按日扫描）：
    - 若 CLI 未指定 --start-date/--end-date：按东八区日期默认同步最近 35 天
      - start = today(Asia/Shanghai) - 35 天
      - end   = today(Asia/Shanghai)（含端点）
    """
    # local_db 参数保留：方便未来按表状态动态调整；当前按需求固定为“最近 35 天”
    _ = local_db
    end_d = now_asia().date()
    start_d = end_d - timedelta(days=35)
    return start_d, end_d


def sync_with_default_date_range(session_dates: list[date] | None = None) -> dict:
    """init_db 后按 default_sync_listing_date_bounds 执行 sync_range（供定时任务与无参 CLI）。"""
    init_db()
    local_db = SessionLocal()
    try:
        start_d, end_d = default_sync_listing_date_bounds(local_db)
    finally:
        local_db.close()
    return sync_range(start_d, end_d, session_dates=session_dates)


def _parse_scheduled_start_date_from_env() -> date:
    raw = (settings.DAILY_UPLOAD_DS_START_DATE or "").strip()
    if not raw:
        return _DEFAULT_SCHEDULED_LISTING_START
    try:
        return _parse_ymd(raw)
    except ValueError:
        logger.warning(
            "[DailyUploadDS] invalid daily_upload_ds_start_date=%r, using default %s",
            raw,
            _DEFAULT_SCHEDULED_LISTING_START.isoformat(),
        )
        return _DEFAULT_SCHEDULED_LISTING_START


def sync_scheduled_listing_date_range_ds() -> dict:
    """
    APScheduler 调用：listing 扫描 [start, end]，start 来自 .env，end 为 SYNC_TIMEZONE 当前日历日（含）。
    """
    start_d = _parse_scheduled_start_date_from_env()
    end_d = now_asia().date()
    if start_d > end_d:
        logger.warning(
            "[DailyUploadDS] scheduled range invalid: start=%s > end=%s (%s today), clamping start=end",
            start_d.isoformat(),
            end_d.isoformat(),
            settings.SYNC_TIMEZONE,
        )
        start_d = end_d
    logger.info(
        "[DailyUploadDS] scheduled sync: start_date=%s (daily_upload_ds_start_date / default), "
        "end_date=%s (%s calendar day, inclusive)",
        start_d.isoformat(),
        end_d.isoformat(),
        settings.SYNC_TIMEZONE,
    )
    return sync_range(start_d, end_d, session_dates=None)


# 定时任务与手动触发共用锁，避免并发双跑
_scheduled_ds_lock = threading.Lock()


def run_daily_upload_ds_scheduled(*, force: bool = False) -> dict | None:
    """
    定时任务包装函数（供 app/main.py 与 /api/daily-upload-ds 调用）。

    - force=False：若本小时（东八区）已成功跑过则跳过（用 app/sync_run_record.py 记录）。
    - force=True：跳过「本小时已跑」检查（用于手动补跑）；仍受锁约束。
    """
    if not _scheduled_ds_lock.acquire(blocking=False):
        logger.info("[DailyUploadDS] scheduled job skipped: previous run still in progress")
        return None
    try:
        if not force and not should_run_daily_upload_ds_sync():
            logger.info("[DailyUploadDS] scheduled job skipped: already run in this hour slot (Asia/Shanghai)")
            return None
        out = sync_scheduled_listing_date_range_ds()
        record_daily_upload_ds_run()
        return out
    except Exception as e:
        logger.exception("[DailyUploadDS] scheduled job failed: %s", e)
        return None
    finally:
        try:
            _scheduled_ds_lock.release()
        except Exception:
            pass


def _iter_dates(start: date, end: date) -> list[date]:
    if start > end:
        return []
    out = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _week_no_str_for_day(d: date) -> str:
    # MySQL WEEK(date, 0) 口径：周日开始
    # 这里保持与 online_sync.py 的 week_no 生成方式一致
    week_num = int(d.strftime("%U"))  # %U: week number Sunday as first day, 00-53
    if week_num <= 0:
        week_num = 1
    return f"{d.year}{week_num:02d}"


def _fetch_variation_parent_asin_map(online_conn, variation_ids: list[int]) -> dict[int, str]:
    if not variation_ids:
        return {}
    out: dict[int, str] = {}
    t0 = time.time()
    batch_size = 800
    for i in range(0, len(variation_ids), batch_size):
        chunk = variation_ids[i : i + batch_size]
        ph = ", ".join([f":v{j}" for j in range(len(chunk))])
        params = {f"v{j}": int(chunk[j]) for j in range(len(chunk))}
        rows = online_conn.execute(
            text(f"SELECT id, asin FROM amazon_variation WHERE id IN ({ph})"),
            params,
        ).fetchall()
        for r in rows:
            if r[0] is None:
                continue
            try:
                vid = int(r[0])
            except (TypeError, ValueError):
                continue
            asin = (r[1] or "").strip()
            if asin:
                out[vid] = asin
    logger.info(
        "[DailyUploadDS] variation parent_asin map: variation_ids=%s mapped=%s elapsed_sec=%.2f",
        len(variation_ids),
        len(out),
        time.time() - t0,
    )
    return out


def _fetch_daily_sessions_map(online_conn, pairs: list[tuple[str, int]], session_day: date) -> dict[tuple[str, int], int]:
    """
    返回 (asin, store_id) -> sessions
    优先尝试 amazon_sales_and_traffic(session_date)，失败则回退到 amazon_sales_traffic(week_no)。
    """
    if not pairs:
        return {}
    out: dict[tuple[str, int], int] = {}
    batch_size = 600
    day_str = session_day.strftime("%Y-%m-%d")

    def _run_and_fill(sql: str, params: dict) -> None:
        rows = online_conn.execute(text(sql), params).fetchall()
        for r in rows:
            asin = (r[0] or "").strip()
            sid = int(r[1]) if r[1] is not None else None
            if not asin or sid is None:
                continue
            out[(asin, sid)] = int(r[2] or 0)

    # 1) try amazon_sales_and_traffic_daily (current_date)
    t0 = time.time()
    try:
        for i in range(0, len(pairs), batch_size):
            chunk = pairs[i : i + batch_size]
            ph = ", ".join([f"(:a{j}, :s{j})" for j in range(len(chunk))])
            params = {"d": day_str}
            for j, (asin, sid) in enumerate(chunk):
                params[f"a{j}"] = asin
                params[f"s{j}"] = int(sid)
            sql = (
                "SELECT d.asin, d.store_id, SUM(COALESCE(d.sessions, 0)) AS sessions "
                "FROM amazon_sales_and_traffic_daily AS d "
                f"WHERE (d.asin, d.store_id) IN ({ph}) AND DATE(d.`current_date`) = :d "
                "GROUP BY d.asin, d.store_id"
            )
            _run_and_fill(sql, params)
        logger.info(
            "[DailyUploadDS] sessions source=amazon_sales_and_traffic_daily day=%s pairs=%s hits=%s elapsed_sec=%.2f",
            day_str,
            len(pairs),
            len(out),
            time.time() - t0,
        )
        return out
    except Exception as exc:
        logger.warning(
            "[DailyUploadDS] amazon_sales_and_traffic_daily not available or query failed (%s), fallback to amazon_sales_and_traffic then amazon_sales_traffic",
            exc,
        )

    # 2) try amazon_sales_and_traffic (session_date)
    t_mid = time.time()
    try:
        for i in range(0, len(pairs), batch_size):
            chunk = pairs[i : i + batch_size]
            ph = ", ".join([f"(:a{j}, :s{j})" for j in range(len(chunk))])
            params = {"d": day_str}
            for j, (asin, sid) in enumerate(chunk):
                params[f"a{j}"] = asin
                params[f"s{j}"] = int(sid)
            sql = (
                "SELECT asin, store_id, SUM(COALESCE(sessions, 0)) AS sessions "
                "FROM amazon_sales_and_traffic "
                f"WHERE (asin, store_id) IN ({ph}) AND session_date = :d "
                "GROUP BY asin, store_id"
            )
            _run_and_fill(sql, params)
        logger.info(
            "[DailyUploadDS] sessions source=amazon_sales_and_traffic day=%s pairs=%s hits=%s elapsed_sec=%.2f",
            day_str,
            len(pairs),
            len(out),
            time.time() - t_mid,
        )
        return out
    except Exception as exc:
        logger.warning(
            "[DailyUploadDS] amazon_sales_and_traffic not available or query failed (%s), fallback to amazon_sales_traffic week sessions",
            exc,
        )

    # 3) fallback: amazon_sales_traffic week_no
    t1 = time.time()
    wk = _week_no_str_for_day(session_day)
    for i in range(0, len(pairs), batch_size):
        chunk = pairs[i : i + batch_size]
        ph = ", ".join([f"(:a{j}, :s{j})" for j in range(len(chunk))])
        params = {"wk": wk}
        for j, (asin, sid) in enumerate(chunk):
            params[f"a{j}"] = asin
            params[f"s{j}"] = int(sid)
        sql = (
            "SELECT asin, store_id, SUM(COALESCE(sessions, 0)) AS sessions "
            "FROM amazon_sales_traffic "
            f"WHERE (asin, store_id) IN ({ph}) AND week_no = :wk "
            "GROUP BY asin, store_id"
        )
        _run_and_fill(sql, params)
    logger.info(
        "[DailyUploadDS] sessions source=amazon_sales_traffic week_no=%s day=%s pairs=%s hits=%s elapsed_sec=%.2f",
        wk,
        day_str,
        len(pairs),
        len(out),
        time.time() - t1,
    )
    return out


def _fetch_sessions_rows_daily(
    online_conn,
    pairs: list[tuple[str, int]],
    session_dates: list[date] | None,
) -> dict[tuple[str, int], dict[date, int]]:
    """
    从 amazon_sales_and_traffic_daily 拉取 sessions。
    返回: (asin, store_id) -> {session_date: sessions}
    - session_dates=None：拉取该 asin 在表中所有 current_date
    - session_dates!=None：仅拉取这些日期（未命中留给上层补 0）
    """
    if not pairs:
        return {}
    batch_size = 500
    out: dict[tuple[str, int], dict[date, int]] = {}
    t0 = time.time()

    # 预处理日期过滤
    date_params = {}
    date_filter_sql = ""
    if session_dates:
        uniq = sorted({d.strftime("%Y-%m-%d") for d in session_dates})
        ph_d = ", ".join([f":d{i}" for i in range(len(uniq))])
        # 用 DATE()：列若为 DATETIME，直接 IN ('YYYY-MM-DD') 在部分环境下可能漏行；GROUP BY 也需按日历日
        date_filter_sql = f" AND DATE(d.`current_date`) IN ({ph_d})"
        date_params = {f"d{i}": uniq[i] for i in range(len(uniq))}

    for i in range(0, len(pairs), batch_size):
        chunk = pairs[i : i + batch_size]
        ph = ", ".join([f"(:a{j}, :s{j})" for j in range(len(chunk))])
        params = dict(date_params)
        for j, (asin, sid) in enumerate(chunk):
            params[f"a{j}"] = asin
            params[f"s{j}"] = int(sid)
        sql = (
            "SELECT d.asin, d.store_id, DATE(d.`current_date`) AS traffic_day, "
            "SUM(COALESCE(d.sessions, 0)) AS sessions "
            "FROM amazon_sales_and_traffic_daily AS d "
            f"WHERE (d.asin, d.store_id) IN ({ph}){date_filter_sql} "
            "GROUP BY d.asin, d.store_id, DATE(d.`current_date`)"
        )
        rows = online_conn.execute(text(sql), params).fetchall()
        for r in rows:
            asin = (r[0] or "").strip()
            sid = int(r[1]) if r[1] is not None else None
            if not asin or sid is None or r[2] is None:
                continue
            # 表列 current_date（须反引号，避免与 MySQL CURRENT_DATE 函数混淆）可能是 date/datetime/str
            if isinstance(r[2], date) and not isinstance(r[2], datetime):
                d = r[2]
            else:
                d = _parse_ymd(str(r[2])[:10])
            out.setdefault((asin, sid), {})[d] = int(r[3] or 0)

    logger.info(
        "[DailyUploadDS] sessions source=amazon_sales_and_traffic_daily pairs=%s dates_filter=%s pair_hits=%s elapsed_sec=%.2f",
        len(pairs),
        len(session_dates) if session_dates else "ALL",
        len(out),
        time.time() - t0,
    )
    return out


def _upsert_local_rows(local_db, rows: list[dict]) -> dict[str, int]:
    if not rows:
        return {"inserted": 0, "updated": 0, "unchanged": 0}

    # 先查询 existing，用于“无变化不更新”
    t0 = time.time()
    key_tuples = sorted(
        {
            (r["asin"], r.get("created_at"), int(r.get("pid") or 0), int(r["store_id"]), r["session_date"])
            for r in rows
        }
    )
    existing: dict[tuple[str, date | None, int, int, date], dict] = {}
    for chunk in [key_tuples[i : i + 800] for i in range(0, len(key_tuples), 800)]:
        ph = ", ".join([f"(:a{i}, :c{i}, :p{i}, :s{i}, :d{i})" for i in range(len(chunk))])
        params = {}
        for i, (asin, created_at, pid, sid, d) in enumerate(chunk):
            params[f"a{i}"] = asin
            params[f"c{i}"] = created_at
            params[f"p{i}"] = int(pid)
            params[f"s{i}"] = int(sid)
            params[f"d{i}"] = d
        q = (
            "SELECT asin, created_at, COALESCE(pid, 0) AS pid, store_id, session_date, "
            "       COALESCE(sessions, 0), open_date "
            "FROM daily_upload_asin_dates "
            f"WHERE (asin, created_at, COALESCE(pid, 0), store_id, session_date) IN ({ph})"
        )
        for r in local_db.execute(text(q), params).fetchall():
            existing[(str(r[0]), r[1], int(r[2] or 0), int(r[3]), r[4])] = {
                "sessions": int(r[5] or 0),
                "open_date": r[6],
            }

    inserts = []
    updates = []
    unchanged = 0
    for r in rows:
        key = (
            r["asin"],
            r.get("created_at"),
            int(r.get("pid") or 0),
            int(r["store_id"]),
            r["session_date"],
        )
        prev = existing.get(key)
        if not prev:
            inserts.append(r)
            continue
        # 需求口径：
        # - 定位字段为 asin, created_at, pid, store_id, session_date
        # - 若 sessions 与获取到的数据一致则不更新，否则只更新 sessions
        changed = (int(prev.get("sessions") or 0) != int(r.get("sessions") or 0)) or (
            not _same_calendar_date(prev.get("open_date"), r.get("open_date"))
        )
        if changed:
            updates.append(r)
        else:
            unchanged += 1

    inserted = 0
    updated = 0

    if inserts:
        stmt = mysql_insert(DailyUploadAsinData).values(inserts)
        res = local_db.execute(stmt)
        inserted = int(getattr(res, "rowcount", 0) or 0)

    if updates:
        # ON DUPLICATE KEY UPDATE（避免先查 id）
        stmt = mysql_insert(DailyUploadAsinData).values(updates)
        stmt = stmt.on_duplicate_key_update(
            sessions=stmt.inserted.sessions,
            open_date=stmt.inserted.open_date,
        )
        res = local_db.execute(stmt)
        updated = int(getattr(res, "rowcount", 0) or 0)

    logger.info(
        "[DailyUploadDS] local upsert: payload=%s existing=%s inserts=%s updates=%s unchanged=%s inserted=%s updated=%s elapsed_sec=%.2f",
        len(rows),
        len(existing),
        len(inserts),
        len(updates),
        unchanged,
        inserted,
        updated,
        time.time() - t0,
    )
    return {"inserted": inserted, "updated": updated, "unchanged": unchanged}


def _build_day_payload(
    online_engine,
    created_day: date,
    session_dates: list[date] | None = None,
) -> tuple[date, list[dict], dict]:
    """
    单日在线拉取 + 组装 payload。
    在 worker 线程中运行：线程内自建 connection，避免跨线程共享连接。
    """
    day_str = created_day.strftime("%Y-%m-%d")
    t0 = time.time()
    with online_engine.connect() as online_conn:
        listing_rows = online_conn.execute(
            text(
                "SELECT asin, pid, variation_id, store_id, status, "
                "       DATE(created_at) AS created_at, open_date AS open_date_raw "
                "FROM amazon_listing "
                "WHERE created_at >= :d0 AND created_at < :d1 "
                "  AND asin IS NOT NULL AND asin <> ''"
            ),
            {"d0": day_str, "d1": (created_day + timedelta(days=1)).strftime("%Y-%m-%d")},
        ).fetchall()
        if not listing_rows:
            return created_day, [], {"listing_rows": 0, "pairs": 0, "sessions_pair_hits": 0, "sessions_missing": 0, "elapsed_sec": time.time() - t0}

        try:
            n_open_null = sum(1 for r in listing_rows if _coerce_sql_date(r[6], field="open_date") is None)
            if n_open_null:
                logger.warning(
                    "[DailyUploadDS] listing open_date is NULL for %s/%s rows (created_day=%s); local open_date will remain NULL for those rows.",
                    n_open_null,
                    len(listing_rows),
                    day_str,
                )
        except Exception:
            pass

        variation_ids = sorted({int(r[2]) for r in listing_rows if r[2] is not None})
        var_map = _fetch_variation_parent_asin_map(online_conn, variation_ids)

        pairs = sorted({((r[0] or "").strip(), int(r[3])) for r in listing_rows if r[0] and r[3] is not None})
        # 默认拉取 [上新日+offset_start, 上新日+offset_end] 的 current_date；若指定 session_dates，则只拉这些日期
        sessions_by_pair: dict[tuple[str, int], dict[date, int]] = {}
        sessions_missing = 0
        session_window_first: date | None = None
        session_window_last: date | None = None
        try:
            if session_dates:
                sessions_filter = session_dates
                requested_dates = sorted(session_dates)
            else:
                sessions_filter = [
                    created_day + timedelta(days=i)
                    for i in range(_DEFAULT_SESSION_OFFSET_START, _DEFAULT_SESSION_OFFSET_END + 1)
                ]
                session_window_first = sessions_filter[0]
                session_window_last = sessions_filter[-1]
                requested_dates = None  # 默认不强制补 0，仅写入实际存在的日期行
            sessions_by_pair = _fetch_sessions_rows_daily(online_conn, pairs, sessions_filter)
        except Exception as exc:
            logger.warning(
                "[DailyUploadDS] amazon_sales_and_traffic_daily query failed (%s); sessions empty for created_day=%s",
                exc,
                day_str,
                exc_info=logger.isEnabledFor(logging.DEBUG),
            )
            sessions_by_pair = {}

        # 若用户指定日期但没命中，后续补 0（requested_dates 非 None）

        n_listing = len(listing_rows)
        total_daily_cells = sum(len(m) for m in sessions_by_pair.values())
        if n_listing and total_daily_cells == 0:
            win = (
                f"{session_window_first.isoformat()}..{session_window_last.isoformat()}"
                if session_window_first and session_window_last
                else ",".join(d.strftime("%Y-%m-%d") for d in (requested_dates or sessions_filter or []))
            )
            logger.warning(
                "[DailyUploadDS] created_day=%s: %s amazon_listing rows, %s (asin,store_id) pairs, "
                "0 hits in amazon_sales_and_traffic_daily for current_date in [%s]. "
                "Often: online 无该日数据，或 listing.asin/store_id 与 daily 表不一致；可试 --session-date YYYY-MM-DD 强制日期。",
                day_str,
                n_listing,
                len(pairs),
                win,
            )

        payload: list[dict] = []
        for r in listing_rows:
            asin = (r[0] or "").strip()
            if not asin:
                continue
            store_id = int(r[3]) if r[3] is not None else None
            if store_id is None:
                continue
            pid = int(r[1]) if r[1] is not None else None
            variation_id = int(r[2]) if r[2] is not None else None
            parent_asin = var_map.get(variation_id) if variation_id is not None else None
            status = (r[4] or "").strip() or None
            created_at = _coerce_sql_date(r[5], field="created_at") or created_day
            open_date = _coerce_sql_date(r[6], field="open_date")
            date_map = sessions_by_pair.get((asin, store_id), {})
            if requested_dates is None:
                # 未指定 session_dates：只写入 daily 表中存在的日期行
                for sd, sess in sorted(date_map.items()):
                    payload.append(
                        {
                            "asin": asin,
                            "pid": pid,
                            "paren_asin": parent_asin,
                            "store_id": store_id,
                            "status": status,
                            "created_at": created_at,
                            "open_date": open_date,
                            "session_date": sd,
                            "sessions": int(sess or 0),
                        }
                    )
            else:
                # 指定 session_dates：每个日期都写入（未命中则 0）
                for sd in requested_dates:
                    payload.append(
                        {
                            "asin": asin,
                            "pid": pid,
                            "paren_asin": parent_asin,
                            "store_id": store_id,
                            "status": status,
                            "created_at": created_at,
                            "open_date": open_date,
                            "session_date": sd,
                            "sessions": int(date_map.get(sd, 0) or 0),
                        }
                    )

        meta = {
            "listing_rows": len(payload),
            "pairs": len(pairs),
            "sessions_pair_hits": len(sessions_by_pair),
            "sessions_missing": sessions_missing,
            "variation_ids": len(variation_ids),
            "variation_mapped": len(var_map),
            "elapsed_sec": time.time() - t0,
            "created_day": day_str,
            "session_dates": (
                [d.strftime("%Y-%m-%d") for d in requested_dates]
                if requested_dates
                else (
                    f"{(created_day + timedelta(days=_DEFAULT_SESSION_OFFSET_START)).strftime('%Y-%m-%d')}.."
                    f"{(created_day + timedelta(days=_DEFAULT_SESSION_OFFSET_END)).strftime('%Y-%m-%d')}"
                )
            ),
        }
        return created_day, payload, meta


def backfill_open_dates_from_online(
    local_db,
    online_engine,
    *,
    batch_size: int = 400,
    max_batches: int | None = None,
) -> dict[str, int]:
    """
    本地 ``open_date IS NULL`` 时，按 (asin, store_id, created_at, pid) 与线上 ``amazon_listing`` 对齐，
    用线上非空 ``open_date`` 的日历日（MAX）批量 UPDATE。不扩大 session 行集合，仅补字段。

    适用于：历史上同步时 listing 尚未填 open_date，或默认同步窗口未再扫到该 created_at 日。
    """
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        raise ValueError("Online DB config missing")

    updated_cells = 0
    batches_done = 0
    keys_processed = 0

    with online_engine.connect() as online_conn:
        while True:
            if max_batches is not None and batches_done >= max_batches:
                break
            key_rows = local_db.execute(
                text(
                    """
                    SELECT DISTINCT asin, created_at, COALESCE(pid, 0) AS pid, store_id
                    FROM daily_upload_asin_dates
                    WHERE open_date IS NULL AND created_at IS NOT NULL
                    ORDER BY asin, store_id, created_at, pid
                    LIMIT :lim
                    """
                ),
                {"lim": batch_size},
            ).fetchall()
            if not key_rows:
                break
            batches_done += 1
            keys_processed += len(key_rows)

            valid_specs: list[tuple[str, date, int, int]] = []
            for row in key_rows:
                asin_v, ca_v, pid_v, sid_v = row[0], row[1], row[2], row[3]
                ca_d = _coerce_sql_date(ca_v, field="created_at")
                if ca_d is None:
                    logger.warning(
                        "[DailyUploadDS] backfill skip row with unparseable created_at: asin=%r store_id=%s",
                        asin_v,
                        sid_v,
                    )
                    continue
                valid_specs.append(
                    (str(asin_v or "").strip(), ca_d, int(pid_v or 0), int(sid_v))
                )

            if not valid_specs:
                logger.warning(
                    "[DailyUploadDS] backfill: batch has no parseable keys; stop to avoid infinite loop"
                )
                break

            ph = ", ".join([f"(:a{i}, :s{i}, :c{i}, :p{i})" for i in range(len(valid_specs))])
            params: dict = {}
            for i, (asin_v, ca_d, pid_v, sid_v) in enumerate(valid_specs):
                params[f"a{i}"] = asin_v
                params[f"s{i}"] = sid_v
                params[f"c{i}"] = ca_d.strftime("%Y-%m-%d")
                params[f"p{i}"] = pid_v

            sql = (
                "SELECT al.asin, al.store_id, DATE(al.created_at) AS cd, COALESCE(al.pid, 0) AS p, "
                "MAX(DATE(al.open_date)) AS od "
                "FROM amazon_listing AS al "
                f"WHERE (al.asin, al.store_id, DATE(al.created_at), COALESCE(al.pid, 0)) IN ({ph}) "
                "AND al.open_date IS NOT NULL "
                "GROUP BY al.asin, al.store_id, DATE(al.created_at), COALESCE(al.pid, 0)"
            )
            mapping_rows = online_conn.execute(text(sql), params).fetchall()

            batch_updated = 0
            for mr in mapping_rows:
                od = _coerce_sql_date(mr[4], field="listing.open_date")
                if od is None:
                    continue
                cd = _coerce_sql_date(mr[2], field="listing.created_at")
                if cd is None:
                    continue
                res = local_db.execute(
                    text(
                        """
                        UPDATE daily_upload_asin_dates
                        SET open_date = :od
                        WHERE asin = :a AND store_id = :s AND created_at = :c
                          AND COALESCE(pid, 0) = :p AND open_date IS NULL
                        """
                    ),
                    {
                        "od": od,
                        "a": str(mr[0] or "").strip(),
                        "s": int(mr[1]),
                        "c": cd,
                        "p": int(mr[3] or 0),
                    },
                )
                batch_updated += int(getattr(res, "rowcount", 0) or 0)

            updated_cells += batch_updated
            if batch_updated == 0:
                logger.warning(
                    "[DailyUploadDS] backfill: no rows updated (online miss or keys mismatch); stopping after %s keys",
                    len(valid_specs),
                )
                break

    logger.info(
        "[DailyUploadDS] backfill_open_date batches=%s keys_touched=%s rows_updated=%s",
        batches_done,
        keys_processed,
        updated_cells,
    )
    return {
        "batches": batches_done,
        "distinct_keys_processed": keys_processed,
        "local_rows_updated": updated_cells,
    }


def sync_range(start_date: date, end_date: date, session_dates: list[date] | None = None) -> dict:
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        raise ValueError("Online DB config missing: set online_db_host, online_db_user, online_db_pwd, online_db_name in .env")

    init_db()
    online_engine = get_online_engine()
    local_db = SessionLocal()
    t0 = time.time()
    total_rows = 0
    total_inserted = 0
    total_updated = 0
    total_unchanged = 0
    days_scanned = 0
    days_with_rows = 0
    days_empty = 0
    days_failed = 0

    try:
        days = _iter_dates(start_date, end_date)
        days_scanned = len(days)
        if not days:
            out_empty = {
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "days_scanned": 0,
                "days_with_rows": 0,
                "days_empty": 0,
                "days_failed": 0,
                "rows_fetched": 0,
                "rows_inserted": 0,
                "rows_updated": 0,
                "rows_unchanged": 0,
                "elapsed_sec": round(time.time() - t0, 2),
            }
            logger.warning(
                "[DailyUploadDS] sync skipped: empty date range start=%s end=%s",
                start_date,
                end_date,
            )
            return out_empty

        worker_count = max(1, min(len(days), int(os.cpu_count() or 4), 8))
        online_cfg_ok = bool(
            str(settings.ONLINE_DB_HOST or "").strip()
            and str(settings.ONLINE_DB_USER or "").strip()
            and str(settings.ONLINE_DB_NAME or "").strip()
        )
        session_mode = (
            "explicit:" + ",".join(d.strftime("%Y-%m-%d") for d in session_dates)
            if session_dates
            else f"per_listing_day window [{_DEFAULT_SESSION_OFFSET_START},{_DEFAULT_SESSION_OFFSET_END}]"
        )
        logger.info(
            "[DailyUploadDS] sync start: start_date=%s end_date=%s days=%s workers=%s online_db_configured=%s sessions=%s",
            start_date,
            end_date,
            len(days),
            worker_count,
            online_cfg_ok,
            session_mode,
        )

        # 1) 并发拉取线上数据（按天）
        fetched_by_day: dict[date, tuple[list[dict], dict]] = {}
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="daily-upload") as ex:
            future_map = {ex.submit(_build_day_payload, online_engine, d, session_dates): d for d in days}
            done = 0
            for fut in as_completed(future_map):
                d = future_map[fut]
                done += 1
                try:
                    day, payload, meta = fut.result()
                    fetched_by_day[day] = (payload, meta)
                    logger.info(
                        "[DailyUploadDS] fetched %s/%s day=%s listing_rows=%s pairs=%s sessions_hits=%s missing=%s elapsed_sec=%.2f",
                        done,
                        len(days),
                        day.strftime("%Y-%m-%d"),
                        int(meta.get("listing_rows", 0)),
                        int(meta.get("pairs", 0)),
                        int(meta.get("sessions_pair_hits", 0)),
                        int(meta.get("sessions_missing", 0)),
                        float(meta.get("elapsed_sec", 0.0)),
                    )
                except Exception as exc:
                    days_failed += 1
                    logger.exception("[DailyUploadDS] fetch failed day=%s error=%s", d.strftime("%Y-%m-%d"), exc)

        t_after_fetch = time.time()
        logger.info(
            "[DailyUploadDS] fetch phase done: days_expected=%s days_fetched_ok=%s days_failed=%s elapsed_sec=%.2f",
            len(days),
            len(fetched_by_day),
            days_failed,
            t_after_fetch - t0,
        )

        # 2) 本地写入顺序提交（避免多线程写本地库锁冲突）
        t_write0 = time.time()
        for d in sorted(fetched_by_day.keys()):
            payload, meta = fetched_by_day[d]
            if not payload:
                days_empty += 1
                continue
            days_with_rows += 1
            day_str = d.strftime("%Y-%m-%d")
            t_day_write = time.time()
            total_rows += len(payload)
            write_stats = _upsert_local_rows(local_db, payload)
            local_db.commit()
            total_inserted += int(write_stats.get("inserted", 0))
            total_updated += int(write_stats.get("updated", 0))
            total_unchanged += int(write_stats.get("unchanged", 0))
            logger.info(
                "[DailyUploadDS] wrote created_day=%s session_day=%s listing_rows=%s inserted=%s updated=%s unchanged=%s elapsed_sec=%.2f",
                day_str,
                ("ALL" if not session_dates else ",".join([d.strftime("%Y-%m-%d") for d in session_dates])),
                len(payload),
                write_stats.get("inserted", 0),
                write_stats.get("updated", 0),
                write_stats.get("unchanged", 0),
                time.time() - t_day_write,
            )

        logger.info(
            "[DailyUploadDS] write phase done: days_with_listings=%s days_empty_payload=%s elapsed_sec=%.2f",
            days_with_rows,
            days_empty,
            time.time() - t_write0,
        )

        out = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "days_scanned": days_scanned,
            "days_with_rows": days_with_rows,
            "days_empty": days_empty,
            "days_failed": days_failed,
            "rows_fetched": total_rows,
            "rows_inserted": total_inserted,
            "rows_updated": total_updated,
            "rows_unchanged": total_unchanged,
            "elapsed_sec": round(time.time() - t0, 2),
            "elapsed_fetch_sec": round(t_after_fetch - t0, 2),
            "elapsed_write_sec": round(time.time() - t_write0, 2),
        }
        logger.info(
            "[DailyUploadDS] sync done: inserted=%s updated=%s unchanged=%s failed_days=%s total_sec=%s",
            total_inserted,
            total_updated,
            total_unchanged,
            days_failed,
            out["elapsed_sec"],
        )
        logger.info("[DailyUploadDS] sync summary: %s", out)
        return out
    finally:
        try:
            local_db.close()
        except Exception:
            pass


def main(argv: list[str]) -> int:
    setup_logging(level=logging.INFO)
    p = argparse.ArgumentParser(description="同步每日上新 ASIN 到 daily_upload_asin_dates")
    p.add_argument("--date", type=str, default="", help="单日 YYYY-MM-DD（与 start/end 二选一）")
    p.add_argument("--start-date", type=str, default="", help="开始日期 YYYY-MM-DD（可与 --end-date 同时省略则默认东八区最近 35 天）")
    p.add_argument("--end-date", type=str, default="", help="结束日期 YYYY-MM-DD（含）")
    p.add_argument(
        "--session-date",
        action="append",
        default=[],
        help="可选：sessions 统计日期 YYYY-MM-DD；可重复传参或逗号分隔。不传则同步 daily 表中该 ASIN 的所有 current_date。",
    )
    p.add_argument(
        "--backfill-open-date",
        action="store_true",
        help="在本次同步结束后，对本地 open_date IS NULL 的行按线上 amazon_listing 批量回填（不依赖 created_at 扫描窗口）。",
    )
    args = p.parse_args(argv)

    if args.date.strip():
        d = _parse_ymd(args.date)
        start_d = end_d = d
    else:
        has_start = bool(args.start_date.strip())
        has_end = bool(args.end_date.strip())
        if has_start ^ has_end:
            p.error("请同时指定 --start-date 与 --end-date，或两者皆省略以使用默认范围（东八区最近 35 天）")
        if not has_start and not has_end:
            init_db()
            ldb = SessionLocal()
            try:
                start_d, end_d = default_sync_listing_date_bounds(ldb)
            finally:
                ldb.close()
            logger.info(
                "[DailyUploadDS] 默认区间: start_date=%s end_date=%s",
                start_d.isoformat(),
                end_d.isoformat(),
            )
        else:
            start_d = _parse_ymd(args.start_date)
            end_d = _parse_ymd(args.end_date)

    try:
        raw_list = []
        for item in (args.session_date or []):
            raw_list.extend([p.strip() for p in str(item).replace("，", ",").split(",") if p.strip()])
        session_dates = sorted({_parse_ymd(x) for x in raw_list}) if raw_list else None
        sync_range(start_d, end_d, session_dates=session_dates)
        if args.backfill_open_date:
            init_db()
            ldb = SessionLocal()
            try:
                bf = backfill_open_dates_from_online(ldb, get_online_engine())
                ldb.commit()
                logger.info("[DailyUploadDS] --backfill-open-date: %s", bf)
            finally:
                ldb.close()
        return 0
    except Exception as e:
        logger.exception("[DailyUploadDS] sync failed: %s", e)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))