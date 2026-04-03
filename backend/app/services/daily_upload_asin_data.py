"""
从线上库拉取每日上新 ASIN，并写入本地 daily_upload_asin_dates。

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

用法（在 backend 目录下）：
  python3.11 -m app.services.daily_upload_asin_data
      # 省略起止日时：开始 = daily_upload_asin_dates.MAX(session_date) - 1 日，结束 = 今天 + 1 日；表空则开始=今天
  python3.11 -m app.services.daily_upload_asin_data --start-date 2026-02-20 --end-date 2026-03-31
  python3.11 -m app.services.daily_upload_asin_data --date 2026-03-31
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
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

logger = logging.getLogger(__name__)

# 相对「上新日历日」拉取 amazon_sales_and_traffic_daily.current_date 的区间（含端点）
_DEFAULT_SESSION_OFFSET_START = -1
_DEFAULT_SESSION_OFFSET_END = 30


def _parse_ymd(s: str) -> date:
    s = str(s or "").strip()
    return datetime.strptime(s, "%Y-%m-%d").date()


def default_sync_listing_date_bounds(local_db) -> tuple[date, date]:
    """
    默认 listing 日区间（created_at 按日扫描）：
    - 结束：当前日期 + 1 日（与命令行习惯一致，含「明天」边界）
    - 开始：本地表 daily_upload_asin_dates 中 MAX(session_date) 的前一日；表无数据时为今天
    - 若推算开始晚于结束，则钳制为结束日
    """
    row = local_db.execute(text("SELECT MAX(session_date) AS m FROM daily_upload_asin_dates")).fetchone()
    max_sd = row[0] if row else None
    end_d = date.today() + timedelta(days=1)
    if max_sd is None:
        start_d = date.today()
    else:
        if isinstance(max_sd, datetime):
            max_sd = max_sd.date()
        elif not isinstance(max_sd, date):
            max_sd = _parse_ymd(str(max_sd)[:10])
        start_d = max_sd - timedelta(days=1)
    if start_d > end_d:
        start_d = end_d
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
        "[DailyUpload] variation parent_asin map: variation_ids=%s mapped=%s elapsed_sec=%.2f",
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
            "[DailyUpload] sessions source=amazon_sales_and_traffic_daily day=%s pairs=%s hits=%s elapsed_sec=%.2f",
            day_str,
            len(pairs),
            len(out),
            time.time() - t0,
        )
        return out
    except Exception as exc:
        logger.warning(
            "[DailyUpload] amazon_sales_and_traffic_daily not available or query failed (%s), fallback to amazon_sales_and_traffic then amazon_sales_traffic",
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
            "[DailyUpload] sessions source=amazon_sales_and_traffic day=%s pairs=%s hits=%s elapsed_sec=%.2f",
            day_str,
            len(pairs),
            len(out),
            time.time() - t_mid,
        )
        return out
    except Exception as exc:
        logger.warning(
            "[DailyUpload] amazon_sales_and_traffic not available or query failed (%s), fallback to amazon_sales_traffic week sessions",
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
        "[DailyUpload] sessions source=amazon_sales_traffic week_no=%s day=%s pairs=%s hits=%s elapsed_sec=%.2f",
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
        "[DailyUpload] sessions source=amazon_sales_and_traffic_daily pairs=%s dates_filter=%s pair_hits=%s elapsed_sec=%.2f",
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
            "SELECT asin, created_at, COALESCE(pid, 0) AS pid, store_id, session_date, COALESCE(sessions, 0) "
            "FROM daily_upload_asin_dates "
            f"WHERE (asin, created_at, COALESCE(pid, 0), store_id, session_date) IN ({ph})"
        )
        for r in local_db.execute(text(q), params).fetchall():
            existing[(str(r[0]), r[1], int(r[2] or 0), int(r[3]), r[4])] = {
                "sessions": int(r[5] or 0),
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
        changed = int(prev.get("sessions") or 0) != int(r.get("sessions") or 0)
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
        )
        res = local_db.execute(stmt)
        updated = int(getattr(res, "rowcount", 0) or 0)

    logger.info(
        "[DailyUpload] local upsert: payload=%s existing=%s inserts=%s updates=%s unchanged=%s inserted=%s updated=%s elapsed_sec=%.2f",
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
                "SELECT asin, pid, variation_id, store_id, status, DATE(created_at) AS created_at "
                "FROM amazon_listing "
                "WHERE created_at >= :d0 AND created_at < :d1 "
                "  AND asin IS NOT NULL AND asin <> ''"
            ),
            {"d0": day_str, "d1": (created_day + timedelta(days=1)).strftime("%Y-%m-%d")},
        ).fetchall()
        if not listing_rows:
            return created_day, [], {"listing_rows": 0, "pairs": 0, "sessions_pair_hits": 0, "sessions_missing": 0, "elapsed_sec": time.time() - t0}

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
                "[DailyUpload] amazon_sales_and_traffic_daily query failed (%s); sessions empty for created_day=%s",
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
                "[DailyUpload] created_day=%s: %s amazon_listing rows, %s (asin,store_id) pairs, "
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
            created_at = r[5]  # DATE(created_at)
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
        worker_count = max(1, min(len(days), int(os.cpu_count() or 4), 8))
        logger.info(
            "[DailyUpload] sync start: start_date=%s end_date=%s days=%s workers=%s",
            start_date,
            end_date,
            len(days),
            worker_count,
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
                        "[DailyUpload] fetched %s/%s day=%s listing_rows=%s pairs=%s sessions_hits=%s missing=%s elapsed_sec=%.2f",
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
                    logger.exception("[DailyUpload] fetch failed day=%s error=%s", d.strftime("%Y-%m-%d"), exc)

        # 2) 本地写入顺序提交（避免多线程写本地库锁冲突）
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
                "[DailyUpload] wrote created_day=%s session_day=%s listing_rows=%s inserted=%s updated=%s unchanged=%s elapsed_sec=%.2f",
                day_str,
                ("ALL" if not session_dates else ",".join([d.strftime("%Y-%m-%d") for d in session_dates])),
                len(payload),
                write_stats.get("inserted", 0),
                write_stats.get("updated", 0),
                write_stats.get("unchanged", 0),
                time.time() - t_day_write,
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
        }
        logger.info("[DailyUpload] sync done: %s", out)
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
    p.add_argument("--start-date", type=str, default="", help="开始日期 YYYY-MM-DD（可与 --end-date 同时省略则用表 MAX(session_date)-1～今天+1）")
    p.add_argument("--end-date", type=str, default="", help="结束日期 YYYY-MM-DD（含）")
    p.add_argument(
        "--session-date",
        action="append",
        default=[],
        help="可选：sessions 统计日期 YYYY-MM-DD；可重复传参或逗号分隔。不传则同步 daily 表中该 ASIN 的所有 current_date。",
    )
    args = p.parse_args(argv)

    if args.date.strip():
        d = _parse_ymd(args.date)
        start_d = end_d = d
    else:
        has_start = bool(args.start_date.strip())
        has_end = bool(args.end_date.strip())
        if has_start ^ has_end:
            p.error("请同时指定 --start-date 与 --end-date，或两者皆省略以使用默认范围（MAX(session_date)-1 ～ 今天+1）")
        if not has_start and not has_end:
            init_db()
            ldb = SessionLocal()
            try:
                start_d, end_d = default_sync_listing_date_bounds(ldb)
            finally:
                ldb.close()
            logger.info(
                "[DailyUpload] 默认区间: start_date=%s end_date=%s",
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
        return 0
    except Exception as e:
        logger.exception("[DailyUpload] sync failed: %s", e)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

