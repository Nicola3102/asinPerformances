"""
Group A: 从 online_db 拉取指定 week_no 的子 ASIN impression/cart，
解析父 ASIN；若父 ASIN 在本地 asin_performances(week_no,store_id) 已存在则跳过，
否则抓取该父 ASIN 下所有子 ASIN 的 search_query + sessions，并写入本地 group_A 表。
"""

import logging
import sys
import time
from datetime import date, datetime, timedelta

from sqlalchemy import func, text, tuple_
from sqlalchemy.dialects.mysql import insert as mysql_insert

from app.config import settings
from app.database import SessionLocal, init_db
from app.models import AsinPerformance
from app.models.group_a import GroupA
from app.online_engine import get_online_engine

logger = logging.getLogger(__name__)


def _chunks(xs: list, n: int):
    for i in range(0, len(xs), n):
        yield xs[i : i + n]


def _get_sync_date_range():
    """
    同步日期范围：DATE_START = 当周周日的日期，DATE_END = 当天+1。
    若当天本身为周日，则 DATE_START 即为当天；DATE_END 恒为当天+1。
    返回 (date_start_str, date_end_str) 格式 "YYYY-MM-DD"。
    """
    today = date.today()
    # 当周周日：Python weekday() 周一=0、周日=6，周日 = today - (weekday+1)%7 天
    days_since_sunday = (today.weekday() + 1) % 7
    sunday = today - timedelta(days=days_since_sunday)
    date_start_str = sunday.strftime("%Y-%m-%d")
    date_end = today + timedelta(days=1)
    date_end_str = date_end.strftime("%Y-%m-%d")
    return date_start_str, date_end_str


def _date_to_week_no(d: date):
    """单日对应的 week_no（周日为一周开始，第1周=当年第一个周日），返回 (week_no_str, week_no_int)。"""
    days_since_sunday = (d.weekday() + 1) % 7
    week_start = d - timedelta(days=days_since_sunday)
    year = week_start.year
    jan1 = date(year, 1, 1)
    first_sunday = jan1 + timedelta(days=(6 - jan1.weekday()) % 7)
    week_num = (week_start - first_sunday).days // 7 + 1
    week_no_str = f"{year}{week_num:02d}"
    try:
        week_no_int = int(week_no_str)
    except ValueError:
        week_no_int = None
    return (week_no_str, week_no_int)


def sync_group_a_impression(week_no: str | int) -> dict:
    """
    执行 Group A 同步。
    - **week_no**: online 库中的 week_no（通常为字符串，如 "202608"），这里统一按字符串传入 online 查询。
    返回：统计信息（抓取/跳过/写入数量）。
    """
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        raise ValueError("Online DB config missing: set online_db_host, online_db_user, online_db_pwd, online_db_name in .env")

    wk_str = str(week_no).strip().replace(",", "")
    if not wk_str.isdigit():
        raise ValueError("week_no 格式不合法")
    wk_int = int(wk_str)

    online_engine = get_online_engine()
    local_db = SessionLocal()

    t0 = time.time()
    fetched_children = 0
    skipped_parents_with_orders = 0
    inserted = 0
    updated = 0
    missing_parent_map = 0
    ambiguous_parent_map = 0
    missing_parent_id = 0
    parents_with_no_children = 0
    parents_with_no_search_data = 0
    online_sql_calls = 0
    migrated_to_asin_performances = 0

    logger.info("[GroupA] sync started: week_no=%s", wk_str)
    try:
        # 1) amazon_search: 一次性拉取本周高曝光子 asin 的 impression/cart + parent 映射。
        #    仅接受能唯一映射到单个 variation_id 的子 ASIN，避免用 MIN(variation_id) 误绑父体。
        t_step1 = time.time()
        min_impression_count = 5
        logger.info(
            "[GroupA] Step1 start: querying amazon_search + uniquely-resolved parent mapping for week_no=%s with impression_count > %s (single joined query)",
            wk_str,
            min_impression_count,
        )
        with online_engine.connect() as conn:
            t_query = time.time()
            rows = conn.execute(
                text(
                    "SELECT s.asin AS child_asin, s.store_id, "
                    "COALESCE(s.impression_count, 0) AS impression_count, "
                    "COALESCE(s.cart_count, 0) AS cart_count, "
                    "av.asin AS parent_asin, av.id AS parent_id, av.created_at AS parent_created_at, "
                    "COALESCE(lm.variation_count, 0) AS parent_variation_count "
                    "FROM amazon_search s "
                    "LEFT JOIN ("
                    "    SELECT al.asin, al.store_id, "
                    "           MIN(al.variation_id) AS variation_id, "
                    "           COUNT(DISTINCT al.variation_id) AS variation_count "
                    "    FROM amazon_listing al "
                    "    INNER JOIN ("
                    "        SELECT DISTINCT asin, store_id "
                    "        FROM amazon_search "
                    "        WHERE store_id IN (1,7,12,25) AND week_no = :wk "
                    "          AND COALESCE(impression_count, 0) > :min_impression_count "
                    "    ) s0 ON s0.asin = al.asin AND s0.store_id = al.store_id "
                    "    WHERE al.store_id IN (1,7,12,25) "
                    "    GROUP BY al.asin, al.store_id"
                    ") lm ON lm.asin = s.asin AND lm.store_id = s.store_id "
                    "LEFT JOIN amazon_variation av ON av.id = lm.variation_id AND lm.variation_count = 1 "
                    "WHERE s.store_id IN (1,7,12,25) AND s.week_no = :wk "
                    "  AND COALESCE(s.impression_count, 0) > :min_impression_count "
                    "ORDER BY COALESCE(s.impression_count, 0) DESC"
                ),
                {"wk": wk_str, "min_impression_count": min_impression_count},
            ).fetchall()
            online_sql_calls += 1
            logger.info("[GroupA] Step1 query returned in %.2fs, fetching %s rows...", time.time() - t_query, len(rows) if rows else 0)

        fetched_children = len(rows)
        logger.info(
            "[GroupA] Step1 amazon_search done: week_no=%s rows=%s elapsed=%.2fs",
            wk_str,
            fetched_children,
            time.time() - t_step1,
        )
        if not rows:
            return {
                "week_no": wk_int,
                "fetched_children": 0,
                "skipped_parents_with_orders": 0,
                "inserted": 0,
                "updated": 0,
                "missing_parent_map": 0,
                "ambiguous_parent_map": 0,
            }

        # 预先构建 (child_asin, store_id) -> (impression, cart)
        child_metrics = {(r[0], int(r[1]) if r[1] is not None else None): (int(r[2] or 0), int(r[3] or 0)) for r in rows}
        try:
            top5 = [(r[0], int(r[1]), int(r[2] or 0), int(r[3] or 0)) for r in rows[:5]]
            logger.info("[GroupA] Step1 top5 by impression: %s", top5)
        except Exception:
            pass

        # 2) 解析父 asin：直接消费 Step1 联表结果，在内存中去重并统计缺失映射
        child_pairs = [(r[0], int(r[1])) for r in rows if r[0] and r[1] is not None]
        if not child_pairs:
            return {
                "week_no": wk_int,
                "fetched_children": fetched_children,
                "skipped_parents_with_orders": 0,
                "inserted": 0,
                "updated": 0,
                "missing_parent_map": 0,
                "ambiguous_parent_map": 0,
            }

        seen_parent_key = set()
        mapped_child_keys = set()
        num_step2_rows = len(rows)
        STEP2_LOG_EVERY = 50000

        t_step2 = time.time()
        logger.info(
            "[GroupA] Step2 start: deduplicating parent mapping from %s joined rows in memory (log every %s rows)...",
            num_step2_rows,
            STEP2_LOG_EVERY,
        )
        all_parents = []
        for row_idx, r in enumerate(rows, start=1):
            child_asin = r[0]
            sid = int(r[1]) if r[1] is not None else None
            parent_asin = r[4]
            parent_id = int(r[5]) if r[5] is not None else None
            parent_created_at = r[6]
            variation_count = int(r[7] or 0)

            if child_asin and sid is not None:
                ckey = (child_asin, sid)
                if variation_count > 1:
                    ambiguous_parent_map += 1
                elif parent_asin or parent_id is not None:
                    mapped_child_keys.add(ckey)

            if variation_count > 1:
                pass
            elif parent_id is None:
                if child_asin and sid is not None:
                    missing_parent_id += 1
            elif parent_asin and sid is not None:
                pkey = (parent_asin, sid)
                if pkey not in seen_parent_key:
                    seen_parent_key.add(pkey)
                    all_parents.append((parent_asin, sid, parent_id, parent_created_at))

            if row_idx == 1 or row_idx % STEP2_LOG_EVERY == 0 or row_idx == num_step2_rows:
                logger.info(
                    "[GroupA] Step2 progress: rows=%s/%s mapped_children=%s unique_parents=%s ambiguous_parent_map=%s missing_parent_id=%s elapsed=%.2fs",
                    row_idx,
                    num_step2_rows,
                    len(mapped_child_keys),
                    len(all_parents),
                    ambiguous_parent_map,
                    missing_parent_id,
                    time.time() - t_step2,
                )

        missing_parent_map = max(0, len(child_pairs) - len(mapped_child_keys) - ambiguous_parent_map)
        logger.info(
            "[GroupA] Step2 parent mapping done: pairs=%s mapped_children=%s unique_parents=%s ambiguous_map=%s missing_map=%s missing_parent_id=%s elapsed=%.2fs",
            len(child_pairs),
            len(mapped_child_keys),
            len(all_parents),
            ambiguous_parent_map,
            missing_parent_map,
            missing_parent_id,
            time.time() - t_step2,
        )

        if not all_parents:
            return {
                "week_no": wk_int,
                "fetched_children": fetched_children,
                "skipped_parents_with_orders": 0,
                "inserted": 0,
                "updated": 0,
                "missing_parent_map": missing_parent_map,
                "ambiguous_parent_map": ambiguous_parent_map,
            }

        # 3) 先加载本周 asin_performances 中“已存在且 parent_order_total > 0”的 (parent_asin, store_id)，再用集合过滤出待处理父 ASIN
        logger.info(
            "[GroupA] Step3 start: loading (parent_asin, store_id) from asin_performances with parent_order_total > 0 for week_no=%s...",
            wk_str,
        )
        t_step3_load = time.time()
        existing_parent_keys = set(
            local_db.query(AsinPerformance.parent_asin, AsinPerformance.store_id)
            .filter(
                AsinPerformance.week_no == wk_int,
                AsinPerformance.store_id.isnot(None),
                AsinPerformance.parent_asin.isnot(None),
                AsinPerformance.parent_asin != "",
            )
            .group_by(AsinPerformance.parent_asin, AsinPerformance.store_id)
            .having(func.max(AsinPerformance.parent_order_total) > 0)
            .all()
        )
        existing_parent_keys = {(r[0], int(r[1])) for r in existing_parent_keys if r[0] and r[1] is not None}
        logger.info(
            "[GroupA] Step3 existing_parent_keys loaded (parent_order_total > 0): %s in %.2fs",
            len(existing_parent_keys),
            time.time() - t_step3_load,
        )

        # 3.1) 已在 asin_performances 中存在且 parent_order_total > 0 的父 ASIN，从 group_A 业务视角视为“已迁移”
        t_step3_mark = time.time()
        if existing_parent_keys:
            migrated_to_asin_performances = (
                local_db.query(GroupA)
                .filter(
                    GroupA.week_no == wk_int,
                    tuple_(GroupA.parent_asin, GroupA.store_id).in_(list(existing_parent_keys)),
                    GroupA.migrated_to_asin_performances == False,
                )
                .update(
                    {"migrated_to_asin_performances": True},
                    synchronize_session=False,
                )
            )
            local_db.commit()
        logger.info(
            "[GroupA] Step3 mark migrated parents: week_no=%s existing_in_asin_perf_with_orders=%s marked=%s elapsed=%.2fs",
            wk_str,
            len(existing_parent_keys),
            migrated_to_asin_performances,
            time.time() - t_step3_mark,
        )

        t_step3_filter = time.time()
        final_parents = [(pa, sid, pid, pc) for (pa, sid, pid, pc) in all_parents if (pa, sid) not in existing_parent_keys and pid is not None]
        skipped_parents_with_orders = len(all_parents) - len(final_parents)
        logger.info(
            "[GroupA] Step3 filter existing parents: week_no=%s parents_total=%s skipped_parents_with_orders=%s to_process=%s existing_in_asin_perf_with_orders=%s elapsed=%.2fs",
            wk_str,
            len(all_parents),
            skipped_parents_with_orders,
            len(final_parents),
            len(existing_parent_keys),
            time.time() - t_step3_filter,
        )
        if not final_parents:
            logger.warning(
                "[GroupA] No data will be written to group_A: all parents for week_no=%s already exist in asin_performances with parent_order_total > 0 (count=%s). "
                "group_A only stores parents NOT in asin_performances with parent_order_total > 0 for this week.",
                wk_str,
                len(existing_parent_keys),
            )
            return {
                "week_no": wk_int,
                "fetched_children": fetched_children,
                "skipped_parents_with_orders": skipped_parents_with_orders,
                "inserted": 0,
                "updated": 0,
                "missing_parent_map": missing_parent_map,
                "ambiguous_parent_map": ambiguous_parent_map,
                "parents_no_children": parents_with_no_children,
                "parents_no_search_data": parents_with_no_search_data,
            }

        # 4/5) 批量处理：按 parent 批量拉 listing->children，批量拉 sessions/search_data，批量 upsert 到 group_A
        parent_batch_size = 50
        num_batches = (len(final_parents) + parent_batch_size - 1) // parent_batch_size
        logger.info(
            "[GroupA] Step4+5 start batched fetch/upsert: parents=%s batches=%s (parent_batch_size=%s, sessions/search_data use single joined query per batch)",
            len(final_parents),
            num_batches,
            parent_batch_size,
        )
        with online_engine.connect() as conn:
            for bidx, batch_parents in enumerate(_chunks(final_parents, parent_batch_size), start=1):
                t_batch = time.time()
                logger.info("[GroupA] batch %s/%s start: parents=%s", bidx, num_batches, len(batch_parents))

                # 4.1 拉该批父下所有子（同时带 parent_asin/created_at）
                t_listing = time.time()
                placeholders = ", ".join([f"(:p{i}, :s{i})" for i in range(len(batch_parents))])
                params = {}
                parent_meta = {}
                for i, (pa, sid, pid, pc) in enumerate(batch_parents):
                    params[f"p{i}"] = pid
                    params[f"s{i}"] = sid
                    parent_meta[(pid, sid)] = (pa, pc)
                listing_sql = (
                    "SELECT DISTINCT al.store_id, al.variation_id AS parent_id, al.asin AS child_asin "
                    "FROM amazon_listing al "
                    f"WHERE (al.variation_id, al.store_id) IN ({placeholders})"
                )
                listing_rows = conn.execute(text(listing_sql), params).fetchall()
                online_sql_calls += 1
                logger.info(
                    "[GroupA] batch %s/%s listing fetched: rows=%s elapsed=%.2fs",
                    bidx,
                    num_batches,
                    len(listing_rows),
                    time.time() - t_listing,
                )

                if not listing_rows:
                    parents_with_no_children += len(batch_parents)
                    logger.info(
                        "[GroupA] batch %s/%s skip: parents=%s reason=no_children elapsed=%.2fs",
                        bidx,
                        (len(final_parents) + parent_batch_size - 1) // parent_batch_size,
                        len(batch_parents),
                        time.time() - t_batch,
                    )
                    continue

                child_to_parent = {}  # (child_asin, store_id) -> (parent_asin, parent_created_at)
                child_pairs_all = []
                for r in listing_rows:
                    sid = int(r[0]) if r[0] is not None else None
                    pid = int(r[1]) if r[1] is not None else None
                    ca = r[2]
                    parent_info = parent_meta.get((pid, sid))
                    if sid is None or not ca or not parent_info:
                        continue
                    pa, pc = parent_info
                    key = (ca, sid)
                    if key in child_to_parent:
                        continue
                    child_to_parent[key] = (pa, pc)
                    child_pairs_all.append(key)

                if not child_pairs_all:
                    logger.info(
                        "[GroupA] batch %s/%s skip: no valid child ASINs found for current parent batch elapsed=%.2fs",
                        bidx,
                        (len(final_parents) + parent_batch_size - 1) // parent_batch_size,
                        time.time() - t_batch,
                    )
                    continue

                # 4.2 sessions：按父批次一次 JOIN 拉取，避免按 child keys 多次 round-trip
                t_sessions = time.time()
                sessions_map = {}
                session_sql = (
                    "SELECT t.asin, t.store_id, COALESCE(t.sessions, 0) AS sessions "
                    "FROM amazon_sales_traffic t "
                    "INNER JOIN ("
                    "    SELECT DISTINCT al.asin, al.store_id "
                    "    FROM amazon_listing al "
                    f"    WHERE (al.variation_id, al.store_id) IN ({placeholders})"
                    ") scope ON scope.asin = t.asin AND scope.store_id = t.store_id "
                    "WHERE t.week_no = :wk"
                )
                trows = conn.execute(text(session_sql), {**params, "wk": wk_str}).fetchall()
                online_sql_calls += 1
                for r in trows:
                    sessions_map[(r[0], int(r[1]) if r[1] is not None else None)] = int(r[2] or 0)
                logger.info(
                    "[GroupA] batch %s/%s sessions fetched: keys=%s hits=%s queries=%s elapsed=%.2fs",
                    bidx,
                    num_batches,
                    len(child_pairs_all),
                    len(sessions_map),
                    1,
                    time.time() - t_sessions,
                )

                # 4.3 search_data：按父批次一次 JOIN 拉取，避免按 child keys 多次 round-trip
                t_search = time.time()
                search_sql = (
                    "SELECT sd.asin, sd.store_id, sd.search_query, sd.search_query_volume, "
                    "sd.impression_count, sd.cart_count, sd.total_impression_count, sd.click_count, sd.total_click_count "
                    "FROM amazon_search_data sd "
                    "INNER JOIN ("
                    "    SELECT DISTINCT al.asin, al.store_id "
                    "    FROM amazon_listing al "
                    f"    WHERE (al.variation_id, al.store_id) IN ({placeholders})"
                    ") scope ON scope.asin = sd.asin AND scope.store_id = sd.store_id "
                    "WHERE sd.week_no = :wk"
                )
                search_rows = conn.execute(text(search_sql), {**params, "wk": wk_str}).fetchall()
                online_sql_calls += 1
                logger.info(
                    "[GroupA] batch %s/%s search_data fetched: child_keys=%s rows=%s queries=%s elapsed=%.2fs",
                    bidx,
                    num_batches,
                    len(child_pairs_all),
                    len(search_rows),
                    1,
                    time.time() - t_search,
                )

                if not search_rows:
                    parents_with_no_search_data += len(batch_parents)
                    logger.info(
                        "[GroupA] batch %s/%s skip: search_data empty for children=%s elapsed=%.2fs",
                        bidx,
                        (len(final_parents) + parent_batch_size - 1) // parent_batch_size,
                        len(child_pairs_all),
                        time.time() - t_batch,
                    )
                    continue

                # 5) 批量 upsert 到本地 group_A（减少逐行 SELECT）
                values = []
                for r in search_rows:
                    child_asin = r[0]
                    sid = int(r[1]) if r[1] is not None else None
                    if not child_asin or sid is None:
                        continue
                    parent_info = child_to_parent.get((child_asin, sid))
                    if not parent_info:
                        continue
                    parent_asin, parent_created_at = parent_info
                    if (parent_asin, sid) in existing_parent_keys:
                        continue
                    imp_child, cart_child = child_metrics.get((child_asin, sid), (0, 0))
                    sess = sessions_map.get((child_asin, sid), 0)
                    sq = (r[2] or "").strip()  # 归一化，避免 UNIQUE 中 NULL 导致重复插入
                    values.append(
                        {
                            "store_id": sid,
                            "parent_asin": parent_asin,
                            "parent_asin_created_at": parent_created_at,
                            "child_asin": child_asin,
                            "child_impression_count": imp_child,
                            "child_cart": cart_child,
                            "child_session_count": sess,
                            "week_no": wk_int,
                            "search_query": sq,
                            "search_query_volume": r[3],
                            "search_query_impression_count": r[4],
                            "search_query_cart_count": r[5],
                            "search_query_total_impression_count": r[6],
                            "search_query_click_count": r[7],
                            "search_query_total_click_count": r[8],
                            "migrated_to_asin_performances": False,
                        }
                    )

                if not values:
                    logger.info(
                        "[GroupA] batch %s/%s: no rows to upsert after filtering elapsed=%.2fs",
                        bidx,
                        (len(final_parents) + parent_batch_size - 1) // parent_batch_size,
                        time.time() - t_batch,
                    )
                    continue

                # 5) 写入 group_A：先尝试批量 upsert；失败则回退为逐条 insert 并记录首条错误原因
                t_upsert = time.time()
                try:
                    stmt = mysql_insert(GroupA).values(values)
                    # on_duplicate_key_update 这里必须传字段名字符串，不能用列对象做 **kwargs
                    update_cols = {
                        "parent_asin_created_at": stmt.inserted.parent_asin_created_at,
                        "child_impression_count": stmt.inserted.child_impression_count,
                        "child_cart": stmt.inserted.child_cart,
                        "child_session_count": stmt.inserted.child_session_count,
                        "search_query_volume": stmt.inserted.search_query_volume,
                        "search_query_impression_count": stmt.inserted.search_query_impression_count,
                        "search_query_cart_count": stmt.inserted.search_query_cart_count,
                        "search_query_total_impression_count": stmt.inserted.search_query_total_impression_count,
                        "search_query_click_count": stmt.inserted.search_query_click_count,
                        "search_query_total_click_count": stmt.inserted.search_query_total_click_count,
                        "migrated_to_asin_performances": stmt.inserted.migrated_to_asin_performances,
                    }
                    res = local_db.execute(stmt.on_duplicate_key_update(**update_cols))
                    affected = int(getattr(res, "rowcount", 0) or 0)
                    local_db.commit()
                    inserted += max(0, min(len(values), affected))
                    updated += max(0, affected - min(len(values), affected))
                    logger.info(
                        "[GroupA] batch %s/%s committed to group_A: parents=%s listing_rows=%s children_filtered=%s search_rows=%s upsert_rows=%s affected=%s upsert_elapsed=%.2fs total_elapsed=%.2fs",
                        bidx,
                        (len(final_parents) + parent_batch_size - 1) // parent_batch_size,
                        len(batch_parents),
                        len(listing_rows),
                        len(child_pairs_all),
                        len(search_rows),
                        len(values),
                        affected,
                        time.time() - t_upsert,
                        time.time() - t_batch,
                    )
                except Exception as e:
                    logger.exception(
                        "[GroupA] batch %s/%s bulk upsert failed: %s; falling back to row-by-row insert",
                        bidx,
                        (len(final_parents) + parent_batch_size - 1) // parent_batch_size,
                        e,
                    )
                    local_db.rollback()
                    ins_batch, upd_batch = 0, 0
                    for vi, v in enumerate(values):
                        try:
                            existing = (
                                local_db.query(GroupA)
                                .filter(
                                    GroupA.store_id == v["store_id"],
                                    GroupA.parent_asin == v["parent_asin"],
                                    GroupA.child_asin == v["child_asin"],
                                    GroupA.week_no == v["week_no"],
                                    GroupA.search_query == (v.get("search_query") or ""),
                                )
                                .first()
                            )
                            if existing is None:
                                local_db.add(GroupA(**v))
                                ins_batch += 1
                            else:
                                for k, val in v.items():
                                    if k != "id":
                                        setattr(existing, k, val)
                                upd_batch += 1
                        except Exception as row_err:
                            logger.warning(
                                "[GroupA] row %s/%s insert failed: store_id=%s parent_asin=%s child_asin=%s week_no=%s search_query=%s error=%s",
                                vi + 1,
                                len(values),
                                v.get("store_id"),
                                v.get("parent_asin"),
                                v.get("child_asin"),
                                v.get("week_no"),
                                (v.get("search_query") or "")[:50],
                                row_err,
                            )
                    try:
                        local_db.commit()
                    except Exception as commit_err:
                        logger.exception("[GroupA] fallback commit failed: %s", commit_err)
                        local_db.rollback()
                    inserted += ins_batch
                    updated += upd_batch
                    logger.info(
                        "[GroupA] batch %s/%s fallback committed to group_A: inserted=%s updated=%s upsert_elapsed=%.2fs total_elapsed=%.2fs",
                        bidx,
                        (len(final_parents) + parent_batch_size - 1) // parent_batch_size,
                        ins_batch,
                        upd_batch,
                        time.time() - t_upsert,
                        time.time() - t_batch,
                    )

        total_elapsed = time.time() - t0
        logger.info(
            "[GroupA] DONE: week_no=%s fetched_children=%s ambiguous_parent_map=%s missing_parent_map=%s skipped_parents_with_orders=%s parents_no_children=%s parents_no_search_data=%s inserted=%s updated=%s online_sql_calls=%s elapsed=%.2fs",
            wk_str,
            fetched_children,
            ambiguous_parent_map,
            missing_parent_map,
            skipped_parents_with_orders,
            parents_with_no_children,
            parents_with_no_search_data,
            inserted,
            updated,
            online_sql_calls,
            total_elapsed,
        )
        logger.info("[GroupA] sync finished: week_no=%s total_elapsed=%.2fs inserted=%s updated=%s", wk_str, total_elapsed, inserted, updated)
        return {
            "week_no": wk_int,
            "fetched_children": fetched_children,
            "skipped_parents_with_orders": skipped_parents_with_orders,
            "inserted": inserted,
            "updated": updated,
            "missing_parent_map": missing_parent_map,
            "ambiguous_parent_map": ambiguous_parent_map,
            "parents_no_children": parents_with_no_children,
            "parents_no_search_data": parents_with_no_search_data,
            "online_sql_calls": online_sql_calls,
            "migrated_to_asin_performances": migrated_to_asin_performances,
        }
    finally:
        try:
            local_db.close()
        except Exception:
            pass


def _main(argv: list[str]) -> int:
    from app.logging_config import setup_logging
    setup_logging(level=logging.INFO)
    init_db()

    week_arg = argv[1] if len(argv) > 1 else ""
    if week_arg and week_arg.strip():
        wk_str = week_arg.strip().replace(",", "")
    else:
        date_start_str, date_end_str = _get_sync_date_range()
        date_end_d = datetime.strptime(date_end_str, "%Y-%m-%d").date()
        reference_date = date_end_d - timedelta(days=1)
        wk_str, _ = _date_to_week_no(reference_date)
        logger.info(
            "[GroupA] auto week_no by date_end-1: date_start=%s date_end=%s reference_date=%s -> week_no=%s",
            date_start_str,
            date_end_str,
            reference_date.strftime("%Y-%m-%d"),
            wk_str,
        )

    out = sync_group_a_impression(wk_str)
    logger.info("[GroupA] result: %s", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv))

