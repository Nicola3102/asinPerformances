"""
从 online_db 分步执行 SQL：先写入有订单的子 ASIN 及其指标，再按父 ASIN 分批从本地读 parent_asin、从远程查子 ASIN 的 traffic/search 数据并写入。
"""
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta
import time
import random

import pymysql
from sqlalchemy import or_, text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

from app.config import settings
from app.database import SessionLocal, init_db
from app.online_engine import get_online_engine
from app.models import AsinPerformance


def _log_mysql_exception(context: str, exc: BaseException) -> None:
    """把 pymysql / MySQL 的 errno、message 打全（docker logs 截断时便于对照）。"""
    parts = [context, f"{type(exc).__name__}: {exc!s}"]
    orig = getattr(exc, "orig", None)
    if orig is not None:
        parts.append(f"orig={orig!s}")
        oa = getattr(orig, "args", None)
        if oa:
            parts.append(f"orig.args={oa!r}")
    ea = getattr(exc, "args", None)
    if ea:
        parts.append(f"exc.args={ea!r}")
    logger.error(" | ".join(parts), exc_info=True)


def _is_retryable_db_error(exc: Exception) -> bool:
    """MySQL 死锁/锁等待超时判定：1213 deadlock, 1205 lock wait timeout。"""
    msg = str(exc).lower()
    if "deadlock found" in msg or "lock wait timeout" in msg:
        return True
    code = None
    orig = getattr(exc, "orig", None)
    if orig is not None:
        try:
            if isinstance(getattr(orig, "args", None), tuple) and orig.args:
                code = int(orig.args[0])
        except Exception:
            code = None
    try:
        if isinstance(getattr(exc, "args", None), tuple) and exc.args:
            code = int(exc.args[0])
    except Exception:
        pass
    return code in (1205, 1213)


def _run_with_deadlock_retry(local_db: Session, op_name: str, fn, max_retries: int = 4):
    """执行写库函数，遇到死锁/锁等待超时自动 rollback + 退避重试。"""
    attempt = 0
    while True:
        attempt += 1
        try:
            return fn()
        except Exception as exc:
            if not _is_retryable_db_error(exc) or attempt > max_retries:
                raise
            try:
                local_db.rollback()
            except Exception:
                pass
            sleep_s = min(3.0, 0.2 * (2 ** (attempt - 1)) + random.uniform(0.0, 0.2))
            logger.warning(
                "%s retry due to deadlock/lock-timeout: attempt=%s/%s sleep=%.2fs error=%s",
                op_name,
                attempt,
                max_retries,
                sleep_s,
                exc,
            )
            time.sleep(sleep_s)


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


# 店铺（与 online 库各表索引匹配）
STORE_IDS = "(1, 7,12,25)"

# 依赖 online 库各表已有索引（如 store_id, week_no, purchase_utc_date 等）以加速执行
# 第一步拆为：1) 轻量核心查询只取有订单子 ASIN+父 ASIN 信息；2) 分批查 traffic/search/search_data 并合并，避免单次大 JOIN 超时
# week_no 以 date_end 减 1 天为参考日计算，保证 02-21～02-22 同步写入 202607（02-15～02-21 所在周）
STEP1_CORE_SQL = """
WITH
-- 单周：参考日 = date_end - 1，与 MySQL WEEK(...,0) 一致（周日为一周开始，第1周=当年第一个周日）
target_weeks AS (
    SELECT CONCAT(YEAR(:reference_date), LPAD(GREATEST(1, WEEK(:reference_date, 0)), 2, '0')) AS week_no
),
-- 先取本期有订单的 (asin, store_id)，再查 listing，避免全表扫描 amazon_listing
ordered_child_raw AS (
    SELECT asin, store_id, purchase_utc_date, order_id
    FROM order_item
    WHERE store_id IN (1, 7,12,25) AND purchase_utc_date BETWEEN :date_start AND :date_end
),
unique_listing AS (
    SELECT al.store_id, al.asin, MIN(al.variation_id) AS variation_id
    FROM amazon_listing al
    INNER JOIN (SELECT DISTINCT asin, store_id FROM ordered_child_raw) oc ON al.asin = oc.asin AND al.store_id = oc.store_id
    WHERE al.store_id IN (1, 7,12,25)
    GROUP BY al.store_id, al.asin
),
-- 本批同步统一使用 reference_date 所在周为 week_no，按 (asin, store_id) 聚合订单数并收集 order_id（逗号分隔）
ordered_child AS (
    SELECT asin, store_id,
        CONCAT(YEAR(:reference_date), LPAD(GREATEST(1, WEEK(:reference_date, 0)), 2, '0')) AS week_no,
        COUNT(DISTINCT order_id) AS order_num,
        GROUP_CONCAT(DISTINCT order_id ORDER BY order_id SEPARATOR ',') AS order_ids
    FROM ordered_child_raw
    GROUP BY asin, store_id
),
ordered_child_with_parent AS (
    SELECT oc.asin, oc.store_id, oc.week_no, oc.order_num, oc.order_ids, ul.variation_id AS parent_id
    FROM ordered_child oc
    INNER JOIN unique_listing ul ON oc.asin = ul.asin AND oc.store_id = ul.store_id
),
parent_order_total_agg AS (
    SELECT parent_id, SUM(order_num) AS parent_order_total
    FROM ordered_child_with_parent
    GROUP BY parent_id
),
parent_asin_map AS (
    SELECT av.id AS parent_id, av.asin AS parent_asin, av.created_at AS parent_asin_create_at
    FROM amazon_variation av
    INNER JOIN (SELECT DISTINCT parent_id FROM ordered_child_with_parent) vc ON av.id = vc.parent_id
)
SELECT
    oc.asin AS child_asin,
    oc.store_id,
    pam.parent_asin,
    pam.parent_asin_create_at,
    pota.parent_order_total,
    oc.order_num,
    oc.order_ids,
    oc.week_no
FROM ordered_child_with_parent oc
INNER JOIN parent_asin_map pam ON oc.parent_id = pam.parent_id
INNER JOIN parent_order_total_agg pota ON oc.parent_id = pota.parent_id
ORDER BY pota.parent_order_total DESC, oc.asin
"""


def _get_active_asins(online_conn, asins: list) -> set:
    """
    在 amazon_listing 中，若同一 asin 存在任一条 status 非 'active'，则认定该 asin 为非 active。
    仅当该 asin 在所有 listing 记录中均为 status='active' 时才视为 active。返回 active 的 asin 集合。
    """
    if not asins:
        return set()
    active_set = set()
    batch = 500
    for i in range(0, len(asins), batch):
        chunk = list(dict.fromkeys(asins[i : i + batch]))
        placeholders = ", ".join([f"(:a{j})" for j in range(len(chunk))])
        params = {f"a{j}": a for j, a in enumerate(chunk)}
        try:
            # 仅保留：在 listing 中且所有记录的 status 均为 'active' 的 asin
            rows = online_conn.execute(
                text(
                    f"SELECT asin FROM amazon_listing WHERE store_id IN (1, 7,12,25) AND asin IN ({placeholders}) GROUP BY asin "
                    "HAVING SUM(CASE WHEN COALESCE(status, '') != 'active' THEN 1 ELSE 0 END) = 0"
                ),
                params,
            ).fetchall()
            for r in rows:
                active_set.add(r[0])
        except Exception:
            pass
    return active_set


def _step1_attach_metrics(online_conn, core_rows: list, active_asins: set, batch_size: int = 200) -> list:
    """
    对 Step1 核心结果 core_rows（每行 child_asin, store_id, parent_asin, parent_asin_create_at, parent_order_total, order_num, order_ids, week_no）
    分批查 amazon_search / amazon_sales_traffic / amazon_search_data，合并为 17 列行（含 order_id），且仅保留 impression 或 session 非 0 的行。
    """
    if not core_rows:
        return []
    out = []
    n_has_order = 0
    n_has_traffic = 0
    n_skipped_no_order_zero_traffic_not_active = 0
    n_keys_with_search_data = 0
    n_keys_without_search_data = 0
    for i in range(0, len(core_rows), batch_size):
        batch = core_rows[i : i + batch_size]
        placeholders = ", ".join([f"(:a{j}, :s{j}, :w{j})" for j in range(len(batch))])
        params_t = {}
        for j, r in enumerate(batch):
            params_t[f"a{j}"] = r[0]
            params_t[f"s{j}"] = r[1]
            params_t[f"w{j}"] = r[7]
        try:
            ia_rows = online_conn.execute(
                text(f"SELECT asin, store_id, week_no, impression_count FROM amazon_search WHERE (asin, store_id, week_no) IN ({placeholders})"),
                params_t,
            ).fetchall()
        except Exception:
            ia_rows = []
        try:
            sa_rows = online_conn.execute(
                text(f"SELECT asin, store_id, week_no, sessions FROM amazon_sales_traffic WHERE (asin, store_id, week_no) IN ({placeholders})"),
                params_t,
            ).fetchall()
        except Exception:
            sa_rows = []
        try:
            sda_rows = online_conn.execute(
                text(
                    "SELECT asin, store_id, week_no, search_query, search_query_volume, impression_count, purchase_count, "
                    "total_impression_count, click_count, total_click_count FROM amazon_search_data WHERE (asin, store_id, week_no) IN ({})".format(placeholders)
                ),
                params_t,
            ).fetchall()
        except Exception as e:
            logger.warning("Step 1 amazon_search_data query failed for batch: %s", e)
            sda_rows = []

        def _wk(a, s, w):
            return (a, s, str(w) if w is not None else w)
        ia_map = {_wk(r[0], r[1], r[2]): (r[3] or 0) for r in ia_rows}
        sa_map = {_wk(r[0], r[1], r[2]): (r[3] or 0) for r in sa_rows}
        sda_by_key = defaultdict(list)
        for r in sda_rows:
            extra = (r[7], r[8], r[9]) if len(r) >= 10 else (None, None, None)
            sda_by_key[_wk(r[0], r[1], r[2])].append((r[3], r[4], r[5], r[6], extra[0], extra[1], extra[2]))

        for r in batch:
            child_asin, store_id, parent_asin, parent_asin_create_at, parent_order_total, order_num, order_ids_str, week_no = r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]
            wk_key = _wk(child_asin, store_id, week_no)
            imp = ia_map.get(wk_key, 0) or 0
            sess = sa_map.get(wk_key, 0) or 0
            try:
                order_val = int(order_num) if order_num is not None else 0
            except (TypeError, ValueError):
                order_val = int(float(order_num)) if order_num is not None else 0
            has_order = order_val > 0
            if has_order:
                n_has_order += 1
            if imp > 0 or sess > 0:
                n_has_traffic += 1
            # 仅当该周期无订单且 impression/session 均为 0 时才要求 listing 中 status 全为 active
            if not has_order and imp == 0 and sess == 0 and child_asin not in active_asins:
                n_skipped_no_order_zero_traffic_not_active += 1
                continue
            try:
                pot_float = float(parent_order_total) if parent_order_total is not None else None
            except (TypeError, ValueError):
                pot_float = None
            try:
                week_no_int = int(week_no) if week_no is not None else None
            except (TypeError, ValueError):
                week_no_int = week_no
            list_sdata = sda_by_key.get(wk_key, [])
            if not list_sdata:
                n_keys_without_search_data += 1
                list_sdata = [(None, None, None, None, None, None, None)]
            else:
                n_keys_with_search_data += 1
            for sdata in list_sdata:
                sq, sqv, sqi, sqp = sdata[0], sdata[1], sdata[2], sdata[3]
                total_imp = sdata[4] if len(sdata) > 4 else None
                click_cnt = sdata[5] if len(sdata) > 5 else None
                total_click = sdata[6] if len(sdata) > 6 else None
                row = (
                    child_asin,
                    store_id,
                    parent_asin,
                    parent_asin_create_at,
                    pot_float,
                    order_val,
                    (order_ids_str or "").strip() or None,
                    week_no_int,
                    imp,
                    sess,
                    sq,
                    sqv,
                    sqi,
                    sqp,
                    total_imp,
                    click_cnt,
                    total_click,
                )
                out.append(row)
    logger.info(
        "Step 1 metrics: has_order=%s, has_traffic=%s, skipped(no_order+0/0+not_active)=%s, out_rows=%s",
        n_has_order, n_has_traffic, n_skipped_no_order_zero_traffic_not_active, len(out),
    )
    logger.info(
        "Step 1 search_data: (asin,store_id,week_no) with data=%s, without data (search_query* NULL)=%s",
        n_keys_with_search_data, n_keys_without_search_data,
    )
    return out


# 第二步改为：从本地 asin_performances 读 parent_asin，按父 ASIN 分批在 online 查子 ASIN 的 traffic/search/search_data，写入 order_num=0 的行


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


def _step1_fallback_recover(online_engine, params: dict, included_pairs: set, reference_date_str: str):
    """
    对 order_item 本期有订单但未进入 Step1 核心结果的 (asin, store_id)，用「任意 store 的 listing」解析 parent_asin，
    仍以订单的 store_id 写入，避免同一父 ASIN 在 store_id=7 的订单被漏记（仅 listing 在 store_id=12 存在时）。
    返回与 core_rows 同格式的 list[(child_asin, store_id, parent_asin, parent_asin_create_at, parent_order_total, order_num, order_ids, week_no)]。
    """
    from sqlalchemy import text as sql_text
    week_no_str, _ = _date_to_week_no(datetime.strptime(reference_date_str, "%Y-%m-%d").date())
    fallback_rows = []
    with online_engine.connect() as conn:
        order_agg = conn.execute(
            sql_text(
                "SELECT asin, store_id, "
                "COUNT(DISTINCT order_id) AS order_num, "
                "GROUP_CONCAT(DISTINCT order_id ORDER BY order_id SEPARATOR ',') AS order_ids "
                "FROM order_item "
                "WHERE store_id IN (1, 7, 12, 25) AND purchase_utc_date BETWEEN :date_start AND :date_end "
                "GROUP BY asin, store_id"
            ),
            params,
        ).fetchall()
        excluded_triples = [
            (r[0], r[1], int(r[2]) if r[2] is not None else 0, r[3] or "")
            for r in order_agg
            if (r[0], r[1]) not in included_pairs
        ]
        if not excluded_triples:
            return fallback_rows
        unique_asins = list({t[0] for t in excluded_triples})
        asin_to_parent = {}
        batch = 200
        for i in range(0, len(unique_asins), batch):
            chunk = unique_asins[i : i + batch]
            placeholders = ", ".join([f"(:a{j})" for j in range(len(chunk))])
            prm = {f"a{j}": a for j, a in enumerate(chunk)}
            try:
                rows = conn.execute(
                    sql_text(
                        "SELECT al.asin, av.id AS variation_id, av.asin AS parent_asin, av.created_at AS parent_asin_create_at "
                        "FROM amazon_listing al "
                        "INNER JOIN amazon_variation av ON av.id = al.variation_id "
                        f"WHERE al.asin IN ({placeholders}) AND al.store_id IN (1, 7, 12, 25) "
                        "GROUP BY al.asin, av.id, av.asin, av.created_at"
                    ),
                    prm,
                ).fetchall()
            except Exception as e:
                logger.warning("Step1 fallback: lookup parent from listing failed for batch: %s", e)
                continue
            for r in rows:
                asin = r[0]
                if asin and asin not in asin_to_parent:
                    asin_to_parent[asin] = (r[2], r[3])
        # (parent_asin, store_id) -> sum(order_num)
        pot_by_key = {}
        for asin, store_id, order_num, order_ids in excluded_triples:
            parent_info = asin_to_parent.get(asin)
            if not parent_info:
                continue
            parent_asin, parent_asin_create_at = parent_info
            key = (parent_asin, store_id)
            pot_by_key[key] = pot_by_key.get(key, 0) + order_num
        for asin, store_id, order_num, order_ids in excluded_triples:
            parent_info = asin_to_parent.get(asin)
            if not parent_info:
                continue
            parent_asin, parent_asin_create_at = parent_info
            parent_order_total = pot_by_key.get((parent_asin, store_id), 0)
            fallback_rows.append((
                asin,
                store_id,
                parent_asin,
                parent_asin_create_at,
                parent_order_total,
                order_num,
                (order_ids or "").strip() or None,
                week_no_str,
            ))
    if fallback_rows:
        logger.info(
            "Step1 fallback: recovered %s rows for (asin, store_id) excluded from main core (parent from any store, store_id kept from order)",
            len(fallback_rows),
        )
    return fallback_rows


def _get_target_weeks(date_start: str, date_end: str):
    """一周为周日至周六，第1周=当年第一个周日，与 MySQL WEEK(..., 0) 一致，返回 (week_no_str, week_no_int) 列表。"""
    start = datetime.strptime(date_start, "%Y-%m-%d").date()
    end = datetime.strptime(date_end, "%Y-%m-%d").date()
    out = []
    d = start
    while d <= end:
        wk = _date_to_week_no(d)
        if wk not in out:
            out.append(wk)
        d += timedelta(days=1)
    return out


def _step2_fetch_for_parents(
    local_db: Session,
    online_conn,
    parent_asins: list,
    parent_lookup: dict,
    target_weeks: list,
    store_ids: tuple = (1, 7,12,25),
    batch_size: int = 200,
):
    """
    对一批 parent_asin：从 online 取该父下所有子 ASIN，排除本地已有（step1 订单），
    再按 (child_asin, store_id, week_no) 查 traffic/search/search_data，拼成 12 列行。
    parent_lookup: (parent_asin, week_no, store_id) -> parent_order_total (decimal/float).
    target_weeks: [(week_no_str, week_no_int), ...].
    返回 (rows: list of 12-tuple, parent_qualified_count: dict parent_asin -> 该父下符合要求的子 asin 行数)。
    """
    week_strs = [w[0] for w in target_weeks]
    week_ints = [w[1] for w in target_weeks]
    rows_out = []
    parent_qualified_count = {}
    for parent_asin in parent_asins:
        # online: parent_id 与 create_at（同一 parent_asin 共用同一 create_at）
        rv = online_conn.execute(
            text("SELECT id, created_at FROM amazon_variation WHERE asin = :pa LIMIT 1"),
            {"pa": parent_asin},
        ).fetchone()
        if not rv or not rv[0]:
            continue
        parent_id = rv[0]
        parent_asin_create_at = rv[1] if len(rv) > 1 else None
        # online: (store_id, asin) 该父下所有子
        listing = online_conn.execute(
            text(
                "SELECT store_id, asin FROM amazon_listing WHERE variation_id = :pid AND store_id IN (1, 7,12,25)"
            ),
            {"pid": parent_id},
        ).fetchall()
        if not listing:
            continue
        # Step2 为无订单子 asin，仅当 imp/sess 均为 0 时才需 status 全为 active；先取 active 集合供下面判断
        listing_asins = list({r[1] for r in listing})
        active_asins = _get_active_asins(online_conn, listing_asins)
        # 本地：该父下已有 (child_asin, store_id, week_no)（step1 订单）
        existing = local_db.query(AsinPerformance.child_asin, AsinPerformance.store_id, AsinPerformance.week_no).filter(
            AsinPerformance.parent_asin == parent_asin,
        ).distinct().all()
        ordered_set = {(r[0], r[1], r[2]) for r in existing}
        # 其他子：(child_asin, store_id, week_no_str, week_no_int) 且不在 ordered_set；
        # 仅当 (parent_asin, week_no, store_id) 在 parent_lookup 中时才纳入，避免写入「该父在本 store 本周无订单」的行，
        # 否则 _recompute_parent_order_totals 会将该组 parent_order_total 置为 0。
        other_triples = []
        for (store_id, asin) in listing:
            for (wk_str, wk_int) in target_weeks:
                if (asin, store_id, wk_int) in ordered_set:
                    continue
                if (parent_asin, wk_int, store_id) not in parent_lookup:
                    continue
                other_triples.append((asin, store_id, wk_str, wk_int))
        if not other_triples:
            continue
        # 分批查 online：traffic, search, search_data
        for i in range(0, len(other_triples), batch_size):
            batch = other_triples[i : i + batch_size]
            keys = [(a, s, w) for (a, s, w, _) in batch]
            # traffic: (asin, store_id, week_no) -> sessions
            placeholders = ", ".join([f"(:a{j}, :s{j}, :w{j})" for j in range(len(batch))])
            params_t = {}
            for j, (a, s, w, _) in enumerate(batch):
                params_t[f"a{j}"] = a
                params_t[f"s{j}"] = s
                params_t[f"w{j}"] = w
            try:
                traffic_rows = online_conn.execute(
                    text(
                        f"SELECT asin, store_id, week_no, sessions FROM amazon_sales_traffic "
                        f"WHERE (asin, store_id, week_no) IN ({placeholders})"
                    ),
                    params_t,
                ).fetchall()
            except Exception:
                traffic_rows = []
            def _wk_key(a, s, w):
                return (a, s, str(w) if w is not None else w)
            traffic_map = {_wk_key(r[0], r[1], r[2]): (r[3] or 0) for r in traffic_rows}
            # search: (asin, store_id, week_no) -> impression_count
            try:
                search_rows = online_conn.execute(
                    text(
                        f"SELECT asin, store_id, week_no, impression_count FROM amazon_search "
                        f"WHERE (asin, store_id, week_no) IN ({placeholders})"
                    ),
                    params_t,
                ).fetchall()
            except Exception:
                search_rows = []
            search_map = {_wk_key(r[0], r[1], r[2]): (r[3] or 0) for r in search_rows}
            # search_data: 含 total_impression_count, click_count, total_click_count
            try:
                # amazon_search_data 字段：total_impression_count, click_count（或 clickt_cout 别名）, total_click_count
                sdata_rows = online_conn.execute(
                    text(
                        "SELECT asin, store_id, week_no, search_query, search_query_volume, impression_count, purchase_count, "
                        "total_impression_count, click_count, total_click_count "
                        f"FROM amazon_search_data WHERE (asin, store_id, week_no) IN ({placeholders})"
                    ),
                    params_t,
                ).fetchall()
            except Exception:
                sdata_rows = []
            sdata_by_key = defaultdict(list)
            for r in sdata_rows:
                # r[3..6] 原有；r[7],r[8],r[9] 为 total_impression_count, click_count, total_click_count（若列存在）
                extra = (r[7], r[8], r[9]) if len(r) >= 10 else (None, None, None)
                sdata_by_key[_wk_key(r[0], r[1], r[2])].append((r[3], r[4], r[5], r[6], extra[0], extra[1], extra[2]))
            for (child_asin, store_id, wk_str, wk_int) in batch:
                pot = parent_lookup.get((parent_asin, wk_int, store_id)) or parent_lookup.get((parent_asin, wk_int, None))
                if pot is not None:
                    try:
                        pot = float(pot)
                    except (TypeError, ValueError):
                        pass
                sess = traffic_map.get((child_asin, store_id, wk_str), 0) or 0
                imp = search_map.get((child_asin, store_id, wk_str), 0) or 0
                # Step2 无订单；仅当 imp/sess 均为 0 时才要求 listing 中 status 全为 active
                if imp == 0 and sess == 0 and child_asin not in active_asins:
                    continue
                list_sdata = sdata_by_key.get((child_asin, store_id, wk_str), [])
                if not list_sdata:
                    list_sdata = [(None, None, None, None, None, None, None)]
                for sdata_tuple in list_sdata:
                    sq, sqv, sqi, sqp = sdata_tuple[0], sdata_tuple[1], sdata_tuple[2], sdata_tuple[3]
                    total_imp = sdata_tuple[4] if len(sdata_tuple) > 4 else None
                    click_cnt = sdata_tuple[5] if len(sdata_tuple) > 5 else None
                    total_click = sdata_tuple[6] if len(sdata_tuple) > 6 else None
                    row = (
                        child_asin,
                        store_id,
                        parent_asin,
                        parent_asin_create_at,
                        pot,
                        0,
                        wk_int,
                        imp,
                        sess,
                        sq,
                        sqv,
                        sqi,
                        sqp,
                        total_imp,
                        click_cnt,
                        total_click,
                    )
                    rows_out.append(row)
                    parent_qualified_count[parent_asin] = parent_qualified_count.get(parent_asin, 0) + 1
    return rows_out, parent_qualified_count


def _step3_backfill_search_query(
    local_db: Session,
    online_engine,
    batch_size: int = 200,
    week_nos: list[int] | None = None,
) -> tuple:
    """
    对表中 search_query 为空的记录，从线上 amazon_search_data 查询并回填。
    返回 (rows_inserted, rows_updated, rows_backfilled)。
    """
    t0 = time.time()
    logger.info(
        "Step 3 backfill start: batch_size=%s, week_filter=%s",
        batch_size,
        week_nos if week_nos else "ALL",
    )
    # 查询表中 search_query 为空的 (child_asin, store_id, week_no)，并取该组第一条的父信息、order、impression/session
    q = (
        local_db.query(
            AsinPerformance.child_asin,
            AsinPerformance.store_id,
            AsinPerformance.week_no,
            AsinPerformance.parent_asin,
            AsinPerformance.parent_asin_create_at,
            AsinPerformance.parent_order_total,
            AsinPerformance.order_num,
            AsinPerformance.order_id,
            AsinPerformance.child_impression_count,
            AsinPerformance.child_session_count,
        )
        .filter(
            or_(AsinPerformance.search_query.is_(None), AsinPerformance.search_query == ""),
            AsinPerformance.child_asin.isnot(None),
            AsinPerformance.child_asin != "",
        )
    )
    if week_nos:
        q = q.filter(AsinPerformance.week_no.in_([int(w) for w in week_nos]))
    placeholders_q = q.distinct().all()
    logger.info(
        "Step 3 placeholders loaded: rows=%s elapsed_sec=%.2f",
        len(placeholders_q),
        time.time() - t0,
    )
    if not placeholders_q:
        return 0, 0, 0

    # 按 (child_asin, store_id, week_no) 去重，保留第一条（含父信息）
    seen = set()
    placeholders = []
    for r in placeholders_q:
        key = (r[0], r[1], r[2])
        if key in seen:
            continue
        seen.add(key)
        placeholders.append(r)

    rows_out = []
    total_batches = (len(placeholders) + batch_size - 1) // batch_size
    progress_every = max(1, total_batches // 10)
    for i in range(0, len(placeholders), batch_size):
        batch = placeholders[i : i + batch_size]
        placeholders_sql = ", ".join([f"(:a{j}, :s{j}, :w{j})" for j in range(len(batch))])
        params_t = {}
        for j, r in enumerate(batch):
            params_t[f"a{j}"] = r[0]
            params_t[f"s{j}"] = r[1]
            params_t[f"w{j}"] = r[2]
        try:
            with online_engine.connect() as conn:
                sda_rows = conn.execute(
                    text(
                        "SELECT asin, store_id, week_no, search_query, search_query_volume, impression_count, purchase_count, "
                        "total_impression_count, click_count, total_click_count "
                        f"FROM amazon_search_data WHERE (asin, store_id, week_no) IN ({placeholders_sql})"
                    ),
                    params_t,
                ).fetchall()
        except Exception as e:
            logger.warning("Step 3 amazon_search_data query failed: %s", e)
            continue

        def _wk_key(a, s, w):
            return (a, s, str(w) if w is not None else w)

        sda_by_key = defaultdict(list)
        for r in sda_rows:
            extra = (r[7], r[8], r[9]) if len(r) >= 10 else (None, None, None)
            sda_by_key[_wk_key(r[0], r[1], r[2])].append((r[3], r[4], r[5], r[6], extra[0], extra[1], extra[2]))

        for r in batch:
            child_asin, store_id, week_no, parent_asin, parent_asin_create_at, parent_order_total, order_num, order_id, imp, sess = (
                r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]
            )
            wk_key = _wk_key(child_asin, store_id, week_no)
            list_sdata = sda_by_key.get(wk_key, [])
            if not list_sdata:
                continue
            try:
                week_no_int = int(week_no) if week_no is not None else None
            except (TypeError, ValueError):
                week_no_int = week_no
            try:
                order_val = int(order_num) if order_num is not None else 0
            except (TypeError, ValueError):
                order_val = int(float(order_num)) if order_num is not None else 0
            try:
                pot_float = float(parent_order_total) if parent_order_total is not None else None
            except (TypeError, ValueError):
                pot_float = None
            order_ids_str = (order_id or "").strip() or None
            for sdata in list_sdata:
                sq, sqv, sqi, sqp = sdata[0], sdata[1], sdata[2], sdata[3]
                total_imp = sdata[4] if len(sdata) > 4 else None
                click_cnt = sdata[5] if len(sdata) > 5 else None
                total_click = sdata[6] if len(sdata) > 6 else None
                if sq is None or (isinstance(sq, str) and not sq.strip()):
                    continue
                row = (
                    child_asin,
                    store_id,
                    parent_asin,
                    parent_asin_create_at,
                    pot_float,
                    order_val,
                    order_ids_str,
                    week_no_int,
                    imp,
                    sess,
                    sq,
                    sqv,
                    sqi,
                    sqp,
                    total_imp,
                    click_cnt,
                    total_click,
                )
                rows_out.append(row)
        batch_idx = i // batch_size + 1
        if batch_idx % progress_every == 0 or batch_idx == total_batches:
            logger.info(
                "Step 3 fetch progress: %s/%s batches, rows_out=%s elapsed_sec=%.2f",
                batch_idx,
                total_batches,
                len(rows_out),
                time.time() - t0,
            )

    if not rows_out:
        logger.info("Step 3 backfill done: no rows to upsert, elapsed_sec=%.2f", time.time() - t0)
        return 0, 0, 0
    logger.info(
        "Step 3 upsert start: rows_out=%s elapsed_sec=%.2f",
        len(rows_out),
        time.time() - t0,
    )
    progress_interval = max(1, len(rows_out) // 10)
    ins, upd = _upsert_batch(local_db, rows_out, "step3_backfill", progress_interval)
    logger.info(
        "Step 3 backfill done: rows_out=%s inserted=%s updated=%s elapsed_sec=%.2f",
        len(rows_out),
        ins,
        upd,
        time.time() - t0,
    )
    return ins, upd, len(rows_out)


def _normalize_search_query(v):
    """统一 search_query 格式以便去重：strip 空白，空串视为 None。"""
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _upsert_key(d: dict) -> tuple:
    """(store_id, parent_asin, child_asin, week_no, search_query) 用于去重与查询。"""
    return (
        d.get("store_id"),
        d.get("parent_asin"),
        d.get("child_asin"),
        d.get("week_no"),
        d.get("search_query"),
    )


def _row_to_dict(row, with_store_id=True) -> dict:
    """Step1 为 17 列（含 order_id）；Step2 为 12 列。列顺序：child_asin, store_id, parent_asin, parent_asin_create_at, parent_order_total, order_num, order_id, week_no, child_impression_count, ..."""
    n = len(row)
    if n >= 17:
        week_no = row[7]
    elif n > 6:
        week_no = row[6]
    else:
        week_no = row[5]
    if week_no is not None:
        try:
            week_no = int(week_no)
        except (TypeError, ValueError):
            week_no = None
    if n >= 17:
        d = {
            "child_asin": row[0],
            "parent_asin": row[2],
            "parent_asin_create_at": row[3],
            "parent_order_total": float(row[4]) if row[4] is not None else None,
            "order_num": int(row[5]) if row[5] is not None else None,
            "order_id": str(row[6]).strip() if row[6] else None,
            "week_no": week_no,
            "child_impression_count": row[8],
            "child_session_count": row[9],
            "search_query": _normalize_search_query(row[10]),
            "search_query_volume": row[11],
            "search_query_impression_count": row[12],
            "search_query_purchase_count": row[13],
            "search_query_total_impression": row[14],
            "search_query_click_count": row[15],
            "search_query_total_click": row[16],
        }
    elif n >= 16:
        d = {
            "child_asin": row[0],
            "parent_asin": row[2],
            "parent_asin_create_at": row[3],
            "parent_order_total": float(row[4]) if row[4] is not None else None,
            "order_num": int(row[5]) if row[5] is not None else None,
            "order_id": None,
            "week_no": week_no,
            "child_impression_count": row[7],
            "child_session_count": row[8],
            "search_query": _normalize_search_query(row[9]),
            "search_query_volume": row[10],
            "search_query_impression_count": row[11],
            "search_query_purchase_count": row[12],
            "search_query_total_impression": row[13],
            "search_query_click_count": row[14],
            "search_query_total_click": row[15],
        }
    else:
        d = {
            "child_asin": row[0],
            "parent_asin": row[2],
            "parent_asin_create_at": None,
            "parent_order_total": float(row[3]) if row[3] is not None else None,
            "order_num": int(row[4]) if row[4] is not None else None,
            "order_id": None,
            "week_no": week_no,
            "child_impression_count": row[6],
            "child_session_count": row[7],
            "search_query": _normalize_search_query(row[8]),
            "search_query_volume": row[9],
            "search_query_impression_count": row[10],
            "search_query_purchase_count": row[11],
            "search_query_total_impression": None,
            "search_query_click_count": None,
            "search_query_total_click": None,
        }
    if with_store_id:
        d["store_id"] = int(row[1]) if row[1] is not None else None
    return d


def _parse_order_ids(s: str) -> set:
    """将逗号分隔的 order_id 字符串解析为集合（去重）。"""
    if not s or not str(s).strip():
        return set()
    return set(x.strip() for x in str(s).split(",") if x.strip())


def _format_order_ids(ids: set) -> str | None:
    """将 order_id 集合格式化为逗号分隔字符串（按数值排序）。"""
    if not ids:
        return None
    try:
        sorted_ids = sorted(ids, key=lambda x: int(x) if str(x).isdigit() else x)
    except (ValueError, TypeError):
        sorted_ids = sorted(ids)
    return ",".join(str(i) for i in sorted_ids)


# 更新时「补全」字段：仅当本次从 online 取到的值非空时才覆盖，否则保留表中已有值（避免后续同步用空覆盖掉已写入的指标）
_METRIC_FIELDS_TO_MERGE = (
    "child_impression_count",
    "child_session_count",
    "search_query",
    "search_query_volume",
    "search_query_impression_count",
    "search_query_purchase_count",
    "search_query_total_impression",
    "search_query_click_count",
    "search_query_total_click",
)

# 用于比对「线上一条」与「表中已有行」是否数据内容完全一致（一致则跳过更新）
_CONTENT_COMPARE_FIELDS = (
    "parent_asin_create_at",
    "parent_order_total",
    "order_num",
    "order_id",
    "child_impression_count",
    "child_session_count",
    "search_query",
    "search_query_volume",
    "search_query_impression_count",
    "search_query_purchase_count",
    "search_query_total_impression",
    "search_query_click_count",
    "search_query_total_click",
)


def _normalize_value_for_compare(v):
    """比对时统一格式：None/空串一致，数值与 Decimal 可比较。"""
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s else None
    if hasattr(v, "__float__"):
        try:
            return float(v)
        except (TypeError, ValueError):
            return v
    return v


def _row_content_equal(d: dict, existing: AsinPerformance) -> bool:
    """判断 incoming 字典 d 与表中已有行 existing 在 _CONTENT_COMPARE_FIELDS 上是否一致（一致则跳过更新）。"""
    for k in _CONTENT_COMPARE_FIELDS:
        inc = _normalize_value_for_compare(d.get(k))
        cur = _normalize_value_for_compare(getattr(existing, k, None))
        if inc != cur:
            return False
    return True


def _has_search_query_data(orm_rows: list) -> bool:
    """判断表中该组是否已有非空 search_query 的记录（有则按条比对/更新，无则删占位后按条插入）。"""
    for row in orm_rows:
        sq = getattr(row, "search_query", None)
        if sq is not None and isinstance(sq, str) and sq.strip():
            return True
    return False


def _upsert_batch(local_db: Session, rows: list, table_name: str, progress_interval: int) -> tuple:
    """
    按 (store_id, parent_asin, child_asin, week_no, search_query) 去重；同一子 asin 多条 search_query 会保留多条，一条 search_query 一条记录。
    search_query 逻辑：若线上该 asin 有多条 search_query，
      - 库里该组无 search_query 数据（仅 NULL 占位）：删掉占位记录，按条插入线上数据；
      - 库里该组已有 search_query 数据：按条比对条数与内容，相同则跳过，不同则补缺或按线上数据更新。
    使用 order_id 判断是否已写入：同组内合并 order_id；更新时对指标类字段做补全。
    """
    log_prefix = f"[AsinUpsert:{table_name}]"
    # _do() 内会对 rows 重新赋值；若直接读 rows 会与「局部 rows」冲突导致 UnboundLocalError
    _incoming_rows = rows

    def _do():
        key_to_row = {_upsert_key(_row_to_dict(r)): r for r in _incoming_rows}
        original_count = len(_incoming_rows)
        rows = list(key_to_row.values())
        if len(rows) < original_count:
            logger.info(
                "%s deduped: %s -> %s rows by (store_id, parent_asin, child_asin, week_no, search_query)",
                log_prefix,
                original_count,
                len(rows),
            )

        # 本批中「有非空 search_query」的 (store_id, parent_asin, child_asin, week_no) 组
        groups_with_incoming_search = set()
        for r in rows:
            d = _row_to_dict(r)
            sq = d.get("search_query")
            if sq is not None and isinstance(sq, str) and sq.strip():
                groups_with_incoming_search.add((d.get("store_id"), d.get("parent_asin"), d.get("child_asin"), d.get("week_no")))

        # 若库里该组无 search_query 数据（仅 NULL 占位），则删掉占位记录，后续按条插入；若库里已有 search_query 数据则不删，后续按条比对/更新
        deleted_placeholders = 0
        for gkey in groups_with_incoming_search:
            sid, pa, ca, wn = gkey
            existing_rows = (
                local_db.query(AsinPerformance)
                .filter(
                    AsinPerformance.store_id == sid,
                    AsinPerformance.parent_asin == pa,
                    AsinPerformance.child_asin == ca,
                    AsinPerformance.week_no == wn,
                )
                .all()
            )
            if not existing_rows:
                continue
            if not _has_search_query_data(existing_rows):
                n = (
                    local_db.query(AsinPerformance)
                    .filter(
                        AsinPerformance.store_id == sid,
                        AsinPerformance.parent_asin == pa,
                        AsinPerformance.child_asin == ca,
                        AsinPerformance.week_no == wn,
                        or_(AsinPerformance.search_query.is_(None), AsinPerformance.search_query == ""),
                    )
                    .delete(synchronize_session=False)
                )
                deleted_placeholders += n
        if deleted_placeholders:
            local_db.flush()
            logger.info(
                "%s deleted %s placeholder row(s) (empty search_query) for groups that now have search_query data from online",
                log_prefix,
                deleted_placeholders,
            )

        # 按 (store_id, parent_asin, child_asin, week_no) 预计算合并后的 order_id 与 order_num（与库中已有合并、仅新 id 累加）
        group_key_to_order = {}
        seen_groups = set()
        for r in rows:
            d = _row_to_dict(r)
            incoming_ids_str = d.get("order_id")
            incoming_num = d.get("order_num") or 0
            key = (d.get("store_id"), d.get("parent_asin"), d.get("child_asin"), d.get("week_no"))
            if key in seen_groups or (incoming_ids_str is None and incoming_num == 0):
                continue
            seen_groups.add(key)
            existing_row = (
                local_db.query(AsinPerformance.order_id, AsinPerformance.order_num)
                .filter(
                    AsinPerformance.store_id == key[0],
                    AsinPerformance.parent_asin == key[1],
                    AsinPerformance.child_asin == key[2],
                    AsinPerformance.week_no == key[3],
                )
                .first()
            )
            existing_ids = _parse_order_ids(existing_row[0]) if existing_row and existing_row[0] else set()
            existing_num = int(existing_row[1]) if existing_row and existing_row[1] is not None else 0
            incoming_ids = _parse_order_ids(incoming_ids_str) if incoming_ids_str else set()
            merged_ids = existing_ids | incoming_ids
            new_count = len(merged_ids) - len(existing_ids)
            group_order_num = existing_num + new_count
            group_key_to_order[key] = (_format_order_ids(merged_ids), group_order_num)

        rows_inserted = 0
        rows_updated = 0
        total = len(rows)
        for i, row in enumerate(rows):
            d = _row_to_dict(row)
            gkey = (d.get("store_id"), d.get("parent_asin"), d.get("child_asin"), d.get("week_no"))
            if gkey in group_key_to_order:
                ord_str, ord_num = group_key_to_order[gkey]
                d["order_id"] = ord_str
                d["order_num"] = ord_num
            incoming_order = d.get("order_num") or 0
            # 按 (store_id, parent_asin, child_asin, week_no, search_query) 查找已有记录；search_query 将 NULL 与空串视为一致（避免 MySQL UNIQUE 允许多个 NULL 导致重复插入）
            q = local_db.query(AsinPerformance).filter(
                AsinPerformance.store_id == d.get("store_id"),
                AsinPerformance.parent_asin == d.get("parent_asin"),
                AsinPerformance.child_asin == d.get("child_asin"),
                AsinPerformance.week_no == d.get("week_no"),
            )
            sq = d.get("search_query")
            if sq is None or (isinstance(sq, str) and sq.strip() == ""):
                q = q.filter(or_(AsinPerformance.search_query.is_(None), AsinPerformance.search_query == ""))
            else:
                q = q.filter(AsinPerformance.search_query == sq)
            existing = q.first()
            if existing:
                # 若本行带非空 search_query：比对内容；相同则跳过，不同则按线上更新
                if sq is not None and isinstance(sq, str) and sq.strip():
                    if _row_content_equal(d, existing):
                        if progress_interval and ((i + 1) % progress_interval == 0 or (i + 1) == total):
                            logger.info(
                                "%s progress: %s / %s (inserted=%s, updated=%s)",
                                log_prefix,
                                i + 1,
                                total,
                                rows_inserted,
                                rows_updated,
                            )
                        continue
                if incoming_order > 0:
                    existing.order_num = d["order_num"]
                    existing.order_id = d.get("order_id")
                for k, v in d.items():
                    if k in ("order_num", "order_id"):
                        continue
                    if k in _METRIC_FIELDS_TO_MERGE and v is None:
                        # 本次为空时保留表中已有值，实现补全逻辑（第二次/第三次同步用 online 有值覆盖表中原无数据）
                        continue
                    setattr(existing, k, v)
                rows_updated += 1
            else:
                if incoming_order == 0 and table_name == "step2":
                    d["order_id"] = None
                    d["order_num"] = None
                # 插入时 search_query 空串统一为 None，与查找时 NULL/空串等价一致，避免日后重复插入
                if isinstance(d.get("search_query"), str) and d["search_query"].strip() == "":
                    d["search_query"] = None
                local_db.add(AsinPerformance(**d))
                rows_inserted += 1
            if progress_interval and ((i + 1) % progress_interval == 0 or (i + 1) == total):
                logger.info(
                    "%s progress: %s / %s (inserted=%s, updated=%s)",
                    log_prefix,
                    i + 1,
                    total,
                    rows_inserted,
                    rows_updated,
                )
        # 同组所有记录同步 order_id/order_num（含未在本批出现的 search_query 行）
        for (sid, pa, ca, wn), (oid_str, onum) in group_key_to_order.items():
            local_db.query(AsinPerformance).filter(
                AsinPerformance.store_id == sid,
                AsinPerformance.parent_asin == pa,
                AsinPerformance.child_asin == ca,
                AsinPerformance.week_no == wn,
            ).update({"order_id": oid_str, "order_num": onum}, synchronize_session=False)
        return rows_inserted, rows_updated
    try:
        return _do()
    except Exception as e:
        _log_mysql_exception(
            f"{log_prefix} 本地 asin_performances 查询/更新失败（如 .first()、列缺失、连接中断）",
            e,
        )
        raise


def _recompute_parent_order_totals(local_db: Session) -> int:
    """
    按 (parent_asin, week_no, store_id) 重算 parent_order_total = 该组内「按 child_asin 去重后」的 order_num 之和。
    即：同一父 asin 下每个子 asin 只计一次 order_num，再对所有子 asin 累加，保证 parent_order_total >= 任意子 order_num。
    返回被更新的组数。
    """
    from decimal import Decimal
    from sqlalchemy import func
    # 仅统计有 child_asin 的行；按 (parent_asin, week_no, store_id, child_asin) 取 max(order_num)，再按父分组求和
    subq = (
        local_db.query(
            AsinPerformance.parent_asin,
            AsinPerformance.week_no,
            AsinPerformance.store_id,
            AsinPerformance.child_asin,
            func.max(AsinPerformance.order_num).label("child_order"),
        )
        .filter(
            AsinPerformance.parent_asin.isnot(None),
            AsinPerformance.parent_asin != "",
            AsinPerformance.child_asin.isnot(None),
            AsinPerformance.child_asin != "",
        )
        .group_by(
            AsinPerformance.parent_asin,
            AsinPerformance.week_no,
            AsinPerformance.store_id,
            AsinPerformance.child_asin,
        )
    ).subquery()
    groups = (
        local_db.query(
            subq.c.parent_asin,
            subq.c.week_no,
            subq.c.store_id,
            func.sum(subq.c.child_order).label("tot"),
        )
        .group_by(subq.c.parent_asin, subq.c.week_no, subq.c.store_id)
        .all()
    )
    updated_groups = 0
    for pa, wn, sid, tot in groups:
        tot_val = int(tot) if tot is not None else 0
        # 不将 parent_order_total 更新为 0，避免覆盖为「该组仅有 order_num=0 的 Step2 行」导致的 0
        if tot_val == 0:
            continue
        n = local_db.query(AsinPerformance).filter(
            AsinPerformance.parent_asin == pa,
            AsinPerformance.week_no == wn,
            AsinPerformance.store_id == sid,
        ).update({"parent_order_total": Decimal(tot_val)}, synchronize_session=False)
        if n:
            updated_groups += 1
    return updated_groups


def sync_from_online_db() -> dict:
    """
    分步执行：第一步从订单查有订单的子 ASIN 及父 ASIN、total_order、impression/sessions/search_query* 并插入；
    第二步查同父 ASIN 下该段时间内有 impression/session 但无订单的其他子 ASIN，写入其指标（order_num=0）并插入。
    """
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        raise ValueError("Online DB config missing: set online_db_host, online_db_user, online_db_pwd, online_db_name in .env")

    init_db()

    date_start_str, date_end_str = _get_sync_date_range()
    # date_start_str, date_end_str = "2026-03-29", "2026-04-05"
    logger.info("Sync date range: date_start=%s, date_end=%s", date_start_str, date_end_str)

    # week_no 以 date_end 减 1 天为参考日，保证 02-21～02-22 写入 202607（02-15～02-21 所在周）
    date_end_d = datetime.strptime(date_end_str, "%Y-%m-%d").date()
    reference_date = date_end_d - timedelta(days=1)
    reference_date_str = reference_date.strftime("%Y-%m-%d")
    target_week_no = _date_to_week_no(reference_date)
    logger.info("week_no reference date (date_end-1): %s -> week_no=%s", reference_date_str, target_week_no[0])

    table_name = settings.MYSQL_DB_NAME
    online_engine = get_online_engine()
    local_db: Session = SessionLocal()
    params = {"date_start": date_start_str, "date_end": date_end_str, "reference_date": reference_date_str}
    total_fetched = 0
    total_inserted = 0
    total_updated = 0
    period_order_rows = None
    period_distinct_triples = None
    n_core = 0
    core_sum_order_num = None
    parent_asins = []
    step2_per_parent_qualified = {}

    try:
        
        with online_engine.connect() as conn:
            try:
                period_order_rows = conn.execute(
                    text(
                        "SELECT COUNT(*) FROM order_item WHERE store_id IN (1, 7,12,25) AND purchase_utc_date BETWEEN :date_start AND :date_end"
                    ),
                    params,
                ).scalar()
            except Exception:
                period_order_rows = None
            try:
                period_distinct_triples = conn.execute(
                    text(
                        "SELECT COUNT(*) FROM ("
                        "SELECT asin, store_id FROM order_item "
                        "WHERE store_id IN (1, 7,12,25) AND purchase_utc_date BETWEEN :date_start AND :date_end"
                        " GROUP BY asin, store_id"
                        ") t"
                    ),
                    params,
                ).scalar()
            except Exception:
                period_distinct_triples = None
        logger.info(
            "Sync period: date_start=%s, date_end=%s; order_item rows(store 1,7,12,25)=%s; distinct(asin,store_id)=%s",
            date_start_str, date_end_str, period_order_rows, period_distinct_triples,
        )

        # 第一步：先跑轻量核心查询，再分批查 traffic/search/search_data 并合并，避免单次大 SQL 超时
        logger.info("Step 1: core query (ordered children + parent info)...")
        with online_engine.connect() as conn:
            core_result = conn.execute(text(STEP1_CORE_SQL), params)
            core_rows = core_result.fetchall()
        n_core = len(core_rows)
        try:
            core_sum_order_num = sum(int(r[5]) if r[5] is not None else 0 for r in core_rows)
        except (TypeError, ValueError):
            core_sum_order_num = None
        logger.info(
            "Step 1 core: %s rows, sum(order_num)=%s (total orders included; diff vs order_item = excluded by amazon_listing or amazon_variation)",
            n_core,
            core_sum_order_num,
        )
        included_pairs = {(r[0], r[1]) for r in core_rows}
        excluded_count = 0
        if period_order_rows is not None and core_sum_order_num is not None:
            excluded_count = period_order_rows - core_sum_order_num
        if excluded_count != 0:
            excluded_asins_for_log = []
            try:
                with online_engine.connect() as conn:
                    order_item_pairs = conn.execute(
                        text(
                            "SELECT asin, store_id FROM order_item "
                            "WHERE store_id IN (1, 7,12,25) AND purchase_utc_date BETWEEN :date_start AND :date_end "
                            "GROUP BY asin, store_id"
                        ),
                        params,
                    ).fetchall()
                    for row in order_item_pairs:
                        key = (row[0], row[1])
                        if key not in included_pairs:
                            excluded_asins_for_log.append(f"{row[0]}(store_id={row[1]})")
            except Exception as e:
                logger.warning("Could not fetch excluded (asin, store_id) for log: %s", e)
                excluded_asins_for_log = []
            logger.warning(
                "Order count mismatch: order_item rows in period=%s, core sum(order_num)=%s, excluded=%s (asin not in listing or no parent in amazon_variation). Excluded ASINs: %s",
                period_order_rows,
                core_sum_order_num,
                excluded_count,
                excluded_asins_for_log if excluded_asins_for_log else "(failed to list)",
            )
        rows1 = []
        if core_rows:
            logger.info("Step 1: fetching impression/sessions/search_data by batch (batch_size=200)...")
            with online_engine.connect() as conn:
                step1_asins = list({r[0] for r in core_rows})
                active_asins = _get_active_asins(conn, step1_asins)
                logger.info("Step 1: active asins (all listing status=active): %s", len(active_asins))
                rows1 = _step1_attach_metrics(conn, core_rows, active_asins, batch_size=200)
        n1 = len(rows1)
        total_fetched += n1
        logger.info("Step 1 total rows with metrics: %s", n1)
        if rows1:
            progress_interval = max(1, n1 // 10)
            ins1, upd1 = _run_with_deadlock_retry(
                local_db,
                "Step 1 upsert",
                lambda: _upsert_batch(local_db, rows1, "step1", progress_interval),
            )
            total_inserted += ins1
            total_updated += upd1
            local_db.flush()
            groups_updated = _run_with_deadlock_retry(
                local_db,
                "Step 1 recompute parent_order_total",
                lambda: _recompute_parent_order_totals(local_db),
            )
            logger.info("Recomputed parent_order_total for %s (parent_asin, week_no, store_id) groups", groups_updated)
        if excluded_count > 0:
            fallback_core_rows = _step1_fallback_recover(
                online_engine, params, included_pairs, reference_date_str
            )
            if fallback_core_rows:
                with online_engine.connect() as conn:
                    fallback_rows1 = _step1_attach_metrics(
                        conn, fallback_core_rows, set(), batch_size=200
                    )
                if fallback_rows1:
                    progress_interval_fb = max(1, len(fallback_rows1) // 5)
                    ins_fb, upd_fb = _run_with_deadlock_retry(
                        local_db,
                        "Step1 fallback upsert",
                        lambda: _upsert_batch(
                            local_db, fallback_rows1, "step1_fallback", progress_interval_fb
                        ),
                    )
                    total_inserted += ins_fb
                    total_updated += upd_fb
                    local_db.flush()
                    groups_updated_fb = _run_with_deadlock_retry(
                        local_db,
                        "Step1 fallback recompute parent_order_total",
                        lambda: _recompute_parent_order_totals(local_db),
                    )
                    logger.info(
                        "Step1 fallback: inserted=%s, updated=%s; recomputed %s groups",
                        ins_fb, upd_fb, groups_updated_fb,
                    )
        local_db.commit()

        # 第二步：复用同一 online_engine，从本地 asin_performances 读 parent_asin，按父 ASIN 分批在 online 查子 ASIN
        step2_error = None
        rows2 = []
        try:
            target_weeks = [target_week_no]
            # 本地：有订单的 (parent_asin, week_no, store_id) -> parent_order_total
            local_rows = (
                local_db.query(
                    AsinPerformance.parent_asin,
                    AsinPerformance.week_no,
                    AsinPerformance.store_id,
                    AsinPerformance.parent_order_total,
                )
                .filter(
                    AsinPerformance.parent_asin.isnot(None),
                    AsinPerformance.parent_asin != "",
                    AsinPerformance.parent_order_total > 0,
                )
                .distinct()
                .all()
            )
            parent_lookup = {}
            parent_asins_set = set()
            for r in local_rows:
                parent_lookup[(r[0], r[1], r[2])] = r[3]
                parent_asins_set.add(r[0])
            parent_asins = list(parent_asins_set)
            step2_per_parent_qualified = {}
            logger.info("Step 2: %s parent ASINs from local table, fetching other children from online by parent...", len(parent_asins))
            parent_batch_size = 30
            for i in range(0, len(parent_asins), parent_batch_size):
                batch = parent_asins[i : i + parent_batch_size]
                with online_engine.connect() as conn:
                    chunk, per_parent = _step2_fetch_for_parents(
                        local_db,
                        conn,
                        batch,
                        parent_lookup,
                        target_weeks,
                        store_ids=(1, 7),
                        batch_size=200,
                    )
                rows2.extend(chunk)
                step2_per_parent_qualified.update(per_parent)
                if chunk:
                    logger.info("Step 2: parent batch %s-%s -> %s rows (total so far %s)", i + 1, min(i + parent_batch_size, len(parent_asins)), len(chunk), len(rows2))
            n2 = len(rows2)
            total_fetched += n2
            logger.info("Step 2 fetched %s rows total", n2)
            if rows2:
                progress_interval = max(1, n2 // 10)
                ins2, upd2 = _run_with_deadlock_retry(
                    local_db,
                    "Step 2 upsert",
                    lambda: _upsert_batch(local_db, rows2, "step2", progress_interval),
                )
                total_inserted += ins2
                total_updated += upd2
                local_db.flush()
                groups_updated = _run_with_deadlock_retry(
                    local_db,
                    "Step 2 recompute parent_order_total",
                    lambda: _recompute_parent_order_totals(local_db),
                )
                logger.info("After Step 2: recomputed parent_order_total for %s groups", groups_updated)
        except pymysql.err.OperationalError as e:
            step2_error = str(e)
            logger.warning("Step 2 failed: %s; 仅保留 Step 1 数据。", step2_error)
        except Exception as e:
            step2_error = str(e)
            logger.warning("Step 2 failed: %s; 仅保留 Step 1 数据。", step2_error)
        local_db.commit()

        # 第三步：对表中 search_query 为空的记录，从线上 amazon_search_data 回填
        try:
            ins3, upd3, backfill_count = _run_with_deadlock_retry(
                local_db,
                "Step 3 backfill upsert",
                lambda: _step3_backfill_search_query(
                    local_db,
                    online_engine,
                    batch_size=200,
                    week_nos=[int(target_week_no)],
                ),
            )
            if backfill_count > 0:
                total_inserted += ins3
                total_updated += upd3
                local_db.commit()
                logger.info("Step 3 backfill: %s rows from amazon_search_data, inserted=%s, updated=%s", backfill_count, ins3, upd3)
        except Exception as e:
            logger.warning("Step 3 backfill failed: %s", e)

        local_count = local_db.query(AsinPerformance).count()
        insert_ok = (total_inserted + total_updated) == total_fetched

        # 执行摘要写入 log：周期、订单数、父 asin 数、insert/update、Step2 每父符合要求的子 asin 数
        logger.info(
            "Sync summary: period=%s~%s | order_item_rows(1,7)=%s | core_sum(order_num)=%s | Step1_core_rows=%s | parent_asin_count=%s | inserted=%s | updated=%s | table_count=%s",
            date_start_str, date_end_str, period_order_rows, core_sum_order_num, n_core, len(parent_asins), total_inserted, total_updated, local_count,
        )
        for pa in sorted(parent_asins):
            logger.info("Step2 parent %s: qualified child rows=%s", pa, step2_per_parent_qualified.get(pa, 0))

        logger.info("Sync done: fetched=%s, inserted=%s, updated=%s, table_count=%s", total_fetched, total_inserted, total_updated, local_count)

        return {
            "rows_fetched_from_online": total_fetched,
            "rows_inserted": total_inserted,
            "rows_updated": total_updated,
            "local_table_count_after": local_count,
            "table_name": table_name,
            "insert_ok": insert_ok,
            "step2_error": step2_error,
        }
    finally:
        local_db.close()


if __name__ == "__main__":
    import sys
    from app.logging_config import setup_logging
    from app.sync_run_record import record_sync_run
    setup_logging(level=logging.INFO)
    try:
        print("开始同步（分步：先有订单子 ASIN，再同父下仅有曝光/会话子 ASIN）...", flush=True)
        out = sync_from_online_db()
        record_sync_run()
        print(f"从 online 共查询: {out['rows_fetched_from_online']} 条", flush=True)
        print(f"写入表 {out['table_name']}: 插入 {out['rows_inserted']} 条, 更新 {out.get('rows_updated', 0)} 条", flush=True)
        print(f"同步后本地表行数: {out['local_table_count_after']} 条, insert_ok={out['insert_ok']}", flush=True)
        sys.exit(0 if out["insert_ok"] else 1)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)