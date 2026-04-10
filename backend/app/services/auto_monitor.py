import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
import time
import sys

from sqlalchemy import func, text

from app.config import settings
from app.database import SessionLocal, init_db
from app.models import AsinPerformance
from app.online_engine import get_online_engine
from app.logging_config import setup_logging
from app.services.online_sync import (
    _get_target_weeks,
    _recompute_parent_order_totals,
    _upsert_batch,
)

logger = logging.getLogger(__name__)

STORE_IDS_SQL = "(1, 7, 12, 25)"


def _week_start(d: date) -> date:
    days_since_sunday = (d.weekday() + 1) % 7
    return d - timedelta(days=days_since_sunday)


def _week_no_to_week_start(week_no: int | str | None) -> date | None:
    if week_no is None:
        return None
    wk_str = str(week_no).strip()
    if not wk_str.isdigit() or len(wk_str) < 6:
        return None
    try:
        year = int(wk_str[:4])
        week_num = int(wk_str[4:])
    except (TypeError, ValueError):
        return None
    if week_num <= 0:
        return None
    jan1 = date(year, 1, 1)
    first_sunday = jan1 + timedelta(days=(6 - jan1.weekday()) % 7)
    return first_sunday + timedelta(weeks=week_num - 1)


def _fetch_parent_listing(
    online_conn,
    parent_asin: str,
    *,
    emit_load_log: bool = True,
) -> tuple[int | None, datetime | None, list[tuple[int, str]]]:
    t0 = time.time()
    parent = online_conn.execute(
        text("SELECT id, created_at FROM amazon_variation WHERE asin = :pa LIMIT 1"),
        {"pa": parent_asin},
    ).fetchone()
    if not parent or not parent[0]:
        log = logger.info if emit_load_log else logger.debug
        log("[AutoMonitor] parent listing lookup: parent_asin=%s not found in amazon_variation", parent_asin)
        return None, None, []
    parent_id = int(parent[0])
    parent_created_at = parent[1] if len(parent) > 1 else None
    listing_rows = online_conn.execute(
        text(
            "SELECT DISTINCT store_id, asin "
            "FROM amazon_listing "
            "WHERE variation_id = :pid AND store_id IN (1, 7, 12, 25)"
        ),
        {"pid": parent_id},
    ).fetchall()
    listing = []
    for row in listing_rows:
        sid = int(row[0]) if row[0] is not None else None
        asin = (row[1] or "").strip() if row[1] else ""
        if sid is None or not asin:
            continue
        listing.append((sid, asin))
    elapsed = time.time() - t0
    if emit_load_log:
        logger.info(
            "[AutoMonitor] parent listing loaded: parent_asin=%s parent_id=%s children=%s stores=%s elapsed=%.2fs",
            parent_asin,
            parent_id,
            len(listing),
            len({sid for sid, _ in listing}),
            elapsed,
        )
    else:
        logger.debug(
            "[AutoMonitor] parent listing loaded: parent_asin=%s parent_id=%s children=%s stores=%s elapsed=%.2fs",
            parent_asin,
            parent_id,
            len(listing),
            len({sid for sid, _ in listing}),
            elapsed,
        )
    return parent_id, parent_created_at, listing


def _fetch_search_status_map(
    online_conn,
    store_id: int,
    week_no: int,
    child_asins: list[str],
    *,
    emit_load_log: bool = True,
) -> dict[str, int | None]:
    if not child_asins:
        return {}
    t0 = time.time()
    status_map: dict[str, int | None] = {}
    batch_size = 200
    week_str = str(week_no)
    for offset in range(0, len(child_asins), batch_size):
        batch = child_asins[offset : offset + batch_size]
        asin_ph = ", ".join([f":a{idx}" for idx in range(len(batch))])
        params = {"sid": store_id, "week_no": week_str}
        for idx, asin in enumerate(batch):
            params[f"a{idx}"] = asin
        rows = online_conn.execute(
            text(
                f"SELECT asin, status FROM amazon_search "
                f"WHERE store_id = :sid AND week_no = :week_no AND asin IN ({asin_ph})"
            ),
            params,
        ).fetchall()
        for row in rows:
            asin = str(row[0]) if row[0] is not None else ""
            if not asin:
                continue
            try:
                status_map[asin] = int(row[1]) if row[1] is not None else None
            except (TypeError, ValueError):
                status_map[asin] = None
    elapsed = time.time() - t0
    if emit_load_log:
        logger.info(
            "[AutoMonitor] status map loaded: store_id=%s week_no=%s child_asins=%s hits=%s elapsed=%.2fs",
            store_id,
            week_no,
            len(child_asins),
            len(status_map),
            elapsed,
        )
    else:
        logger.debug(
            "[AutoMonitor] status map loaded: store_id=%s week_no=%s child_asins=%s hits=%s elapsed=%.2fs",
            store_id,
            week_no,
            len(child_asins),
            len(status_map),
            elapsed,
        )
    return status_map


def check_parent_store_week_completed(online_conn, parent_asin: str, store_id: int, week_no: int) -> tuple[bool, int]:
    """
    供 Monitor / query-status 刷新接口按组调用；默认不在 INFO 打明细，避免与前端轮询叠加刷屏。
    明细见 logger DEBUG。
    """
    _, _, listing = _fetch_parent_listing(online_conn, parent_asin, emit_load_log=False)
    child_asins = sorted({asin for sid, asin in listing if sid == int(store_id)})
    if not child_asins:
        logger.debug(
            "[AutoMonitor] completion check skipped: parent_asin=%s store_id=%s week_no=%s reason=no_children",
            parent_asin,
            store_id,
            week_no,
        )
        return False, 0
    status_map = _fetch_search_status_map(
        online_conn, int(store_id), int(week_no), child_asins, emit_load_log=False
    )
    has_any_rows = len(status_map) > 0
    done = has_any_rows and all((asin not in status_map) or (status_map.get(asin) == 3) for asin in child_asins)
    missing_count = sum(1 for asin in child_asins if asin not in status_map)
    non_completed_count = sum(1 for asin in child_asins if asin in status_map and status_map.get(asin) != 3)
    logger.debug(
        "[AutoMonitor] completion checked: parent_asin=%s store_id=%s week_no=%s child_count=%s started=%s missing_in_amazon_search=%s non_completed_status=%s completed=%s",
        parent_asin,
        store_id,
        week_no,
        len(child_asins),
        has_any_rows,
        missing_count,
        non_completed_count,
        done,
    )
    return done, len(child_asins)


def get_parent_week_status_details(online_conn, parent_asin: str, week_nos: list[int]) -> dict[int, dict]:
    week_nos = sorted({int(w) for w in week_nos if w is not None})
    if not week_nos:
        return {}

    _, _, listing = _fetch_parent_listing(online_conn, parent_asin)
    if not listing:
        logger.info("[AutoMonitor] week status details skipped: parent_asin=%s reason=no_listing", parent_asin)
        return {
            week_no: {"completed": False, "incomplete_count": 0, "incomplete_child_asins": []}
            for week_no in week_nos
        }

    triples = [(asin, sid, str(week_no), week_no) for sid, asin in sorted(set(listing)) for week_no in week_nos]
    status_map: dict[tuple[str, int, str], int | None] = {}
    batch_size = 300
    for offset in range(0, len(triples), batch_size):
        batch = triples[offset : offset + batch_size]
        if not batch:
            continue
        placeholders = ", ".join([f"(:a{idx}, :s{idx}, :w{idx})" for idx in range(len(batch))])
        params = {}
        for idx, (asin, sid, week_str, _week_no) in enumerate(batch):
            params[f"a{idx}"] = asin
            params[f"s{idx}"] = sid
            params[f"w{idx}"] = week_str
        rows = online_conn.execute(
            text(
                "SELECT asin, store_id, week_no, status "
                f"FROM amazon_search WHERE (asin, store_id, week_no) IN ({placeholders})"
            ),
            params,
        ).fetchall()
        for row in rows:
            asin = (row[0] or "").strip() if row[0] else ""
            sid = int(row[1]) if row[1] is not None else None
            week_str = str(row[2]) if row[2] is not None else None
            if not asin or sid is None or week_str is None:
                continue
            try:
                status_map[(asin, sid, week_str)] = int(row[3]) if row[3] is not None else None
            except (TypeError, ValueError):
                status_map[(asin, sid, week_str)] = None

    result = {}
    for week_no in week_nos:
        week_str = str(week_no)
        incomplete_child_asins = sorted({
            asin
            for sid, asin in listing
            if ((asin, sid, week_str) in status_map) and (status_map[(asin, sid, week_str)] != 3)
        })
        started = any((asin, sid, week_str) in status_map for sid, asin in listing)
        result[week_no] = {
            "completed": started and len(incomplete_child_asins) == 0,
            "started": started,
            "incomplete_count": len(incomplete_child_asins),
            "incomplete_child_asins": incomplete_child_asins,
        }

    logger.info(
        "[AutoMonitor] week status details loaded: parent_asin=%s weeks=%s listing_children=%s",
        parent_asin,
        len(week_nos),
        len(listing),
    )
    return result


def _build_auto_monitor_rows_for_parent(
    online_conn,
    parent_asin: str,
    operated_at: datetime,
    first_week_no: int | None,
    today: date,
) -> tuple[list[tuple], dict[tuple[int, int], bool], list[tuple[str, int]]]:
    t0 = time.time()
    _, parent_created_at, listing = _fetch_parent_listing(online_conn, parent_asin)
    if not listing:
        logger.info("[AutoMonitor] build rows skipped: parent_asin=%s reason=no_listing", parent_asin)
        return [], {}, []

    week_start = _week_no_to_week_start(first_week_no) or _week_start(operated_at.date())
    target_weeks = _get_target_weeks(week_start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))
    if not target_weeks:
        logger.info("[AutoMonitor] build rows skipped: parent_asin=%s reason=no_target_weeks", parent_asin)
        return [], {}, []
    logger.info(
        "[AutoMonitor] build rows start: parent_asin=%s operated_at=%s first_week_no=%s week_range=%s..%s week_count=%s listing_children=%s",
        parent_asin,
        operated_at.isoformat() if operated_at else None,
        first_week_no,
        target_weeks[0][0],
        target_weeks[-1][0],
        len(target_weeks),
        len(listing),
    )

    week_str_to_int = {wk_str: wk_int for wk_str, wk_int in target_weeks if wk_int is not None}
    child_pairs = sorted(set(listing))
    unique_asins = sorted({asin for _, asin in child_pairs})

    earliest_date = week_start.strftime("%Y-%m-%d")
    date_end = (today + timedelta(days=1)).strftime("%Y-%m-%d")

    order_map: dict[tuple[str, int, int], tuple[int, str | None]] = {}
    parent_totals: dict[tuple[int, int], int] = defaultdict(int)
    if unique_asins:
        asin_ph = ", ".join([f":a{idx}" for idx in range(len(unique_asins))])
        params = {"date_start": earliest_date, "date_end": date_end}
        for idx, asin in enumerate(unique_asins):
            params[f"a{idx}"] = asin
        order_rows = online_conn.execute(
            text(
                "SELECT asin, store_id, "
                "CONCAT(YEAR(purchase_utc_date), LPAD(GREATEST(1, WEEK(purchase_utc_date, 0)), 2, '0')) AS week_no, "
                "COUNT(DISTINCT order_id) AS order_num, "
                "GROUP_CONCAT(DISTINCT order_id ORDER BY order_id SEPARATOR ',') AS order_ids "
                "FROM order_item "
                f"WHERE store_id IN {STORE_IDS_SQL} AND asin IN ({asin_ph}) "
                "  AND purchase_utc_date >= :date_start AND purchase_utc_date < :date_end "
                "GROUP BY asin, store_id, week_no"
            ),
            params,
        ).fetchall()
        logger.info(
            "[AutoMonitor] order rows loaded: parent_asin=%s unique_asins=%s order_rows=%s date_range=%s..%s",
            parent_asin,
            len(unique_asins),
            len(order_rows),
            earliest_date,
            date_end,
        )
        for row in order_rows:
            asin = (row[0] or "").strip() if row[0] else ""
            sid = int(row[1]) if row[1] is not None else None
            wk_int = week_str_to_int.get(str(row[2])) if row[2] is not None else None
            if not asin or sid is None or wk_int is None:
                continue
            order_num = int(row[3] or 0)
            order_ids = (row[4] or "").strip() or None
            order_map[(asin, sid, wk_int)] = (order_num, order_ids)
            parent_totals[(wk_int, sid)] += order_num

    rows_out: list[tuple] = []
    all_children_by_group: dict[tuple[int, int], set[str]] = defaultdict(set)
    all_statuses_by_group: dict[tuple[int, int], dict[str, int | None]] = defaultdict(dict)
    started_groups: set[tuple[int, int]] = set()
    triples = [(asin, sid, wk_str, wk_int) for sid, asin in child_pairs for wk_str, wk_int in target_weeks if wk_int is not None]
    batch_size = 200
    for offset in range(0, len(triples), batch_size):
        batch = triples[offset : offset + batch_size]
        if not batch:
            continue
        placeholders = ", ".join([f"(:a{idx}, :s{idx}, :w{idx})" for idx in range(len(batch))])
        params = {}
        for idx, (asin, sid, wk_str, _) in enumerate(batch):
            params[f"a{idx}"] = asin
            params[f"s{idx}"] = sid
            params[f"w{idx}"] = wk_str

        search_rows = online_conn.execute(
            text(
                "SELECT asin, store_id, week_no, impression_count, status "
                f"FROM amazon_search WHERE (asin, store_id, week_no) IN ({placeholders})"
            ),
            params,
        ).fetchall()
        search_map = {}
        status_map = {}
        for row in search_rows:
            key = ((row[0] or "").strip(), int(row[1]), str(row[2]))
            search_map[key] = int(row[3] or 0)
            try:
                status_map[key] = int(row[4]) if row[4] is not None else None
            except (TypeError, ValueError):
                status_map[key] = None

        traffic_rows = online_conn.execute(
            text(
                "SELECT asin, store_id, week_no, sessions "
                f"FROM amazon_sales_traffic WHERE (asin, store_id, week_no) IN ({placeholders})"
            ),
            params,
        ).fetchall()
        traffic_map = {
            ((row[0] or "").strip(), int(row[1]), str(row[2])): int(row[3] or 0)
            for row in traffic_rows
            if row[0] and row[1] is not None and row[2] is not None
        }

        search_data_rows = online_conn.execute(
            text(
                "SELECT asin, store_id, week_no, search_query, search_query_volume, impression_count, purchase_count, "
                "total_impression_count, click_count, total_click_count "
                f"FROM amazon_search_data WHERE (asin, store_id, week_no) IN ({placeholders})"
            ),
            params,
        ).fetchall()
        logger.info(
            "[AutoMonitor] parent batch loaded: parent_asin=%s batch=%s-%s triples=%s search=%s traffic=%s search_data=%s",
            parent_asin,
            offset + 1,
            min(offset + batch_size, len(triples)),
            len(batch),
            len(search_rows),
            len(traffic_rows),
            len(search_data_rows),
        )
        sdata_by_key = defaultdict(list)
        for row in search_data_rows:
            key = ((row[0] or "").strip(), int(row[1]), str(row[2]))
            sdata_by_key[key].append((row[3], row[4], row[5], row[6], row[7], row[8], row[9]))

        for asin, sid, wk_str, wk_int in batch:
            all_children_by_group[(wk_int, sid)].add(asin)
            status_key = (asin, sid, wk_str)
            if status_key in status_map:
                all_statuses_by_group[(wk_int, sid)][asin] = status_map[status_key]
                started_groups.add((wk_int, sid))

            order_num, order_ids = order_map.get((asin, sid, wk_int), (0, None))
            parent_order_total = parent_totals.get((wk_int, sid), 0)
            imp = search_map.get((asin, sid, wk_str), 0)
            sess = traffic_map.get((asin, sid, wk_str), 0)
            sdata_rows = sdata_by_key.get((asin, sid, wk_str), [])
            if not sdata_rows:
                sdata_rows = [(None, None, None, None, None, None, None)]
            for sdata in sdata_rows:
                rows_out.append(
                    (
                        asin,
                        sid,
                        parent_asin,
                        parent_created_at,
                        float(parent_order_total),
                        int(order_num),
                        order_ids,
                        wk_int,
                        int(imp),
                        int(sess),
                        sdata[0],
                        sdata[1],
                        sdata[2],
                        sdata[3],
                        sdata[4],
                        sdata[5],
                        sdata[6],
                    )
                )

    completion_by_group = {
        group_key: (group_key in started_groups) and all(
            (asin not in all_statuses_by_group.get(group_key, {}))
            or (all_statuses_by_group.get(group_key, {}).get(asin) == 3)
            for asin in child_asins
        )
        for group_key, child_asins in all_children_by_group.items()
    }
    completed_groups = sum(1 for done in completion_by_group.values() if done)
    not_started_groups = sum(1 for group_key in all_children_by_group if group_key not in started_groups)
    missing_status_rows = sum(
        1
        for group_key, child_asins in all_children_by_group.items()
        for asin in child_asins
        if asin not in all_statuses_by_group.get(group_key, {})
    )
    logger.info(
        "[AutoMonitor] build rows done: parent_asin=%s rows=%s groups=%s completed_groups=%s not_started_groups=%s missing_status_rows=%s elapsed=%.2fs",
        parent_asin,
        len(rows_out),
        len(completion_by_group),
        completed_groups,
        not_started_groups,
        missing_status_rows,
        time.time() - t0,
    )
    return rows_out, completion_by_group, target_weeks


def sync_auto_monitor() -> dict:
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        raise ValueError("Online DB config missing: set online_db_host, online_db_user, online_db_pwd, online_db_name in .env")

    init_db()
    local_db = SessionLocal()
    online_engine = get_online_engine()
    now = datetime.now(timezone(timedelta(hours=8))).replace(tzinfo=None)
    today = now.date()
    t0 = time.time()

    parents = (
        local_db.query(
            AsinPerformance.parent_asin,
            func.min(AsinPerformance.operated_at).label("operated_at"),
            func.min(AsinPerformance.week_no).label("first_week_no"),
        )
        .filter(
            AsinPerformance.operation_status == True,
            AsinPerformance.parent_asin.isnot(None),
            AsinPerformance.parent_asin != "",
            AsinPerformance.operated_at.isnot(None),
        )
        .group_by(AsinPerformance.parent_asin)
        .order_by(AsinPerformance.parent_asin)
        .all()
    )
    logger.info(
        "[AutoMonitor] sync start: tracked_parents=%s checked_at=%s",
        len(parents),
        now.isoformat(),
    )

    total_inserted = 0
    total_updated = 0
    total_prepared = 0
    total_completed_groups = 0
    total_pending_groups = 0
    processed_parents = 0
    processed_weeks: set[int] = set()

    try:
        with online_engine.connect() as conn:
            for idx, (parent_asin, operated_at, first_week_no) in enumerate(parents, start=1):
                pa = (parent_asin or "").strip() if parent_asin else ""
                if not pa or operated_at is None:
                    logger.info(
                        "[AutoMonitor] parent skipped: index=%s/%s parent_asin=%s operated_at=%s first_week_no=%s",
                        idx,
                        len(parents),
                        parent_asin,
                        operated_at,
                        first_week_no,
                    )
                    continue
                t_parent = time.time()
                logger.info(
                    "[AutoMonitor] parent start: index=%s/%s parent_asin=%s operated_at=%s first_week_no=%s",
                    idx,
                    len(parents),
                    pa,
                    operated_at.isoformat() if operated_at else None,
                    first_week_no,
                )
                rows_out, completion_by_group, target_weeks = _build_auto_monitor_rows_for_parent(
                    conn,
                    pa,
                    operated_at,
                    int(first_week_no) if first_week_no is not None else None,
                    today,
                )
                week_ints = sorted({wk_int for _, wk_int in target_weeks if wk_int is not None})
                processed_weeks.update(week_ints)
                total_prepared += len(rows_out)
                processed_parents += 1
                parent_inserted = 0
                parent_updated = 0

                if rows_out:
                    progress_interval = max(1, len(rows_out) // 10)
                    ins, upd = _upsert_batch(local_db, rows_out, "auto_monitor", progress_interval)
                    total_inserted += ins
                    total_updated += upd
                    parent_inserted += ins
                    parent_updated += upd
                    local_db.flush()
                else:
                    logger.info("[AutoMonitor] parent has no rows to upsert: parent_asin=%s", pa)

                # 只在「实际操作发生的那一周」标记 operation_status / operated_at。
                # 其他 target_weeks 仅做监控数据回填（checked_status 等），不应将已操作状态复制到后续 week_no，
                # 否则 Monitor 页面会在未操作的 week_no 下错误展示“已操作于 <earliest_operated_at>”。
                anchor_week_no = int(first_week_no) if first_week_no is not None else None
                if anchor_week_no is not None:
                    marked_operation = (
                        local_db.query(AsinPerformance)
                        .filter(
                            AsinPerformance.parent_asin == pa,
                            AsinPerformance.week_no == anchor_week_no,
                        )
                        .update({"operation_status": True}, synchronize_session=False)
                    )
                    backfilled_operated_at = (
                        local_db.query(AsinPerformance)
                        .filter(
                            AsinPerformance.parent_asin == pa,
                            AsinPerformance.week_no == anchor_week_no,
                            AsinPerformance.operated_at.is_(None),
                        )
                        .update({"operated_at": operated_at}, synchronize_session=False)
                    )
                else:
                    marked_operation = 0
                    backfilled_operated_at = 0

                parent_completed_groups = 0
                parent_pending_groups = 0
                for (week_no, store_id), completed in completion_by_group.items():
                    new_status = "completed" if completed else "pending"
                    n = (
                        local_db.query(AsinPerformance)
                        .filter(
                            AsinPerformance.parent_asin == pa,
                            AsinPerformance.week_no == week_no,
                            AsinPerformance.store_id == store_id,
                        )
                        .update(
                            {"checked_status": new_status, "checked_at": now},
                            synchronize_session=False,
                        )
                    )
                    if n:
                        if completed:
                            total_completed_groups += 1
                            parent_completed_groups += 1
                        else:
                            total_pending_groups += 1
                            parent_pending_groups += 1

                logger.info(
                    "[AutoMonitor] parent done: index=%s/%s parent_asin=%s weeks=%s rows=%s inserted=%s updated=%s marked_operation=%s backfilled_operated_at=%s completed_groups=%s pending_groups=%s elapsed=%.2fs",
                    idx,
                    len(parents),
                    pa,
                    len(week_ints),
                    len(rows_out),
                    parent_inserted,
                    parent_updated,
                    marked_operation,
                    backfilled_operated_at,
                    parent_completed_groups,
                    parent_pending_groups,
                    time.time() - t_parent,
                )

        groups_updated = _recompute_parent_order_totals(local_db)
        local_db.commit()
        result = {
            "parents_total": len(parents),
            "parents_processed": processed_parents,
            "weeks_processed": len(processed_weeks),
            "rows_prepared": total_prepared,
            "rows_inserted": total_inserted,
            "rows_updated": total_updated,
            "completed_groups": total_completed_groups,
            "pending_groups": total_pending_groups,
            "parent_totals_recomputed": groups_updated,
            "checked_at": now.isoformat(),
        }
        logger.info("[AutoMonitor] sync done: %s elapsed=%.2fs", result, time.time() - t0)
        return result
    finally:
        try:
            local_db.close()
        except Exception:
            pass


def _main(argv: list[str]) -> int:
    setup_logging(level=logging.INFO)
    out = sync_auto_monitor()
    logger.info("[AutoMonitor] result: %s", out)
    print(out, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv))
