from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import logging
import os
from queue import Queue
import sys
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from sqlalchemy import text, tuple_
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.exc import OperationalError

from app.config import settings
from app.database import SessionLocal, init_db
from app.logging_config import setup_logging
from app.models import ListingTracking
from app.online_engine import get_online_engine

logger = logging.getLogger(__name__)

MODEL_TABLE_CANDIDATES = (
    ("ai_generated_designs", "generated_amazon_listing_id"),
    ("ai_generated_amazon_listings", "id"),
    ("ai_generated_listing", "id"),
)
METRIC_CHUNK_SIZE = 100
PID_PROCESS_CHUNK_SIZE = 50
ASIN_QUERY_CHUNK_SIZE = 500
PID_CHUNK_RETRY_SINGLE_MAX = 2


@dataclass
class _Progress:
    started_at: float
    last_reader_ts: float
    last_writer_ts: float
    completed_chunks: int = 0
    total_chunks: int = 0
    rows_prepared: int = 0


def _progress_heartbeat_loop(progress: _Progress, lock: threading.Lock, write_queue: Queue, writer_thread: threading.Thread, write_stats: dict, interval_sec: float = 30.0):
    while True:
        time.sleep(interval_sec)
        with lock:
            now = time.time()
            idle_reader = now - progress.last_reader_ts
            idle_writer = now - progress.last_writer_ts
            logger.info(
                "[ListingTracking] heartbeat elapsed_sec=%.0f chunks=%s/%s rows_prepared=%s queue=%s writer_alive=%s idle_reader_sec=%.0f idle_writer_sec=%.0f written=%s inserted=%s updated=%s unchanged=%s",
                now - progress.started_at,
                progress.completed_chunks,
                progress.total_chunks,
                progress.rows_prepared,
                getattr(write_queue, "qsize", lambda: -1)(),
                bool(writer_thread.is_alive()),
                idle_reader,
                idle_writer,
                int(write_stats.get("rows_written", 0)),
                int(write_stats.get("rows_inserted", 0)),
                int(write_stats.get("rows_updated", 0)),
                int(write_stats.get("rows_unchanged", 0)),
            )


def _week_start(d: date) -> date:
    days_since_sunday = (d.weekday() + 1) % 7
    return d - timedelta(days=days_since_sunday)


def _normalize_asin(value) -> str:
    """统一 ASIN 比对口径，避免大小写/空白导致 join 丢失。"""
    return (str(value or "").strip().upper())


def _date_to_week_no(d: date) -> int:
    # 业务周口径：周日到周六为一周，但 week_no 取该周周六所在的 ISO 周序号。
    week_end = _week_start(d) + timedelta(days=6)
    iso_year, iso_week, _ = week_end.isocalendar()
    return int(f"{iso_year}{iso_week:02d}")


def _iter_week_nos(start_date: date, end_date: date) -> list[int]:
    start_week = _week_start(start_date)
    end_week = _week_start(end_date)
    out = []
    cur = start_week
    while cur <= end_week:
        out.append(_date_to_week_no(cur))
        cur += timedelta(days=7)
    return out


def _parse_batch_ids(argv: list[str]) -> list[int]:
    values, _ = _parse_cli_args(argv)
    return values


def _parse_numeric_values(raw_value: str, label: str) -> list[int]:
    values = []
    for piece in str(raw_value).replace("，", ",").split(","):
        raw = piece.strip()
        if not raw:
            continue
        if not raw.isdigit():
            raise ValueError(f"{label} 格式不合法: {raw}")
        values.append(int(raw))
    return values


def _parse_cli_args(argv: list[str]) -> tuple[list[int], list[int]]:
    batch_ids: list[int] = []
    pids: list[int] = []
    idx = 1
    while idx < len(argv):
        arg = str(argv[idx]).strip()
        if arg in {"--batch-id", "--batch-ids"}:
            idx += 1
            if idx >= len(argv):
                raise ValueError("缺少 --batch-id 对应的值")
            batch_ids.extend(_parse_numeric_values(argv[idx], "batch_id"))
        elif arg in {"--pid", "--pids"}:
            idx += 1
            if idx >= len(argv):
                raise ValueError("缺少 --pid 对应的值")
            pids.extend(_parse_numeric_values(argv[idx], "pid"))
        elif arg.startswith("--batch-id=") or arg.startswith("--batch-ids="):
            batch_ids.extend(_parse_numeric_values(arg.split("=", 1)[1], "batch_id"))
        elif arg.startswith("--pid=") or arg.startswith("--pids="):
            pids.extend(_parse_numeric_values(arg.split("=", 1)[1], "pid"))
        elif arg.startswith("batch_id="):
            batch_ids.extend(_parse_numeric_values(arg.split("=", 1)[1], "batch_id"))
        elif arg.startswith("pid="):
            pids.extend(_parse_numeric_values(arg.split("=", 1)[1], "pid"))
        else:
            batch_ids.extend(_parse_numeric_values(arg, "batch_id"))
        idx += 1

    batch_ids = sorted(set(batch_ids))
    pids = sorted(set(pids))
    if batch_ids and pids:
        raise ValueError("batch_id 和 pid 不能同时指定，请二选一")
    return batch_ids, pids


def _chunked(items: list, size: int):
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _execute_online_rows_with_split(online_engine, query_builder, asin_chunk: list[str], query_name: str, store_id: int, depth: int = 0):
    if not asin_chunk:
        return []
    try:
        stmt, params = query_builder(asin_chunk)
        with online_engine.connect() as conn:
            return conn.execute(stmt, params).fetchall()
    except OperationalError as exc:
        if len(asin_chunk) <= 1:
            logger.warning(
                "[ListingTracking] %s query failed after split store_id=%s asin=%s depth=%s error=%s",
                query_name,
                store_id,
                asin_chunk[0] if asin_chunk else "",
                depth,
                exc,
            )
            return []
        mid = max(1, len(asin_chunk) // 2)
        logger.warning(
            "[ListingTracking] %s query timeout store_id=%s asin_count=%s depth=%s; split retry",
            query_name,
            store_id,
            len(asin_chunk),
            depth,
        )
        return (
            _execute_online_rows_with_split(online_engine, query_builder, asin_chunk[:mid], query_name, store_id, depth + 1)
            + _execute_online_rows_with_split(online_engine, query_builder, asin_chunk[mid:], query_name, store_id, depth + 1)
        )
    except Exception as exc:
        logger.exception(
            "[ListingTracking] %s query failed and will be skipped store_id=%s asin_count=%s depth=%s error=%s",
            query_name,
            store_id,
            len(asin_chunk),
            depth,
            exc,
        )
        return []


def _normalize_model_used(model_used: str | None) -> str | None:
    if model_used is None:
        return None
    value = str(model_used).strip()
    if not value:
        return None
    if value.lower().startswith("qwen-edit"):
        return "qwen-image-edit-2511"
    return value


def _split_model_used(model_used: str | None) -> tuple[str | None, str | None]:
    normalized = _normalize_model_used(model_used)
    if not normalized:
        return (None, None)
    image_prefixes = [item.lower() for item in settings.listing_tracking_image_model_prefixes]
    if any(normalized.lower().startswith(prefix) for prefix in image_prefixes):
        return (None, normalized)
    return (normalized, None)


def _select_target_batch_ids(online_conn, local_db, explicit_ids: list[int]) -> list[int]:
    if explicit_ids:
        return explicit_ids

    existing_ids = {
        int(row[0])
        for row in local_db.query(ListingTracking.batch_id).distinct().all()
        if row[0] is not None
    }
    rows = online_conn.execute(
        text(
            "SELECT DISTINCT id "
            "FROM ai_test_batch "
            "WHERE id IS NOT NULL "
            "ORDER BY id DESC"
        )
    ).fetchall()
    for row in rows:
        try:
            batch_id = int(row[0])
        except (TypeError, ValueError):
            continue
        if batch_id not in existing_ids:
            return [batch_id]
    return []


def _fetch_batch_ranges(online_conn, batch_ids: list[int]) -> dict[int, list[tuple[int, int]]]:
    if not batch_ids:
        return {}
    placeholders = ", ".join([f":b{i}" for i in range(len(batch_ids))])
    params = {f"b{i}": int(batch_id) for i, batch_id in enumerate(batch_ids)}
    rows = online_conn.execute(
        text(
            f"SELECT id, generated_listing_start_id, generated_listing_end_id "
            f"FROM ai_test_batch "
            f"WHERE id IN ({placeholders}) "
            f"  AND generated_listing_start_id IS NOT NULL "
            f"  AND generated_listing_end_id IS NOT NULL "
            f"ORDER BY id DESC, generated_listing_start_id"
        ),
        params,
    ).fetchall()
    out: dict[int, list[tuple[int, int]]] = {}
    for row in rows:
        try:
            batch_id = int(row[0])
            start_id = int(row[1])
            end_id = int(row[2])
        except (TypeError, ValueError):
            continue
        out.setdefault(batch_id, []).append((start_id, end_id))
    return out


def _fetch_pid_rows_by_conditions(
    online_conn,
    condition_sql: str,
    params: dict,
    batch_id_by_pid: dict[int, int],
    log_context: str,
    default_batch_id: int | None = None,
) -> list[dict]:
    if not condition_sql:
        return []
    started = time.time()
    listing_rows = online_conn.execute(
        text(
            "SELECT al.store_id, al.pid, al.created_at, al.variation_id, al.asin, al.status "
            "FROM amazon_listing al "
            f"WHERE al.pid IS NOT NULL AND ({condition_sql}) "
            "ORDER BY al.store_id, al.pid, al.created_at, al.variation_id"
        ),
        params,
    ).fetchall()
    logger.info(
        "[ListingTracking] raw listing rows fetched context=%s rows=%s elapsed_sec=%.2f",
        log_context,
        len(listing_rows),
        time.time() - started,
    )

    grouped: dict[tuple[int, int], list[dict]] = defaultdict(list)
    for row in listing_rows:
        try:
            pid = int(row[1])
        except (TypeError, ValueError):
            continue
        store_id = int(row[0]) if row[0] is not None else None
        if store_id is None:
            continue
        grouped[(store_id, pid)].append(
            {
                "created_at": row[2],
                "variation_id": int(row[3]) if row[3] is not None else None,
                "asin": (row[4] or "").strip() or None,
                "status": (row[5] or "").strip() or None,
            }
        )

    representative_rows = []
    variation_ids = set()
    for (store_id, pid), items in grouped.items():
        batch_id = batch_id_by_pid.get(pid, default_batch_id)
        if batch_id is None:
            logger.warning("[ListingTracking] skip pid=%s in %s because batch_id not resolved", pid, log_context)
            continue
        valid_items = [item for item in items if item["created_at"] is not None]
        if not valid_items:
            continue
        picked = min(
            valid_items,
            key=lambda item: (
                item["created_at"],
                1 if not item["asin"] else 0,
                item["variation_id"] if item["variation_id"] is not None else 10**18,
            ),
        )
        if picked["variation_id"] is not None:
            variation_ids.add(int(picked["variation_id"]))
        representative_rows.append(
            {
                "batch_id": int(batch_id),
                "store_id": store_id,
                "pid": pid,
                "created_at": picked["created_at"],
                "variation_id": picked["variation_id"],
                "pid_asin_count": len({item["asin"] for item in items if item["asin"]}),
                "pid_active_asin_count": len(
                    {
                        item["asin"]
                        for item in items
                        if item["asin"] and (item.get("status") or "").lower() == "active"
                    }
                ),
                "created_at_list": " | ".join(
                    sorted(
                        {
                            item["created_at"].strftime("%Y-%m-%d %H:%M:%S")
                            for item in valid_items
                            if item["created_at"] is not None
                        }
                    )
                ),
            }
        )

    variation_map: dict[int, str | None] = {}
    if variation_ids:
        var_started = time.time()
        ordered_variation_ids = sorted(variation_ids)
        placeholders = ", ".join([f":v{i}" for i in range(len(ordered_variation_ids))])
        variation_rows = online_conn.execute(
            text(f"SELECT id, asin FROM amazon_variation WHERE id IN ({placeholders})"),
            {f"v{i}": variation_id for i, variation_id in enumerate(ordered_variation_ids)},
        ).fetchall()
        variation_map = {
            int(row[0]): ((row[1] or "").strip() or None)
            for row in variation_rows
            if row[0] is not None
        }
        logger.info(
            "[ListingTracking] variation lookup context=%s variation_ids=%s matched=%s elapsed_sec=%.2f",
            log_context,
            len(ordered_variation_ids),
            len(variation_map),
            time.time() - var_started,
        )

    out: list[dict] = []
    for item in representative_rows:
        out.append(
            {
                "batch_id": item["batch_id"],
                "store_id": item["store_id"],
                "pid": item["pid"],
                "created_at": item["created_at"],
                "parent_asin": variation_map.get(item["variation_id"]),
                "pid_asin_count": item["pid_asin_count"],
                "pid_active_asin_count": item["pid_active_asin_count"],
                "created_at_list": item["created_at_list"],
            }
        )
    pid_store_map: dict[int, list[dict]] = defaultdict(list)
    for row in out:
        pid_store_map[int(row["pid"])].append(row)
    for pid, store_rows in sorted(pid_store_map.items()):
        logger.info(
            "[ListingTracking] pid summary context=%s batch_id=%s pid=%s store_count=%s stores=%s",
            log_context,
            store_rows[0]["batch_id"],
            pid,
            len(store_rows),
            "; ".join(
                [
                    (
                        f"store_id={int(item['store_id'])}, "
                        f"pid_asin_count={int(item.get('pid_asin_count') or 0)}, "
                        f"pid_active_asin_count={int(item.get('pid_active_asin_count') or 0)}, "
                        f"created_at={item['created_at']}, "
                        f"created_at_list={item.get('created_at_list') or ''}"
                    )
                    for item in sorted(store_rows, key=lambda x: int(x["store_id"]))
                ]
            ),
        )
    logger.info(
        "[ListingTracking] pid rows resolved context=%s rows=%s elapsed_sec=%.2f",
        log_context,
        len(out),
        time.time() - started,
    )
    return out


def _fetch_pid_rows_for_batch(online_conn, batch_id: int, ranges: list[tuple[int, int]]) -> list[dict]:
    if not ranges:
        return []
    out: list[dict] = []
    chunk_size = 50
    batch_started = time.time()
    for offset in range(0, len(ranges), chunk_size):
        chunk = ranges[offset : offset + chunk_size]
        conditions = []
        params = {}
        for idx, (start_id, end_id) in enumerate(chunk):
            params[f"s{idx}"] = int(start_id)
            params[f"e{idx}"] = int(end_id)
            conditions.append(f"(CAST(al.pid AS UNSIGNED) BETWEEN :s{idx} AND :e{idx})")
        out.extend(
            _fetch_pid_rows_by_conditions(
                online_conn,
                " OR ".join(conditions),
                params,
                {},
                f"batch_id={batch_id}",
                default_batch_id=int(batch_id),
            )
        )
    logger.info(
        "[ListingTracking] batch_id=%s resolved pid rows=%s from ranges=%s elapsed_sec=%.2f",
        batch_id,
        len(out),
        len(ranges),
        time.time() - batch_started,
    )
    return out


def _resolve_batch_ids_for_pids(online_conn, pids: list[int]) -> dict[int, int]:
    if not pids:
        return {}
    pid_candidates: dict[int, list[int]] = defaultdict(list)
    batch_size = 200
    started = time.time()
    for chunk in _chunked(pids, batch_size):
        scope_sql = []
        params = {}
        for idx, pid in enumerate(chunk):
            scope_sql.append(f"SELECT :p{idx} AS pid")
            params[f"p{idx}"] = int(pid)
        rows = online_conn.execute(
            text(
                "SELECT scope.pid, b.id AS batch_id "
                f"FROM ({' UNION ALL '.join(scope_sql)}) scope "
                "LEFT JOIN ai_test_batch b "
                "  ON scope.pid BETWEEN b.generated_listing_start_id AND b.generated_listing_end_id "
                " AND b.generated_listing_start_id IS NOT NULL "
                " AND b.generated_listing_end_id IS NOT NULL "
                "ORDER BY scope.pid, b.id DESC"
            ),
            params,
        ).fetchall()
        for row in rows:
            try:
                pid = int(row[0])
            except (TypeError, ValueError):
                continue
            if row[1] is None:
                pid_candidates.setdefault(pid, [])
                continue
            pid_candidates[pid].append(int(row[1]))

    resolved: dict[int, int] = {}
    for pid in pids:
        candidates = sorted(set(pid_candidates.get(int(pid), [])), reverse=True)
        if not candidates:
            logger.warning("[ListingTracking] pid=%s did not match any ai_test_batch range", pid)
            continue
        resolved[int(pid)] = candidates[0]
        if len(candidates) > 1:
            logger.warning(
                "[ListingTracking] pid=%s matched multiple batch_ids=%s, use latest=%s",
                pid,
                candidates,
                candidates[0],
            )
    logger.info(
        "[ListingTracking] resolved batch_ids for explicit pids matched=%s requested=%s elapsed_sec=%.2f",
        len(resolved),
        len(pids),
        time.time() - started,
    )
    return resolved


def _fetch_pid_rows_for_explicit_pids(online_conn, pids: list[int], pid_batch_map: dict[int, int]) -> list[dict]:
    if not pids:
        return []
    out: list[dict] = []
    batch_size = 300
    started = time.time()
    for chunk in _chunked(pids, batch_size):
        placeholders = []
        params = {}
        for idx, pid in enumerate(chunk):
            params[f"p{idx}"] = int(pid)
            placeholders.append(f":p{idx}")
        out.extend(
            _fetch_pid_rows_by_conditions(
                online_conn,
                f"al.pid IN ({', '.join(placeholders)})",
                params,
                pid_batch_map,
                f"explicit_pids={chunk}",
            )
        )
    logger.info(
        "[ListingTracking] explicit pid fetch resolved rows=%s requested_pids=%s elapsed_sec=%.2f",
        len(out),
        len(pids),
        time.time() - started,
    )
    return out


def _fetch_model_map(online_conn, pids: list[int]) -> dict[int, str | None]:
    if not pids:
        return {}
    batch_size = 300
    for table, id_column in MODEL_TABLE_CANDIDATES:
        model_map: dict[int, str | None] = {}
        try:
            started = time.time()
            for offset in range(0, len(pids), batch_size):
                chunk = pids[offset : offset + batch_size]
                placeholders = ", ".join([f":p{i}" for i in range(len(chunk))])
                params = {f"p{i}": int(pid) for i, pid in enumerate(chunk)}
                rows = online_conn.execute(
                    text(
                        f"SELECT {id_column}, model_used "
                        f"FROM {table} "
                        f"WHERE {id_column} IN ({placeholders})"
                    ),
                    params,
                ).fetchall()
                for row in rows:
                    try:
                        model_map[int(row[0])] = row[1]
                    except (TypeError, ValueError):
                        continue
            logger.info(
                "[ListingTracking] loaded model_used from %s.%s matched_pids=%s requested_pids=%s elapsed_sec=%.2f",
                table,
                id_column,
                len(model_map),
                len(pids),
                time.time() - started,
            )
            return model_map
        except Exception as exc:
            logger.warning("[ListingTracking] model_used lookup failed on %s: %s", table, exc)
    return {}


def _metric_cache_key(pid_row: dict) -> tuple[int, int, str]:
    created_at = pid_row["created_at"]
    created_date = created_at.date() if isinstance(created_at, datetime) else created_at
    return (
        int(pid_row["pid"]),
        int(pid_row["store_id"]),
        created_date.strftime("%Y-%m-%d"),
    )


def _aggregate_metrics_for_keys(online_engine, metric_keys: list[tuple[int, int, str]]) -> dict[tuple[int, int, str], dict[str, dict[int, int] | dict[int, set[str]]]]:
    if not metric_keys:
        return {}

    total_chunks = (len(metric_keys) + METRIC_CHUNK_SIZE - 1) // METRIC_CHUNK_SIZE
    end_week_no = _date_to_week_no(date.today())
    end_date = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    aggregated: dict[tuple[int, int, str], dict[str, dict[int, int]]] = {}

    for chunk_idx, chunk in enumerate(_chunked(metric_keys, METRIC_CHUNK_SIZE), start=1):
        chunk_started = time.time()
        chunk_result = {
            key: {
                "impressions": {},
                "clicks": {},
                "sessions": {},
                "orders": {},
                "impression_asins": defaultdict(set),
                "session_asins": defaultdict(set),
            }
            for key in chunk
        }
        key_meta = {
            key: {
                "created_date": datetime.strptime(key[2], "%Y-%m-%d").date(),
                "start_week_no": _date_to_week_no(datetime.strptime(key[2], "%Y-%m-%d").date()),
            }
            for key in chunk
        }
        start_week_no = min(meta["start_week_no"] for meta in key_meta.values())
        min_created_date = min(key[2] for key in chunk)

        pid_values = sorted({str(pid) for pid, _, _ in chunk})
        store_values = sorted({int(store_id) for _, store_id, _ in chunk})
        pid_placeholders = ", ".join([f":p{i}" for i in range(len(pid_values))])
        store_placeholders = ", ".join([f":s{i}" for i in range(len(store_values))])
        listing_params = {
            **{f"p{i}": pid for i, pid in enumerate(pid_values)},
            **{f"s{i}": store_id for i, store_id in enumerate(store_values)},
        }
        try:
            with online_engine.connect() as conn:
                listing_rows = conn.execute(
                    text(
                        "SELECT al.pid, al.store_id, al.asin "
                        "FROM amazon_listing al "
                        f"WHERE al.pid IN ({pid_placeholders}) "
                        f"  AND al.store_id IN ({store_placeholders}) "
                        "  AND al.asin IS NOT NULL"
                    ),
                    listing_params,
                ).fetchall()
        except Exception as exc:
            logger.exception(
                "[ListingTracking] listing row fetch failed for metric chunk=%s/%s keys=%s error=%s",
                chunk_idx,
                total_chunks,
                len(chunk),
                exc,
            )
            aggregated.update(chunk_result)
            continue

        key_by_pid_store = {(int(pid), int(store_id)): key for key in chunk for pid, store_id, _ in [key]}
        asin_to_keys: dict[tuple[int, str], set[tuple[int, int, str]]] = defaultdict(set)
        store_asins: dict[int, set[str]] = defaultdict(set)
        listing_match_count = 0
        for row in listing_rows:
            try:
                pid = int(row[0])
            except (TypeError, ValueError):
                continue
            store_id = int(row[1]) if row[1] is not None else None
            asin = _normalize_asin(row[2])
            if store_id is None or not asin:
                continue
            key = key_by_pid_store.get((pid, store_id))
            if not key:
                continue
            asin_to_keys[(store_id, asin)].add(key)
            store_asins[store_id].add(asin)
            listing_match_count += 1

        impression_row_count = 0
        session_row_count = 0
        order_row_count = 0
        session_unmatched_count = 0
        session_unmatched_samples: list[str] = []
        order_sets: dict[tuple[tuple[int, int, str], int], set[str]] = defaultdict(set)

        for store_id, asin_set in store_asins.items():
            asin_list = sorted(asin_set)
            for asin_chunk in _chunked(asin_list, ASIN_QUERY_CHUNK_SIZE):
                def _build_base_params(current_asin_chunk: list[str]) -> tuple[str, dict]:
                    asin_placeholders = ", ".join([f":a{i}" for i in range(len(current_asin_chunk))])
                    base_params = {
                        "sid": int(store_id),
                        "start_week_no": int(start_week_no),
                        "end_week_no": int(end_week_no),
                        "min_created_date": min_created_date,
                        "end_date": end_date,
                        **{f"a{i}": asin for i, asin in enumerate(current_asin_chunk)},
                    }
                    return asin_placeholders, base_params

                impression_rows = _execute_online_rows_with_split(
                    online_engine,
                    lambda current_asin_chunk: (
                        text(
                            "SELECT UPPER(TRIM(s.asin)) AS asin, CAST(s.week_no AS UNSIGNED) AS week_no, "
                            "       SUM(COALESCE(s.impression_count, 0)) AS total_impression, "
                            "       SUM(COALESCE(s.click_count, 0)) AS total_click "
                            "FROM amazon_search s "
                            "WHERE s.store_id = :sid "
                            f"  AND UPPER(TRIM(s.asin)) IN ({_build_base_params(current_asin_chunk)[0]}) "
                            "  AND CAST(s.week_no AS UNSIGNED) BETWEEN :start_week_no AND :end_week_no "
                            "GROUP BY UPPER(TRIM(s.asin)), CAST(s.week_no AS UNSIGNED)"
                        ),
                        _build_base_params(current_asin_chunk)[1],
                    ),
                    asin_chunk,
                    "amazon_search",
                    int(store_id),
                )
                impression_row_count += len(impression_rows)
                for row in impression_rows:
                    asin = _normalize_asin(row[0])
                    if not asin or row[1] is None:
                        continue
                    week_no = int(row[1])
                    total_impression = int(row[2] or 0)
                    total_click = int(row[3] or 0)
                    for key in asin_to_keys.get((store_id, asin), []):
                        if week_no < key_meta[key]["start_week_no"]:
                            continue
                        chunk_result[key]["impressions"][week_no] = (
                            int(chunk_result[key]["impressions"].get(week_no, 0)) + total_impression
                        )
                        chunk_result[key]["clicks"][week_no] = (
                            int(chunk_result[key]["clicks"].get(week_no, 0)) + total_click
                        )
                        if total_impression > 0:
                            chunk_result[key]["impression_asins"][week_no].add(asin)

                session_rows = _execute_online_rows_with_split(
                    online_engine,
                    lambda current_asin_chunk: (
                        text(
                            "SELECT UPPER(TRIM(t.asin)) AS asin, CAST(t.week_no AS UNSIGNED) AS week_no, "
                            "       SUM(COALESCE(t.sessions, 0)) AS total_session "
                            "FROM amazon_sales_traffic t "
                            "WHERE t.store_id = :sid "
                            f"  AND UPPER(TRIM(t.asin)) IN ({_build_base_params(current_asin_chunk)[0]}) "
                            "  AND CAST(t.week_no AS UNSIGNED) BETWEEN :start_week_no AND :end_week_no "
                            "GROUP BY UPPER(TRIM(t.asin)), CAST(t.week_no AS UNSIGNED)"
                        ),
                        _build_base_params(current_asin_chunk)[1],
                    ),
                    asin_chunk,
                    "amazon_sales_traffic",
                    int(store_id),
                )
                session_row_count += len(session_rows)
                for row in session_rows:
                    asin = _normalize_asin(row[0])
                    if not asin or row[1] is None:
                        continue
                    week_no = int(row[1])
                    total_value = int(row[2] or 0)
                    matched_keys = asin_to_keys.get((store_id, asin), [])
                    if not matched_keys:
                        session_unmatched_count += 1
                        if len(session_unmatched_samples) < 8:
                            session_unmatched_samples.append(f"{store_id}:{asin}@{week_no}")
                    for key in matched_keys:
                        if week_no < key_meta[key]["start_week_no"]:
                            continue
                        chunk_result[key]["sessions"][week_no] = (
                            int(chunk_result[key]["sessions"].get(week_no, 0)) + total_value
                        )
                        if total_value > 0:
                            chunk_result[key]["session_asins"][week_no].add(asin)

                order_rows = _execute_online_rows_with_split(
                    online_engine,
                    lambda current_asin_chunk: (
                        text(
                            "SELECT UPPER(TRIM(oi.asin)) AS asin, oi.order_id, oi.purchase_date "
                            "FROM order_item oi "
                            "WHERE oi.store_id = :sid "
                            f"  AND UPPER(TRIM(oi.asin)) IN ({_build_base_params(current_asin_chunk)[0]}) "
                            "  AND oi.purchase_date >= :min_created_date "
                            "  AND oi.purchase_date < :end_date"
                        ),
                        _build_base_params(current_asin_chunk)[1],
                    ),
                    asin_chunk,
                    "order_item",
                    int(store_id),
                )
                order_row_count += len(order_rows)
                for row in order_rows:
                    asin = _normalize_asin(row[0])
                    order_id = str(row[1]).strip() if row[1] is not None else ""
                    purchase_date = row[2]
                    if not asin or not order_id or purchase_date is None:
                        continue
                    purchase_day = purchase_date.date() if isinstance(purchase_date, datetime) else purchase_date
                    week_no = _date_to_week_no(purchase_day)
                    for key in asin_to_keys.get((store_id, asin), []):
                        if purchase_day < key_meta[key]["created_date"]:
                            continue
                        if week_no < key_meta[key]["start_week_no"] or week_no > end_week_no:
                            continue
                        order_sets[(key, week_no)].add(order_id)

        for (key, week_no), order_ids in order_sets.items():
            chunk_result[key]["orders"][int(week_no)] = len(order_ids)

        aggregated.update(chunk_result)
        logger.info(
            "[ListingTracking] metric chunk done: %s/%s keys=%s listing_rows=%s listing_matches=%s impression_rows=%s session_rows=%s session_unmatched=%s order_rows=%s elapsed_sec=%.2f",
            chunk_idx,
            total_chunks,
            len(chunk),
            len(listing_rows),
            listing_match_count,
            impression_row_count,
            session_row_count,
            session_unmatched_count,
            order_row_count,
            time.time() - chunk_started,
        )
        if session_unmatched_count > 0:
            logger.warning(
                "[ListingTracking] session rows unmatched to amazon_listing keys chunk=%s/%s count=%s samples=%s",
                chunk_idx,
                total_chunks,
                session_unmatched_count,
                session_unmatched_samples,
            )

    return aggregated


def _build_values_for_pid(pid_row: dict, model_used: str | None, metrics: dict[str, dict[int, int]]) -> list[dict]:
    created_at = pid_row["created_at"]
    created_date = created_at.date() if isinstance(created_at, datetime) else created_at
    week_nos = _iter_week_nos(created_date, date.today())
    if not week_nos:
        return []
    used_text_model, used_image_model = _split_model_used(model_used)
    impressions = metrics.get("impressions", {})
    clicks = metrics.get("clicks", {})
    sessions = metrics.get("sessions", {})
    orders = metrics.get("orders", {})
    session_asins = metrics.get("session_asins", {})
    impression_asins = metrics.get("impression_asins", {})

    values = []
    for week_no in week_nos:
        values.append(
            {
                "batch_id": int(pid_row["batch_id"]),
                "pid": int(pid_row["pid"]),
                "pid_asin_count": int(pid_row.get("pid_asin_count") or 0),
                "pid_active_asin_count": int(pid_row.get("pid_active_asin_count") or 0),
                "parent_asin": pid_row["parent_asin"],
                "created_at": created_at,
                "week_no": int(week_no),
                "total_impression": int(impressions.get(week_no, 0)),
                "total_session": int(sessions.get(week_no, 0)),
                "total_click": int(clicks.get(week_no, 0)),
                "total_order": int(orders.get(week_no, 0)),
                "session_asin": ",".join(sorted(session_asins.get(week_no, set()))),
                "impression_asin": ",".join(sorted(impression_asins.get(week_no, set()))),
                "used_text_model": used_text_model,
                "store_id": int(pid_row["store_id"]),
                "used_image_model": used_image_model,
            }
        )
    return values


def _fetch_existing_listing_tracking_map(local_db, values: list[dict]) -> dict[tuple[int, int, int, int], dict]:
    if not values:
        return {}
    key_tuples = sorted(
        {
            (
                int(value["batch_id"]),
                int(value["pid"]),
                int(value["week_no"]),
                int(value["store_id"]),
            )
            for value in values
        }
    )
    existing_map: dict[tuple[int, int, int, int], dict] = {}
    for chunk in _chunked(key_tuples, 500):
        rows = (
            local_db.query(
                ListingTracking.id,
                ListingTracking.batch_id,
                ListingTracking.pid,
                ListingTracking.pid_asin_count,
                ListingTracking.pid_active_asin_count,
                ListingTracking.week_no,
                ListingTracking.store_id,
                ListingTracking.parent_asin,
                ListingTracking.created_at,
                ListingTracking.total_impression,
                ListingTracking.total_session,
                ListingTracking.total_click,
                ListingTracking.total_order,
                ListingTracking.session_asin,
                ListingTracking.impression_asin,
                ListingTracking.used_text_model,
                ListingTracking.used_image_model,
            )
            .filter(
                tuple_(
                    ListingTracking.batch_id,
                    ListingTracking.pid,
                    ListingTracking.week_no,
                    ListingTracking.store_id,
                ).in_(chunk)
            )
            .all()
        )
        for row in rows:
            key = (int(row[1]), int(row[2]), int(row[5]), int(row[6]))
            existing_map[key] = {
                "id": int(row[0]),
                "pid_asin_count": int(row[3] or 0),
                "pid_active_asin_count": int(row[4] or 0),
                "parent_asin": row[7],
                "created_at": row[8],
                "total_impression": int(row[9] or 0),
                "total_session": int(row[10] or 0),
                "total_click": int(row[11] or 0),
                "total_order": int(row[12] or 0),
                "session_asin": row[13] or "",
                "impression_asin": row[14] or "",
                "used_text_model": row[15],
                "used_image_model": row[16],
            }
    return existing_map


def _prepare_listing_tracking_changes(values: list[dict], existing_map: dict[tuple[int, int, int, int], dict]) -> tuple[list[dict], list[dict], int]:
    inserts: list[dict] = []
    updates: list[dict] = []
    unchanged = 0

    for value in values:
        key = (
            int(value["batch_id"]),
            int(value["pid"]),
            int(value["week_no"]),
            int(value["store_id"]),
        )
        existing = existing_map.get(key)
        if not existing:
            inserts.append(value)
            continue

        changed = (
            int(existing.get("pid_asin_count") or 0) != int(value.get("pid_asin_count") or 0)
            or int(existing.get("pid_active_asin_count") or 0) != int(value.get("pid_active_asin_count") or 0)
            or (existing.get("parent_asin") or None) != (value.get("parent_asin") or None)
            or existing.get("created_at") != value.get("created_at")
            or int(existing.get("total_impression") or 0) != int(value.get("total_impression") or 0)
            or int(existing.get("total_session") or 0) != int(value.get("total_session") or 0)
            or int(existing.get("total_click") or 0) != int(value.get("total_click") or 0)
            or int(existing.get("total_order") or 0) != int(value.get("total_order") or 0)
            or (existing.get("session_asin") or "") != (value.get("session_asin") or "")
            or (existing.get("impression_asin") or "") != (value.get("impression_asin") or "")
            or (existing.get("used_text_model") or None) != (value.get("used_text_model") or None)
            or (existing.get("used_image_model") or None) != (value.get("used_image_model") or None)
        )
        if not changed:
            unchanged += 1
            continue

        updates.append(
            {
                "id": int(existing["id"]),
                "pid_asin_count": int(value.get("pid_asin_count") or 0),
                "pid_active_asin_count": int(value.get("pid_active_asin_count") or 0),
                "parent_asin": value.get("parent_asin"),
                "created_at": value.get("created_at"),
                "total_impression": int(value.get("total_impression") or 0),
                "total_session": int(value.get("total_session") or 0),
                "total_click": int(value.get("total_click") or 0),
                "total_order": int(value.get("total_order") or 0),
                "session_asin": value.get("session_asin") or "",
                "impression_asin": value.get("impression_asin") or "",
                "used_text_model": value.get("used_text_model"),
                "used_image_model": value.get("used_image_model"),
            }
        )

    return inserts, updates, unchanged


def _upsert_listing_tracking(local_db, values: list[dict]) -> dict:
    if not values:
        return {"rows_written": 0, "rows_inserted": 0, "rows_updated": 0, "rows_unchanged": 0}
    started = time.time()
    existing_map = _fetch_existing_listing_tracking_map(local_db, values)
    inserts, updates, unchanged = _prepare_listing_tracking_changes(values, existing_map)
    inserted = 0
    updated = 0

    if inserts:
        stmt = mysql_insert(ListingTracking).values(inserts)
        res = local_db.execute(stmt)
        inserted = int(getattr(res, "rowcount", 0) or 0)

    if updates:
        local_db.bulk_update_mappings(ListingTracking, updates)
        updated = len(updates)

    rows_written = inserted + updated
    logger.info(
        "[ListingTracking] incremental write payload_rows=%s existing_rows=%s inserted=%s updated=%s unchanged=%s elapsed_sec=%.2f",
        len(values),
        len(existing_map),
        inserted,
        updated,
        unchanged,
        time.time() - started,
    )
    return {
        "rows_written": rows_written,
        "rows_inserted": inserted,
        "rows_updated": updated,
        "rows_unchanged": unchanged,
    }


def _merge_write_stats(total: dict, current: dict) -> dict:
    return {
        "rows_written": int(total.get("rows_written", 0)) + int(current.get("rows_written", 0)),
        "rows_inserted": int(total.get("rows_inserted", 0)) + int(current.get("rows_inserted", 0)),
        "rows_updated": int(total.get("rows_updated", 0)) + int(current.get("rows_updated", 0)),
        "rows_unchanged": int(total.get("rows_unchanged", 0)) + int(current.get("rows_unchanged", 0)),
    }


def _group_pid_rows(all_pid_rows: list[dict]) -> dict[int, list[dict]]:
    grouped: dict[int, list[dict]] = defaultdict(list)
    for row in all_pid_rows:
        grouped[int(row["pid"])].append(row)
    return dict(sorted(grouped.items()))


def _get_pid_worker_count(total_pids: int) -> int:
    if total_pids <= 0:
        return 1
    cpu_count = os.cpu_count() or 4
    configured = max(1, int(settings.LISTING_TRACKING_READER_WORKERS or 1))
    return max(1, min(total_pids, configured, cpu_count))


def _chunk_pid_groups(pid_groups: dict[int, list[dict]], chunk_size: int = PID_PROCESS_CHUNK_SIZE) -> list[list[tuple[int, list[dict]]]]:
    items = list(pid_groups.items())
    return [items[idx : idx + chunk_size] for idx in range(0, len(items), chunk_size)]


def _process_pid_group_chunk_once(pid_chunk: list[tuple[int, list[dict]]], model_map: dict[int, str | None]) -> list[dict]:
    started = time.time()
    online_engine = get_online_engine()
    metric_keys = sorted({_metric_cache_key(row) for _, pid_rows in pid_chunk for row in pid_rows})
    metrics_map = _aggregate_metrics_for_keys(online_engine, metric_keys)
    payloads = []
    for pid, pid_rows in pid_chunk:
        model_used = model_map.get(int(pid))
        values: list[dict] = []
        for pid_row in pid_rows:
            metric_key = _metric_cache_key(pid_row)
            values.extend(_build_values_for_pid(pid_row, model_used, metrics_map.get(metric_key, {})))
        payloads.append(
            {
                "pid": int(pid),
                "pid_rows_count": len(pid_rows),
                "batch_ids": sorted({int(row["batch_id"]) for row in pid_rows}),
                "store_ids": sorted({int(row["store_id"]) for row in pid_rows}),
                "rows_prepared": len(values),
                "values": values,
                "model_used": _normalize_model_used(model_used),
            }
        )
    logger.info(
        "[ListingTracking] pid chunk computed pids=%s metric_keys=%s elapsed_sec=%.2f",
        len(pid_chunk),
        len(metric_keys),
        time.time() - started,
    )
    return payloads


def _process_pid_group_chunk(
    pid_chunk: list[tuple[int, list[dict]]],
    model_map: dict[int, str | None],
    depth: int = 0,
) -> tuple[list[dict], list[int]]:
    try:
        return (_process_pid_group_chunk_once(pid_chunk, model_map), [])
    except Exception as exc:
        if len(pid_chunk) <= 1:
            failed_pid = int(pid_chunk[0][0]) if pid_chunk else None
            if depth >= PID_CHUNK_RETRY_SINGLE_MAX:
                logger.exception(
                    "[ListingTracking] single pid chunk failed after retries and will be skipped pid=%s metric_keys=%s depth=%s error=%s",
                    failed_pid,
                    len({_metric_cache_key(row) for _, pid_rows in pid_chunk for row in pid_rows}),
                    depth,
                    exc,
                )
                return ([], [failed_pid] if failed_pid is not None else [])
            logger.warning(
                "[ListingTracking] single pid chunk failed, auto retry pid=%s depth=%s/%s error=%s",
                failed_pid,
                depth + 1,
                PID_CHUNK_RETRY_SINGLE_MAX,
                exc,
            )
            return _process_pid_group_chunk(pid_chunk, model_map, depth + 1)

        mid = max(1, len(pid_chunk) // 2)
        left_chunk = pid_chunk[:mid]
        right_chunk = pid_chunk[mid:]
        logger.warning(
            "[ListingTracking] pid chunk failed, auto split retry pids=%s left=%s right=%s depth=%s error=%s",
            len(pid_chunk),
            len(left_chunk),
            len(right_chunk),
            depth,
            exc,
        )
        left_payloads, left_failed_pids = _process_pid_group_chunk(left_chunk, model_map, depth + 1)
        right_payloads, right_failed_pids = _process_pid_group_chunk(right_chunk, model_map, depth + 1)
        return (left_payloads + right_payloads, left_failed_pids + right_failed_pids)


def _writer_loop(write_queue: Queue, stats_holder: dict, error_holder: list):
    local_db = SessionLocal()
    try:
        while True:
            payload = write_queue.get()
            try:
                if payload is None:
                    return
                started = time.time()
                write_stats = _upsert_listing_tracking(local_db, payload["values"])
                local_db.commit()
                stats_holder.update(_merge_write_stats(stats_holder, write_stats))
                logger.info(
                    "[ListingTracking] stream write committed pid=%s batch_ids=%s store_ids=%s payload_rows=%s written=%s inserted=%s updated=%s unchanged=%s elapsed_sec=%.2f",
                    payload["pid"],
                    payload["batch_ids"],
                    payload["store_ids"],
                    len(payload["values"]),
                    int(write_stats["rows_written"]),
                    int(write_stats["rows_inserted"]),
                    int(write_stats["rows_updated"]),
                    int(write_stats["rows_unchanged"]),
                    time.time() - started,
                )
            except Exception as exc:
                try:
                    local_db.rollback()
                except Exception:
                    pass
                error_holder.append(exc)
                logger.exception("[ListingTracking] writer loop failed: %s", exc)
                return
            finally:
                write_queue.task_done()
    finally:
        try:
            local_db.close()
        except Exception:
            pass


def sync_listing_tracking(batch_ids: list[int] | None = None, pids: list[int] | None = None) -> dict:
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        raise ValueError("Online DB config missing: set online_db_host, online_db_user, online_db_pwd, online_db_name in .env")

    init_db()
    local_db = SessionLocal()
    online_engine = get_online_engine()
    t0 = time.time()
    logger.info("[ListingTracking] sync start input_batch_ids=%s input_pids=%s", batch_ids or [], pids or [])
    try:
        with online_engine.connect() as conn:
            all_pid_rows = []
            target_batch_ids: list[int] = []
            if pids:
                pid_batch_map = _resolve_batch_ids_for_pids(conn, pids)
                all_pid_rows = _fetch_pid_rows_for_explicit_pids(conn, pids, pid_batch_map)
                target_batch_ids = sorted({int(row["batch_id"]) for row in all_pid_rows})
                if not all_pid_rows:
                    logger.info("[ListingTracking] no pid rows resolved for explicit pids=%s", pids)
                    return {"batch_ids": target_batch_ids, "pids": pids, "pid_rows": 0, "rows_prepared": 0, "rows_written": 0}
                logger.info("[ListingTracking] explicit pid mode resolved batch_ids=%s", target_batch_ids)
            else:
                target_batch_ids = _select_target_batch_ids(conn, local_db, batch_ids or [])
                if not target_batch_ids:
                    logger.info("[ListingTracking] no batch_id to process")
                    return {"batch_ids": [], "pids": [], "pid_rows": 0, "rows_prepared": 0, "rows_written": 0}
                logger.info("[ListingTracking] target batch_ids=%s", target_batch_ids)

                batch_ranges = _fetch_batch_ranges(conn, target_batch_ids)
                logger.info(
                    "[ListingTracking] fetched batch ranges batch_count=%s total_ranges=%s",
                    len(batch_ranges),
                    sum(len(ranges) for ranges in batch_ranges.values()),
                )
                for batch_id in target_batch_ids:
                    pid_rows = _fetch_pid_rows_for_batch(conn, batch_id, batch_ranges.get(batch_id, []))
                    all_pid_rows.extend(pid_rows)

                if not all_pid_rows:
                    logger.info("[ListingTracking] no pid rows resolved for batch_ids=%s", target_batch_ids)
                    return {"batch_ids": target_batch_ids, "pids": [], "pid_rows": 0, "rows_prepared": 0, "rows_written": 0}

            pid_groups = _group_pid_rows(all_pid_rows)
            model_map = _fetch_model_map(conn, sorted(pid_groups.keys()))
            worker_count = _get_pid_worker_count(len(pid_groups))
            pid_group_chunks = _chunk_pid_groups(pid_groups)
            logger.info(
                "[ListingTracking] start threaded processing total_pids=%s pid_rows=%s workers=%s pid_chunks=%s",
                len(pid_groups),
                len(all_pid_rows),
                worker_count,
                len(pid_group_chunks),
            )

        rows_prepared = 0
        write_stats_total = {"rows_written": 0, "rows_inserted": 0, "rows_updated": 0, "rows_unchanged": 0}
        writer_errors: list[Exception] = []
        skipped_reader_chunks = 0
        failed_pids: set[int] = set()
        # queue 太小会导致 reader 在 put() 阻塞，控制台长时间“无反应”；适当放大缓冲
        write_queue: Queue = Queue(maxsize=max(50, worker_count * 10))
        writer_thread = threading.Thread(
            target=_writer_loop,
            args=(write_queue, write_stats_total, writer_errors),
            name="listing-tracking-writer",
            daemon=True,
        )
        writer_thread.start()
        progress_lock = threading.Lock()
        progress = _Progress(
            started_at=t0,
            last_reader_ts=time.time(),
            last_writer_ts=time.time(),
            completed_chunks=0,
            total_chunks=len(pid_group_chunks),
            rows_prepared=0,
        )
        heartbeat_thread = threading.Thread(
            target=_progress_heartbeat_loop,
            args=(progress, progress_lock, write_queue, writer_thread, write_stats_total),
            name="listing-tracking-heartbeat",
            daemon=True,
        )
        heartbeat_thread.start()
        try:
            with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="listing-tracking-reader") as executor:
                future_map = {
                    executor.submit(_process_pid_group_chunk, pid_chunk, model_map): {
                        "idx": idx,
                        "pid_count": len(pid_chunk),
                        "pids": [int(pid) for pid, _pid_rows in pid_chunk],
                    }
                    for idx, pid_chunk in enumerate(pid_group_chunks, start=1)
                }
                completed_count = 0
                total_chunks = len(future_map)
                for future in as_completed(future_map):
                    if writer_errors:
                        raise writer_errors[0]
                    completed_count += 1
                    with progress_lock:
                        progress.completed_chunks = completed_count
                        progress.last_reader_ts = time.time()
                    try:
                        payloads, chunk_failed_pids = future.result()
                        failed_pids.update(int(pid) for pid in chunk_failed_pids if pid is not None)
                    except Exception as exc:
                        skipped_reader_chunks += 1
                        meta = future_map[future]
                        failed_pids.update(meta["pids"])
                        logger.exception(
                            "[ListingTracking] pid chunk failed after auto retry and was skipped: %s/%s chunk_index=%s pids=%s error=%s",
                            completed_count,
                            total_chunks,
                            meta["idx"],
                            meta["pid_count"],
                            exc,
                        )
                        continue
                    logger.info(
                        "[ListingTracking] pid chunk ready for write: %s/%s payloads=%s",
                        completed_count,
                        total_chunks,
                        len(payloads),
                    )
                    for payload in payloads:
                        rows_prepared += int(payload["rows_prepared"])
                        with progress_lock:
                            progress.rows_prepared = rows_prepared
                            progress.last_reader_ts = time.time()
                        logger.info(
                            "[ListingTracking] pid ready for write chunk=%s/%s pid=%s pid_rows=%s rows_prepared=%s model=%s",
                            completed_count,
                            total_chunks,
                            payload["pid"],
                            payload["pid_rows_count"],
                            payload["rows_prepared"],
                            payload["model_used"],
                        )
                        # 如果 writer 慢导致 queue 满，这里会阻塞；用超时重试并持续输出等待日志
                        while True:
                            try:
                                write_queue.put(payload, timeout=10)
                                break
                            except Exception:
                                logger.warning(
                                    "[ListingTracking] write_queue is full; waiting... queue=%s writer_alive=%s pid=%s",
                                    getattr(write_queue, "qsize", lambda: -1)(),
                                    bool(writer_thread.is_alive()),
                                    payload.get("pid"),
                                )
                                if writer_errors:
                                    raise writer_errors[0]
            write_queue.put(None)
            write_queue.join()
            writer_thread.join()
            if writer_errors:
                raise writer_errors[0]
        finally:
            if writer_thread.is_alive():
                write_queue.put(None)
                write_queue.join()
                writer_thread.join()
        result = {
            "batch_ids": target_batch_ids,
            "pids": sorted({int(row["pid"]) for row in all_pid_rows}),
            "pid_rows": len(all_pid_rows),
            "rows_prepared": rows_prepared,
            "rows_written": int(write_stats_total["rows_written"]),
            "rows_inserted": int(write_stats_total["rows_inserted"]),
            "rows_updated": int(write_stats_total["rows_updated"]),
            "rows_unchanged": int(write_stats_total["rows_unchanged"]),
            "skipped_reader_chunks": int(skipped_reader_chunks),
            "failed_pids": sorted(failed_pids),
            "elapsed_sec": round(time.time() - t0, 2),
        }
        if failed_pids:
            logger.warning(
                "[ListingTracking] failed pids for this run count=%s pids=%s",
                len(failed_pids),
                sorted(failed_pids),
            )
        else:
            logger.info("[ListingTracking] failed pids for this run count=0 pids=[]")
        logger.info("[ListingTracking] sync done: %s", result)
        return result
    finally:
        try:
            local_db.rollback()
        except Exception:
            pass
        try:
            local_db.close()
        except Exception:
            pass


def _main(argv: list[str]) -> int:
    setup_logging(level=logging.INFO)
    batch_ids, pids = _parse_cli_args(argv)
    out = sync_listing_tracking(batch_ids=batch_ids, pids=pids)
    print(out, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv))
