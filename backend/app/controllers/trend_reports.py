"""
Trend 子栏目 HTML 报表（与 CLI 脚本等价的数据源）。
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from collections import OrderedDict
from datetime import date, datetime, timedelta, timezone
from typing import Literal, Optional

from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse, JSONResponse

from app.config import settings
from app.database import SessionLocal, init_db
from app.services.daily_upload_asin_data_ds import sync_range, sync_with_default_date_range
from app.services.daily_upload_session_report_html_pst import (
    DEFAULT_LISTING_SINCE,
    build_report_payload,
    render_html,
)
from app.services.weekly_upload_asin_date_add_impression_add_ads import (
    DEFAULT_TRAFFIC_IMPRESSION_ADS_START,
    build_report_html_for_range,
)

logger = logging.getLogger(__name__)

UTC8 = timezone(timedelta(hours=8))

# 进程内内存缓存（比读盘快）；配合 Cache-Control / ETag 让浏览器可缓存 GET 响应
_report_build_lock = threading.Lock()
_session_impression_mem_lock = threading.Lock()
# end_d.isoformat() -> 最近一次成功生成的 HTML
_session_impression_html_by_end: dict[str, str] = {}
_session_impression_latest_html: str | None = None

_SESSION_IMPRESSION_CACHE_CONTROL = "private, max-age=600, stale-while-revalidate=86400"


def _weak_etag(body: str) -> str:
    h = hashlib.sha256(body.encode("utf-8")).hexdigest()[:24]
    return f'W/"{h}"'


def _read_session_impression_memory(end_d: date) -> str | None:
    k = end_d.isoformat()
    with _session_impression_mem_lock:
        if k in _session_impression_html_by_end:
            return _session_impression_html_by_end[k]
        if _session_impression_latest_html:
            return _session_impression_latest_html
    return None


def _write_session_impression_memory(end_d: date, html: str) -> None:
    with _session_impression_mem_lock:
        global _session_impression_latest_html
        _session_impression_html_by_end[end_d.isoformat()] = html
        _session_impression_latest_html = html

_SESSION_IMPRESSION_STUB_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>报表缓存占位</title>
  <style>
    body { margin: 0; font-family: system-ui, sans-serif; padding: 2rem; background: #0f1419; color: #94a3b8; }
  </style>
</head>
<body>
  <p>尚无可用缓存（内存/浏览器），请刷新页面触发重算或等待重建完成。</p>
</body>
</html>"""
_new_listing_sync_report_lock = threading.Lock()

# format=json 短 TTL 内存缓存（按日期参数 + 是否跳过同步）；本地表变化后最多滞后 TTL
_new_listing_json_cache_lock = threading.Lock()
_new_listing_json_cache: "OrderedDict[tuple, tuple[float, dict]]" = OrderedDict()


def _new_listing_json_cache_get(key: tuple) -> dict | None:
    with _new_listing_json_cache_lock:
        item = _new_listing_json_cache.get(key)
        if not item:
            return None
        exp_mono, payload = item
        if exp_mono < time.monotonic():
            try:
                del _new_listing_json_cache[key]
            except KeyError:
                pass
            return None
        _new_listing_json_cache.move_to_end(key)
        return payload


def _new_listing_json_cache_set(key: tuple, payload: dict) -> None:
    ttl = max(1, int(settings.NEW_LISTING_JSON_CACHE_TTL_SEC))
    max_keys = max(1, int(settings.NEW_LISTING_JSON_CACHE_MAX_KEYS))
    with _new_listing_json_cache_lock:
        _new_listing_json_cache[key] = (time.monotonic() + ttl, payload)
        _new_listing_json_cache.move_to_end(key)
        while len(_new_listing_json_cache) > max_keys:
            _new_listing_json_cache.popitem(last=False)


def _new_listing_json_cache_stats_payload() -> dict:
    """进程内 New Listing JSON 缓存：条数与按 UTF-8 JSON 序列化估算的体积（与 HTTP 响应体同口径）。"""
    now = time.monotonic()
    ttl_cfg = max(1, int(settings.NEW_LISTING_JSON_CACHE_TTL_SEC))
    max_keys_cfg = max(1, int(settings.NEW_LISTING_JSON_CACHE_MAX_KEYS))
    with _new_listing_json_cache_lock:
        snap = list(_new_listing_json_cache.items())
    total_bytes = 0
    active: list[dict] = []
    stale = 0
    for key, (exp_mono, payload) in snap:
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        b = len(raw.encode("utf-8"))
        total_bytes += b
        is_active = exp_mono >= now
        if not is_active:
            stale += 1
            continue
        active.append(
            {
                "cache_key": list(key),
                "ttl_remaining_sec": round(exp_mono - now, 3),
                "approx_json_bytes_utf8": b,
            }
        )
    return {
        "configured": {"ttl_sec": ttl_cfg, "max_keys": max_keys_cfg},
        "stored_slots": len(snap),
        "active_entries": len(active),
        "stale_entries_not_yet_purged": stale,
        "total_approx_json_bytes_utf8": total_bytes,
        "entries": active,
    }


router = APIRouter()


def _si_cache_headers(body: str) -> dict[str, str]:
    return {
        "Cache-Control": _SESSION_IMPRESSION_CACHE_CONTROL,
        "ETag": _weak_etag(body),
        "Vary": "Accept-Encoding",
    }


@router.get("/session-impression", response_class=HTMLResponse)
def trend_session_impression_ads_html(
    embed: bool = Query(
        False,
        description="嵌入页首屏：读进程内存缓存；带 ETag/Cache-Control 便于浏览器缓存",
    ),
    rebuild: bool = Query(False, description="为 true 时全量重算并更新内存；失败时若有旧缓存则仍返回 200 + 旧内容"),
    nocache: bool = Query(False, description="同 rebuild=1（兼容旧参数）"),
):
    """
    与 ``python -m app.services.weekly_upload_asin_date_add_impression_add_ads`` 相同数据逻辑；
    周 impression 来自 ``amazon_search``：区间内涉及的 ``week_no`` 做**整表** ``SUM(impression_count)``，
    点落在该周周三（如 202609→2026-02-25）。

    **嵌入页**请先 ``?embed=1`` 取缓存，刷新浏览器后再 ``?rebuild=1`` 后台重算（由前端控制）。
    直接打开不带参数：有缓存则秒开，无缓存则同步首次生成。
    """
    rebuild = rebuild or nocache
    end_d = datetime.now(UTC8).date()
    start_d = DEFAULT_TRAFFIC_IMPRESSION_ADS_START

    # 嵌入首屏：仅内存，不访问线上库；支持 If-None-Match → 304
    if embed and not rebuild:
        body = _read_session_impression_memory(end_d)
        if body is not None:
            logger.info("GET /api/trend/session-impression embed=1 cache=hit end_d=%s", end_d)
            return HTMLResponse(
                body,
                headers={
                    **_si_cache_headers(body),
                    "X-Session-Impression-Cache": "hit",
                },
            )
        logger.info("GET /api/trend/session-impression embed=1 cache=miss end_d=%s", end_d)
        return HTMLResponse(
            _SESSION_IMPRESSION_STUB_HTML,
            headers={
                "Cache-Control": "no-store",
                "X-Session-Impression-Cache": "miss",
            },
        )

    # 后台刷新：重算；失败则回退内存
    if rebuild:
        try:
            with _report_build_lock:
                logger.info(
                    "GET /api/trend/session-impression rebuild=1 range=%s..%s",
                    start_d,
                    end_d,
                )
                html = build_report_html_for_range(start_d, end_d)
                _write_session_impression_memory(end_d, html)
            return HTMLResponse(
                html,
                headers={
                    **_si_cache_headers(html),
                    "X-Session-Impression-Cache": "refreshed",
                },
            )
        except Exception as e:
            logger.exception("GET /api/trend/session-impression rebuild failed: %s", e)
            body = _read_session_impression_memory(end_d)
            if body is not None:
                return HTMLResponse(
                    body,
                    headers={
                        **_si_cache_headers(body),
                        "X-Session-Impression-Cache": "stale-fallback",
                        "X-Session-Impression-Build-Error": str(e)[:220],
                    },
                )
            return HTMLResponse(
                f"<!DOCTYPE html><html><body><pre>报表生成失败且无缓存: {e!s}</pre></body></html>",
                status_code=500,
            )

    # 直接访问：有内存即返回，否则同步构建
    try:
        body = _read_session_impression_memory(end_d)
        if body is not None:
            return HTMLResponse(
                body,
                headers={**_si_cache_headers(body), "X-Session-Impression-Cache": "hit"},
            )
        with _report_build_lock:
            body = _read_session_impression_memory(end_d)
            if body is not None:
                return HTMLResponse(
                    body,
                    headers={**_si_cache_headers(body), "X-Session-Impression-Cache": "hit-race"},
                )
            logger.info(
                "GET /api/trend/session-impression first build range=%s..%s",
                start_d,
                end_d,
            )
            html = build_report_html_for_range(start_d, end_d)
            _write_session_impression_memory(end_d, html)
        return HTMLResponse(
            html,
            headers={**_si_cache_headers(html), "X-Session-Impression-Cache": "built"},
        )
    except Exception as e:
        logger.exception("GET /api/trend/session-impression failed: %s", e)
        return HTMLResponse(
            f"<!DOCTYPE html><html><body><pre>报表生成失败: {e!s}</pre></body></html>",
            status_code=500,
        )


@router.get("/new-listing/json-cache-stats")
def trend_new_listing_json_cache_stats():
    """查看 ``format=json`` 进程内短 TTL 缓存当前占用（条数、估算 JSON 字节数、各 key 剩余 TTL）。"""
    return JSONResponse(content=_new_listing_json_cache_stats_payload())


@router.get("/new-listing")
def trend_new_listing_report(
    listing_since: date = Query(
        DEFAULT_LISTING_SINCE,
        description="KPI / cohort 起点；图表仅展示 open_date ≥ 该日的批次，每批次统计自上新日起 30 个日历日 sessions",
    ),
    session_start: Optional[date] = Query(
        None,
        description="图表横轴 session_date 起始；默认与 listing_since 相同",
    ),
    session_end: Optional[date] = Query(None, description="图表横轴 session_date 结束；默认今天"),
    sync_start: Optional[date] = Query(
        None,
        description="同步 listing 扫描下界（DATE(created_at)）；与 sync_end 同时传则 sync_range，否则默认增量",
    ),
    sync_end: Optional[date] = Query(None, description="同步 listing 扫描上界（含）"),
    skip_sync: Optional[bool] = Query(
        None,
        description="为 true 跳过同步；为 false 强制同步。省略时：format=json 默认跳过（首屏快），format=html 默认同步",
    ),
    response_format: Literal["html", "json"] = Query(
        "html",
        alias="format",
        description="html：内嵌完整报表；json：堆叠图 + KPI 结构化数据（横轴仅含本地有 session 的日期）",
    ),
    nocache: bool = Query(
        False,
        description="format=json 时跳过服务端进程内短缓存（与前端 ?refresh=1 配合时可传 nocache=1）",
    ),
    profile: bool = Query(
        False,
        description="为 true 时在 JSON 中返回各阶段耗时 profileTimingsSec（用于排查卡顿）",
    ),
    json_views: Literal["all", "full", "store"] = Query(
        "all",
        description="JSON 专用：all=仅 views.all+元数据（首屏）；full=全部店铺视图（兼容旧行为）；store=仅单店（配合 store_id）",
    ),
    store_id: Optional[int] = Query(
        None,
        ge=1,
        description="json_views=store 时指定店铺 id",
    ),
):
    """
    1) 同步：``daily_upload_asin_data_ds.sync_*`` 写入本地表（JSON 默认 ``skip_sync`` 以缩短首屏；需最新数据时传 ``skip_sync=false`` 或 ``format=html``）。
    2) 展示：与 ``daily_upload_session_report_html_pst`` 相同——按 ``open_date`` 分批次、每批 30 日内 ``session_date``
       聚合 sessions 堆叠柱 + 合计折线；横轴为区间内**实际有 session 数据**的日期。``format=json`` 返回同构 JSON。
    """
    end_d = session_end or date.today()
    # cohort / 上新统计不早于 PST 报表默认起点 2026-02-20
    listing_since = max(listing_since, DEFAULT_LISTING_SINCE)
    start_d = session_start or listing_since
    if start_d < listing_since:
        start_d = listing_since
    if start_d > end_d:
        return HTMLResponse(
            "<!DOCTYPE html><html><body><p>session_start 不能晚于 session_end</p></body></html>",
            status_code=400,
        )

    if (sync_start is None) ^ (sync_end is None):
        return HTMLResponse(
            "<!DOCTYPE html><html><body><p>sync_start 与 sync_end 需同时指定或同时省略</p></body></html>",
            status_code=400,
        )
    if sync_start is not None and sync_end is not None and sync_start > sync_end:
        return HTMLResponse(
            "<!DOCTYPE html><html><body><p>sync_start 不能晚于 sync_end</p></body></html>",
            status_code=400,
        )

    effective_skip_sync = (
        skip_sync if skip_sync is not None else (response_format == "json")
    )

    json_views_mode = "full"
    single_store: int | None = None
    if response_format == "json":
        if json_views == "all":
            json_views_mode = "all_only"
        elif json_views == "store":
            if store_id is None:
                return JSONResponse(
                    content={"detail": "json_views=store 时必须传 store_id"},
                    status_code=400,
                )
            json_views_mode = "store"
            single_store = int(store_id)
        else:
            json_views_mode = "full"

    if response_format == "json" and not nocache:
        cache_key = (
            listing_since.isoformat(),
            start_d.isoformat(),
            end_d.isoformat(),
            effective_skip_sync,
            json_views_mode,
            single_store,
        )
        cached = _new_listing_json_cache_get(cache_key)
        if cached is not None:
            return JSONResponse(
                content=cached,
                headers={"X-New-Listing-Server-Cache": "hit"},
            )

    try:
        # 原先整段包在锁里会把「只读 JSON」与耗时的 build_report_payload 串行化，多标签/多用户互相等待。
        # 锁仅用于 HTML 同步路径，避免并发 sync 写库重叠。
        if not effective_skip_sync and response_format == "json":
            # JSON：后台同步，不阻塞本次响应
            def _bg_sync():
                try:
                    if sync_start is not None and sync_end is not None:
                        stats = sync_range(sync_start, sync_end)
                        logger.info("GET /api/trend/new-listing bg sync_range done: %s", stats)
                    else:
                        stats = sync_with_default_date_range()
                        logger.info(
                            "GET /api/trend/new-listing bg sync_with_default_date_range done: %s",
                            stats,
                        )
                except Exception as e:
                    logger.warning(
                        "GET /api/trend/new-listing bg online sync failed (showing local data only): %s",
                        e,
                    )

            try:
                threading.Thread(target=_bg_sync, daemon=True).start()
            except Exception as e:
                logger.warning("GET /api/trend/new-listing bg sync start failed: %s", e)
        elif not effective_skip_sync:
            with _new_listing_sync_report_lock:
                init_db()
                try:
                    if sync_start is not None and sync_end is not None:
                        stats = sync_range(sync_start, sync_end)
                        logger.info("GET /api/trend/new-listing sync_range done: %s", stats)
                    else:
                        stats = sync_with_default_date_range()
                        logger.info(
                            "GET /api/trend/new-listing sync_with_default_date_range done: %s",
                            stats,
                        )
                except Exception as e:
                    logger.warning(
                        "GET /api/trend/new-listing online sync failed (showing local data only): %s",
                        e,
                    )

        init_db()
        db = SessionLocal()
        try:
            # KPI / cohort 与 amazon_listing 均须来自 online_db_host；不因 format=json 回退本地 daily_upload KPI。
            prefer_online = True
            prefer_listing_online = True
            payload = build_report_payload(
                db,
                listing_since,
                start_d,
                end_d,
                prefer_online=prefer_online,
                prefer_listing_online=prefer_listing_online,
                profile=profile,
                json_views_mode=json_views_mode
                if response_format == "json"
                else "full",
                single_store_id=single_store if response_format == "json" else None,
            )
        finally:
            db.close()
        if response_format == "json":
            headers: dict[str, str] = {}
            if not effective_skip_sync:
                headers["X-New-Listing-Sync"] = "triggered-in-background"
            if not nocache:
                _new_listing_json_cache_set(
                    (
                        listing_since.isoformat(),
                        start_d.isoformat(),
                        end_d.isoformat(),
                        effective_skip_sync,
                        json_views_mode,
                        single_store,
                    ),
                    payload,
                )
                headers["X-New-Listing-Server-Cache"] = "miss"
            else:
                headers["X-New-Listing-Server-Cache"] = "bypass"
            return JSONResponse(content=payload, headers=headers)
        return HTMLResponse(render_html(payload))
    except Exception as e:
        logger.exception("GET /api/trend/new-listing failed: %s", e)
        return HTMLResponse(
            f"<!DOCTYPE html><html><body><pre>New Listing 报表失败: {e!s}</pre></body></html>",
            status_code=500,
        )
