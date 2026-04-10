import logging
import threading
from contextlib import asynccontextmanager
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.middleware.gzip import GZipMiddleware

from app.config import settings
from app.online_engine import dispose_all_online_engines
from app.controllers import asin_router, ads_router
from app.controllers.asin_controller import get_trend_data
from app.controllers.sync_controller import router as sync_router
from app.controllers.trend_reports import router as trend_reports_router
from app.views.asin_schemas import TrendResponse
from app.database import SessionLocal, init_db
from app.logging_config import setup_logging
from app.services.online_sync import sync_from_online_db
from app.services.groupA_impression import (
    sync_group_a_impression,
    _get_sync_date_range,
    _date_to_week_no,
)
from app.services.auto_monitor import sync_auto_monitor
from app.services.listing_tracking import sync_listing_tracking_recent_weeks_scheduled
from app.services.daily_upload_asin_data import sync_with_default_date_range
from app.services.daily_upload_asin_data_ds import run_daily_upload_ds_scheduled
from app.services.daily_ad_cost_sales import run_daily_ad_cost_sales_scheduled
from app.services.daily_upload_session_report_html import (
    DEFAULT_LISTING_SINCE,
    build_report_payload,
    render_html,
    write_daily_upload_session_report_file,
)
from app.sync_run_record import (
    now_asia,
    is_n_hour_slot,
    should_run_monitor_sync,
    should_run_scheduled_sync,
    record_monitor_run,
    record_sync_run,
    should_run_listing_tracking_sync,
    record_listing_tracking_run,
    should_run_daily_upload_ds_sync,
    should_run_daily_ad_cost_sales_sync,
)

# 按日期将日志写入 app/log/YYYY-MM-DD.log
setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)

_scheduler: Optional[BackgroundScheduler] = None


def _run_scheduled_sync():
    """
    定时任务（东八区每 N 小时整点）：若本小时内已执行过（含手动）则跳过，否则执行 online_sync 并记录时间。
    """
    now = now_asia()
    logger.info(
        "Scheduled sync triggered at %s (Asia/Shanghai), checking should_run...",
        now.strftime("%Y-%m-%d %H:%M:%S"),
    )
    try:
        if not should_run_scheduled_sync():
            logger.info(
                "Scheduled sync skipped: already run in this hour slot (Asia/Shanghai)."
            )
            return
        logger.info("Scheduled sync (every %sh Asia/Shanghai): starting...", settings.SYNC_INTERVAL_HOURS)
        out = sync_from_online_db()
        record_sync_run()
        logger.info(
            "Scheduled sync done: fetched=%s, inserted=%s, updated=%s, step2_error=%s",
            out.get("rows_fetched_from_online"),
            out.get("rows_inserted"),
            out.get("rows_updated"),
            out.get("step2_error"),
        )
    except Exception as e:
        logger.exception("Scheduled sync failed: %s", e)


def _run_monitor_daily_track():
    """
    定时任务（东八区每 6 小时整点：0/6/12/18）：
    若本小时内已执行过则跳过，否则为 operation_status=1 的父 ASIN 自动补齐自 operated_at 起的周数据，
    并同步 checked_status，用于 Monitor 页面展示周完成状态。
    """
    now = now_asia()
    logger.info(
        "[Monitor] Scheduled track triggered at %s (Asia/Shanghai), checking should_run...",
        now.strftime("%Y-%m-%d %H:%M:%S"),
    )
    try:
        if not should_run_monitor_sync(interval_hours=6):
            logger.info("[Monitor] Scheduled track skipped: already run in this hour (Asia/Shanghai).")
            return
        logger.info("[Monitor] Scheduled track (every 6h Asia/Shanghai): starting auto monitor sync.")
        out = sync_auto_monitor()
        record_monitor_run()
        logger.info("[Monitor] Daily track done: %s", out)
    except Exception as e:
        logger.exception("[Monitor] Daily track failed: %s", e)


def _run_scheduled_group_a_sync():
    """
    定时任务（东八区每 N 小时整点）：执行 Group A 同步。
    week_no 计算逻辑与 groupA_impression.py 命令行默认行为一致。
    """
    now = now_asia()
    logger.info(
        "[GroupA] Scheduled sync triggered at %s (Asia/Shanghai)",
        now.strftime("%Y-%m-%d %H:%M:%S"),
    )
    try:
        date_start_str, date_end_str = _get_sync_date_range()
        date_end_d = now.date() + timedelta(days=1)
        reference_date = date_end_d - timedelta(days=1)
        wk_str, _ = _date_to_week_no(reference_date)
        logger.info(
            "[GroupA] Scheduled sync starting: date_start=%s date_end=%s reference_date=%s -> week_no=%s",
            date_start_str,
            date_end_str,
            reference_date.strftime("%Y-%m-%d"),
            wk_str,
        )
        out = sync_group_a_impression(wk_str)
        logger.info("[GroupA] Scheduled sync done: week_no=%s result=%s", wk_str, out)
    except Exception as e:
        logger.exception("[GroupA] Scheduled sync failed: %s", e)


_listing_tracking_lock = threading.Lock()
_daily_upload_sync_lock = threading.Lock()


def _run_scheduled_daily_upload_and_report():
    """
    东八区每 2 小时整点：按 daily_upload_asin_data 默认 listing 日区间同步，
    再导出与 `python -m app.services.daily_upload_session_report_html --out ./charts/report4.html` 等价的 HTML。
    """
    if not _daily_upload_sync_lock.acquire(blocking=False):
        logger.info("[DailyUpload] scheduled job skipped: previous run still in progress")
        return
    try:
        logger.info("[DailyUpload] Scheduled sync + report starting")
        out = sync_with_default_date_range()
        logger.info("[DailyUpload] Scheduled sync done: %s", out)
        report_path = Path(__file__).resolve().parent.parent / "charts" / "report4.html"
        write_daily_upload_session_report_file(report_path)
        logger.info("[DailyUpload] Report exported to %s", report_path)
    except Exception as e:
        logger.exception("[DailyUpload] Scheduled sync + report failed: %s", e)
    finally:
        try:
            _daily_upload_sync_lock.release()
        except Exception:
            pass


def _run_scheduled_daily_upload_ds():
    """
    定时：按 settings.daily_upload_ds_cron_hours()（.env：daily_upload_ds_first_run_hour / daily_times 等）在 SYNC_TIMEZONE 整点执行；
    listing 区间为东八区「当日-35 天」至当日（与无参 CLI 一致），再执行 daily_upload_asin_data_ds 同步。
    """
    run_daily_upload_ds_scheduled(force=False)


def _run_scheduled_daily_ad_cost_sales():
    """按 .env：daily_ad_cost_sales_first_run_* / daily_times 在 SYNC_TIMEZONE 执行广告日花费/销售额 gap 同步。"""
    run_daily_ad_cost_sales_scheduled(force=False)


def _run_scheduled_listing_tracking():
    """
    定时任务（ListingTracking）：
    - 依据本地 listing_tracking 表内所有 pid
    - 回填最近 N 周（不含当前周）的所有字段
    """
    if not _listing_tracking_lock.acquire(blocking=False):
        logger.info("[ListingTracking] scheduled job skipped: previous run still in progress")
        return
    try:
        recent_weeks = int(settings.LISTING_TRACKING_RECENT_WEEKS or 2)
        logger.info("[ListingTracking] Scheduled sync starting: recent_weeks=%s", recent_weeks)
        out = sync_listing_tracking_recent_weeks_scheduled(recent_weeks=recent_weeks)
        record_listing_tracking_run()
        logger.info("[ListingTracking] Scheduled sync done: %s", out)
    except Exception as e:
        logger.exception("[ListingTracking] Scheduled sync failed: %s", e)
    finally:
        try:
            _listing_tracking_lock.release()
        except Exception:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler
    logger.info("Application startup: initializing database")
    init_db()
    if settings.ENABLE_SCHEDULED_SYNC:
        try:
            _scheduler = BackgroundScheduler(timezone=settings.SYNC_TIMEZONE)
            sync_interval = max(1, int(settings.SYNC_INTERVAL_HOURS or 4))
            sync_hours = ",".join(str(h) for h in range(0, 24, sync_interval))
            # Online Sync：东八区每 N 小时整点执行一次
            _scheduler.add_job(
                _run_scheduled_sync,
                "cron",
                hour=sync_hours,
                minute=0,
                id="online_sync",
                misfire_grace_time=300,
            )
            monitor_interval = max(1, int(settings.MONITOR_SYNC_INTERVAL_HOURS or 12))
            monitor_hours = ",".join(str(h) for h in range(0, 24, monitor_interval))
            # Monitor：东八区每 N 小时整点执行一次
            _scheduler.add_job(
                _run_monitor_daily_track,
                "cron",
                hour=monitor_hours,
                minute=0,
                id="monitor_daily_track",
                misfire_grace_time=300,
            )
            group_a_interval = max(1, int(settings.GROUP_A_SYNC_INTERVAL_HOURS or 8))
            group_a_hours = ",".join(str(h) for h in range(0, 24, group_a_interval))
            # Group A：东八区每 N 小时整点执行一次
            _scheduler.add_job(
                _run_scheduled_group_a_sync,
                "cron",
                hour=group_a_hours,
                minute=0,
                id="group_a_sync",
                misfire_grace_time=300,
            )

            # ListingTracking：每天均匀执行若干次（minute=0）
            listing_times = max(1, int(settings.LISTING_TRACKING_DAILY_TIMES or 1))
            if listing_times == 1:
                listing_hours_list = [0]
            else:
                listing_hours_list = sorted({int(round(i * 24 / listing_times)) % 24 for i in range(listing_times)})
                # 去重后不足时，补到下一个整点
                i = 0
                while len(listing_hours_list) < listing_times and i < 24:
                    candidate = (listing_hours_list[-1] + 1 + i) % 24 if listing_hours_list else i
                    if candidate not in listing_hours_list:
                        listing_hours_list.append(candidate)
                    i += 1
                listing_hours_list = sorted(listing_hours_list)[:listing_times]
            listing_hours = ",".join(str(h) for h in listing_hours_list)
            _scheduler.add_job(
                _run_scheduled_listing_tracking,
                "cron",
                hour=listing_hours,
                minute=0,
                id="listing_tracking_recent_weeks",
                misfire_grace_time=300,
            )
            daily_upload_next_run = None
            if settings.ENABLE_DAILY_UPLOAD_SCHEDULE:
                daily_upload_hours = ",".join(str(h) for h in range(0, 24, 2))
                _scheduler.add_job(
                    _run_scheduled_daily_upload_and_report,
                    "cron",
                    hour=daily_upload_hours,
                    minute=0,
                    id="daily_upload_asin_data_and_report",
                    misfire_grace_time=300,
                )
            ds_hours_list, ds_schedule_desc = settings.daily_upload_ds_cron_hours()
            ds_next_run = None
            if settings.ENABLE_DAILY_UPLOAD_DS_SCHEDULE:
                daily_upload_ds_hours = ",".join(str(h) for h in ds_hours_list)
                _scheduler.add_job(
                    _run_scheduled_daily_upload_ds,
                    "cron",
                    hour=daily_upload_ds_hours,
                    minute=0,
                    id="daily_upload_asin_data_ds",
                    # 整点略有延迟或进程短暂卡住时仍补跑（小时点来自 .env first_run_hour / daily_times）
                    misfire_grace_time=3600,
                )
            ad_slots: list[tuple[int, int]] = []
            ad_schedule_desc = ""
            ad_next_run = None
            if settings.ENABLE_DAILY_AD_COST_SALES_SCHEDULE:
                ad_slots, ad_schedule_desc = settings.daily_ad_cost_sales_cron_slots()
                for idx, (ad_h, ad_m) in enumerate(ad_slots):
                    _scheduler.add_job(
                        _run_scheduled_daily_ad_cost_sales,
                        "cron",
                        hour=ad_h,
                        minute=ad_m,
                        id=f"daily_ad_cost_sales_slot_{idx}",
                        misfire_grace_time=3600,
                    )
            _scheduler.start()
            job = _scheduler.get_job("online_sync")
            next_run = job.next_run_time if job else None
            monitor_job = _scheduler.get_job("monitor_daily_track")
            monitor_next_run = monitor_job.next_run_time if monitor_job else None
            group_a_job = _scheduler.get_job("group_a_sync")
            group_a_next_run = group_a_job.next_run_time if group_a_job else None
            listing_job = _scheduler.get_job("listing_tracking_recent_weeks")
            listing_next_run = listing_job.next_run_time if listing_job else None
            if settings.ENABLE_DAILY_UPLOAD_SCHEDULE:
                daily_upload_job = _scheduler.get_job("daily_upload_asin_data_and_report")
                daily_upload_next_run = daily_upload_job.next_run_time if daily_upload_job else None
            if settings.ENABLE_DAILY_UPLOAD_DS_SCHEDULE:
                ds_job = _scheduler.get_job("daily_upload_asin_data_ds")
                ds_next_run = ds_job.next_run_time if ds_job else None
            if settings.ENABLE_DAILY_AD_COST_SALES_SCHEDULE and ad_slots:
                ad0 = _scheduler.get_job("daily_ad_cost_sales_slot_0")
                ad_next_run = ad0.next_run_time if ad0 else None
            # 若当前为东八区命中 online sync 的小时窗口且本小时内未执行过，启动时补跑一次
            now = now_asia()
            if is_n_hour_slot(now, sync_interval) and should_run_scheduled_sync():
                logger.info(
                    "Scheduled sync: current hour %s matches %sh window (Asia/Shanghai), no run yet this hour — running once in background.",
                    now.hour,
                    sync_interval,
                )
                try:
                    t = threading.Thread(target=_run_scheduled_sync, daemon=True)
                    t.start()
                except Exception as e:
                    logger.exception("Startup sync failed: %s", e)
            if is_n_hour_slot(now, monitor_interval) and should_run_monitor_sync(interval_hours=monitor_interval):
                logger.info(
                    "[Monitor] Scheduled track: current hour %s matches %sh window (Asia/Shanghai) — running once in background.",
                    now.hour,
                    monitor_interval,
                )
                try:
                    t = threading.Thread(target=_run_monitor_daily_track, daemon=True)
                    t.start()
                except Exception as e:
                    logger.exception("[Monitor] Startup track failed: %s", e)
            if now.hour % settings.GROUP_A_SYNC_INTERVAL_HOURS == 0:
                logger.info(
                    "[GroupA] Scheduled sync: current hour %s matches %sh window (Asia/Shanghai) — running once in background.",
                    now.hour,
                    settings.GROUP_A_SYNC_INTERVAL_HOURS,
                )
                try:
                    t = threading.Thread(target=_run_scheduled_group_a_sync, daemon=True)
                    t.start()
                except Exception as e:
                    logger.exception("[GroupA] Startup sync failed: %s", e)
            if now.hour in listing_hours_list and should_run_listing_tracking_sync():
                logger.info(
                    "[ListingTracking] Startup recent-weeks sync: current hour %s matches %s — running once in background.",
                    now.hour,
                    listing_hours_list,
                )
                try:
                    t = threading.Thread(target=_run_scheduled_listing_tracking, daemon=True)
                    t.start()
                except Exception as e:
                    logger.exception("[ListingTracking] Startup recent-weeks sync failed: %s", e)
            if (
                settings.ENABLE_DAILY_UPLOAD_DS_SCHEDULE
                and now.hour in ds_hours_list
                and should_run_daily_upload_ds_sync()
            ):
                logger.info(
                    "[DailyUploadDS] Startup catch-up: current hour %s is a scheduled slot (Asia/Shanghai), "
                    "no run yet this hour — running once in background.",
                    now.hour,
                )
                try:
                    t = threading.Thread(target=_run_scheduled_daily_upload_ds, daemon=True)
                    t.start()
                except Exception as e:
                    logger.exception("[DailyUploadDS] Startup catch-up failed: %s", e)
            if (
                settings.ENABLE_DAILY_AD_COST_SALES_SCHEDULE
                and ad_slots
                and (now.hour, now.minute) in ad_slots
                and should_run_daily_ad_cost_sales_sync()
            ):
                logger.info(
                    "[DailyAdCostSales] Startup catch-up: current time matches slot %02d:%02d (%s), "
                    "no run yet this minute — running once in background.",
                    now.hour,
                    now.minute,
                    settings.SYNC_TIMEZONE,
                )
                try:
                    t = threading.Thread(target=_run_scheduled_daily_ad_cost_sales, daemon=True)
                    t.start()
                except Exception as e:
                    logger.exception("[DailyAdCostSales] Startup catch-up failed: %s", e)
            logger.info(
                "Scheduled sync enabled: every %sh at :00 (Asia/Shanghai), next_run_time=%s",
                sync_interval,
                next_run,
            )
            logger.info(
                "[Monitor] Scheduled track enabled: every %sh at :00 (Asia/Shanghai), next_run_time=%s",
                monitor_interval,
                monitor_next_run,
            )
            logger.info(
                "[GroupA] Scheduled sync enabled: every %sh at :00 (Asia/Shanghai), next_run_time=%s",
                settings.GROUP_A_SYNC_INTERVAL_HOURS,
                group_a_next_run,
            )
            logger.info(
                "[ListingTracking] Scheduled sync enabled: daily_times=%s at hours=%s (Asia/Shanghai), next_run_time=%s",
                listing_times,
                listing_hours_list,
                listing_next_run,
            )
            if settings.ENABLE_DAILY_UPLOAD_SCHEDULE:
                logger.info(
                    "[DailyUpload] Scheduled sync + report enabled: every 2h at :00 (Asia/Shanghai), next_run_time=%s",
                    daily_upload_next_run,
                )
            else:
                logger.info("[DailyUpload] Scheduled sync + report disabled (enable_daily_upload_schedule=false)")
            if settings.ENABLE_DAILY_UPLOAD_DS_SCHEDULE:
                logger.info(
                    "[DailyUploadDS] Scheduled sync enabled: %s at :00 (%s), hours=%s, next_run_time=%s",
                    ds_schedule_desc,
                    settings.SYNC_TIMEZONE,
                    ds_hours_list,
                    ds_next_run,
                )
            else:
                logger.info("[DailyUploadDS] Scheduled sync disabled (enable_daily_upload_ds_schedule=false)")
            if settings.ENABLE_DAILY_AD_COST_SALES_SCHEDULE and ad_slots:
                logger.info(
                    "[DailyAdCostSales] Scheduled sync enabled: %s (%s), slots=%s, next_run_time=%s",
                    ad_schedule_desc,
                    settings.SYNC_TIMEZONE,
                    ad_slots,
                    ad_next_run,
                )
            else:
                logger.info(
                    "[DailyAdCostSales] Scheduled sync disabled (enable_daily_ad_cost_sales_schedule=false)"
                )
        except Exception as e:
            logger.warning("Scheduled sync not started: %s", e)
            _scheduler = None
    else:
        logger.info("Scheduled sync disabled (enable_scheduled_sync=false)")
    yield
    if _scheduler:
        try:
            _scheduler.shutdown(wait=False)
        except Exception:
            pass
        _scheduler = None
    try:
        dispose_all_online_engines()
    except Exception:
        pass
    logger.info("Application shutdown")


app = FastAPI(
    title="ASIN Performances API",
    lifespan=lifespan,
)


@app.exception_handler(Exception)
def unhandled_exception_handler(request: Request, exc: Exception):
    """保证 500 等未处理异常也返回 JSON，便于前端展示原因。"""
    logger.exception("Unhandled exception: %s", exc)
    return JSONResponse(
        status_code=500,
        content={"detail": f"{type(exc).__name__}: {exc!s}"},
    )


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://frontend:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# 压缩 JSON/HTML 等文本响应，减小 New Listing 等大 payload 传输体积
app.add_middleware(GZipMiddleware, minimum_size=1024)
app.include_router(asin_router)
app.include_router(ads_router)
app.include_router(sync_router)
app.include_router(trend_reports_router, prefix="/api/trend")
app.add_api_route(
    "/api/trend",
    get_trend_data,
    methods=["GET"],
    response_model=TrendResponse,
)


@app.get("/health")
def health():
    logger.debug("Health check")
    return {"status": "ok"}


@app.get("/reports/daily-upload-sessions", response_class=HTMLResponse)
def daily_upload_sessions_report(
    listing_since: date = DEFAULT_LISTING_SINCE,
    session_start: Optional[date] = None,
    session_end: Optional[date] = None,
):
    """
    交互式 HTML：按店铺筛选堆叠 session 柱图；KPI 为 listing_since 以来去重 ASIN 与 active 数。
    session_end 默认当天；session_start 默认与 listing_since 相同。
    """
    end_d = session_end or date.today()
    start_d = session_start or listing_since
    if start_d > end_d:
        return HTMLResponse(
            "<p>session_start 不能晚于 session_end</p>",
            status_code=400,
        )
    db = SessionLocal()
    try:
        payload = build_report_payload(db, listing_since, start_d, end_d)
        return render_html(payload)
    finally:
        db.close()
