import logging
from contextlib import asynccontextmanager
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import settings
from app.controllers import asin_router
from app.controllers.sync_controller import router as sync_router
from app.database import init_db
from app.logging_config import setup_logging
from app.services.online_sync import sync_from_online_db
from app.sync_run_record import (
    now_asia,
    is_even_hour,
    should_run_scheduled_sync,
    record_sync_run,
)

# 按日期将日志写入 app/log/YYYY-MM-DD.log
setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)

_scheduler: Optional[BackgroundScheduler] = None


def _run_scheduled_sync():
    """
    定时任务（东八区每偶数整点）：若本小时内已执行过（含手动）则跳过，否则执行 online_sync 并记录时间。
    """
    now = now_asia()
    logger.info(
        "Scheduled sync triggered at %s (Asia/Shanghai), checking should_run...",
        now.strftime("%Y-%m-%d %H:%M:%S"),
    )
    try:
        if not should_run_scheduled_sync():
            logger.info(
                "Scheduled sync skipped: already run in this even hour (Asia/Shanghai), next at next even hour."
            )
            return
        logger.info("Scheduled sync (even hour Asia/Shanghai): starting...")
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler
    logger.info("Application startup: initializing database")
    init_db()
    if settings.ENABLE_SCHEDULED_SYNC:
        try:
            _scheduler = BackgroundScheduler(timezone=settings.SYNC_TIMEZONE)
            # 东八区每偶数整点（0,2,4,...,22）执行
            _scheduler.add_job(
                _run_scheduled_sync,
                "cron",
                hour="0,2,4,6,8,10,12,14,16,18,20,22",
                minute=0,
                id="online_sync",
                misfire_grace_time=300,
            )
            _scheduler.start()
            job = _scheduler.get_job("online_sync")
            next_run = job.next_run_time if job else None
            # 若当前为东八区偶数小时且本小时内未执行过，启动时补跑一次
            now = now_asia()
            if is_even_hour(now) and should_run_scheduled_sync():
                logger.info(
                    "Scheduled sync: current hour %s is even (Asia/Shanghai), no run yet this hour — running once now.",
                    now.hour,
                )
                try:
                    _run_scheduled_sync()
                except Exception as e:
                    logger.exception("Startup sync failed: %s", e)
            logger.info(
                "Scheduled sync enabled: every 2h at :00 (even hours Asia/Shanghai), next_run_time=%s",
                next_run,
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
app.include_router(asin_router)
app.include_router(sync_router)


@app.get("/health")
def health():
    logger.debug("Health check")
    return {"status": "ok"}
