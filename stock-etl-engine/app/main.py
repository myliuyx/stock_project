from datetime import timedelta
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.core.config import ETL_API_PORT, ETL_VERSION, validate_config
from app.core.response import success_response
from app.core.logger import logger

app = FastAPI(
    title="A股ETL引擎",
    version=ETL_VERSION,
    description="独立 ETL 任务调度与执行服务",
)


@app.middleware("http")
async def api_key_middleware(request: Request, call_next):
    from app.core.config import ETL_API_KEY

    if request.url.path in ("/health", "/"):
        return await call_next(request)

    api_key = request.headers.get("X-API-Key", "")
    if ETL_API_KEY and api_key != ETL_API_KEY:
        return JSONResponse(
            status_code=403,
            content={"code": 4003, "message": "Forbidden", "data": None},
        )
    return await call_next(request)


from app.routers import trigger
app.include_router(trigger.router, prefix="/api/v1/trigger")


scheduler = None


@app.on_event("startup")
async def startup_event():
    global scheduler

    # Fail fast on missing DB credentials instead of cryptic connection errors later
    validate_config()

    from app.scheduler import create_scheduler, acquire_scheduler_lock

    if acquire_scheduler_lock():
        scheduler = create_scheduler()
        scheduler.start()
        logger.info("ETL 引擎启动，定时任务调度器运行中")
    else:
        logger.info("跳过调度器启动（已有其他 worker 持有锁）")


@app.on_event("shutdown")
async def shutdown_event():
    global scheduler
    from app.scheduler import release_scheduler_lock

    if scheduler:
        scheduler.shutdown()
        logger.info("ETL 引擎关闭，调度器已停止")
    release_scheduler_lock()


@app.get("/", summary="健康检查")
def health_check():
    from app.scheduler import get_active_scheduler

    scheduler = get_active_scheduler()
    jobs_status = {}
    if scheduler and scheduler.running:
        for job in scheduler.get_jobs():
            next_run_str = None
            if job.next_run_time:
                # Convert to readable China time format (always treated as Asia/Shanghai)
                from datetime import timezone
                run_time = job.next_run_time
                if run_time.tzinfo is not None and run_time.utcoffset().total_seconds() != 0:
                    run_time = run_time.astimezone(timezone(timedelta(hours=8)))
                next_run_str = run_time.strftime('%Y-%m-%d %H:%M')
            jobs_status[job.id] = {
                "name": job.name,
                "next_run_at": next_run_str,
                "trigger": str(job.trigger),
            }
        return success_response({
            "status": "ok",
            "app": "A股ETL引擎",
            "version": ETL_VERSION,
            "scheduler_running": True,
            "jobs": jobs_status,
        })
    elif scheduler:
        return success_response({
            "status": "degraded",
            "app": "A股ETL引擎",
            "version": ETL_VERSION,
            "scheduler_running": False,
            "error": "Scheduler is not running",
        })
    else:
        return success_response({
            "status": "unhealthy",
            "app": "A股ETL引擎",
            "version": ETL_VERSION,
            "scheduler_running": False,
            "error": "Scheduler was not initialized (lock held by another process?)",
        })


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=ETL_API_PORT)
