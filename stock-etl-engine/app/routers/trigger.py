from fastapi import APIRouter, HTTPException, Header
from fastapi import BackgroundTasks
from pydantic import BaseModel

from app.core.response import success_response
from app.core.config import ETL_API_KEY
from app.core.logger import logger


router = APIRouter(tags=["Trigger"])


class RunJobRequest(BaseModel):
    job_id: int
    job_name: str
    biz_date: str | None = None
    force: bool = False
    params: dict[str, str] | None = None


class BackfillRequest(BaseModel):
    task_id: int
    symbol: str
    data_type: str
    start_date: str | None = None
    end_date: str | None = None
    force: bool = False


class BoardSyncRequest(BaseModel):
    symbol: str
    trade_date: str | None = None


class BoardSyncBatchRequest(BaseModel):
    symbols: list[str]
    trade_date: str | None = None


async def verify_api_key(x_api_key: str = Header(None)):
    if ETL_API_KEY and x_api_key != ETL_API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")


@router.post("/run", summary="执行 ETL 任务")
def trigger_run(req: RunJobRequest, background_tasks: BackgroundTasks):
    logger.info(f"收到 ETL 触发请求: job_id={req.job_id}, job_name={req.job_name}")

    def _execute():
        from app.core.db import SessionLocal
        from app.services.job_service import JobService

        # Note: params are no longer passed via os.environ — job_service uses explicit kwargs instead.
        db = SessionLocal()
        try:
            svc = JobService(db)
            svc.run_job_task(req.job_id, req.job_name, req.biz_date, req.force)
        except SystemExit as e:
            logger.error(f"ETL 任务异常退出 job_id={req.job_id}: sys.exit({e.code})")
        except Exception as e:
            logger.error(f"ETL 执行失败 job_id={req.job_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            db.close()

    background_tasks.add_task(_execute)
    return success_response({"task_id": req.job_id, "status": "accepted"})


@router.post("/backfill", summary="执行补历史任务")
def trigger_backfill(req: BackfillRequest, background_tasks: BackgroundTasks):
    logger.info(f"收到补历史触发请求: task_id={req.task_id}, symbol={req.symbol}")

    def _execute():
        from app.core.db import SessionLocal
        from app.services.backfill_service import BackfillService

        db = SessionLocal()
        try:
            svc = BackfillService(db)
            svc.execute_backfill(req.task_id, req.symbol, req.data_type,
                                 req.start_date, req.end_date, req.force)
        except Exception as e:
            logger.error(f"补历史执行失败 task_id={req.task_id}: {e}")
        finally:
            db.close()

    background_tasks.add_task(_execute)
    return success_response({"task_id": req.task_id, "status": "accepted"})


@router.post("/board-sync", summary="同步单只股票板块")
def trigger_board_sync(req: BoardSyncRequest, background_tasks: BackgroundTasks):
    logger.info(f"收到板块同步请求: symbol={req.symbol}")

    def _execute():
        from app.core.db import SessionLocal
        from app.services.board_sync_service import BoardSyncService
        from datetime import date

        db = SessionLocal()
        try:
            trade_date = date.fromisoformat(req.trade_date) if req.trade_date else None
            svc = BoardSyncService(db)
            svc.sync_stock(req.symbol, trade_date)
        except Exception as e:
            logger.error(f"板块同步失败 symbol={req.symbol}: {e}")
        finally:
            db.close()

    background_tasks.add_task(_execute)
    return success_response({"symbol": req.symbol, "status": "accepted"})


@router.post("/board-sync-batch", summary="批量同步板块")
def trigger_board_sync_batch(req: BoardSyncBatchRequest, background_tasks: BackgroundTasks):
    logger.info(f"收到批量板块同步请求: {len(req.symbols)} 只股票")

    def _execute():
        from app.core.db import SessionLocal
        from app.services.board_sync_service import BoardSyncService
        from datetime import date

        db = SessionLocal()
        try:
            trade_date = date.fromisoformat(req.trade_date) if req.trade_date else None
            svc = BoardSyncService(db)
            svc.batch_sync(req.symbols, trade_date)
        except Exception as e:
            logger.error(f"批量板块同步失败: {e}")
        finally:
            db.close()

    background_tasks.add_task(_execute)
    return success_response({"count": len(req.symbols), "status": "accepted"})


@router.get("/health", summary="健康检查")
def health():
    return {"status": "ok", "app": "etl-engine"}
