from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.deps import get_db
from app.core.response import success_response, error_response
from app.services.backfill_service import BackfillService
from app.schemas.backfill import BackfillRunRequest
import logging

router = APIRouter(prefix="/backfill", tags=["Backfill"])
logger = logging.getLogger("stock_api")


@router.post("/run", summary="触发补历史任务")
def run_backfill(
    req: BackfillRunRequest,
    db: Session = Depends(get_db),
):
    service = BackfillService(db)
    result = service.run_backfill(
        symbol=req.symbol,
        data_type=req.data_type,
        start_date=req.start_date,
        end_date=req.end_date,
        force=req.force,
    )
    return success_response(result)


@router.get("/status/{task_id}", summary="查询补历史状态")
def get_backfill_status(task_id: int, db: Session = Depends(get_db)):
    service = BackfillService(db)
    data = service.get_status(task_id)
    if data.get("status") == "NOT_FOUND":
        return error_response(code=4043, message="任务不存在")
    return success_response(data)
