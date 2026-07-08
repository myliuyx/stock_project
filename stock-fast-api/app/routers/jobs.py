from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.core.deps import get_db
from app.core.response import success_response, error_response
from app.services.job_service import JobService
from app.schemas.job import RunJobRequest
from app.core.timezone import now as dt_now, CST
import datetime
import logging

logger = logging.getLogger("stock_api")

router = APIRouter(prefix="/jobs", tags=["Jobs"])


@router.get("", summary="获取任务列表")
def list_jobs(
    job_name: str | None = Query(None, description="任务名模糊搜索"),
    status: str | None = Query(None, description="PENDING / RUNNING / SUCCESS / FAILED / CANCELLED"),
    biz_date: str | None = Query(None, description="业务日期 YYYY-MM-DD"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    db: Session = Depends(get_db),
):
    service = JobService(db)
    data = service.list_jobs(page=page, page_size=page_size, job_name=job_name, status=status, biz_date=biz_date)
    return success_response(data)


@router.post("/sync-trade-calendar", summary="手动触发交易日历同步")
def trigger_trade_calendar_sync(
    start_date: str = Query(None, description="开始日期 YYYY-MM-DD，不传则从年初"),
    end_date: str = Query(None, description="结束日期 YYYY-MM-DD，不传则同步到今年年底+1年"),
    db: Session = Depends(get_db),
):
    import datetime

    if end_date is None:
        end_date = (dt_now() + datetime.timedelta(days=365)).strftime('%Y-%m-%d')
    if start_date is None:
        start_date = dt_now().strftime('%Y-01-01')

    logger.info(f"【手动触发】交易日历同步 start={start_date} end={end_date}")

    service = JobService(db)
    job_id = service.init_job_run(f"trade_calendar_sync_{start_date}_{end_date}", end_date)

    result = service.trigger_etl(job_id, f"trade_calendar_sync", end_date, False,
                                 params={"start_date": start_date, "end_date": end_date})
    if result["code"] != 0:
        return error_response(code=5021, message=result["message"])
    return success_response({
        "status": "triggered",
        "job_id": job_id,
        "start_date": start_date,
        "end_date": end_date,
        "message": f"交易日历同步任务已触发，job_id={job_id}",
    })


@router.post("/sync-daily", summary="手动触发日线同步")
def trigger_daily_sync(
    trade_date: str = Query(None, description="交易日期 YYYY-MM-DD"),
    force_restart: bool = Query(False, description="是否强制从头开始"),
    db: Session = Depends(get_db),
):
    import datetime
    if trade_date is None:
        trade_date = dt_now().strftime("%Y-%m-%d")

    logger.info(f"【手动触发】日线同步 trade_date={trade_date}, force={force_restart}")

    service = JobService(db)
    job_id = service.init_job_run(f"daily_kline_{trade_date}", trade_date)
    result = service.trigger_etl(job_id, f"daily_kline_{trade_date}", trade_date, force_restart)
    if result["code"] != 0:
        return error_response(code=5021, message=result["message"])

    return success_response({
        "status": "triggered",
        "job_id": job_id,
        "trade_date": trade_date,
        "force_restart": force_restart,
        "message": f"日线同步任务已触发，job_id={job_id}",
    })


@router.post("/sync-financial", summary="手动触发财务指标同步")
def trigger_financial_sync(
    year: int = Query(None, description="年份，如 2026"),
    quarter: int = Query(None, description="季度 1-4"),
    start_year: int = Query(None, description="起始年份，如 2021"),
    end_year: int = Query(None, description="结束年份，如 2026"),
    db: Session = Depends(get_db),
):
    import datetime

    # 默认同步当前季度
    if year is None or quarter is None:
        now = dt_now()
        month = now.month
        if month <= 3:
            quarter = 1
        elif month <= 6:
            quarter = 2
        elif month <= 9:
            quarter = 3
        else:
            quarter = 4
        year = now.year

    # 构造 job_name
    if start_year is not None and end_year is not None:
        job_name = f"financial_indicator_sync_{start_year}_{end_year}"
    else:
        job_name = f"financial_indicator_sync_{year}Q{quarter}"

    logger.info(f"【手动触发】财务指标同步 job={job_name}")

    # 先创建 job 记录
    service = JobService(db)
    job_id = service.init_job_run(job_name, str(year))

    params = {}
    if year is not None and quarter is not None:
        params = {"SYNC_YEAR": str(year), "SYNC_QUARTER": str(quarter)}
    elif start_year is not None and end_year is not None:
        params = {"SYNC_START_YEAR": str(start_year), "SYNC_END_YEAR": str(end_year)}
    result = service.trigger_etl(job_id, f"financial", str(year), False, params=params)
    if result["code"] != 0:
        return error_response(code=5021, message=result["message"])
    return success_response({
        "status": "triggered",
        "job_id": job_id,
        "year": year,
        "quarter": quarter,
        "start_year": start_year,
        "end_year": end_year,
        "message": f"财务指标同步任务已触发，job_id={job_id}",
    })


@router.post("/sync-factor", summary="手动触发技术因子计算")
def trigger_factor_sync(
    trade_date: str = Query(None, description="交易日期 YYYY-MM-DD"),
    full: bool = Query(False, description="是否全量重算"),
    db: Session = Depends(get_db),
):
    import datetime
    logger.info(f"【手动触发】技术因子计算 trade_date={trade_date}, full={full}")

    service = JobService(db)
    job_id = service.init_job_run(f"factor_compute_{trade_date or 'full'}", trade_date)
    result = service.trigger_etl(job_id, "factor_compute", trade_date, full)
    if result["code"] != 0:
        return error_response(code=5021, message=result["message"])

    return success_response({
        "status": "triggered",
        "job_id": job_id,
        "trade_date": trade_date,
        "full": full,
        "message": f"技术因子计算任务已触发，job_id={job_id}",
    })


@router.post("/sync-selection", summary="手动触发选股宽表构建")
def trigger_selection_sync(
    trade_date: str = Query(None, description="交易日期 YYYY-MM-DD"),
    full: bool = Query(False, description="是否全量重算"),
    db: Session = Depends(get_db),
):
    import datetime
    logger.info(f"【手动触发】选股宽表构建 trade_date={trade_date}, full={full}")

    service = JobService(db)
    job_id = service.init_job_run(f"selection_mart_{trade_date or 'full'}", trade_date)
    trigger_result = service.trigger_etl(job_id, "selection_mart", trade_date, full)
    if trigger_result["code"] != 0:
        return error_response(code=5021, message=trigger_result["message"])

    return success_response({
        "status": "triggered",
        "job_id": job_id,
        "trade_date": trade_date,
        "full": full,
        "message": f"选股宽表构建任务已触发，job_id={job_id}",
    })


@router.post("/sync-adjust-factor", summary="手动触发复权因子同步")
def trigger_adjust_factor_sync(
    start_year: int = Query(2010, ge=2000, le=2030, description="起始年份，默认 2010"),
    end_year: int = Query(None, description="结束年份，不传则用今年"),
    db: Session = Depends(get_db),
):
    import datetime

    if end_year is None:
        end_year = dt_now().year

    logger.info(f"【手动触发】复权因子同步 {start_year}-{end_year}")

    service = JobService(db)
    job_id = service.init_job_run(f"adjust_factor_sync_{start_year}_{end_year}", str(end_year))

    result = service.trigger_etl(job_id, f"adjust_factor_sync", str(end_year), False,
                                 params={"SYNC_START_YEAR": str(start_year), "SYNC_END_YEAR": str(end_year)})
    if result["code"] != 0:
        return error_response(code=5021, message=result["message"])
    return success_response({
        "status": "triggered",
        "job_id": job_id,
        "start_year": start_year,
        "end_year": end_year,
        "message": f"复权因子同步任务已触发，job_id={job_id}",
    })


@router.post("/run", summary="手工触发任务")
def run_job(
    req: RunJobRequest,
    db: Session = Depends(get_db),
):
    service = JobService(db)
    result = service.prepare_run_job(req.job_name, req.biz_date, req.force)
    task_id = result.get("task_id")

    if task_id is not None:
        trigger_result = service.trigger_etl(task_id, req.job_name, req.biz_date, req.force)
        if trigger_result["code"] != 0:
            return error_response(code=5021, message=trigger_result["message"])

    return success_response(result)


@router.get("/{job_id}", summary="获取任务详情")
def get_job_detail(job_id: int, db: Session = Depends(get_db)):
    service = JobService(db)
    data = service.get_job(job_id)
    if data is None:
        return error_response(code=4043, message="job not found")
    return success_response(data)


@router.get("/{job_id}/logs", summary="获取任务日志")
def get_job_logs(
    job_id: int,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    service = JobService(db)
    data = service.get_logs(job_id, offset, limit)
    return success_response(data)


@router.post("/{job_id}/cancel", summary="取消任务")
def cancel_job(job_id: int, db: Session = Depends(get_db)):
    service = JobService(db)
    success = service.cancel_job(job_id)
    return success_response({"cancelled": success})


@router.post("/sync-new-ipo-boards", summary="手动触发新股板块增量同步")
def trigger_new_ipo_boards_sync(
    days: int = Query(default=7, ge=1, le=30, description="查询近N天新股，默认7天"),
    db: Session = Depends(get_db),
):
    """
    触发新股板块增量同步（基于 efinance）。
    查询近 N 天内上市的新股，获取其所属板块，写入 dwd_board_master + dwd_board_relation。
    """
    import datetime

    logger.info(f"【手动触发】新股板块增量同步（近 {days} 天）")

    service = JobService(db)
    job_id = service.init_job_run(f"new_ipo_board_sync_{days}days", dt_now().strftime('%Y-%m-%d'))

    result = service.trigger_etl(job_id, f"new_ipo_board_sync", str(days), False,
                                 params={"days": str(days)})
    if result["code"] != 0:
        return error_response(code=5021, message=result["message"])
    return success_response({
        "status": "triggered",
        "job_id": job_id,
        "days": days,
        "message": f"新股板块增量同步已触发，job_id={job_id}",
    })


@router.post("/sync-board-relation-full", summary="手动触发全量板块关系同步")
def trigger_board_relation_full_sync(
    db: Session = Depends(get_db),
):
    """
    触发全量板块关系同步（基于 efinance）。
    对 dwd_security_master 中所有 LISTED 股票重新获取板块数据。
    """
    import datetime

    logger.info("【手动触发】全量板块关系同步（efinance）")

    service = JobService(db)
    job_id = service.init_job_run("board_relation_full_sync", dt_now().strftime('%Y-%m-%d'))

    result = service.trigger_etl(job_id, f"board_relation_full_sync", "", False)
    if result["code"] != 0:
        return error_response(code=5021, message=result["message"])
    return success_response({
        "status": "triggered",
        "job_id": job_id,
        "message": f"全量板块关系同步已触发，job_id={job_id}，请关注日志查看进度",
    })
