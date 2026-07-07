from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.core.deps import get_db
from app.core.response import success_response, error_response
from app.services.board_service import BoardService

router = APIRouter(prefix="/boards", tags=["Boards"])


@router.get("", summary="获取板块列表")
def list_boards(
    board_type: str | None = Query(None, description="INDUSTRY / CONCEPT / INDEX / AREA"),
    keyword: str | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    service = BoardService(db)
    data = service.list_boards(board_type=board_type, keyword=keyword, page=page, page_size=page_size)
    return success_response(data)


@router.get("/{board_code}", summary="获取板块详情")
def get_board(board_code: str, db: Session = Depends(get_db)):
    service = BoardService(db)
    data = service.get_board(board_code)
    if data is None:
        return error_response(code=4042, message="board not found")
    return success_response(data)


@router.get("/{board_code}/members", summary="获取板块成分股")
def get_board_members(
    board_code: str,
    trade_date: str | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    sort_by: str = Query(default="change_pct"),
    sort_order: str = Query(default="desc"),
    db: Session = Depends(get_db),
):
    service = BoardService(db)
    data = service.get_members(board_code, trade_date=trade_date, page=page, page_size=page_size, sort_by=sort_by, sort_order=sort_order)
    return success_response(data)


class SyncRequest(BaseModel):
    """同步请求"""
    symbols: list[str]
    trade_date: str | None = None


@router.post("/sync", summary="同步单只股票板块数据")
def sync_board(symbol: str):
    from app.core.config import settings
    import httpx
    try:
        resp = httpx.post(
            f"{settings.ETL_ENGINE_URL}/board-sync",
            json={"symbol": symbol},
            headers={"X-API-Key": settings.ETL_ENGINE_API_KEY},
            timeout=30,
        )
        return success_response(resp.json().get("data", {}))
    except httpx.RequestError as e:
        return error_response(code=5001, message=f"同步失败: {e}")


@router.post("/sync/batch", summary="批量同步股票板块数据")
def sync_boards_batch(req: SyncRequest):
    from app.core.config import settings
    import httpx
    try:
        resp = httpx.post(
            f"{settings.ETL_ENGINE_URL}/board-sync-batch",
            json={"symbols": req.symbols, "trade_date": req.trade_date},
            headers={"X-API-Key": settings.ETL_ENGINE_API_KEY},
            timeout=120,
        )
        return success_response(resp.json().get("data", {}))
    except httpx.RequestError as e:
        return error_response(code=5001, message=f"批量同步失败: {e}")
