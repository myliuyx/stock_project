from sqlalchemy.orm import Session
from app.repositories.backfill_repository import BackfillRepository
import logging
import httpx

logger = logging.getLogger("stock_api")


class BackfillService:
    def __init__(self, db: Session):
        self.db = db
        self.repo = BackfillRepository(db)

    def run_backfill(self, symbol: str, data_type: str, start_date: str | None,
                     end_date: str | None, force: bool) -> dict:
        result = self.repo.run_backfill(symbol, data_type, start_date, end_date, force)
        task_id = result.get("task_id")
        if task_id:
            self._trigger_backfill(task_id, symbol, data_type, start_date, end_date, force)
        return result

    def get_status(self, task_id: int) -> dict:
        result = self.repo.get_status(task_id)
        if result.get("status") == "NOT_FOUND":
            result["message"] = "任务不存在"
        return result

    def _trigger_backfill(self, task_id: int, symbol: str, data_type: str,
                          start_date: str | None, end_date: str | None, force: bool):
        from app.core.config import settings
        url = f"{settings.ETL_ENGINE_URL}/backfill"
        try:
            httpx.post(
                url,
                json={"task_id": task_id, "symbol": symbol, "data_type": data_type,
                      "start_date": start_date, "end_date": end_date, "force": force},
                headers={"X-API-Key": settings.ETL_ENGINE_API_KEY},
                timeout=5,
            )
        except httpx.RequestError as e:
            logger.error(f"调用 ETL 引擎补历史失败: {e}")
