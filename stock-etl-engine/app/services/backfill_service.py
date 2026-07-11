from sqlalchemy.orm import Session
from app.repositories.backfill_repository import BackfillRepository
import logging

logger = logging.getLogger("etl_engine.backfill_service")


class BackfillService:
    def __init__(self, db: Session):
        self.db = db
        self.repo = BackfillRepository(db)

    def run_backfill(self, symbol: str, data_type: str, start_date: str | None, end_date: str | None, force: bool) -> dict:
        return self.repo.run_backfill(symbol, data_type, start_date, end_date, force)

    def get_status(self, task_id: int) -> dict:
        result = self.repo.get_status(task_id)
        if result.get("status") == "NOT_FOUND":
            result["message"] = "任务不存在"
        return result

    def execute_backfill(self, task_id: int, symbol: str, data_type: str, start_date: str | None, end_date: str | None, force: bool):
        """后台执行补历史任务"""

        self.repo.update_status(task_id, "RUNNING", progress=0)
        logger.info(f"【补历史】task_id={task_id} 开始执行 symbol={symbol} data_type={data_type}")

        try:
            if data_type.upper() == "DAILY":
                self._sync_daily_backfill(task_id, symbol, start_date, end_date, force)
            elif data_type.upper() == "FINANCE":
                self._sync_financial_backfill(task_id, symbol, start_date, end_date)
            elif data_type.upper() == "FACTOR":
                self._sync_factor_backfill(task_id, symbol, start_date, end_date)
            elif data_type.upper() == "ADJUST_FACTOR":
                self._sync_adjust_factor_backfill(task_id, symbol, start_date, end_date)
            else:
                raise ValueError(f"不支持的 data_type: {data_type}")

            self.repo.update_status(task_id, "SUCCESS", progress=100, rows_written=0)
            logger.info(f"【补历史】task_id={task_id} 执行完成")

        except Exception as e:
            logger.error(f"【补历史】task_id={task_id} 执行失败: {e}")
            self.repo.update_status(task_id, "FAILED", error_message=str(e))

    def _sync_daily_backfill(self, task_id: int, symbol: str, start_date: str | None, end_date: str | None, force: bool):
        from app.jobs.sync_stock_daily import sync_stock_daily
        self.repo.update_status(task_id, "RUNNING", progress=10)
        sync_stock_daily(
            force_restart=force,
            start_date=start_date,
            end_date=end_date,
            target_symbol=symbol,
        )
        self.repo.update_status(task_id, "RUNNING", progress=90)

    def _sync_financial_backfill(self, task_id: int, symbol: str, start_date: str | None, end_date: str | None):
        """财务回填：通过函数参数传递年份/季度，不再修改 os.environ（消除竞态）"""
        from app.jobs.etl_financial_indicator import main as financial_main

        self.repo.update_status(task_id, "RUNNING", progress=10)

        # 从日期提取年份 → 区间同步
        start_year = int(start_date[:4]) if start_date and len(start_date) >= 4 else None
        end_year = int(end_date[:4]) if end_date and len(end_date) >= 4 else None

        records_written = financial_main(
            start_year=start_year,
            end_year=end_year,
        ) or 0
        self.repo.update_status(task_id, "RUNNING", progress=90, rows_written=records_written)

    def _sync_factor_backfill(self, task_id: int, symbol: str, start_date: str | None, end_date: str | None):
        """因子回填：通过函数参数调用 compute_factor，不再伪造 sys.argv"""
        from app.jobs.compute_factor import main as factor_main

        self.repo.update_status(task_id, "RUNNING", progress=10)
        records_written = factor_main(start_date=start_date, end_date=end_date) or 0
        self.repo.update_status(task_id, "RUNNING", progress=90, rows_written=records_written)

    def _sync_adjust_factor_backfill(self, task_id: int, symbol: str, start_date: str | None, end_date: str | None):
        self.repo.update_status(task_id, "RUNNING", progress=10)
        raise NotImplementedError("ADJUST_FACTOR 同步暂未实现")

    def mark_failed(self, task_id: int, error_message: str):
        self.repo.update_status(task_id, "FAILED", error_message=error_message)
