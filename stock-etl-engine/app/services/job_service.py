from __future__ import annotations

from collections.abc import Callable
from typing import Any

from sqlalchemy.orm import Session
import re
import time
import logging
from app.repositories.job_repository import JobRepository
from app.services.board_sync_service import BoardSyncService

logger = logging.getLogger("etl_engine.job_service")


# ──────────── JOB 注册表（消除 if/elif 链）───────────────

def resolve_date(biz_date: str | None) -> str:
    """解析业务日期：优先使用入参，否则取当前 CST 日期"""
    from app.core.timezone import now as dt_now
    return biz_date or dt_now().strftime('%Y-%m-%d')


class JobService:
    # 重试配置
    MAX_RETRIES = 3
    RETRY_DELAYS = [2, 4, 8]  # 指数退避：2秒、4秒、8秒

    def __init__(self, db: Session):
        self.db = db
        self.repo = JobRepository(db)

    def list_jobs(self, page: int, page_size: int, job_name: str | None = None, status: str | None = None, biz_date: str | None = None) -> dict:
        return self.repo.list_jobs(page=page, page_size=page_size, job_name=job_name, status=status, biz_date=biz_date)

    def get_job(self, job_id: int) -> dict | None:
        return self.repo.get_job(job_id)

    def get_logs(self, job_id: int, offset: int, limit: int) -> dict:
        return self.repo.get_logs(job_id, offset, limit)

    def run_job(self, job_name: str, biz_date: str | None, force: bool) -> dict:
        """兼容接口，内部调用 prepare_run_job + run_job_task"""
        task_id = self.prepare_run_job(job_name, biz_date, force).get("task_id")
        if task_id is not None:
            self.run_job_task(task_id, job_name, biz_date, force)
        return {"task_id": task_id, "job_name": job_name, "biz_date": biz_date}

    def prepare_run_job(self, job_name: str, biz_date: str | None, force: bool) -> dict:
        """创建任务记录，立即返回（不执行 ETL）"""
        job_id = self.init_job_run(job_name, biz_date)
        return {
            "task_id": job_id,
            "job_name": job_name,
            "biz_date": biz_date,
            "message": f"任务已创建，job_id={job_id}",
        }

    def run_job_task(self, task_id: int, job_name: str, biz_date: str | None, force: bool):
        """后台执行 ETL 任务（由 BackgroundTasks 调用），支持自动重试"""
        # 自动创建 etl_job_run 记录（兼容 trigger API 直接传自定义 job_id 的场景）
        if not self.repo.get_job(task_id):
            task_id = self.init_job_run(job_name, biz_date)
        self.repo.add_log(task_id, "INFO", f"开始执行 job_name={job_name}, biz_date={biz_date}, force={force}")

        last_error = None
        for attempt in range(self.MAX_RETRIES):
            try:
                self._execute_job_logic(task_id, job_name, biz_date, force)
                return  # 成功执行，直接返回
            except SystemExit as e:
                # A job called sys.exit() — treat as failure, don't let it propagate
                last_error = RuntimeError(f"Job {job_name} called sys.exit({e.code})")
                self.repo.add_log(task_id, "ERROR", f"任务异常退出（尝试 {attempt + 1}/{self.MAX_RETRIES}）: {last_error}")

                if attempt < self.MAX_RETRIES - 1:
                    delay = self.RETRY_DELAYS[attempt]
                    self.repo.add_log(task_id, "INFO", f"等待 {delay} 秒后重试...")
                    time.sleep(delay)
                else:
                    error_msg = f"任务异常退出，已重试 {self.MAX_RETRIES} 次。最后错误: {last_error}"
                    self.update_job_run(task_id, "FAILED", error_message=error_msg)
                    self.repo.add_log(task_id, "ERROR", f"任务彻底失败，已重试 {self.MAX_RETRIES} 次。错误: {last_error}")
                    self._send_alert(task_id, job_name, error_msg)
                    raise last_error

            except Exception as e:
                last_error = e
                self.repo.add_log(task_id, "ERROR", f"任务执行失败（尝试 {attempt + 1}/{self.MAX_RETRIES}）: {e}")

                if attempt < self.MAX_RETRIES - 1:
                    delay = self.RETRY_DELAYS[attempt]
                    self.repo.add_log(task_id, "INFO", f"等待 {delay} 秒后重试...")
                    time.sleep(delay)
                else:
                    # 所有重试都失败
                    error_msg = f"任务执行失败，已重试 {self.MAX_RETRIES} 次。最后错误: {e}"
                    self.update_job_run(task_id, "FAILED", error_message=error_msg)
                    self.repo.add_log(task_id, "ERROR", f"任务彻底失败，已重试 {self.MAX_RETRIES} 次。错误: {e}")
                    self._send_alert(task_id, job_name, error_msg)
                    raise

    def _execute_job_logic(self, task_id: int, job_name: str, biz_date: str | None, force: bool):
        """执行任务的核心逻辑（无重试）"""
        # Special dispatcher (custom transaction/loop logic — not suitable for registry pattern)
        if job_name.startswith("board_relation_full"):
            return self._dispatch_board_relation_full(task_id, job_name, biz_date, force)

        # ── 注册表分发：prefix → (match_fn, dispatch_fn) ──
        REGISTRY = [
            ("daily_kline",   lambda n: n.startswith("daily_kline"),   self._dispatch_daily_kline),
            ("financial",     lambda n: n.startswith("financial"),     self._dispatch_financial),
            ("factor",        lambda n: n.startswith(("factor", "compute")), self._dispatch_factor),
            ("selection",     lambda n: n.startswith("selection"),     self._dispatch_selection),
            ("cleanup",       lambda n: n.startswith("cleanup"),       self._dispatch_cleanup),
            ("trade_calendar",lambda n: n.startswith("trade_calendar"),self._dispatch_trade_calendar),
            ("adjust_factor", lambda n: n.startswith("adjust_factor"), self._dispatch_adjust_factor),
            ("security_master",lambda n: n.startswith("security_master"), self._dispatch_security_master),
            ("new_ipo_board", lambda n: n.startswith("new_ipo_board"), self._dispatch_new_ipo_board),
        ]

        for label, match_fn, dispatch_fn in REGISTRY:
            if match_fn(job_name):
                # dispatch_fn 内部自行处理 update_job_run / add_log / retry
                return dispatch_fn(task_id, job_name, biz_date, force)

        # Unknown job type — mark as FAILED (not just WARN) to be consistent with other branches.
        self.repo.add_log(task_id, "WARN", f"未知的 job_name: {job_name}")
        self.update_job_run(task_id, "FAILED", error_message=f"未知任务类型: {job_name}")

    # ── 各 Job Dispatcher（消除重复的 try/except + update_pattern）──

    def _dispatch_simple(
        self, task_id: int, job_name: str, callable_fn, success_log: str,
        rows_written: int | None = None, *, on_complete: Callable[[Any], int | None] | None = None,
    ):
        """通用执行包装：调用 + 更新状态为 COMPLETED，失败则 re-raise。

        **必须 re-raise** —— _execute_job_logic 在外层 run_job_task 中
        实现了重试/告警逻辑，吞掉异常会导致外层误判"成功"而跳过重试。

        Args:
            callable_fn: 要执行的 job callable
            on_complete: 可选回调 (result) -> rows_written, 在更新状态前调用。
                         用于需要消费返回值再决定 rows_written 的场景（如 new_ipo_board）。
        """
        try:
            if on_complete is not None and callable_fn is not None:
                result = callable_fn()
                rw = on_complete(result)
                self.update_job_run(task_id, "COMPLETED", rows_written=rw)
                self.repo.add_log(task_id, "INFO", success_log)
            elif callable_fn is not None:
                callable_fn()
                self.update_job_run(task_id, "COMPLETED", rows_written=rows_written)
                self.repo.add_log(task_id, "INFO", success_log)
        except Exception as e:
            logger.warning(f"[Job] {job_name} 执行失败: {e}")
            self.update_job_run(task_id, "FAILED", error_message=str(e))
            raise

    def _dispatch_daily_kline(self, task_id, job_name, biz_date, force):
        from app.jobs.sync_stock_daily import sync_stock_daily
        trade_date = resolve_date(biz_date)
        # sync_stock_daily 内部已通过 update_job_run(rows_written=stocks_success) 更新状态，
        # 此处不传 rows_written，避免 _dispatch_simple 覆盖为默认值 0。
        self._dispatch_simple(
            task_id, job_name,
            callable_fn=lambda: sync_stock_daily(force_restart=force, start_date=trade_date, end_date=trade_date, task_id=task_id),
            success_log="日线同步完成",
        )

    @staticmethod
    def _natural_to_bs_quarter(month: int) -> int:
        """自然月份 → Baostock API quarter 参数（1=Q1, 2=H1, 3=Q3, 4=annual）"""
        if month <= 3:
            return 1
        elif month <= 6:
            return 2
        elif month <= 9:
            return 3
        else:
            return 4

    def _dispatch_financial(self, task_id, job_name, biz_date, force):
        from app.jobs.etl_financial_indicator import main as financial_main
        from app.core.timezone import now

        # 从 job_name 解析 year/quarter（兼容手动触发与定时调度）：
        #   financial_indicator_sync_2024_1       → year=2024, quarter=1 (Q1)
        #   financial_indicator_sync_2023_2025    → start_year=2023, end_year=2025 (区间)
        #   financial                            → 定时调度：用当前季度（回退逻辑）
        m = re.search(r"financial_indicator_sync_(\d{4})_((\d{4}))?$", job_name)
        if m:
            year = int(m.group(1))
            end_year = m.group(3)  # None → 单季度；有值 → 区间同步
            if end_year is not None:
                callable_fn = lambda y=year, ey=int(end_year): financial_main(start_year=y, end_year=ey)
            else:
                bs_quarter = self._natural_to_bs_quarter(now().month)
                callable_fn = lambda y=year, q=bs_quarter: financial_main(sync_year=y, sync_quarter=q)
        else:
            # 定时调度 / 未知格式：取当前季度
            current = now()
            bs_quarter = self._natural_to_bs_quarter(current.month)
            callable_fn = lambda y=current.year, q=bs_quarter: financial_main(sync_year=y, sync_quarter=q)

        self._dispatch_simple(
            task_id, job_name,
            callable_fn=callable_fn,
            success_log="财务指标同步完成",
        )

    def _dispatch_factor(self, task_id, job_name, biz_date, force):
        from app.jobs.compute_factor import main as factor_main
        self._dispatch_simple(task_id, job_name, factor_main, "技术因子计算完成")

    def _dispatch_selection(self, task_id, job_name, biz_date, force):
        from app.jobs.build_selection_mart import main as selection_main
        self._dispatch_simple(task_id, job_name, selection_main, "选股宽表构建完成")

    def _dispatch_cleanup(self, task_id, job_name, biz_date, force):
        from app.scheduler import run_cleanup_logs_job
        self._dispatch_simple(
            task_id, job_name,
            callable_fn=lambda: run_cleanup_logs_job(task_id, job_name, biz_date, force),
            success_log="日志清理完成",
        )

    def _dispatch_trade_calendar(self, task_id, job_name, biz_date, force):
        from app.jobs.sync_trade_calendar import sync_trade_calendar
        self._dispatch_simple(
            task_id, job_name,
            callable_fn=lambda: sync_trade_calendar(start_date=biz_date if biz_date else None, end_date=None),
            success_log="交易日历同步完成",
        )

    def _dispatch_adjust_factor(self, task_id, job_name, biz_date, force):
        from app.jobs.sync_adjust_factor import main as adjust_factor_main

        # 从 job_name 解析年份: adjust_factor_sync_2024_2026 → start_year=2024, end_year=2026
        parts = job_name.split("_")
        if len(parts) >= 5 and parts[-3] == "sync":
            try:
                _start_yr = int(parts[-2])
                _end_yr = int(parts[-1])
                callable_fn = lambda sy=_start_yr, ey=_end_yr: adjust_factor_main(
                    task_id=task_id, start_year=sy, end_year=ey
                )
            except ValueError:
                callable_fn = lambda: adjust_factor_main(task_id=task_id)
        else:
            # 无年份信息，默认增量模式（近3年）
            callable_fn = lambda: adjust_factor_main(task_id=task_id)

        # on_complete: 消费 main() 返回值（total_written），避免 _dispatch_simple 覆盖为默认值 0
        self._dispatch_simple(
            task_id, job_name, callable_fn, "复权因子同步完成",
            on_complete=lambda r: int(r) if r else None,
        )

    def _dispatch_security_master(self, task_id, job_name, biz_date, force):
        from app.jobs.sync_security_master import main as security_main
        # sync_security_master returns total records written; capture it via on_complete
        self._dispatch_simple(
            task_id, job_name, callable_fn=security_main, success_log="股票主数据同步完成",
            on_complete=lambda r: int(r) if r else None,
        )

    def _dispatch_new_ipo_board(self, task_id, job_name, biz_date, force):
        """New IPO board sync — 需要消费返回值再决定 rows_written，通过 on_complete 回调处理。"""
        from app.jobs.sync_new_ipo_boards import sync_new_ipo_boards

        self._dispatch_simple(
            task_id, job_name,
            callable_fn=lambda: sync_new_ipo_boards(days=7),
            success_log="新股板块增量同步完成",
            on_complete=lambda r: r.get("boards", 0) if isinstance(r, dict) else None,
        )

    # ── 特殊 dispatcher（含自定义事务/循环逻辑，不适合注册表模式）──
    def _dispatch_board_relation_full(self, task_id, job_name, biz_date, force):
        from app.core.db import SessionLocal
        from sqlalchemy import text
        db_session = SessionLocal()
        try:
            result = db_session.execute(text("SELECT symbol FROM dwd_security_master WHERE status = 'LISTED' ORDER BY symbol"))
            batch_size = 500
            success_count = 0
            total_boards = 0

            while True:
                rows = result.fetchmany(batch_size)
                if not rows:
                    break
                for (symbol,) in rows:
                    sync_svc = BoardSyncService(db_session)
                    r = sync_svc.sync_stock(symbol)
                    if r["success"]:
                        success_count += 1
                        total_boards += r.get("boards_synced", 0)
                    time.sleep(0.2)

            self.update_job_run(task_id, "COMPLETED", rows_written=total_boards)
            self.repo.add_log(task_id, "INFO", f"全量板块关系同步完成: {success_count} ok, {total_boards} boards")
        except Exception as e:
            self.update_job_run(task_id, "FAILED", error_message=str(e))
            self.repo.add_log(task_id, "ERROR", f"全量板块关系同步失败: {e}")
        finally:
            db_session.close()

    def _send_alert(self, task_id: int, job_name: str, error_message: str):
        """
        发送告警（目前仅记录日志，后续可扩展为 webhook/邮件/Slack 等）
        """
        logger.error(f"🚨 [ALERT] 任务 {job_name} (task_id={task_id}) 执行失败: {error_message}")
        # TODO: 扩展为 webhook 告警
        # self._send_webhook_alert(task_id, job_name, error_message)

    def cancel_job(self, job_id: int) -> bool:
        return self.repo.cancel_job(job_id)

    def init_job_run(self, job_name: str, biz_date: str | None = None) -> int:
        return self.repo.init_job_run(job_name, biz_date)

    def update_job_run(
        self,
        job_id: int,
        status: str,
        rows_raw: int | None = None,
        rows_written: int | None = None,
        error_message: str | None = None,
    ):
        self.repo.update_job_run(job_id, status, rows_raw, rows_written, error_message)
