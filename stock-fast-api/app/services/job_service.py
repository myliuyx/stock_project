from sqlalchemy.orm import Session
import datetime
import logging
import httpx
from app.repositories.job_repository import JobRepository

logger = logging.getLogger("stock_api")


class JobService:
    def __init__(self, db: Session):
        self.db = db
        self.repo = JobRepository(db)

    # ── DB query helpers (unchanged) ────────────────────────────────

    def list_jobs(self, page: int, page_size: int, job_name: str | None = None,
                  status: str | None = None, biz_date: str | None = None) -> dict:
        return self.repo.list_jobs(page=page, page_size=page_size, job_name=job_name,
                                   status=status, biz_date=biz_date)

    def get_job(self, job_id: int) -> dict | None:
        return self.repo.get_job(job_id)

    def get_logs(self, job_id: int, offset: int, limit: int) -> dict:
        return self.repo.get_logs(job_id, offset, limit)

    # ── Job lifecycle helpers (unchanged) ───────────────────────────

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

    def cancel_job(self, job_id: int) -> bool:
        return self.repo.cancel_job(job_id)

    # ── trigger_etl (C1 + H1 fix — never raises, always returns dict) ─

    def trigger_etl(
        self,
        job_id: int,
        job_name: str,
        biz_date: str | None,
        force: bool,
        params: dict[str, str] | None = None,
    ) -> dict:
        """Call ETL engine /api/v1/trigger/run to start a background task.

        Always returns {"code": <int>, "message": str, "data": Any}.
        - code == 0 → accepted (ETL engine will run the job asynchronously)
        - code != 0 → failure; job status updated to FAILED in DB

        Never raises — callers should check result["code"] and respond accordingly.
        """
        from app.core.config import settings

        url = f"{settings.ETL_ENGINE_URL}/run"

        try:
            resp = httpx.post(
                url,
                json={
                    "job_id": job_id,
                    "job_name": job_name,
                    "biz_date": biz_date,
                    "force": force,
                    "params": params,
                },
                headers={"X-API-Key": settings.ETL_ENGINE_API_KEY},
                timeout=30,
            )
            resp.raise_for_status()
            body = resp.json()
            return {"code": 0, "message": body.get("message", "accepted"), "data": body}

        except httpx.HTTPStatusError as e:
            msg = f"ETL引擎返回错误 HTTP {e.response.status_code}: {e}"
            self._update_failed(job_id, msg)
            logger.error(f"[trigger_etl] {msg}")
            return {"code": 502, "message": msg, "data": None}

        except httpx.RequestError as e:
            msg = f"ETL引擎不可达 ({e.request.url}): {e}"
            self._update_failed(job_id, msg)
            logger.error(f"[trigger_etl] {msg}")
            return {"code": 503, "message": msg, "data": None}

        except Exception as e:
            msg = f"触发ETL任务异常: {e}"
            self._update_failed(job_id, msg)
            logger.exception(f"[trigger_etl] unexpected error: {msg}")
            return {"code": 500, "message": msg, "data": None}

    # ── Private helpers ─────────────────────────────────────────────

    def _update_failed(self, job_id: int, error_message: str) -> None:
        """Best-effort: update job status to FAILED in DB."""
        try:
            self.update_job_run(job_id, "FAILED", error_message=error_message)
        except Exception:
            # If we can't update the DB either, just log it.
            logger.exception(f"[_update_failed] failed to mark job {job_id} as FAILED")

    # ── Dead code removal (C2 + H2 — removed during ETL engine migration) ─
    # The following methods were replaced by HTTP→ETL-engine calls in routes and are no longer needed:
    #   - run_job_task()        → routes now use trigger_etl instead of local BackgroundTasks execution
    #   - _execute_job_logic()  → dispatch table logic for in-process job running (dead code, never called)
    #   - run_job()             → thin wrapper around prepare_run_job + trigger_etl (unused by any route)
    #   - prepare_run_job()     → callers now use init_job_run directly
