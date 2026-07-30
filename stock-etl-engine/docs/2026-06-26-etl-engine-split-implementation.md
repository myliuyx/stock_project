# ETL 引擎拆分实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `stock-fast-api` 中的定时任务/ETL 代码抽离为独立 `stock-etl-engine` 项目

**Architecture:** 两个独立 FastAPI 服务共享一个 PostgreSQL，通过内网 HTTP 通信。主应用保留对外 API，ETL 引擎负责定时调度 + 任务执行。

**Tech Stack:** FastAPI / APScheduler / PostgreSQL (psycopg2 + SQLAlchemy) / baostock / efinance

---

## 文件变更总览

### 新项目 `stock-etl-engine/`（新建 24 个文件）

| 文件 | 来源 | 说明 |
|------|------|------|
| `app/__init__.py` | 空文件 | 包标记 |
| `app/main.py` | 新建 | FastAPI 入口 + 调度器生命周期 |
| `app/scheduler.py` | 从主应用搬入，适配 | 替换 `is_trade_day()` 连接方式 |
| `app/core/__init__.py` | 新建 | 统一导出 |
| `app/core/config.py` | 新建 | 精简版 DB 配置 |
| `app/core/db.py` | 新建 | SQLAlchemy engine |
| `app/core/logger.py` | 从主应用搬入 | — |
| `app/core/response.py` | 从主应用搬入 | — |
| `app/jobs/__init__.py` | 新建 | 包标记 |
| `app/jobs/*.py` (10 个) | 从主应用搬入 | 零改动 |
| `app/routers/__init__.py` | 新建 | 包标记 |
| `app/routers/trigger.py` | 新建 | HTTP 接收端点 |
| `app/services/__init__.py` | 新建 | 包标记 |
| `app/services/job_service.py` | 从主应用搬入 | 完整搬入 |
| `app/services/backfill_service.py` | 从主应用搬入 | 完整搬入 |
| `app/services/board_sync_service.py` | 从主应用搬入 | 完整搬入 |
| `app/repositories/__init__.py` | 新建 | 包标记 |
| `app/repositories/*.py` (3 个) | 从主应用搬入 | 完整搬入 |
| `requirements.txt` | 新建 | — |
| `Dockerfile` | 新建 | 参考主应用 |
| `.env.example` | 新建 | — |
| `.gitignore` | 新建 | — |

### 主应用 `stock-fast-api/`（修改 8 个文件，删除 13 个文件）

| 操作 | 文件 | 说明 |
|------|------|------|
| 修改 | `app/main.py` | 移除调度器代码 |
| 修改 | `app/core/config.py` | 新增 ETL 引擎连接配置 |
| 修改 | `app/routers/jobs.py` | POST 端点改为 HTTP 调用 |
| 修改 | `app/routers/backfill.py` | POST /run 改为 HTTP 调用 |
| 修改 | `app/routers/boards.py` | POST /sync 改为 HTTP 调用 |
| 修改 | `app/services/job_service.py` | 精简，加 HTTP 客户端方法 |
| 修改 | `app/services/backfill_service.py` | 精简，加 HTTP 客户端方法 |
| 修改 | `app/repositories/job_repository.py` | 只保留读方法 |
| 删除 | `app/scheduler.py` | — |
| 删除 | `app/jobs/` (整个目录 10 个文件) | — |
| 删除 | `app/services/board_sync_service.py` | — |
| 删除 | `app/repositories/board_sync_repository.py` | — |
| 删除 | `app/repositories/backfill_repository.py` 写方法 | 只保留 `run_backfill`, `get_status` |

---

## 第一阶段：创建 ETL 引擎项目

### Task 1: 创建 core 层

**Files:**
- Create: `stock-etl-engine/app/core/__init__.py`
- Create: `stock-etl-engine/app/core/config.py`
- Create: `stock-etl-engine/app/core/db.py`
- Create: `stock-etl-engine/app/core/logger.py`
- Create: `stock-etl-engine/app/core/response.py`

- [ ] **Step 1: Create `app/core/config.py`**

```python
import os


DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", 5432)),
    "dbname": os.environ.get("DB_NAME", "stock_cache_system"),
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", ""),
}

LOG_DIR = os.environ.get("SYNC_LOG_DIR", "/app/logs")
ETL_API_PORT = int(os.environ.get("ETL_API_PORT", 8082))
ETL_API_KEY = os.environ.get("ETL_ENGINE_API_KEY", "")
```

- [ ] **Step 2: Create `app/core/db.py`**

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import DB_CONFIG

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)

engine = create_engine(DATABASE_URL, pool_size=5, max_overflow=10)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
```

- [ ] **Step 3: Create `app/core/logger.py`**

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger("etl_engine")
```

- [ ] **Step 4: Create `app/core/response.py`**

```python
def success_response(data=None, message="success"):
    return {"code": 0, "message": message, "data": data}


def error_response(code: int = 9999, message: str = "error", data=None):
    return {"code": code, "message": message, "data": data}
```

- [ ] **Step 5: Create `app/core/__init__.py`**

```python
from app.core.config import DB_CONFIG, LOG_DIR, ETL_API_PORT, ETL_API_KEY
from app.core.db import engine, SessionLocal
from app.core.logger import logger
from app.core.response import success_response, error_response
```

- [ ] **Step 6: Verify**

```bash
cd /home/shaomai/agent_work/stock_project/stock-etl-engine
python -c "from app.core import DB_CONFIG, logger; logger.info('core ok')"
```

---

### Task 2: 搬入 jobs 目录

**Files:**
- Copy 10 files from `stock-fast-api/app/jobs/` to `stock-etl-engine/app/jobs/`
- Create: `stock-etl-engine/app/jobs/__init__.py`

- [ ] **Step 1: Copy all ETL scripts**

```bash
cd /home/shaomai/agent_work/stock_project
cp stock-fast-api/app/jobs/sync_stock_daily.py stock-etl-engine/app/jobs/
cp stock-fast-api/app/jobs/sync_security_master.py stock-etl-engine/app/jobs/
cp stock-fast-api/app/jobs/sync_board.py stock-etl-engine/app/jobs/
cp stock-fast-api/app/jobs/sync_board_relation.py stock-etl-engine/app/jobs/
cp stock-fast-api/app/jobs/sync_new_ipo_boards.py stock-etl-engine/app/jobs/
cp stock-fast-api/app/jobs/sync_trade_calendar.py stock-etl-engine/app/jobs/
cp stock-fast-api/app/jobs/sync_adjust_factor.py stock-etl-engine/app/jobs/
cp stock-fast-api/app/jobs/etl_financial_indicator.py stock-etl-engine/app/jobs/
cp stock-fast-api/app/jobs/compute_factor.py stock-etl-engine/app/jobs/
cp stock-fast-api/app/jobs/build_selection_mart.py stock-etl-engine/app/jobs/
```

- [ ] **Step 2: Create `app/jobs/__init__.py`**

Empty file (just `# ETL job modules` comment).

- [ ] **Step 3: Verify each file compiles**

```bash
cd /home/shaomai/agent_work/stock_project/stock-etl-engine
for f in app/jobs/*.py; do python -m py_compile "$f" && echo "OK: $f"; done
```

---

### Task 3: 搬入 services

**Files:**
- Copy 3 files from `stock-fast-api/app/services/` to `stock-etl-engine/app/services/`
- Create: `stock-etl-engine/app/services/__init__.py`

- [ ] **Step 1: Copy service files**

```bash
cd /home/shaomai/agent_work/stock_project
cp stock-fast-api/app/services/job_service.py stock-etl-engine/app/services/
cp stock-fast-api/app/services/backfill_service.py stock-etl-engine/app/services/
cp stock-fast-api/app/services/board_sync_service.py stock-etl-engine/app/services/
```

- [ ] **Step 2: Create `app/services/__init__.py`**

Empty file.

- [ ] **Step 3: Adjust imports in copied services**

Edit `stock-etl-engine/app/services/board_sync_service.py`:
- Keep import `from app.repositories.board_sync_repository import BoardSyncRepository` — stays the same

The services reference `app.repositories.*` and `app.core.db` — these will exist in the ETL engine project.

- [ ] **Step 4: Verify compilation**

```bash
cd /home/shaomai/agent_work/stock_project/stock-etl-engine
for f in app/services/*.py; do python -m py_compile "$f" && echo "OK: $f"; done
```

---

### Task 4: 搬入 repositories

**Files:**
- Copy 3 files from `stock-fast-api/app/repositories/` to `stock-etl-engine/app/repositories/`
- Create: `stock-etl-engine/app/repositories/__init__.py`

- [ ] **Step 1: Copy repository files**

```bash
cd /home/shaomai/agent_work/stock_project
cp stock-fast-api/app/repositories/job_repository.py stock-etl-engine/app/repositories/
cp stock-fast-api/app/repositories/backfill_repository.py stock-etl-engine/app/repositories/
cp stock-fast-api/app/repositories/board_sync_repository.py stock-etl-engine/app/repositories/
```

- [ ] **Step 2: Check imports**

`job_repository.py` uses:
- `from app.utils.pagination import paginate` — this is used in `list_jobs`. The ETL engine may not need `list_jobs` (it's a read operation for the main app). Actually, looking at the code in `job_repository.py`, the `paginate` is NOT actually used — the code does manual pagination with `LIMIT/OFFSET`. Let me verify...

Actually looking at `stock-fast-api/app/repositories/job_repository.py` line 4: `from app.utils.pagination import paginate` but `paginate` is never called in the file! It's an unused import. So removing it has no effect.

For the ETL engine version, just remove that import line.

- [ ] **Step 3: Create `app/repositories/__init__.py`**

Empty file.

- [ ] **Step 4: Verify compilation**

```bash
cd /home/shaomai/agent_work/stock_project/stock-etl-engine
python -m py_compile app/repositories/job_repository.py
python -m py_compile app/repositories/backfill_repository.py
python -m py_compile app/repositories/board_sync_repository.py
```

---

### Task 5: 创建 scheduler.py

**Files:**
- Create: `stock-etl-engine/app/scheduler.py`（从主应用搬入并修改 `is_trade_day()`）

- [ ] **Step 1: Create `app/scheduler.py`**

Copy `stock-fast-api/app/scheduler.py` then modify the `is_trade_day()` function.

关键改动：
1. `is_trade_day()` 从 `from app.core.db import engine` 改为原生 `psycopg2` 连接
2. `LOG_DIR` 改为 `from app.core.config import LOG_DIR`
3. 移除对 `app.jobs.*` 的 import，改为相对 import

```python
from datetime import datetime, timedelta
import logging
import os
import fcntl
import psycopg2

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import DB_CONFIG, LOG_DIR


SCHEDULER_LOCK_FILE = "/tmp/etl_engine_scheduler.lock"
_scheduler_lock_fd = None


def acquire_scheduler_lock() -> bool:
    global _scheduler_lock_fd
    try:
        _scheduler_lock_fd = open(SCHEDULER_LOCK_FILE, 'w')
        fcntl.flock(_scheduler_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _scheduler_lock_fd.write(str(os.getpid()))
        _scheduler_lock_fd.flush()
        return True
    except (IOError, OSError):
        _scheduler_lock_fd = None
        return False


def release_scheduler_lock():
    global _scheduler_lock_fd
    if _scheduler_lock_fd is not None:
        try:
            fcntl.flock(_scheduler_lock_fd, fcntl.LOCK_UN)
            _scheduler_lock_fd.close()
        except (IOError, OSError):
            pass
        _scheduler_lock_fd = None


logger = logging.getLogger("etl_engine.scheduler")

LOG_KEEP_DAYS = 3


def is_trade_day() -> bool:
    """检查今天是否是交易日（使用原生 psycopg2 连接）"""
    today = datetime.now().strftime('%Y-%m-%d')
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT is_open FROM dwd_trade_calendar WHERE trade_date = %s AND exchange = 'SH'",
            (today,)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row:
            return row[0] is True
        return False
    except Exception as e:
        logger.warning(f"检查交易日失败: {e}")
        return False


def cleanup_old_logs():
    """清理超过 3 天的日志文件"""
    if not os.path.exists(LOG_DIR):
        return

    log_dir_real = os.path.realpath(LOG_DIR)
    cutoff = datetime.now() - timedelta(days=LOG_KEEP_DAYS)
    removed = 0
    for fname in os.listdir(LOG_DIR):
        if not fname.startswith("sync_stock_daily_") or not fname.endswith(".log"):
            continue
        fpath = os.path.join(LOG_DIR, fname)
        fpath_real = os.path.realpath(fpath)
        if not fpath_real.startswith(log_dir_real + os.sep):
            logger.warning(f"跳过路径外的文件: {fpath}")
            continue
        if not os.path.isfile(fpath_real):
            continue
        mtime = datetime.fromtimestamp(os.path.getmtime(fpath_real))
        if mtime < cutoff:
            os.remove(fpath_real)
            removed += 1
    if removed:
        logger.info(f"已清理 {removed} 个过期日志文件（保留最近 {LOG_KEEP_DAYS} 天）")


def run_cleanup_logs_job(task_id: int, job_name: str, biz_date: str | None, force: bool):
    logger.info(f"日志清理任务开始")
    try:
        cleanup_old_logs()
        logger.info(f"日志清理任务完成")
    except Exception as e:
        logger.error(f"日志清理任务失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


def run_daily_sync():
    """包装函数：执行日线同步任务"""
    from app.jobs.sync_stock_daily import sync_stock_daily

    logger.info("=" * 60)
    logger.info(f"【定时任务】日线行情同步开始 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if not is_trade_day():
        logger.info("跳过非交易日")
        return

    try:
        today = datetime.now().strftime('%Y-%m-%d')
        sync_stock_daily(force_restart=False, start_date=today, end_date=today)
        logger.info(f"【定时任务】日线行情同步完成")
    except Exception as e:
        logger.error(f"日线同步失败: {e}")
        import traceback
        logger.error(traceback.format_exc())


def run_factor_compute():
    """包装函数：执行技术因子计算任务"""
    from app.jobs.compute_factor import main as factor_main
    import sys

    logger.info("=" * 60)
    logger.info(f"【定时任务】技术因子计算开始 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if not is_trade_day():
        logger.info("跳过非交易日")
        return

    try:
        sys.argv = ['compute_factor.py']
        factor_main()
        logger.info(f"【定时任务】技术因子计算完成")
    except Exception as e:
        logger.error(f"技术因子计算失败: {e}")
        import traceback
        logger.error(traceback.format_exc())


def run_selection_mart():
    """包装函数：执行选股宽表构建任务"""
    from app.jobs.build_selection_mart import main as selection_main
    import sys

    logger.info("=" * 60)
    logger.info(f"【定时任务】选股宽表构建开始 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if not is_trade_day():
        logger.info("跳过非交易日")
        return

    try:
        sys.argv = ['build_selection_mart.py']
        selection_main()
        logger.info(f"【定时任务】选股宽表构建完成")
    except Exception as e:
        logger.error(f"选股宽表构建失败: {e}")
        import traceback
        logger.error(traceback.format_exc())


def run_security_master_sync():
    """包装函数：执行股票主数据同步任务"""
    from app.jobs.sync_security_master import main as sync_security_master_main
    logger.info("=" * 60)
    logger.info(f"【定时任务】股票主数据同步开始")
    try:
        sync_security_master_main()
        logger.info(f"【定时任务】股票主数据同步完成")
    except Exception as e:
        logger.error(f"股票主数据同步失败: {e}")
        import traceback
        logger.error(traceback.format_exc())


def run_new_ipo_board_sync():
    """包装函数：执行新股板块增量同步任务"""
    from app.jobs.sync_new_ipo_boards import sync_new_ipo_boards
    logger.info("=" * 60)
    logger.info(f"【定时任务】新股板块增量同步开始")
    try:
        sync_new_ipo_boards(days=7)
        logger.info(f"【定时任务】新股板块增量同步完成")
    except Exception as e:
        logger.error(f"新股板块增量同步失败: {e}")
        import traceback
        logger.error(traceback.format_exc())


def create_scheduler() -> BackgroundScheduler:
    """创建并配置调度器（使用北京时间）"""
    scheduler = BackgroundScheduler(timezone='Asia/Shanghai')

    scheduler.add_job(
        func=run_security_master_sync,
        trigger=CronTrigger(hour=18, minute=0, day_of_week="0-4", timezone='Asia/Shanghai'),
        id="security_master_sync",
        name="股票主数据同步",
        replace_existing=True,
        misfire_grace_time=60 * 60,
        max_instances=3,
    )

    scheduler.add_job(
        func=run_new_ipo_board_sync,
        trigger=CronTrigger(hour=22, minute=0, day_of_week="0-4", timezone='Asia/Shanghai'),
        id="new_ipo_board_sync",
        name="新股板块增量同步",
        replace_existing=True,
        misfire_grace_time=60 * 60,
        max_instances=3,
    )

    scheduler.add_job(
        func=run_daily_sync,
        trigger=CronTrigger(hour=19, minute=0, day_of_week="0-4", timezone='Asia/Shanghai'),
        id="daily_stock_sync",
        name="日线行情同步",
        replace_existing=True,
        misfire_grace_time=60 * 60,
        max_instances=3,
    )

    scheduler.add_job(
        func=run_factor_compute,
        trigger=CronTrigger(hour=20, minute=30, day_of_week="0-4", timezone='Asia/Shanghai'),
        id="factor_compute",
        name="技术因子计算",
        replace_existing=True,
        misfire_grace_time=60 * 60,
        max_instances=3,
    )

    scheduler.add_job(
        func=run_selection_mart,
        trigger=CronTrigger(hour=21, minute=30, day_of_week="0-4", timezone='Asia/Shanghai'),
        id="selection_mart",
        name="选股宽表构建",
        replace_existing=True,
        misfire_grace_time=60 * 60,
        max_instances=3,
    )

    scheduler.add_job(
        func=cleanup_old_logs,
        trigger=CronTrigger(hour=0, minute=5, timezone='Asia/Shanghai'),
        id="cleanup_logs",
        name="日志清理",
        replace_existing=True,
    )

    logger.info("定时任务已注册")
    return scheduler
```

- [ ] **Step 2: Verify compilation**

```bash
cd /home/shaomai/agent_work/stock_project/stock-etl-engine
python -m py_compile app/scheduler.py
```

---

### Task 6: 创建 trigger 路由

**Files:**
- Create: `stock-etl-engine/app/routers/__init__.py`
- Create: `stock-etl-engine/app/routers/trigger.py`

- [ ] **Step 1: Create `app/routers/trigger.py`**

```python
import os
from fastapi import APIRouter, Depends, HTTPException, Header
from fastapi import BackgroundTasks
from pydantic import BaseModel
from typing import Any

from app.core.response import success_response, error_response
from app.core.config import ETL_API_KEY
from app.core.logger import logger


router = APIRouter(tags=["Trigger"])


# ── 请求模型 ──────────────────────────────────────────────


class RunJobRequest(BaseModel):
    job_id: int
    job_name: str
    biz_date: str | None = None
    force: bool = False
    params: dict[str, str] | None = None  # 传递给 ETL 脚本的环境变量


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


# ── API Key 校验 ──────────────────────────────────────────


async def verify_api_key(x_api_key: str = Header(None)):
    if ETL_API_KEY and x_api_key != ETL_API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")


# ── 端点 ──────────────────────────────────────────────────


@router.post("/run", summary="执行 ETL 任务")
def trigger_run(req: RunJobRequest, background_tasks: BackgroundTasks):
    """主应用触发 ETL 任务执行"""
    logger.info(f"收到 ETL 触发请求: job_id={req.job_id}, job_name={req.job_name}")

    def _execute():
        from app.core.db import SessionLocal
        from app.services.job_service import JobService
        import os

        # 设置传入的环境变量（如 SYNC_YEAR, SYNC_START_YEAR 等）
        old_env = {}
        if req.params:
            for k, v in req.params.items():
                old_env[k] = os.environ.get(k)
                os.environ[k] = v

        db = SessionLocal()
        try:
            svc = JobService(db)
            svc.run_job_task(req.job_id, req.job_name, req.biz_date, req.force)
        except Exception as e:
            logger.error(f"ETL 执行失败 job_id={req.job_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            # 恢复环境变量
            if req.params:
                for k in req.params:
                    if old_env.get(k) is not None:
                        os.environ[k] = old_env[k]
                    else:
                        os.environ.pop(k, None)
            db.close()

    background_tasks.add_task(_execute)
    return success_response({"task_id": req.job_id, "status": "accepted"})


@router.post("/backfill", summary="执行补历史任务")
def trigger_backfill(req: BackfillRequest, background_tasks: BackgroundTasks):
    """主应用触发补历史任务执行"""
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
    """主应用触发板块同步"""
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
    """主应用触发批量板块同步"""
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
```

- [ ] **Step 2: Verify compilation**

```bash
cd /home/shaomai/agent_work/stock_project/stock-etl-engine
python -m py_compile app/routers/trigger.py
```

---

### Task 7: 创建 main.py

**Files:**
- Create: `stock-etl-engine/app/main.py`

- [ ] **Step 1: Create `app/main.py`**

```python
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import logging

from app.core.config import ETL_API_PORT
from app.core.logger import logger

app = FastAPI(
    title="A股ETL引擎",
    version="1.0.0",
    description="独立 ETL 任务调度与执行服务",
)


# API Key 校验中间件
@app.middleware("http")
async def api_key_middleware(request: Request, call_next):
    from app.core.config import ETL_API_KEY

    # 健康检查不校验
    if request.url.path.endswith("/health"):
        return await call_next(request)

    api_key = request.headers.get("X-API-Key", "")
    if ETL_API_KEY and api_key != ETL_API_KEY:
        return JSONResponse(
            status_code=403,
            content={"code": 4003, "message": "Forbidden", "data": None},
        )
    return await call_next(request)


# 注册路由
from app.routers import trigger
app.include_router(trigger.router, prefix="/api/v1/trigger")


# 调度器生命周期
scheduler = None


@app.on_event("startup")
async def startup_event():
    global scheduler
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
    return {"status": "ok", "app": "A股ETL引擎", "version": "1.0.0"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=ETL_API_PORT)
```

- [ ] **Step 2: Verify compilation**

```bash
cd /home/shaomai/agent_work/stock_project/stock-etl-engine
python -m py_compile app/main.py
```

---

### Task 8: 创建项目配置文件

**Files:**
- Create: `stock-etl-engine/requirements.txt`
- Create: `stock-etl-engine/Dockerfile`
- Create: `stock-etl-engine/.env.example`
- Create: `stock-etl-engine/.gitignore`

- [ ] **Step 1: Create `requirements.txt`**

```txt
fastapi
uvicorn[standard]
pydantic
sqlalchemy
psycopg2-binary
apscheduler
baostock
efinance
pandas
numpy
python-dotenv
tenacity
```

- [ ] **Step 2: Create `Dockerfile`**

```dockerfile
FROM python:3.11-slim as builder

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

COPY requirements.txt .
RUN pip install --root-user-action=ignore -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

FROM python:3.11-slim

RUN groupadd -r appgroup && useradd -r -g appgroup appuser
WORKDIR /app

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY . .

RUN chown -R appuser:appgroup /app

RUN mkdir -p /usr/local/lib/python3.11/site-packages/efinance/data && \
    chown -R appuser:appgroup /usr/local/lib/python3.11/site-packages/efinance/data

USER appuser
EXPOSE 8082

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8082/')" || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8082"]
```

- [ ] **Step 3: Create `.env.example`**

```txt
DB_HOST=192.168.3.31
DB_PORT=5432
DB_NAME=stock_cache_system
DB_USER=postgres
DB_PASSWORD=
ETL_API_PORT=8082
ETL_ENGINE_API_KEY=
SYNC_LOG_DIR=/app/logs
```

- [ ] **Step 4: Create `.gitignore`**

```txt
__pycache__/
*.pyc
.env
venv/
logs/*.log
```

---

## 第二阶段：改造主应用

### Task 9: 删除 scheduler 和 jobs 目录

**Files:**
- Delete: `stock-fast-api/app/scheduler.py`
- Delete: `stock-fast-api/app/jobs/` (entire directory)

- [ ] **Step 1: Delete scheduler.py and jobs/**

```bash
cd /home/shaomai/agent_work/stock_project
rm stock-fast-api/app/scheduler.py
rm -rf stock-fast-api/app/jobs/
```

- [ ] **Step 2: Remove board_sync_service and board_sync_repository**

```bash
rm stock-fast-api/app/services/board_sync_service.py
rm stock-fast-api/app/repositories/board_sync_repository.py
```

- [ ] **Step 3: Verify remaining files compile**

```bash
cd /home/shaomai/agent_work/stock_project/stock-fast-api
python -m py_compile app/main.py 2>&1 || echo "Expected - will be fixed in next tasks"
```

(Expected to fail until imports are cleaned up)

---

### Task 10: 简化 repositories

**Files:**
- Modify: `stock-fast-api/app/repositories/job_repository.py`
- Modify: `stock-fast-api/app/repositories/backfill_repository.py`

- [ ] **Step 1: Simplify `job_repository.py`**

Remove unused import `from app.utils.pagination import paginate`. Keep all methods as-is (both read and write methods are kept, because the main app still calls `init_job_run()` and `update_job_run()` before delegating to ETL engine).

Actually, looking at the design doc more carefully: the main app keeps `init_job_run`, `update_job_run`, `add_log` methods. So keep the full file, just remove the `paginate` import.

```python
from sqlalchemy.orm import Session
from sqlalchemy import text


class JobRepository:
    def __init__(self, db: Session):
        self.db = db

    # ... keep all methods unchanged, just remove the unused paginate import
```

Simply delete line 4: `from app.utils.pagination import paginate`

- [ ] **Step 2: Verify**

```bash
cd /home/shaomai/agent_work/stock_project/stock-fast-api
python -m py_compile app/repositories/job_repository.py
```

---

### Task 11: 重写 services 为 HTTP 客户端

**Files:**
- Modify: `stock-fast-api/app/services/job_service.py`
- Modify: `stock-fast-api/app/services/backfill_service.py`

- [ ] **Step 1: Rewrite `job_service.py`**

Remove execution methods (`run_job_task`, `_execute_job_logic`, `_send_alert`). Add `trigger_etl()` and keep read/init methods.

```python
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

    def list_jobs(self, page: int, page_size: int, job_name: str | None = None,
                  status: str | None = None, biz_date: str | None = None) -> dict:
        return self.repo.list_jobs(page=page, page_size=page_size, job_name=job_name,
                                   status=status, biz_date=biz_date)

    def get_job(self, job_id: int) -> dict | None:
        return self.repo.get_job(job_id)

    def get_logs(self, job_id: int, offset: int, limit: int) -> dict:
        return self.repo.get_logs(job_id, offset, limit)

    def cancel_job(self, job_id: int) -> bool:
        return self.repo.cancel_job(job_id)

    def init_job_run(self, job_name: str, biz_date: str | None = None) -> int:
        return self.repo.init_job_run(job_name, biz_date)

    def update_job_run(self, job_id: int, status: str, rows_raw: int | None = None,
                       rows_written: int | None = None, error_message: str | None = None):
        self.repo.update_job_run(job_id, status, rows_raw, rows_written, error_message)

    def trigger_etl(self, job_id: int, job_name: str, biz_date: str | None,
                    force: bool, params: dict[str, str] | None = None) -> dict:
        """通过 HTTP 调用 ETL 引擎执行任务"""
        from app.core.config import settings
        url = f"{settings.ETL_ENGINE_URL}/run"
        try:
            resp = httpx.post(
                url,
                json={"job_id": job_id, "job_name": job_name,
                      "biz_date": biz_date, "force": force, "params": params},
                headers={"X-API-Key": settings.ETL_ENGINE_API_KEY},
                timeout=5,
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.RequestError as e:
            logger.error(f"调用 ETL 引擎失败: {e}")
            self.update_job_run(job_id, "FAILED", error_message=f"ETL引擎不可达: {e}")
            return {"code": -1, "message": f"ETL引擎不可达: {e}"}

    # 兼容旧接口调用
    def run_job(self, job_name: str, biz_date: str | None, force: bool) -> dict:
        task_id = self.init_job_run(job_name, biz_date)
        self.trigger_etl(task_id, job_name, biz_date, force)
        return {"task_id": task_id, "job_name": job_name, "biz_date": biz_date}

    def prepare_run_job(self, job_name: str, biz_date: str | None, force: bool) -> dict:
        job_id = self.init_job_run(job_name, biz_date)
        return {"task_id": job_id, "job_name": job_name, "biz_date": biz_date,
                "message": f"任务已创建，job_id={job_id}"}

    def add_log(self, job_id: int, level: str, message: str):
        self.repo.add_log(job_id, level, message)
```

- [ ] **Step 2: Rewrite `backfill_service.py`**

Remove execution methods; add HTTP trigger.

```python
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
        # 先创建任务记录
        result = self.repo.run_backfill(symbol, data_type, start_date, end_date, force)
        task_id = result.get("task_id")
        if task_id:
            # 通知 ETL 引擎执行
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
```

- [ ] **Step 3: Verify compilation**

```bash
cd /home/shaomai/agent_work/stock_project/stock-fast-api
python -m py_compile app/services/job_service.py
python -m py_compile app/services/backfill_service.py
```

---

### Task 12: 修改路由层

**Files:**
- Modify: `stock-fast-api/app/routers/jobs.py`
- Modify: `stock-fast-api/app/routers/backfill.py`
- Modify: `stock-fast-api/app/routers/boards.py`

- [ ] **Step 1: Modify `routers/jobs.py`**

Change the POST trigger endpoints. Two patterns:

**Pattern A — 无 job 追踪（sync-daily, sync-factor, sync-selection）：直接调用 ETL 引擎**

Replace `background_tasks.add_task(_run_sync)` with `service.trigger_etl(...)`:

```python
@router.post("/sync-daily", summary="手动触发日线同步")
def trigger_daily_sync(
    trade_date: str = Query(None, description="交易日期 YYYY-MM-DD"),
    force_restart: bool = Query(False, description="是否强制从头开始"),
):
    import datetime
    if trade_date is None:
        trade_date = datetime.datetime.now().strftime("%Y-%m-%d")

    logger.info(f"【手动触发】日线同步 trade_date={trade_date}, force={force_restart}")

    # 直接调用 ETL 引擎（无 job 追踪）
    from app.core.config import settings
    import httpx
    try:
        httpx.post(
            f"{settings.ETL_ENGINE_URL}/run",
            json={"job_id": 0, "job_name": f"daily_kline_{trade_date}",
                  "biz_date": trade_date, "force": force_restart},
            headers={"X-API-Key": settings.ETL_ENGINE_API_KEY},
            timeout=5,
        )
    except httpx.RequestError as e:
        logger.error(f"调用 ETL 引擎失败: {e}")

    return success_response({
        "status": "triggered",
        "trade_date": trade_date,
        "force_restart": force_restart,
        "message": "日线同步任务已触发",
    })
```

**Pattern B — 有 job 追踪（sync-trade-calendar, sync-financial, sync-adjust-factor, sync-new-ipo-boards, sync-board-relation-full, /run）：先 init_job_run 再触发 ETL**

Example for sync-trade-calendar:

```python
@router.post("/sync-trade-calendar", summary="手动触发交易日历同步")
def trigger_trade_calendar_sync(
    start_date: str = Query(None, description="开始日期 YYYY-MM-DD"),
    end_date: str = Query(None, description="结束日期 YYYY-MM-DD"),
    db: Session = Depends(get_db),
):
    import datetime
    if end_date is None:
        end_date = (datetime.datetime.now() + datetime.timedelta(days=365)).strftime('%Y-%m-%d')
    if start_date is None:
        start_date = datetime.datetime.now().strftime('%Y-01-01')

    logger.info(f"【手动触发】交易日历同步 start={start_date} end={end_date}")

    service = JobService(db)
    job_id = service.init_job_run(f"trade_calendar_sync_{start_date}_{end_date}", end_date)
    service.trigger_etl(job_id, "trade_calendar_sync", end_date, False)

    return success_response({
        "status": "triggered",
        "job_id": job_id,
        "start_date": start_date,
        "end_date": end_date,
        "message": f"交易日历同步任务已触发，job_id={job_id}",
    })
```

Same pattern for all other job-tracked endpoints.

For the `/run` generic endpoint:

```python
@router.post("/run", summary="手工触发任务")
def run_job(
    req: RunJobRequest,
    db: Session = Depends(get_db),
):
    service = JobService(db)
    result = service.prepare_run_job(req.job_name, req.biz_date, req.force)
    task_id = result.get("task_id")
    if task_id:
        service.trigger_etl(task_id, req.job_name, req.biz_date, req.force)
    return success_response(result)
```

Note: Remove all `background_tasks.add_task()` calls — they are no longer needed since ETL engine handles async execution itself.

- [ ] **Step 2: Modify `routers/backfill.py`**

```python
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
```

Remove the `_run_backfill` function and `background_tasks.add_task(_run_backfill)` — `BackfillService.run_backfill()` now handles both DB insert and HTTP trigger synchronously.

Also remove `BackgroundTasks` import from backfill.py.

- [ ] **Step 3: Modify `routers/boards.py`**

Change POST /sync and POST /sync/batch to call ETL engine:

```python
@router.post("/sync", summary="同步单只股票板块数据")
def sync_board(symbol: str):
    from app.core.config import settings
    import httpx

    try:
        resp = httpx.post(
            f"{settings.ETL_ENGINE_URL}/board-sync",
            json={"symbol": symbol},
            headers={"X-API-Key": settings.ETL_ENGINE_API_KEY},
            timeout=5,
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
            timeout=30,
        )
        return success_response(resp.json().get("data", {}))
    except httpx.RequestError as e:
        return error_response(code=5001, message=f"批量同步失败: {e}")
```

Remove import of `BoardSyncService` from the file.

- [ ] **Step 4: Verify compilation**

```bash
cd /home/shaomai/agent_work/stock_project/stock-fast-api
python -m py_compile app/routers/jobs.py
python -m py_compile app/routers/backfill.py
python -m py_compile app/routers/boards.py
```

---

### Task 13: 修改 main.py 和 config.py

**Files:**
- Modify: `stock-fast-api/app/main.py`
- Modify: `stock-fast-api/app/core/config.py`

- [ ] **Step 1: Modify `app/main.py`**

Remove scheduler-related imports and start/shop logic:

```python
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import logging

from app.core.config import settings, Settings
from app.core.exceptions import BizException
from app.core.response import error_response

logger = logging.getLogger("stock_api")

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="A股股票信息缓存系统 FastAPI 后端",
)

# CORS 中间件（仅在配置了 CORS_ORIGINS 时启用）
if settings.cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
elif settings.CORS_ORIGINS == "*":
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# 注册限流中间件
from app.middleware.rate_limit import RateLimitMiddleware
app.add_middleware(RateLimitMiddleware)

# 注册路由
from app.routers import auth, dashboard, selection, stocks, jobs, coverage, boards, backfill, system, watchlist, strategy

# ─── 全局异常处理器 ────────────────────────────────────────────
@app.exception_handler(BizException)
async def biz_exception_handler(request: Request, exc: BizException):
    code = exc.code
    if 1001 <= code <= 1999:
        status_code = 401
    elif 4001 <= code <= 4999:
        status_code = 400
    elif 4041 <= code <= 4049:
        status_code = 404
    elif 5001 <= code <= 5999:
        status_code = 500
    else:
        status_code = 200

    return JSONResponse(
        status_code=status_code,
        content=error_response(code=exc.code, message=exc.message, data=None),
    )

# ─── 注册路由 ──────────────────────────────────────────────────
app.include_router(auth.router, prefix=settings.API_PREFIX)
app.include_router(dashboard.router, prefix=settings.API_PREFIX)
app.include_router(selection.router, prefix=settings.API_PREFIX)
app.include_router(stocks.router, prefix=settings.API_PREFIX)
app.include_router(jobs.router, prefix=settings.API_PREFIX)
app.include_router(coverage.router, prefix=settings.API_PREFIX)
app.include_router(boards.router, prefix=settings.API_PREFIX)
app.include_router(backfill.router, prefix=settings.API_PREFIX)
app.include_router(system.router, prefix=settings.API_PREFIX)
app.include_router(watchlist.router, prefix=settings.API_PREFIX)
app.include_router(strategy.router, prefix=settings.API_PREFIX)

# ─── 启动事件 ────────────────────────────────────────────
@app.on_event("startup")
async def startup_event():
    settings.validate()
    logger.info("应用已启动")

# ─── 健康检查 ──────────────────────────────────────────────────
@app.get("/", summary="健康检查")
def health_check():
    return {"status": "ok", "app": settings.APP_NAME, "version": settings.APP_VERSION}
```

Key changes from original:
- Removed line: `from app.scheduler import create_scheduler, acquire_scheduler_lock, release_scheduler_lock`
- Removed `scheduler = None` global
- Removed scheduler start/shutdown from startup/shutdown events
- Removed `shutdown_event` entirely (nothing to clean up)

- [ ] **Step 2: Modify `app/core/config.py`**

Add ETL engine connection settings:

```python
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    APP_NAME: str = "A股股票数据API"
    APP_VERSION: str = "0.8.11"
    API_PREFIX: str = "/api/v1"

    # 数据库配置
    DB_HOST: str = ""
    DB_PORT: int = 5432
    DB_NAME: str = ""
    DB_USER: str = ""
    DB_PASSWORD: str = ""

    # CORS 配置
    CORS_ORIGINS: str = ""

    # JWT 配置
    JWT_SECRET_KEY: str = ""
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_HOURS: int = 720

    # ETL 引擎连接
    ETL_ENGINE_URL: str = "http://localhost:8082/api/v1/trigger"
    ETL_ENGINE_API_KEY: str = ""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    def validate(self):
        """启动时校验关键配置"""
        if not self.JWT_SECRET_KEY:
            raise ValueError("JWT_SECRET_KEY environment variable is required")
        if not self.DB_HOST:
            raise ValueError("DB_HOST environment variable is required")
        if not self.DB_NAME:
            raise ValueError("DB_NAME environment variable is required")
        if not self.DB_USER:
            raise ValueError("DB_USER environment variable is required")
        if not self.DB_PASSWORD:
            raise ValueError("DB_PASSWORD environment variable is required")

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    @property
    def cors_origins(self) -> list[str]:
        if not self.CORS_ORIGINS:
            return []
        return [origin.strip() for origin in self.CORS_ORIGINS.split(",") if origin.strip()]


settings = Settings()
```

- [ ] **Step 3: Verify compilation**

```bash
cd /home/shaomai/agent_work/stock_project/stock-fast-api
python -m py_compile app/core/config.py
python -m py_compile app/main.py
```

---

## 第三阶段：验证

### Task 14: 端到端验证

- [ ] **Step 1: Add httpx to main app dependencies**

```bash
cd /home/shaomai/agent_work/stock_project/stock-fast-api
echo "httpx" >> requirements.txt
```

- [ ] **Step 2: Install both projects' dependencies**

```bash
cd /home/shaomai/agent_work/stock_project/stock-etl-engine
pip install -r requirements.txt

cd /home/shaomai/agent_work/stock_project/stock-fast-api
pip install -r requirements.txt
```

- [ ] **Step 3: Start ETL engine**

```bash
cd /home/shaomai/agent_work/stock_project/stock-etl-engine
cp .env.example .env
# Edit .env with correct DB credentials
uvicorn app.main:app --host 0.0.0.0 --port 8082
```

- [ ] **Step 4: Verify ETL engine health**

```bash
curl http://localhost:8082/
# Expected: {"status":"ok","app":"A股ETL引擎","version":"1.0.0"}

curl http://localhost:8082/api/v1/trigger/health
# Expected: {"status":"ok","app":"etl-engine"}
```

- [ ] **Step 5: Start main app**

```bash
cd /home/shaomai/agent_work/stock_project/stock-fast-api
uvicorn app.main:app --host 0.0.0.0 --port 8081
```

- [ ] **Step 6: Verify main app health**

```bash
curl http://localhost:8081/
curl http://localhost:8081/api/v1/stocks/search?keyword=茅台
```

- [ ] **Step 7: Test manual trigger**

```bash
curl -X POST "http://localhost:8081/api/v1/jobs/sync-daily?trade_date=2026-06-26"
# Expected: {"code":0,"data":{"status":"triggered",...}}
```

- [ ] **Step 8: Test job list query**

```bash
curl "http://localhost:8081/api/v1/jobs?page=1&page_size=5"
# Expected: job list (reading from DB directly, should still work)
```

- [ ] **Step 9: Test board sync**

```bash
curl -X POST "http://localhost:8081/api/v1/boards/sync?symbol=600519"
# Expected: boards sync triggered via ETL engine
```

- [ ] **Step 10: Verify ETL engine logs**

```bash
# Check ETL engine console for "收到 ETL 触发请求" log messages
```
