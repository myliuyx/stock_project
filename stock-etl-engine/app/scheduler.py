from datetime import datetime, timedelta
from app.core.timezone import now
import logging
import os
import fcntl
import time
import psycopg2
from functools import wraps

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

from app.core.config import DB_CONFIG, LOG_DIR


SCHEDULER_LOCK_FILE = "/tmp/etl_engine_scheduler.lock"
_scheduler_lock_fd = None

# Scheduler global for health checks and watchdog
_active_scheduler = None


def acquire_scheduler_lock() -> bool:
    global _scheduler_lock_fd
    try:
        _scheduler_lock_fd = open(SCHEDULER_LOCK_FILE, 'w')
        fcntl.flock(_scheduler_lock_fd, fcntl.LOCK_EX | LOCK_NB)
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


def get_active_scheduler():
    """Get the active scheduler instance for health checks."""
    return _active_scheduler


logger = logging.getLogger("etl_engine.scheduler")

LOG_KEEP_DAYS = 3


def cleanup_old_logs():
    """清理超过 3 天的日志文件"""
    if not os.path.exists(LOG_DIR):
        return

    log_dir_real = os.path.realpath(LOG_DIR)
    cutoff = now() - timedelta(days=LOG_KEEP_DAYS)
    removed = 0
    for fname in os.listdir(LOG_DIR):
        if not fname.endswith(".log"):
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


# ──────────────────────── etl_job_run 辅助函数（供 APScheduler 定时触发使用）

def _create_job_record(job_name: str, biz_date: str | None = None) -> int | None:
    """在 etl_job_run 表插入一条 RUNNING 记录，返回 job_id；失败则返回 None。"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        current_time = now()
        biz_date = biz_date or current_time.strftime('%Y-%m-%d')
        cur.execute("""
            INSERT INTO etl_job_run (job_name, biz_date, status, start_time, created_at, rows_raw, rows_written)
            VALUES (%s, %s, 'RUNNING', %s, %s, 0, 0)
            RETURNING id
        """, (job_name, biz_date, current_time, current_time))
        job_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return job_id
    except Exception as e:
        logger.error(f"创建任务记录失败 ({job_name}): {e}")
        import traceback
        logger.error(traceback.format_exc())
        try:
            if 'conn' in locals():
                conn.close()
        except Exception:
            pass
        return None


def _complete_job_record(job_id: int, status: str = "COMPLETED", rows_written: int | None = None, error_message: str | None = None):
    """更新 etl_job_run 状态，自动计算 duration_ms。"""
    if job_id is None:
        return
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        updates = ["status = %s", "end_time = NOW()", "rows_written = COALESCE(%s, rows_written)"]
        params = [status, rows_written if rows_written is not None else 0]

        # COMPLETED / FAILED 时自动计算 duration_ms
        if status in ("COMPLETED", "FAILED"):
            updates.append("duration_ms = EXTRACT(EPOCH FROM (NOW() - start_time))::bigint * 1000")

        if error_message:
            updates.append("error_message = %s")
            params.append(error_message)

        sql = f"UPDATE etl_job_run SET {', '.join(updates)} WHERE id = %s"
        params.append(job_id)
        cur.execute(sql, tuple(params))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"更新任务记录失败 (job_id={job_id}): {e}")


def _wrap_job_for_record(func, job_name: str):
    """包装定时任务函数：自动创建/更新 etl_job_run 记录。"""
    def wrapper():
        job_id = _create_job_record(job_name)
        try:
            func()
            _complete_job_record(job_id, "COMPLETED", 0)
        except SystemExit as e:
            _complete_job_record(job_id, "FAILED", error_message=f"Job called sys.exit({e.code})")
            raise
        except Exception as e:
            _complete_job_record(job_id, "FAILED", error_message=str(e))
            # re-raise so safe_wrapper can log it too

    wrapper.__name__ = func.__name__
    return wrapper


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


def is_trade_day() -> bool:
    """检查今天是否是交易日（使用原生 psycopg2 连接）"""
    today = now().strftime('%Y-%m-%d')
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


# ──────────────────────────────────────────────
# Job 函数 — 已移除 @with_job_timeout（signal.alarm在线程中不可用，见 docs/A股定时任务异常排查与修复方案.md）
# 替换为: APScheduler safe_wrapper + sync_stock_daily 内部 4h 超时保护 (Step 2)
# ──────────────────────────────────────────────

def run_daily_sync():
    from app.jobs.sync_stock_daily import sync_stock_daily

    logger.info("=" * 60)
    logger.info(f"【定时任务】日线行情同步开始 {now().strftime('%Y-%m-%d %H:%M:%S')}")

    if not is_trade_day():
        logger.info("跳过非交易日")
        return

    try:
        today = now().strftime('%Y-%m-%d')
        sync_stock_daily(force_restart=False, start_date=today, end_date=today)
        logger.info(f"【定时任务】日线行情同步完成")
    except Exception as e:
        logger.error(f"日线同步失败: {e}")
        import traceback
        logger.error(traceback.format_exc())


def run_factor_compute():
    from app.jobs.compute_factor import main as factor_main

    logger.info("=" * 60)
    logger.info(f"【定时任务】技术因子计算开始 {now().strftime('%Y-%m-%d %H:%M:%S')}")

    if not is_trade_day():
        logger.info("跳过非交易日")
        return

    try:
        factor_main()
        logger.info(f"【定时任务】技术因子计算完成")
    except Exception as e:
        logger.error(f"技术因子计算失败: {e}")
        import traceback
        logger.error(traceback.format_exc())


def run_selection_mart():
    from app.jobs.build_selection_mart import main as selection_main

    logger.info("=" * 60)
    logger.info(f"【定时任务】选股宽表构建开始 {now().strftime('%Y-%m-%d %H:%M:%S')}")

    if not is_trade_day():
        logger.info("跳过非交易日")
        return

    try:
        selection_main()
        logger.info(f"【定时任务】选股宽表构建完成")
    except Exception as e:
        logger.error(f"选股宽表构建失败: {e}")
        import traceback
        logger.error(traceback.format_exc())


def run_security_master_sync():
    from app.jobs.sync_security_master import main as sync_security_master_main

    logger.info("=" * 60)
    logger.info(f"【定时任务】股票主数据同步开始 {now().strftime('%Y-%m-%d %H:%M:%S')}")

    if not is_trade_day():
        logger.info("跳过非交易日（主数据更新）")
        return

    try:
        sync_security_master_main()
        logger.info(f"【定时任务】股票主数据同步完成")
    except Exception as e:
        logger.error(f"股票主数据同步失败: {e}")
        import traceback
        logger.error(traceback.format_exc())


def run_new_ipo_board_sync():
    from app.jobs.sync_new_ipo_boards import sync_new_ipo_boards

    logger.info("=" * 60)
    logger.info(f"【定时任务】新股板块增量同步开始 {now().strftime('%Y-%m-%d %H:%M:%S')}")

    if not is_trade_day():
        logger.info("跳过非交易日")
        return

    try:
        sync_new_ipo_boards(days=7)
        logger.info(f"【定时任务】新股板块增量同步完成")
    except Exception as e:
        logger.error(f"新股板块增量同步失败: {e}")
        import traceback
        logger.error(traceback.format_exc())


def run_adjust_factor_sync():
    from app.jobs.sync_adjust_factor import main as adjust_factor_main

    logger.info("=" * 60)
    logger.info(f"【定时任务】复权因子同步开始 {now().strftime('%Y-%m-%d %H:%M:%S')}")

    if not is_trade_day():
        logger.info("跳过非交易日")
        return

    try:
        adjust_factor_main()
        logger.info(f"【定时任务】复权因子同步完成")
    except Exception as e:
        logger.error(f"复权因子同步失败: {e}")
        import traceback
        logger.error(traceback.format_exc())


def run_financial_indicator_sync():
    from app.jobs.etl_financial_indicator import main as financial_main

    logger.info("=" * 60)
    logger.info(f"【定时任务】财务指标同步开始 {now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        current = now()
        if current.month <= 3:
            year, quarter = now.year, 1
        elif now.month <= 6:
            year, quarter = now.year, 2
        elif now.month <= 9:
            year, quarter = now.year, 3
        else:
            year, quarter = now.year, 4

        os.environ['SYNC_YEAR'] = str(year)
        os.environ['SYNC_QUARTER'] = str(quarter)
        financial_main()
        logger.info(f"【定时任务】财务指标同步完成")
    except Exception as e:
        logger.error(f"财务指标同步失败: {e}")
        import traceback
        logger.error(traceback.format_exc())


def _scheduler_event_listener(event):
    """APScheduler event listener for error and success tracking."""
    if event.code == EVENT_JOB_ERROR:
        logger.error(f"【调度器事件】任务执行出错: {event.job_id} - {event.traceback}")
    elif event.code == EVENT_JOB_EXECUTED:
        logger.info(f"【调度器事件】任务执行成功: {event.job_id}")


def create_scheduler() -> BackgroundScheduler:
    global _active_scheduler

    scheduler = BackgroundScheduler(
        timezone='Asia/Shanghai',
        daemon_threads=True,  # Daemon threads so container exit kills scheduler cleanly
        job_defaults={
            'coalesce': True,
            'max_instances': 1,
            'misfire_grace_time': 60,  # ← 修复：从3600s降到60s，避免容器重启后job被立即作为misfire触发
        },
    )

    # Register event listener for error tracking
    scheduler.add_listener(
        _scheduler_event_listener,
        EVENT_JOB_ERROR | EVENT_JOB_EXECUTED,
    )

    def add_safe_job(func, job_id, name, **kwargs):
        """Add a job that catches exceptions and logs them properly.

        修复前（Step1）: @with_job_timeout 使用 signal.alarm() 在线程中报 ValueError →
          safe_wrapper except Exception 只打日志不 re-raise → APScheduler 误判 SUCCESS → next_run_at=明天

        修复后: 所有异常都 re-raise，APScheduler 正确标记 FAILED 并保留当天触发

        但 APScheduler 的后台线程机制保证了调度器本身不会崩溃。
        """
        @wraps(func)
        def safe_wrapper():
            try:
                func()
            except Exception as e:
                logger.error(f"【定时任务】{name} 执行出错: {e}")
                import traceback
                logger.error(traceback.format_exc())
                raise  # ← re-raise！让 APScheduler 标记 FAILED，不会误判为 SUCCESS

        scheduler.add_job(
            func=safe_wrapper,
            trigger=kwargs.get('trigger'),
            id=job_id,
            name=name,
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=60,  # ← 每个 job 单独设置，与全局一致
            max_instances=1,
        )

    # run_security_master_sync 不自管理 etl_job_run，需包装记录创建
    add_safe_job(
        _wrap_job_for_record(run_security_master_sync, "security_master_sync"),
        "security_master_sync", "股票主数据同步",
        trigger=CronTrigger(hour=17, minute=30, day_of_week="0-4", timezone='Asia/Shanghai'),
    )

    # new_ipo_board_sync 自管理 etl_job_run，不需额外包装
    add_safe_job(
        run_new_ipo_board_sync, "new_ipo_board_sync", "新股板块增量同步",
        trigger=CronTrigger(day_of_week="0-4", hour=17, minute=10, timezone='Asia/Shanghai'),  # Before security_master at 17:30
    )

    # 已暂停：复权因子同步（暂不执行）
    # add_safe_job(
    #     run_adjust_factor_sync, "adjust_factor_sync", "复权因子同步",
    #     trigger=CronTrigger(day_of_week="0-4", hour=20, minute=0, timezone='Asia/Shanghai'),  # Between security_master and daily_sync
    # )

    # 已暂停：财务指标同步（暂不执行）
    # add_safe_job(
    #     run_financial_indicator_sync, "financial_indicator_sync", "财务指标同步",
    #     trigger=CronTrigger(hour=21, minute=30, day_of_week="0-4", timezone='Asia/Shanghai'),  # Before factor_compute at 22:30
    # )

    # run_daily_sync (sync_stock_daily) 自管理 etl_job_run，不需额外包装
    add_safe_job(
        run_daily_sync, "daily_stock_sync", "日线行情同步",
        trigger=CronTrigger(hour=19, minute=0, day_of_week="0-4", timezone='Asia/Shanghai'),
    )

    # run_factor_compute / run_selection_mart / cleanup_old_logs 不自管理 etl_job_run，需包装记录创建
    add_safe_job(
        _wrap_job_for_record(run_factor_compute, "factor_compute"),
        "factor_compute", "技术因子计算",
        trigger=CronTrigger(hour=23, minute=0, day_of_week="0-4", timezone='Asia/Shanghai'),
    )

    add_safe_job(
        _wrap_job_for_record(run_selection_mart, "selection_mart"),
        "selection_mart", "选股宽表构建",
        trigger=CronTrigger(hour=23, minute=30, day_of_week="0-4", timezone='Asia/Shanghai'),
    )

    add_safe_job(
        _wrap_job_for_record(cleanup_old_logs, "cleanup_logs"),
        "cleanup_logs", "日志清理",
        trigger=CronTrigger(hour=0, minute=5, timezone='Asia/Shanghai'),
    )

    _active_scheduler = scheduler
    logger.info("定时任务已注册")
    return scheduler
