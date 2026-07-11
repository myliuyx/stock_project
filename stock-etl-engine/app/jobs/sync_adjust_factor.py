"""
ETL Script: 从 Baostock 同步 dwd_stock_adjust_factor 复权因子表

数据来源:
- query_adjust_factor: 复权因子 (foreAdjustFactor, backAdjustFactor)
- query_dividend_data: 分红送转数据 (cash_dividend, stock_dividend, reserve_to_stock)

目标表: dwd_stock_adjust_factor
主键: (trade_date, symbol)

incremental sync: 通过 start_year/end_year 参数指定同步年份范围。
每只股票独立事务，失败不波及其它股票。

adj_factor 来源: foreAdjustFactor (Baostock 前复权因子)
"""

import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime, date, timedelta

import baostock as bs
import psycopg2
from psycopg2.extras import execute_values

# ========== 配置（统一从 core.config 导入）==========
from app.core.config import DB_CONFIG
from app.core.timezone import now

# ── 超时保护 ──
MAX_SYNC_HOURS = int(os.environ.get('MAX_SYNC_HOURS', '8'))       # 最大执行时间（小时）
PROGRESS_CHECK_INTERVAL = 100                                     # 每处理 N 只股票记录一次进度报告
BAOSTOCK_QUERY_TIMEOUT = 30                                       # 单个 Baostock API 调用超时（秒）
RATE_LIMIT_DELAY = float(os.environ.get('SYNC_RATE_DELAY', '0.05'))

LOG_DIR = os.environ.get('SYNC_LOG_DIR', '/app/logs')

# ── 全局超时线程池 ──
_TIMEOUT_EXECUTOR = ThreadPoolExecutor(max_workers=4)


# ========== 工具函数 ==========

def setup_logging():
    """配置日志，直接添加 handler（不依赖 basicConfig）"""
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, f'sync_adjust_factor_{now().strftime("%Y%m%d")}.log')
    logger = logging.getLogger('sync_adjust_factor')
    logger.setLevel(logging.INFO)  # 默认 WARNING，需要显式设置为 INFO

    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    return logger


def to_baostock_code(symbol: str) -> str:
    """standard symbol -> baostock code, e.g. 600519.SH -> sh.600519"""
    ticker, exchange = symbol.split('.')
    mapping = {'SH': 'sh', 'SZ': 'sz', 'BJ': 'bj'}
    return f"{mapping.get(exchange, 'sh')}.{ticker}"


def parse_dividend_value(val):
    """解析分红数据，处理 '0.045或0.0475' 或空字符串格式"""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    val = str(val).strip()
    if not val or val == 'None':
        return 0.0
    if '或' in val:
        val = val.split('或')[0]
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def get_event_type(cash_dividend: float, stock_dividend: float, reserve_to_stock: float) -> str:
    """根据分红送转数据判断事件类型"""
    has_cash = cash_dividend > 0 if cash_dividend else False
    has_bonus = stock_dividend > 0 if stock_dividend else False
    has_reserve = reserve_to_stock > 0 if reserve_to_stock else False

    if has_bonus or has_reserve:
        return 'BONUS_SHARE_WITH_CASH' if has_cash else 'BONUS_SHARE'
    if has_cash:
        return 'CASH_DIVIDEND'
    return 'OTHER'


def _run_with_timeout(func, *args):
    """在线程池中执行函数，超时则抛出 TimeoutError"""
    future = _TIMEOUT_EXECUTOR.submit(func, *args)
    try:
        return future.result(timeout=BAOSTOCK_QUERY_TIMEOUT)
    except FuturesTimeoutError:
        raise TimeoutError(f"Baostock API 调用超时 ({BAOSTOCK_QUERY_TIMEOUT}s)")


# ========== 数据库操作（单连接贯穿，每只股票独立 commit）==========

def _make_conn():
    """创建新的 psycopg2 连接"""
    return psycopg2.connect(**DB_CONFIG)


def _update_job_record(conn, job_id: int, status: str = None, rows_written: int = None, error_message: str = None):
    """更新任务状态"""
    if job_id is None:
        return

    cur = conn.cursor()
    updates = []
    params = []

    if status:
        updates.append("status = %s")
        params.append(status)
    if rows_written is not None:
        updates.append("rows_written = %s")
        params.append(rows_written)
    if error_message:
        updates.append("error_message = %s")
        params.append(error_message)

    # COMPLETED 或 FAILED 时记录结束时间 + 持续时间
    if status in ('COMPLETED', 'FAILED'):
        updates.append("end_time = NOW()")
        updates.append(
            "duration_ms = EXTRACT(EPOCH FROM (NOW() - start_time))::bigint * 1000"
        )

    if updates:
        cur.execute(f"""
            UPDATE etl_job_run SET {', '.join(updates)} WHERE id = %s
        """, params + [job_id])
        conn.commit()
    cur.close()


def upsert_records(conn, records: list) -> int:
    """批量 Upsert 复权因子数据"""
    if not records:
        return 0

    cur = conn.cursor()
    try:
        execute_values(
            cur,
            """
            INSERT INTO dwd_stock_adjust_factor (
                trade_date, symbol, adj_factor,
                cash_dividend, stock_dividend,
                event_type, source, updated_at
            ) VALUES %s
            ON CONFLICT (trade_date, symbol) DO UPDATE SET
                adj_factor = EXCLUDED.adj_factor,
                cash_dividend = EXCLUDED.cash_dividend,
                stock_dividend = EXCLUDED.stock_dividend,
                event_type = EXCLUDED.event_type,
                source = EXCLUDED.source,
                updated_at = NOW()
            """,
            records,
        )
        conn.commit()
        return len(records)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


# ========== Baostock 操作 ==========

def fetch_adjust_factor_range(bao_code: str, start_date: str, end_date: str):
    """获取指定日期范围的复权因子数据（单次查询，带超时）"""
    def _do_query():
        records = []
        rs = bs.query_adjust_factor(bao_code, start_date=start_date, end_date=end_date)
        while rs.error_code == '0' and rs.next():
            records.append(rs.get_row_data())
        return records

    try:
        return _run_with_timeout(_do_query)
    except TimeoutError as e:
        logging.getLogger('sync_adjust_factor').warning(f"  query_adjust_factor({bao_code}): {e}")
        raise


def fetch_dividend_data(bao_code: str, start_year: int, end_year: int):
    """获取指定年份范围的分红数据（带超时）"""
    def _do_query():
        records = []
        for year in range(start_year, end_year + 1):
            rs = bs.query_dividend_data(bao_code, year=str(year))
            while rs.error_code == '0' and rs.next():
                records.append(rs.get_row_data())
            time.sleep(RATE_LIMIT_DELAY)
        return records

    try:
        return _run_with_timeout(_do_query)
    except TimeoutError as e:
        logging.getLogger('sync_adjust_factor').warning(f"  query_dividend_data({bao_code}): {e}")
        raise


def get_all_listed_symbols(conn) -> list:
    """获取所有在市股票代码"""
    cur = conn.cursor()
    cur.execute("SELECT symbol FROM dwd_security_master WHERE status = 'LISTED' ORDER BY symbol")
    symbols = [row[0] for row in cur.fetchall()]
    cur.close()
    return symbols


def get_latest_adj_dates(conn, symbols):
    """批量查询多只股票在 dwd_stock_adjust_factor 中的最新交易日期

    Returns:
        Dict[symbol, date] — 只有库中有数据的股票
    """
    if not symbols:
        return {}
    cur = conn.cursor()
    # GROUP BY + MAX，比 ROW_NUMBER() 窗口函数快得多（40K+ 行场景）
    cur.execute("""
        SELECT symbol, MAX(trade_date) FROM dwd_stock_adjust_factor
        WHERE symbol = ANY(%s) GROUP BY symbol
    """, (symbols,))
    result = {}
    for sym, td in cur.fetchall():
        result[sym] = td
    cur.close()
    return result


# ========== 主流程 ==========

def main(task_id: int = None, start_year: int | None = None, end_year: int | None = None):
    """复权因子同步主函数

    Args:
        task_id: 外部传入的 etl_job_run id（由 JobService 创建）
        start_year: 起始年份，不传则默认前 3 年（增量模式），传值则为指定范围
        end_year: 结束年份，不传则取今年
    """
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)

    logger = setup_logging()
    start_ts = time.time()          # 用于超时 + 进度计算
    max_sync_seconds = MAX_SYNC_HOURS * 3600

    # ── 确定同步年份范围 ──
    if start_year is not None:
        _sync_start = int(start_year)
    else:
        env_start = os.environ.get('SYNC_START_YEAR')
        _sync_start = int(env_start) if env_start else (datetime.now().year - 3)

    if end_year is not None:
        _sync_end = int(end_year)
    else:
        env_end = os.environ.get('SYNC_END_YEAR')
        _sync_end = int(env_end) if env_end else datetime.now().year

    logger.info("=" * 70)
    logger.info("  复权因子同步 (dwd_stock_adjust_factor)")
    logger.info(f"  数据源: Baostock query_adjust_factor + query_dividend_data")
    logger.info(f"  同步范围: {_sync_start} ~ {_sync_end}")
    logger.info("=" * 70)

    # ── 建立全局连接（用于初始化任务记录和获取股票列表）──
    conn = None
    job_id = None

    if task_id is not None:
        # JobService 已创建任务记录，直接使用
        job_id = task_id
        logger.info(f"使用外部传入的 task_id={job_id}，跳过记录创建")
    else:
        # 独立运行（__main__/手动），自己创建任务记录和连接
        try:
            conn = _make_conn()
            today_str = datetime.now().strftime('%Y-%m-%d')
            job_id = _init_job_record(conn, "adjust_factor_sync", today_str)
            logger.info(f"任务记录已创建 (job_id={job_id})")
        except Exception as e:
            logger.warning(f"创建任务记录失败: {e}，继续运行（无任务追踪）")
            conn = None

    bs.login()

    # 确保有可用的数据库连接用于获取股票列表
    if conn is None:
        conn = _make_conn()

    try:
        symbols = get_all_listed_symbols(conn)
        total_stocks = len(symbols)

        logger.info(f"\n[配置] 在市股票: {total_stocks} 只, 同步年份: {_sync_start}-{_sync_end}")

        # ── 批量查询所有股票的最新复权因子日期（替代逐只查询）──
        latest_dates = get_latest_adj_dates(conn, symbols)
        logger.info(f"[增量预查] 库中有数据的股票: {len(latest_dates)}")

        total_written = 0
        total_skipped = 0
        total_errors = 0

        logger.info("\n[开始同步]")
        logger.info("-" * 70)

        # ── 查询终点：用今天而非年末，确保不会遗漏目标年份之后的事件导致全量跳过 ──
        query_end_limit = date.today()

        for idx, symbol in enumerate(symbols):
            bao_code = to_baostock_code(symbol)

            # ── 全局超时检查 ──
            elapsed = time.time() - start_ts
            if elapsed > max_sync_seconds and idx > 0:
                logger.warning(f"\n⚠️ 达到最大执行时间 {MAX_SYNC_HOURS}h ({elapsed/3600:.1f}h)，停止同步")
                _update_job_record(conn, job_id, "COMPLETED", rows_written=total_written) if conn and job_id else None
                break

            # ── 进度报告 ──
            if idx > 0 and idx % PROGRESS_CHECK_INTERVAL == 0:
                elapsed_min = elapsed / 60
                rate = idx / max(elapsed_min, 1)
                logger.info(f"📊 进度: {idx}/{total_stocks} ({idx*100//max(total_stocks,1)}%), "
                            f"速率={rate:.1f}只/分钟, 已用={elapsed_min:.0f}min | "
                            f"写入={total_written} 跳过={total_skipped} 错误={total_errors}")

            # ── 增量检查：该股票在库中最新复权因子日期（从批量预查结果取）──
            latest_date = latest_dates.get(symbol)
            if latest_date and latest_date >= query_end_limit:
                total_skipped += 1
                continue

            # 只拉取缺失部分（latest_date 之后到 target end year 年底的复权因子数据）
            query_start_str = (latest_date + timedelta(days=1)).strftime('%Y-%m-%d') if latest_date else f'{_sync_start}-01-01'
            query_end_str = date(_sync_end, 12, 31).strftime('%Y-%m-%d')

            try:
                # 创建独立事务连接，避免单只股票失败级联到全局
                stock_conn = _make_conn()

                # 1. 获取复权因子（只查缺失部分）
                adj_records = fetch_adjust_factor_range(bao_code, query_start_str, query_end_str)
                if not adj_records:
                    stock_conn.close()
                    time.sleep(RATE_LIMIT_DELAY)
                    continue

                # 构建 adj_factor 字典: trade_date -> adj_factor
                adj_map = {}
                for row in adj_records:
                    try:
                        adj_val = parse_dividend_value(row[2])
                        if adj_val == 0:
                            continue
                        trade_date = datetime.strptime(row[1], '%Y-%m-%d').date()
                        adj_map[trade_date] = adj_val
                    except (ValueError, IndexError):
                        continue

                if not adj_map:
                    stock_conn.close()
                    time.sleep(RATE_LIMIT_DELAY)
                    continue

                # 2. 获取分红数据
                div_records = fetch_dividend_data(bao_code, int(query_start_str[:4]), _sync_end)

                # 构建 dividend 字典: trade_date -> (cash, stock, reserve)
                div_map = {}
                for row in div_records:
                    try:
                        trade_date = datetime.strptime(row[6], '%Y-%m-%d').date()
                        cash = parse_dividend_value(row[10])  # dividCashPsAfterTax
                        stock = parse_dividend_value(row[12])  # dividStocksPs
                        reserve = parse_dividend_value(row[13])  # dividReserveToStockPs
                        div_map[trade_date] = (cash, stock, reserve)
                    except (ValueError, IndexError):
                        continue

                # 3. 合并数据并写入
                write_records = []
                for trade_date, adj_factor in sorted(adj_map.items()):
                    cash, stock, reserve = div_map.get(trade_date, (0.0, 0.0, 0.0))

                    # 判断 event_type
                    if adj_factor != 1.0 and cash == 0.0 and stock == 0.0 and reserve == 0.0:
                        event_type = 'STOCK_SPLIT'
                    else:
                        event_type = get_event_type(cash, stock, reserve)

                    write_records.append((
                        trade_date,
                        symbol,
                        adj_factor,
                        cash if cash > 0 else None,
                        stock if stock > 0 else None,
                        event_type,
                        'baostock',
                        now(),
                    ))

                # 4. Upsert（一次性传入）
                if write_records:
                    written = upsert_records(stock_conn, write_records)
                    total_written += written

                stock_conn.close()

            except Exception as e:
                total_errors += 1
                logger.warning(f"  处理失败 {symbol}: {e}")
                # 确保连接被关闭（即使异常也要 close，避免连接泄漏）

        # ── 最终进度报告 ──
        elapsed_min = (time.time() - start_ts) / 60
        logger.info("-" * 70)
        logger.info("[同步完成]")
        logger.info(f"总耗时: {elapsed_min:.1f}分钟")
        logger.info(f"总写入: {total_written} 条 | 跳过（已有）: {total_skipped} 只 | 错误: {total_errors}")

        if job_id is not None and conn and elapsed <= max_sync_seconds:
            _update_job_record(conn, job_id, "COMPLETED", rows_written=total_written)

    except TimeoutError as e:
        logger.error(str(e))
        if job_id is not None and conn:
            try:
                _update_job_record(conn, job_id, "FAILED", error_message=str(e))
            except Exception:
                pass

    except Exception as e:
        logger.error(f"同步失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        if job_id is not None and conn:
            try:
                _update_job_record(conn, job_id, "FAILED", error_message=str(e))
            except Exception:
                pass

    finally:
        bs.logout()
        # 关闭全局连接
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    # 返回写入行数，供 _dispatch_simple 使用（避免其覆盖 main 内部已更新的 rows_written）
    return total_written


def _init_job_record(conn, job_name: str, biz_date: str) -> int:
    """创建新的任务记录，返回 job_id"""
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO etl_job_run (job_name, biz_date, status, start_time, rows_raw, rows_written, created_at)
        VALUES (%s, %s, 'RUNNING', NOW(), 0, 0, NOW())
        RETURNING id
    """, (job_name, str(biz_date)))
    job_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    return job_id


if __name__ == '__main__':
    main()
