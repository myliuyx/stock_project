#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日线行情同步脚本 v2.0
========================
支持断点续传：
- 启动时创建任务记录 (etl_job_run)
- 每批处理后保存检查点 (etl_checkpoint)
- 中断后可从上次位置恢复
- 完成时更新任务状态

用法：
    # 全量同步 (从断点恢复或重新开始)
    python sync_stock_daily.py

    # 强制从头开始 (忽略断点)
    python sync_stock_daily.py --force-restart

    # 历史数据回填
    python sync_stock_daily.py --start-date 2021-04-17 --end-date 2024-04-16 --force-restart
"""

import baostock as bs
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from app.core.timezone import now
import time
import logging
import os
import argparse
import socket
import threading
import concurrent.futures
from typing import List, Dict, Optional, Tuple

# ========== 配置（统一从 core.config 导入）==========
from app.core.config import DB_CONFIG, LOG_DIR

# ── Step2/Step3: 定时任务超时保护 + 进度告警配置 ──
MAX_SYNC_HOURS = 4          # 最大执行时间（小时），超过则主动退出并保存 checkpoint
PROGRESS_CHECK_INTERVAL = 500  # 每处理 N 只股票记录一次进度报告

BAOSTOCK_QUERY_TIMEOUT = 30  # 每个 Baostock API 调用的超时时间（秒）


SYNC_CONFIG = {
    'job_name': 'daily_kline_sync',
    'data_days': 730,       # 2年数据
    'batch_size': 50,       # 每批处理股票数
    'checkpoint_interval': 1,    # 每批保存一次，断电最多丢1批
    'retry_times': 3,
    'retry_delay': 5,
    'rate_limit_delay': 0.3,   # 请求间隔 0.3s
}


_TIMEOUT_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=4)


def run_with_timeout(func, timeout, *args, **kwargs):
    """在线程中执行函数，超时则抛出 TimeoutError"""
    future = _TIMEOUT_EXECUTOR.submit(func, *args, **kwargs)
    try:
        return future.result(timeout=timeout)
    except concurrent.futures.TimeoutError:
        raise TimeoutError(f"Function timed out after {timeout}s")


# ========== 日期范围（默认从命令行参数覆盖）==========

# ========== 日志 ==========
def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, f'sync_stock_daily_{now().strftime("%Y%m%d")}.log')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if logger.handlers:
        logger.handlers.clear()
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger

# ========== 数据库操作 ==========
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def init_job_run(conn, job_name: str, biz_date: str) -> int:
    """创建新的任务记录，返回 job_id"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO etl_job_run (job_name, biz_date, status, start_time, rows_raw, rows_written, created_at)
        VALUES (%s, %s, 'RUNNING', %s, 0, 0, %s)
        RETURNING id
    """, (job_name, biz_date, now(), now()))
    job_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    return job_id


def add_job_log(conn, job_id: int, level: str, message: str):
    """写入任务日志到 etl_job_run_log"""
    if job_id is None:
        return
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO etl_job_run_log (job_id, level, message, created_at) VALUES (%s, %s, %s, %s)",
        (job_id, level, message, now()),
    )
    conn.commit()
    cursor.close()


def update_job_run(conn, job_id: int, status: str = None, rows_raw: int = None,
                   rows_written: int = None, error_message: str = None):
    """更新任务状态"""
    if job_id is None:
        return  # 任务未创建，跳过
    
    cursor = conn.cursor()
    updates = []
    params = []
    
    if status:
        updates.append("status = %s")
        params.append(status)
    if rows_raw is not None:
        updates.append("rows_raw = %s")
        params.append(rows_raw)
    if rows_written is not None:
        updates.append("rows_written = %s")
        params.append(rows_written)
    if error_message:
        updates.append("error_message = %s")
        params.append(error_message)
    
    # COMPLETED 或 FAILED 时记录结束时间
    if status in ('COMPLETED', 'FAILED'):
        updates.append("end_time = %s")
        params.append(now())
    
    if updates:
        cursor.execute(f"""
            UPDATE etl_job_run SET {', '.join(updates)}
            WHERE id = %s
        """, params + [job_id])
        conn.commit()
    
    cursor.close()


def save_checkpoint(conn, job_name: str, last_index: int, last_symbol: str,
                    total_processed: int, stocks_success: int, stocks_skipped: int, stocks_failed: int,
                    biz_date: str):
    """保存检查点（单条多值 INSERT，减少 DB 往返）"""
    cursor = conn.cursor()

    checkpoints = {
        'last_processed_index': str(last_index),
        'last_processed_symbol': last_symbol,
        'total_processed': str(total_processed),
        'stocks_success': str(stocks_success),
        'stocks_skipped': str(stocks_skipped),
        'stocks_failed': str(stocks_failed),
        'biz_date': biz_date,
    }

    if not checkpoints:
        cursor.close()
        return

    # Build multi-value INSERT: VALUES (%s,%s,%s,%s),(%s,%s,%s,%s),...
    params = []
    for key, value in checkpoints.items():
        params.extend([job_name, key, value, now()])

    value_groups = ','.join(['(%s,%s,%s,%s)'] * len(checkpoints))
    sql = f"""INSERT INTO etl_checkpoint (job_name, checkpoint_key, checkpoint_value, updated_at)
              VALUES {value_groups}
              ON CONFLICT (job_name, checkpoint_key) DO UPDATE SET
                  checkpoint_value = EXCLUDED.checkpoint_value,
                  updated_at = EXCLUDED.updated_at"""

    cursor.execute(sql, tuple(params))
    conn.commit()
    cursor.close()


def load_checkpoint(conn, job_name: str) -> Dict:
    """加载检查点"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT checkpoint_key, checkpoint_value
        FROM etl_checkpoint
        WHERE job_name = %s
    """, (job_name,))
    
    checkpoint = {}
    for key, value in cursor.fetchall():
        checkpoint[key] = value
    
    cursor.close()
    return checkpoint


def get_all_stocks(conn) -> pd.DataFrame:
    """获取所有股票列表"""
    sql = """
        SELECT ticker, exchange, name, security_type
        FROM dwd_security_master
        WHERE status = 'LISTED'
        ORDER BY exchange, ticker
    """
    df = pd.read_sql(sql, conn)
    return df


def batch_get_stock_date_range(conn, symbols: List[str]) -> Dict[str, Tuple[str, str]]:
    """批量查询多只股票的最小/最大交易日期
    
    Returns:
        Dict[symbol, (min_date_str, max_date_str)]
    """
    if not symbols:
        return {}
    cursor = conn.cursor()
    cursor.execute("""
        SELECT symbol, MIN(trade_date) AS min_date, MAX(trade_date) AS max_date
        FROM dwd_stock_daily
        WHERE symbol = ANY(%s)
        GROUP BY symbol
    """, (symbols,))
    result = {}
    for sym, min_d, max_d in cursor.fetchall():
        min_str = min_d.strftime('%Y-%m-%d') if hasattr(min_d, 'strftime') else str(min_d)[:10]
        max_str = max_d.strftime('%Y-%m-%d') if hasattr(max_d, 'strftime') else str(max_d)[:10]
        result[sym] = (min_str, max_str)
    cursor.close()
    return result


def upsert_daily_data(conn, data: List[Dict]) -> int:
    """批量写入日线数据 (幂等 upsert)"""
    if not data:
        return 0
    
    sql = """
        INSERT INTO dwd_stock_daily (
            trade_date, symbol, open, high, low, close, pre_close,
            volume, amount, turnover_rate, turnover_rate_f, volume_ratio,
            suspended_flag, change_pct, amplitude, change_amount, adj_factor,
            is_limit_up, is_limit_down, market_value, circulating_market_value,
            pe_ttm, pb, ps_ttm, source, created_at, updated_at
        ) VALUES (
            %(trade_date)s, %(symbol)s, %(open)s, %(high)s, %(low)s, %(close)s, %(pre_close)s,
            %(volume)s, %(amount)s, %(turnover_rate)s, %(turnover_rate_f)s, %(volume_ratio)s,
            %(suspended_flag)s, %(change_pct)s, %(amplitude)s, %(change_amount)s, %(adj_factor)s,
            %(is_limit_up)s, %(is_limit_down)s, %(market_value)s, %(circulating_market_value)s,
            %(pe_ttm)s, %(pb)s, %(ps_ttm)s, %(source)s, %(created_at)s, %(updated_at)s
        )
        ON CONFLICT (trade_date, symbol) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            pre_close = EXCLUDED.pre_close,
            volume = EXCLUDED.volume,
            amount = EXCLUDED.amount,
            turnover_rate = EXCLUDED.turnover_rate,
            turnover_rate_f = EXCLUDED.turnover_rate_f,
            volume_ratio = EXCLUDED.volume_ratio,
            suspended_flag = EXCLUDED.suspended_flag,
            change_pct = EXCLUDED.change_pct,
            amplitude = EXCLUDED.amplitude,
            change_amount = EXCLUDED.change_amount,
            adj_factor = EXCLUDED.adj_factor,
            is_limit_up = EXCLUDED.is_limit_up,
            is_limit_down = EXCLUDED.is_limit_down,
            market_value = EXCLUDED.market_value,
            circulating_market_value = EXCLUDED.circulating_market_value,
            pe_ttm = EXCLUDED.pe_ttm,
            pb = EXCLUDED.pb,
            ps_ttm = EXCLUDED.ps_ttm,
            updated_at = EXCLUDED.updated_at
    """
    
    cursor = conn.cursor()
    cursor.executemany(sql, data)
    conn.commit()
    cursor.close()
    return len(data)


# ========== Baostock 操作 ==========
def normalize_symbol(code: str) -> str:
    """sh.600000 -> 600000.SH"""
    if code.startswith("sh."):
        return code.replace("sh.", "") + ".SH"
    elif code.startswith("sz."):
        return code.replace("sz.", "") + ".SZ"
    return code


BAOSTOCK_SOCKET_TIMEOUT = 60  # Baostock API 调用超时（秒）
BAOSTOCK_RECONNECT_THRESHOLD = 400  # Baostock API 调用次数超过此阈值后重新登录
_baostock_api_calls = 0  # Baostock API 调用计数器
_baostock_lock = threading.RLock()  # 保护 Baostock 连接的线程锁（可重入，避免嵌套死锁）


def login_baostock():
    """单次尝试登录（内部使用，直接返回 bs.login() 的结果）。外部统一调用 login_with_retry。"""
    old_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(BAOSTOCK_SOCKET_TIMEOUT)
    try:
        lg = bs.login()
        if lg.error_code != '0':
            msg = f"error_code={lg.error_code}"
            error_msg_str = getattr(lg, 'error_msg', None)
            if error_msg_str:
                msg += f" ({error_msg_str})"
            socket.setdefaulttimeout(old_timeout)
            return False, msg
        return True, None
    except Exception as e:
        socket.setdefaulttimeout(old_timeout)
        return False, str(e)


def login_with_retry(max_attempts=None):
    """登录 Baostock，失败后指数退避重试。

    策略：首次等待 30s → 60s → 90s … 每次 +30s，最高到 max_backoff (300s)。
    全部耗尽后返回 False（由上层标记 FAILED）。
    """
    if max_attempts is None:
        max_attempts = SYNC_CONFIG.get('retry_times', 5) + 2   # 原始次数 + 初始尝试，默认 7

    base_delay = SYNC_CONFIG.get('retry_delay', 30)          # 基础等待秒数
    max_backoff = SYNC_CONFIG.get('max_reconnect_backoff', 300)  # 最大退避秒数
    logger_instance = logging.getLogger(__name__)
    attempt = 0

    while True:
        attempt += 1
        logger_instance.info(f"🔐 Baostock 登录尝试 {attempt}/{max_attempts}")
        ok, err_msg = login_baostock()
        if ok:
            logger_instance.info("✅ Baostock 登录成功")
            return True

        # 失败：判断是否还有重试机会
        if attempt >= max_attempts:
            msg = f"❌ Baostock 登录耗尽所有尝试（{max_attempts}次），任务将失败: {err_msg}"
            logger_instance.error(msg)
            return False

        delay = min(base_delay * attempt, max_backoff)
        logger_instance.warning(f"⚠️ Baostock 登录失败 ({attempt}/{max_attempts}): {err_msg}，{delay}s 后重试")
        time.sleep(delay)


def connect_with_retry(max_attempts=None):
    """先 logout 再 login，带指数退避重试（用于 ensure_baostock_session / 超时重连）。

    与 login_with_retry 的区别：内部会自动 bs.logout() + sleep 1s。
    全部失败后返回 False，调用方可决定是否终止任务。
    """
    if max_attempts is None:
        max_attempts = SYNC_CONFIG.get('retry_times', 5) + 2

    base_delay = 30
    max_backoff = 300
    logger_instance = logging.getLogger(__name__)
    attempt = 0

    while True:
        attempt += 1
        try:
            bs.logout()
        except Exception:
            pass

        if attempt > 1:
            delay = min(base_delay * (attempt - 1), max_backoff)
            logger_instance.warning(f"⚠️ Baostock 重新登录尝试 {attempt}/{max_attempts}，等待 {delay}s")
            time.sleep(delay)

        socket.setdefaulttimeout(BAOSTOCK_SOCKET_TIMEOUT)
        try:
            lg = bs.login()
            if lg.error_code == '0':
                logger_instance.info("✅ Baostock 重新登录成功")
                return True

            err_msg = f"error_code={lg.error_code}"
            error_msg_str = getattr(lg, 'error_msg', None)
            if error_msg_str:
                err_msg += f" ({error_msg_str})"
        except Exception as e:
            err_msg = str(e)

        if attempt >= max_attempts:
            msg = f"⚠️ Baostock 重新登录耗尽所有尝试（{max_attempts}次）: {err_msg}"
            logger_instance.warning(msg)
            return False

        delay = min(base_delay * attempt, max_backoff)
        logger_instance.warning(f"⚠️ Baostock 重新登录失败 ({attempt}/{max_attempts}): {err_msg}，{delay}s 后重试")


def logout_baostock():
    bs.logout()
    socket.setdefaulttimeout(None)  # 恢复默认（无超时）


def ensure_baostock_session():
    global _baostock_api_calls
    with _baostock_lock:
        _baostock_api_calls += 1
        if _baostock_api_calls > BAOSTOCK_RECONNECT_THRESHOLD:
            logger = logging.getLogger(__name__)
            logger.info(f"  🔄 Baostock API 已调用 {_baostock_api_calls} 次，重新登录保持连接")
            # 指数退避重试重连（最多尝试 7 次）
            if not connect_with_retry():
                # 全部失败：继续执行（后续拉取仍会工作，但 baostock 可能不稳定）
                logger.warning("  Baostock 重新登录全部失败，任务将继续运行")
            _baostock_api_calls = 0


def _do_fetch_daily_kline(code, query_start, end_date):
    """实际的 Baostock 日K线查询（供 run_with_timeout 调用）"""
    with _baostock_lock:
        ensure_baostock_session()
    fields = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST"
    rs = bs.query_history_k_data_plus(
        code=code, fields=fields,
        start_date=query_start, end_date=end_date,
        frequency="d", adjustflag="3"
    )
    if rs.error_code != '0':
        return pd.DataFrame()
    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    return pd.DataFrame(data_list, columns=rs.fields) if data_list else pd.DataFrame()


def fetch_daily_kline(code: str, start_date: str, end_date: str, extend_days: int = 0):
    """获取日K线数据（带超时保护，超时返回 None）"""
    query_start = start_date
    if extend_days > 0:
        query_start = (datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=extend_days)).strftime('%Y-%m-%d')
    
    try:
        return run_with_timeout(_do_fetch_daily_kline, BAOSTOCK_QUERY_TIMEOUT, code, query_start, end_date)
    except TimeoutError:
        logger = logging.getLogger(__name__)
        logger.warning(f"  ⏰ 获取日K线超时 {code} ({BAOSTOCK_QUERY_TIMEOUT}s)")
        return None


def safe_float(val):
    if val is None or val == '' or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def fetch_adj_factor_for_stock(conn, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """从 dwd_stock_adjust_factor 获取单只股票在日期范围内的复权因子事件

    返回 DataFrame(columns=['trade_date', 'adj_factor'])，按日期升序。
    无事件时返回空 DataFrame。
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT trade_date, adj_factor
        FROM dwd_stock_adjust_factor
        WHERE symbol = %s AND trade_date >= %s AND trade_date <= %s
          AND adj_factor IS NOT NULL
        ORDER BY trade_date ASC
    """, (symbol, start_date, end_date))
    rows = cur.fetchall()
    cur.close()

    if not rows:
        return pd.DataFrame(columns=['trade_date', 'adj_factor'])

    df = pd.DataFrame(rows, columns=['trade_date', 'adj_factor'])
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    return df


def symbol_to_baostock_code(symbol: str) -> str:
    """将标准symbol转换为baostock代码格式，如 000001.SZ -> sz.000001"""
    ticker = symbol.split('.')[0]
    exchange = symbol.split('.')[1]
    if exchange == 'SH':
        return f'sh.{ticker}'
    elif exchange == 'SZ':
        return f'sz.{ticker}'
    elif exchange == 'BJ':
        return f'bj.{ticker}'
    return None


def fetch_financial_from_db(conn, symbol: str) -> Dict:
    """从本地数据库获取股票最新财务数据

    注意：Q1/Q3 数据通常 total_share/liqa_share 为 NULL，需要取有值的最早一期
    """
    cursor = conn.cursor()
    result = {}

    # eps, roe 取最新一期（Q1 通常有这些）
    cursor.execute("""
        SELECT eps, roe
        FROM dwd_stock_financial_indicator
        WHERE symbol = %s
        ORDER BY report_period DESC
        LIMIT 1
    """, (symbol,))
    row = cursor.fetchone()
    if row:
        result['epsTTM'] = float(row[0]) if row[0] is not None else None
        result['roeAvg'] = float(row[1]) if row[1] is not None else None

    # total_share, liqa_share 取最新一期有值的（Q1/Q3 通常为 NULL）
    cursor.execute("""
        SELECT total_share, liqa_share
        FROM dwd_stock_financial_indicator
        WHERE symbol = %s AND total_share IS NOT NULL AND total_share > 0
        ORDER BY report_period DESC
        LIMIT 1
    """, (symbol,))
    row = cursor.fetchone()
    if row:
        result['totalShare'] = float(row[0]) if row[0] is not None else None
        result['liqaShare'] = float(row[1]) if row[1] is not None else None

    # revenue 取最新一期有值的（年报才有Q1累加值）
    cursor.execute("""
        SELECT revenue
        FROM dwd_stock_financial_indicator
        WHERE symbol = %s AND revenue IS NOT NULL AND revenue > 0
        ORDER BY report_period DESC
        LIMIT 1
    """, (symbol,))
    row = cursor.fetchone()
    result['MBRevenue'] = float(row[0]) if row and row[0] is not None else None

    cursor.close()
    return result if result else {}


def _do_fetch_financial_from_baostock(baostock_code: str) -> Dict:
    """实际的 Baostock 财务数据查询（供 run_with_timeout 调用）"""
    eps_ttm = total_share = liqa_share = roe_avg = mb_revenue = None
    
    year = now().year - 1
    for y in range(year, now().year + 1):
        for q in ["1", "2", "3", "4"]:
            if y >= now().year:
                m = now().month
                if q == "4" or (q == "3" and m < 10) or (q == "2" and m < 7) or (q == "1" and m < 4):
                    continue
            try:
                with _baostock_lock:
                    ensure_baostock_session()
                rs = bs.query_profit_data(code=baostock_code, year=y, quarter=q)
                if rs.error_code != '0':
                    continue
                while rs.next():
                    row_data = rs.get_row_data()
                    v_eps = safe_float(row_data.get('epsTTM'))
                    if v_eps is not None and v_eps > 0 and eps_ttm is None:
                        eps_ttm = v_eps
                    v_share = safe_float(row_data.get('totalShare'))
                    if v_share is not None and v_share > 0 and total_share is None:
                        total_share = v_share
                    v_liqa = safe_float(row_data.get('liqaShare'))
                    if v_liqa is not None and v_liqa > 0 and liqa_share is None:
                        liqa_share = v_liqa
                    v_roe = safe_float(row_data.get('roeAvg'))
                    if v_roe is not None and v_roe > 0 and roe_avg is None:
                        roe_avg = v_roe
                    v_rev = safe_float(row_data.get('MBRevenue'))
                    if v_rev is not None and v_rev > 0 and mb_revenue is None:
                        mb_revenue = v_rev
                if eps_ttm and total_share and liqa_share and roe_avg and mb_revenue:
                    break
            except Exception:
                pass
            time.sleep(SYNC_CONFIG['rate_limit_delay'])
        if eps_ttm and total_share and liqa_share and roe_avg and mb_revenue:
            break
    
    return {
        'epsTTM': eps_ttm,
        'roeAvg': roe_avg,
        'MBRevenue': mb_revenue,
        'totalShare': total_share,
        'liqaShare': liqa_share,
    }


def fetch_financial_from_baostock(baostock_code: str):
    """从 Baostock 获取股票最新财务数据（带超时保护，超时返回 None）"""
    try:
        return run_with_timeout(_do_fetch_financial_from_baostock, BAOSTOCK_QUERY_TIMEOUT, baostock_code)
    except TimeoutError:
        logger = logging.getLogger(__name__)
        logger.warning(f"  ⏰ 获取财务数据超时 {baostock_code} ({BAOSTOCK_QUERY_TIMEOUT}s)")
        return None


# ========== 数据处理 ==========
def calculate_derived_fields(
    df: pd.DataFrame,
    eps_ttm: float,
    total_share: float,
    liqa_share: float,
    roe_avg: float = None,
    mb_revenue: float = None
) -> pd.DataFrame:
    """计算衍生字段（adj_factor 由 process_single_stock 中 per-stock merge_asof + ffill 填充）"""
    if df.empty:
        return df

    # adj_factor 不在这里计算 — 改为 process_single_stock 中逐只查询 dwd_stock_adjust_factor
    # 并通过 merge_asof(direction='backward') + ffill carry-forward。
    # 原因：Baostock query_adjust_factor 只返回事件日，非事件日需继承上一事件值。
    # dwd_stock_adjust_factor 表已存储所有事件（有 idx_adjust_factor_symbol_trade_date 索引），
    # per-stock 查询效率高，且只影响新增股票的数据行，不碰历史数据。

    df['date'] = pd.to_datetime(df['date'])

    # 转换数值类型
    for col in ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    # tradestatus: '0'=停牌, '1'=交易，转为 numeric 便于后续判断
    if 'tradestatus' in df.columns:
        df['tradestatus'] = pd.to_numeric(df['tradestatus'], errors='coerce')
    
    df['code'] = df['code'].apply(normalize_symbol)
    
    # 计算字段
    # amplitude: 先 round(preclose, 4) 再算，减少浮点精度误差，与 baostock 保持一致
    # 公式: (high - low) / round(preclose, 4) * 100
    preclose_rounded = df['preclose'].round(4)
    df['amplitude'] = ((df['high'] - df['low']) / preclose_rounded * 100).round(4)
    df['change_amount'] = (df['close'] - df['preclose']).round(4)
    df['change_pct'] = df['pctChg'].round(4)
    # 涨跌停判断：严格用 <= />= 9.5%，change_pct 已是 float
    df['is_limit_up'] = df['change_pct'] >= 9.5
    df['is_limit_down'] = df['change_pct'] <= -9.5
    
    # 市值和PE
    if total_share and total_share > 0:
        df['market_value'] = (df['close'] * total_share).round(2)
    else:
        df['market_value'] = None
    
    if liqa_share and liqa_share > 0:
        df['circulating_market_value'] = (df['close'] * liqa_share).round(2)
    else:
        df['circulating_market_value'] = None
    
    if eps_ttm and eps_ttm > 0:
        df['pe_ttm'] = (df['close'] / eps_ttm).round(4)
        # pb = close / 每股净资产 = close / (epsTTM / roeAvg) = close * roeAvg / epsTTM
        if roe_avg and roe_avg > 0:
            df['pb'] = (df['close'] * roe_avg / eps_ttm).round(4)
        else:
            df['pb'] = None
    else:
        df['pe_ttm'] = None
        df['pb'] = None
    
    # ps_ttm = close / 每股营收 = close / (MBRevenue / totalShare)
    if mb_revenue and total_share and mb_revenue > 0 and total_share > 0:
        revenue_per_share = mb_revenue / total_share  # 元/股
        if revenue_per_share > 0:
            df['ps_ttm'] = (df['close'] / revenue_per_share).round(4)
        else:
            df['ps_ttm'] = None
    else:
        df['ps_ttm'] = None
    df['turnover_rate'] = df['turn'].round(4)
    # turnover_rate_f = 成交量 / 流通股本 (自由换手率)
    if liqa_share and liqa_share > 0:
        # volume in shares, liqa_share in shares, result should be scaled to match turn's format (e.g., 0.17 = 17%)
        df['turnover_rate_f'] = (df['volume'] / liqa_share * 100).round(4)
    else:
        df['turnover_rate_f'] = None
    
    # volume_ratio: 由本地数据库批量计算（见 update_volume_ratio），此处先留空
    df['volume_ratio'] = None
    
    df['source'] = 'baostock'
    df['created_at'] = now()
    df['updated_at'] = now()
    df['suspended_flag'] = df['tradestatus'].apply(lambda x: str(x) == '0')  # tradestatus='0'停牌=True, '1'交易=False
    
    return df


def process_single_stock(
    code: str,
    start_date: str,
    end_date: str,
    eps_ttm: float = None,
    total_share: float = None,
    liqa_share: float = None,
    roe_avg: float = None,
    mb_revenue: float = None,
    adj_factor_conn=None,  # DB connection for adj_factor lookup
):
    """处理单只股票（超时返回 None，空数据返回 []，成功返回 List[Dict]）

    adj_factor 通过查询 dwd_stock_adjust_factor 并 merge_asof carry-forward。
    """
    try:
        daily_df = fetch_daily_kline(code, start_date, end_date, extend_days=0)
        if daily_df is None:
            return None  # 超时标记
        if daily_df.empty:
            return []

        result_df = calculate_derived_fields(daily_df, eps_ttm, total_share, liqa_share, roe_avg, mb_revenue)

        # 从 dwd_stock_adjust_factor 获取事件日 adj_factor，merge_asof carry-forward
        if adj_factor_conn is not None:
            try:
                symbol = normalize_symbol(code)
                # 查询所有历史事件（从2015年起）以正确 carry-forward，而非仅同步日期范围
                adj_df = fetch_adj_factor_for_stock(adj_factor_conn, symbol, '2015-01-01', end_date)
                if not adj_df.empty:
                    # 统一 datetime 精度（避免 merge_asof 类型不匹配）
                    result_df['date'] = pd.to_datetime(result_df['date']).astype('datetime64[ns]')
                    adj_df['trade_date'] = pd.to_datetime(adj_df['trade_date']).astype('datetime64[ns]')
                    result_df = pd.merge_asof(
                        result_df.sort_values('date'),
                        adj_df.sort_values('trade_date'),
                        left_on='date',
                        right_on='trade_date',
                        direction='backward'
                    )
                    result_df.rename(columns={'adj_factor': '_raw_adj_factor'}, inplace=True)
                else:
                    result_df['_raw_adj_factor'] = None

                # 设置最终 adj_factor：有事件值用事件值，否则 carry-forward，都无则 1.0
                result_df['adj_factor'] = result_df['_raw_adj_factor'].ffill().fillna(1.0)
                if '_raw_adj_factor' in result_df.columns:
                    result_df.drop(columns=['_raw_adj_factor'], inplace=True, errors='ignore')
            except Exception as e2:
                # 防止事务失败连锁：rollback 恢复连接状态，让后续股票能继续处理
                try:
                    adj_factor_conn.rollback()
                except Exception:
                    pass
                logger = logging.getLogger(__name__)
                logger.warning(f"获取 {code} 复权因子失败: {e2}")

        else:
            result_df['adj_factor'] = 1.0

        # 只保留目标日期范围内的数据（去掉延伸的历史部分）
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        result_df = result_df[(result_df['date'] >= start_dt) & (result_df['date'] <= end_dt)].copy()
        
        results = []
        for _, row in result_df.iterrows():
            # 处理停牌日数据 (volume/amount 为空字符串时设为 0)
            vol_str = row.get('volume', 0) or 0
            amount_str = row.get('amount', 0) or 0
            
            trade_date = row['date'].strftime('%Y-%m-%d') if isinstance(row['date'], pd.Timestamp) else str(row['date'])[:10]
            symbol = row['code']
            
            # 停牌日 volume=0，tradestatus='0'
            is_suspended = str(row.get('tradestatus', '1')) == '0'
            
            results.append({
                'trade_date': trade_date,
                'symbol': symbol,
                'open': float(row['open']) if row['open'] not in ('', None) else 0,
                'high': float(row['high']) if row['high'] not in ('', None) else 0,
                'low': float(row['low']) if row['low'] not in ('', None) else 0,
                'close': float(row['close']) if row['close'] not in ('', None) else 0,
                'pre_close': float(row['preclose']) if row['preclose'] not in ('', None) else 0,
                'volume': int(float(vol_str)) if not is_suspended else 0,  # 停牌日 volume=0
                'amount': float(amount_str) if not is_suspended else 0,     # 停牌日 amount=0
                'turnover_rate': float(row.get('turnover_rate', 0) or 0) if not is_suspended else 0,
                'turnover_rate_f': float(row['turnover_rate_f']) if row.get('turnover_rate_f') is not None and not pd.isna(row['turnover_rate_f']) and not is_suspended else None,
                'volume_ratio': None,
                'suspended_flag': is_suspended,
                'change_pct': float(row.get('change_pct', 0) or 0) if not is_suspended else 0,
                'amplitude': float(row.get('amplitude', 0) or 0) if not is_suspended else 0,
                'change_amount': float(row.get('change_amount', 0) or 0) if not is_suspended else 0,
                'adj_factor': float(row.get('adj_factor', 1) or 1),
                'is_limit_up': bool(row.get('is_limit_up', False)) if pd.notna(row.get('is_limit_up')) else False,
                'is_limit_down': bool(row.get('is_limit_down', False)) if pd.notna(row.get('is_limit_down')) else False,
                'market_value': float(row['market_value']) if row.get('market_value') is not None and not pd.isna(row['market_value']) else None,
                'circulating_market_value': float(row['circulating_market_value']) if row.get('circulating_market_value') is not None and not pd.isna(row['circulating_market_value']) else None,
                'pe_ttm': float(row['pe_ttm']) if row.get('pe_ttm') is not None and not pd.isna(row['pe_ttm']) else None,
                'pb': float(row['pb']) if row.get('pb') is not None and not pd.isna(row['pb']) else None,
                'ps_ttm': float(row['ps_ttm']) if row.get('ps_ttm') is not None and not pd.isna(row['ps_ttm']) else None,
                'source': 'baostock',
                'created_at': now(),
                'updated_at': now()
            })
        
        time.sleep(SYNC_CONFIG['rate_limit_delay'])
        return results
        
    except Exception as e:
        # 记录错误而不是静默返回，便于排查问题
        logger = logging.getLogger(__name__)
        logger.warning(f"处理股票 {code} 时出错: {e}")
        return []


def update_volume_ratio(conn, trade_date: str, batch_symbols: list):
    """
    批量更新 volume_ratio（量比）
    
    计算逻辑：当日成交量 / 前5个交易日成交量均值
    数据来源：本地 dwd_stock_daily 表（从已入库的历史数据中查询）
    
    Args:
        conn: 数据库连接
        trade_date: 交易日期（YYYY-MM-DD）
        batch_symbols: 该批次入库的股票代码列表
    """
    if not batch_symbols:
        return
    
    cur = conn.cursor()
    try:
        # 查这批股票当天和前15个日历日的历史 volume（覆盖节假日）
        # 取最近5条交易日数据计算均值
        cur.execute("""
            SELECT symbol, trade_date, volume
            FROM dwd_stock_daily
            WHERE symbol = ANY(%s)
              AND trade_date <= %s
              AND trade_date > CAST(%s AS DATE) - INTERVAL '15 days'
            ORDER BY symbol, trade_date DESC
        """, (batch_symbols, trade_date, trade_date))
        rows = cur.fetchall()
        
        if not rows:
            return
        
        # 按股票分组，取最近5条交易日数据（排序后volumes[0]是最近一天，即目标日）
        from collections import defaultdict
        vol_map = defaultdict(list)  # symbol -> [volume, ...]
        for sym, td, vol in rows:
            if vol is not None:
                vol_map[sym].append(float(vol))

        # 计算并 UPDATE
        for sym, volumes in vol_map.items():
            if len(volumes) < 6:
                continue  # 需要至少6天（1天当天 + 5天历史）才计算量比
            # volumes[0] 是目标日当天，volumes[1:6] 是前5个交易日
            avg_5 = sum(volumes[1:6]) / 5
            if avg_5 <= 0:
                continue
            today_vol = volumes[0]
            ratio = round(today_vol / avg_5, 4)
            cur.execute("""
                UPDATE dwd_stock_daily
                SET volume_ratio = %s, updated_at = NOW()
                WHERE symbol = %s AND trade_date = %s
            """, (ratio, sym, trade_date))
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger = logging.getLogger(__name__)
        logger.warning(f"update_volume_ratio 失败: {e}")
    finally:
        cur.close()


# ========== 主同步函数 ==========
def sync_stock_daily(force_restart: bool = False, start_date: str = None, end_date: str = None, target_symbol: str = None, task_id: int = None):
    """日线行情同步 (支持断点续传、指定日期范围)
    
    Args:
        force_restart: 强制从头开始
        start_date: 指定起始日期 (YYYY-MM-DD)，默认 None（用 data_days 计算）
        end_date: 指定结束日期 (YYYY-MM-DD)，默认 None（用今天）
    """
    logger = setup_logging()
    
    # 判断是历史回填还是日常同步，用不同的 job_name 避免断点冲突
    is_single_day = (start_date is not None and end_date is not None and start_date == end_date)
    is_historical = not is_single_day and (start_date is not None or end_date is not None)
    if is_historical:
        job_name = SYNC_CONFIG['job_name'] + '_historical'
        logger.info("📌 检测为历史回填模式，使用独立 job_name")
    else:
        job_name = SYNC_CONFIG['job_name']

    biz_date = end_date or now().strftime('%Y-%m-%d')
    start_time_dt = now()  # 记录开始时间（用于日志展示）
    start_ts = time.time()          # 记录开始时间戳（用于超时检查 + 进度计算）
    max_sync_seconds = MAX_SYNC_HOURS * 3600  # 最大执行秒数（4h）
    current_batch_idx: int | None = None   # 当前批次索引（供异常处理块使用）
    current_batch: pd.DataFrame | None = None  # 当前批次数据（供异常处理块使用）
    job_id = None  # 任务 ID，初始化为 None，避免 except 块中未定义
    
    logger.info("="*60)
    logger.info("日线行情同步开始")
    logger.info(f"任务名称: {job_name}")
    logger.info("="*60)

    # ── 先建立 DB 连接 & 创建 RUNNING 任务记录 ────────────────
    conn = get_db_connection()

    if task_id is not None:
        job_id = task_id
        add_job_log(conn, job_id, "INFO", f"使用外部 job_id={job_id}，跳过 init_job_run")
        logger.info(f"✅ 使用外部 job_id={job_id}")
    else:
        job_id = init_job_run(conn, job_name, biz_date)
        add_job_log(conn, job_id, "INFO", f"任务记录已创建，job_name={job_name}, biz_date={biz_date}")
        logger.info(f"✅ 任务记录已创建 (job_id={job_id})")

    # ── Baostock 登录（带指数退避重试） ────────────────
    if not login_with_retry():
        add_job_log(conn, job_id, "ERROR", "Baostock 登录失败：所有重试耗尽，任务终止")
        update_job_run(conn, job_id, status='FAILED', error_message="Baostock login exhausted all retries")
        logger.error("❌ Baostock 登录失败，任务已标记 FAILED")
        conn.close()
        raise RuntimeError("Baostock login exhausted all retries")

    # ── 登录成功，进入同步流程 ────────────────

    try:
        # conn already open from above — reuse it for the entire sync body
        pass

        # ========== 检查断点 ==========
        start_index = 0
        total_processed = 0
        stocks_success = 0
        stocks_skipped = 0
        stocks_failed = 0
        
        if force_restart:
            # 强制从头：清除旧检查点
            cursor = conn.cursor()
            cursor.execute("DELETE FROM etl_checkpoint WHERE job_name = %s", (job_name,))
            conn.commit()
            cursor.close()
            add_job_log(conn, job_id, "INFO", "强制从头开始，已清除旧检查点")
            logger.info("📌 强制从头开始，已清除旧检查点")
        else:
            checkpoint = load_checkpoint(conn, job_name)
            if checkpoint and 'last_processed_index' in checkpoint:
                # 检查断点日期是否匹配，不匹配则忽略断点
                ck_biz_date = checkpoint.get('biz_date')
                if ck_biz_date and ck_biz_date != biz_date:
                    logger.info(f"📌 断点日期 ({ck_biz_date}) 与目标日期 ({biz_date}) 不一致，忽略断点，从头开始")
                    checkpoint = {}
                else:
                    start_index = int(checkpoint['last_processed_index']) + 1
                    total_processed = int(checkpoint.get('total_processed', 0))
                    stocks_success = int(checkpoint.get('stocks_success', 0))
                    stocks_skipped = int(checkpoint.get('stocks_skipped', 0))
                    stocks_failed = int(checkpoint.get('stocks_failed', 0))
                    logger.info(f"📌 从断点恢复: index={start_index}, 已完成={total_processed}, 成功={stocks_success}, 跳过={stocks_skipped}, 失败={stocks_failed}")
            else:
                logger.info("📌 无断点记录，将从头开始")
        
        # 获取股票列表
        stocks_df = get_all_stocks(conn)
        if target_symbol:
            stocks_df = stocks_df[
                stocks_df.apply(lambda s: normalize_symbol(f"{str(s['exchange']).lower()}.{str(s['ticker']).zfill(6)}") == target_symbol, axis=1)
            ]
            logger.info(f"📌 补历史模式，仅处理股票: {target_symbol}")
        total_stocks = len(stocks_df)
        logger.info(f"待同步股票总数: {total_stocks} 只")

        if total_stocks == 0:
            logger.warning(f"⚠️ 未找到股票: {target_symbol}")
            conn.close()
            return
        
        if start_index >= total_stocks:
            logger.info("✅ 所有股票已处理完成，无需重复同步")
            update_job_run(conn, job_id, status='COMPLETED', rows_raw=0, rows_written=0)
            conn.close()
            return
        
        # 计算日期范围
        if end_date:
            end_date_str = end_date
        else:
            end_date_str = now().strftime('%Y-%m-%d')
        
        if start_date:
            start_date_str = start_date
        else:
            start_date_str = (now() - timedelta(days=SYNC_CONFIG['data_days'])).strftime('%Y-%m-%d')
        
        logger.info(f"数据范围: {start_date_str} ~ {end_date_str}")
        
        # ========== 批量预查所有股票的最小/最大日期 ==========
        all_symbols = [normalize_symbol(f"{str(s['exchange']).lower()}.{str(s['ticker']).zfill(6)}") for _, s in stocks_df.iterrows()]
        stock_date_map = batch_get_stock_date_range(conn, all_symbols)
        logger.info(f"📊 批量查询 {len(stock_date_map)} 只股票的日期范围完成")
        # ========== 处理每批股票 ==========
        batch_count = 0
        _consecutive_timeout = 0  # 连续超时计数，用于触发 Baostock 重连
        all_updated_symbols: list[str] = []  # 累积已写入的 symbol，供最终 volume_ratio 统一计算
        for i in range(start_index, total_stocks, SYNC_CONFIG['batch_size']):
            current_batch_idx = i
            batch = stocks_df.iloc[i:i + SYNC_CONFIG['batch_size']]
            current_batch = batch
            batch_num = (i // SYNC_CONFIG['batch_size']) + 1
            total_batches = (total_stocks + SYNC_CONFIG['batch_size'] - 1) // SYNC_CONFIG['batch_size']

            # ── Step2: 超时检查 — 运行超过 MAX_SYNC_HOURS 主动退出，保存 checkpoint ──
            elapsed = time.time() - start_ts
            if elapsed > max_sync_seconds and i > start_index:
                logger.warning(f"\n⚠️ 已达到最大执行时间 {MAX_SYNC_HOURS}h ({elapsed/3600:.1f}h)，停止同步")
                add_job_log(conn, job_id, "WARN", f"达到最大执行时间 {MAX_SYNC_HOURS}h，主动退出 (已处理={total_processed})")
                # 保存最终 checkpoint
                save_checkpoint(conn, job_name, i - SYNC_CONFIG['batch_size'], '',
                              total_processed, stocks_success, stocks_skipped, stocks_failed, biz_date)
                update_job_run(conn, job_id, status='COMPLETED', rows_raw=total_processed, rows_written=stocks_success)
                add_job_log(conn, job_id, "INFO", f"因超时主动退出，已处理 {total_processed} 只 (成功={stocks_success})")
                break

            # ── Step3: 进度报告 — 每 PROGRESS_CHECK_INTERVAL 只记录一次 ──
            if total_processed > 0 and total_processed % PROGRESS_CHECK_INTERVAL == 0:
                elapsed_min = elapsed / 60
                rate = total_processed / max(elapsed_min, 1)
                logger.info(f"📊 进度报告: {total_processed}/{total_stocks} ({total_processed*100//max(total_stocks,1)}%), "
                            f"速率={rate:.1f}只/分钟, 已用={elapsed_min:.0f}min")
                # 如果速率异常低，记录告警（Baostock 可能不稳定）
                if rate < 2:
                    logger.warning(f"⚠️ 同步速率异常低: {rate:.1f}只/分钟 — 请检查 Baostock API 状态")

            logger.info(f"\n批次 {batch_num}/{total_batches} (股票 {i+1}~{min(i+SYNC_CONFIG['batch_size'], total_stocks)}/{total_stocks})")
            add_job_log(conn, job_id, "INFO", f"批次 {batch_num}/{total_batches} 开始处理")
            
            batch_data = []
            batch_success = 0
            batch_fail = 0
            batch_skipped = 0
            
            for j, (_, stock) in enumerate(batch.iterrows()):
                ticker = str(stock['ticker']).zfill(6)  # 补零到6位，如 '1' -> '000001'
                baostock_code = f"{stock['exchange'].lower()}.{ticker}"  # e.g., 'sh.600000'

                # ========== 预检查：跳过库中已有完整数据的股票 ==========
                stock_symbol = all_symbols[i + j]  # Use precomputed symbol (L912)
                date_range = stock_date_map.get(stock_symbol)
                if date_range:
                    min_date_in_db, max_date_in_db = date_range
                else:
                    min_date_in_db = None
                    max_date_in_db = None
                
                # 判断是否已有完整历史数据
                # 如果用户指定了具体日期（start_date == end_date），只检查这一天是否有数据
                # 否则检查日期范围是否被覆盖
                if start_date_str == end_date_str:
                    # 单日同步：只检查这一天是否有数据
                    has_complete_history = False  # 始终拉取，让 process_single_stock 返回那一天的数据
                    if max_date_in_db == end_date_str:
                        # 数据库最新日期恰好是要同步的日期，再检查是否有这一天
                        cur_check = conn.cursor()
                        cur_check.execute(
                            "SELECT 1 FROM dwd_stock_daily WHERE symbol = %s AND trade_date = %s LIMIT 1",
                            (stock_symbol, end_date_str)
                        )
                        date_exists = cur_check.fetchone() is not None
                        cur_check.close()
                        if date_exists:
                            has_complete_history = True
                            logger.info(f"  ⏭️ {ticker} 已有 {end_date_str} 数据，跳过")
                            batch_skipped += 1
                            stocks_skipped += 1
                            total_processed += 1
                            continue
                else:
                    # 范围同步：检查日期范围是否被覆盖
                    has_complete_history = (
                        max_date_in_db is not None and
                        max_date_in_db >= end_date_str and
                        min_date_in_db is not None and
                        min_date_in_db <= start_date_str
                    )
                
                if has_complete_history:
                    logger.info(f"  ⏭️ {ticker} 已有完整数据（{min_date_in_db}~{max_date_in_db}），跳过")
                    batch_skipped += 1
                    stocks_skipped += 1
                    total_processed += 1
                    continue
                
                # 增量拉取起始日：库中有数据则从次日开始，避免重复拉取
                # 前提：max_date >= start_date 且 min_date <= start_date（历史数据完整）
                # 否则说明历史数据不完整，应该全量拉取
                fetch_start_date = start_date_str
                if max_date_in_db and max_date_in_db >= start_date_str:
                    # 检查历史是否完整：min_date <= start_date_str 才算真正增量
                    if min_date_in_db and min_date_in_db <= start_date_str:
                        # 真正的增量：只拉缺的数据
                        next_day = (datetime.strptime(max_date_in_db, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                        # 如果算出的起始日比结束日晚，说明目标日期已被覆盖，使用 start_date_str
                        if next_day > end_date_str:
                            fetch_start_date = start_date_str
                            logger.info(f"  📈 {ticker} 目标日期已被覆盖，使用 {fetch_start_date} ~ {end_date_str}（库中最新: {max_date_in_db}）")
                        else:
                            fetch_start_date = next_day
                            logger.info(f"  📈 {ticker} 增量拉取 {fetch_start_date} ~ {end_date_str}（库中最新: {max_date_in_db}）")
                    else:
                        # 历史数据不完整（min_date > start_date），当全量拉取处理
                        logger.info(f"  📈 {ticker} 全量拉取 {start_date_str} ~ {end_date_str}（库中数据: {min_date_in_db}~{max_date_in_db}，缺失早期历史）")
                elif not max_date_in_db:
                    logger.info(f"  🆕 {ticker} 全量拉取（库中无数据）{start_date_str} ~ {end_date_str}")
                else:
                    # max_date_in_db < start_date_str，说明库中有数据但在目标范围之前
                    logger.info(f"  📈 {ticker} 全量拉取 {start_date_str} ~ {end_date_str}（库中最新: {max_date_in_db}，数据在目标范围之前）")
                
                # 先尝试从本地数据库获取财务数据，本地没有则从 Baostock 拉取
                eps_ttm = total_share = liqa_share = roe_avg = mb_revenue = None
                try:
                    fin_data = fetch_financial_from_db(conn, stock_symbol)
                    if fin_data:
                        eps_ttm = fin_data.get('epsTTM')
                        roe_avg = fin_data.get('roeAvg')
                        mb_revenue = fin_data.get('MBRevenue')
                        total_share = fin_data.get('totalShare')
                        liqa_share = fin_data.get('liqaShare')
                    else:
                        fin_data = fetch_financial_from_baostock(baostock_code)
                        if fin_data is not None:
                            eps_ttm = fin_data.get('epsTTM')
                            roe_avg = fin_data.get('roeAvg')
                            mb_revenue = fin_data.get('MBRevenue')
                            total_share = fin_data.get('totalShare')
                            liqa_share = fin_data.get('liqaShare')
                            logger.info(f"  📊 {ticker} 从 Baostock 获取财务数据: eps={eps_ttm}, roe={roe_avg}, revenue={mb_revenue}")
                except Exception as e:
                    logger.warning(f"  ⚠️ 获取财务数据失败 {ticker}，本次日线数据将无财务指标: {e}")
                
                # 获取日K线（只拉缺失的日期范围）
                stock_data = process_single_stock(baostock_code, fetch_start_date, end_date_str, eps_ttm, total_share, liqa_share, roe_avg, mb_revenue, adj_factor_conn=conn)
                if stock_data:
                    logger.debug(f"  -> {ticker} 获取 {len(stock_data)} 条数据")
                
                # 判断成功/失败
                days_needed = (datetime.strptime(end_date_str, '%Y-%m-%d') - datetime.strptime(fetch_start_date, '%Y-%m-%d')).days
                is_incremental_short = (days_needed <= 5) and (max_date_in_db is not None)
                
                if stock_data is None:
                    _consecutive_timeout += 1
                    batch_fail += 1
                    stocks_failed += 1
                    logger.warning(f"  ❌ {ticker} Baostock 超时({_consecutive_timeout}次连续)，标记为失败")
                    if _consecutive_timeout >= 3:
                        logger.warning(f"  🔄 连续 {_consecutive_timeout} 只超时，强制重新登录 Baostock")
                        # 指数退避重试重连（最多尝试 5 次）
                        reconnected = connect_with_retry(max_attempts=5)
                        _consecutive_timeout = 0
                        with _baostock_lock:
                            _baostock_api_calls = 0
                        if not reconnected:
                            logger.warning("  Baostock 重新登录全部失败，后续股票可能继续超时")
                elif stock_data:
                    _consecutive_timeout = 0
                    batch_data.extend(stock_data)
                    batch_success += 1
                    stocks_success += 1
                elif is_incremental_short and fetch_start_date != end_date_str:
                    # 仅对真正的"短范围增量拉取"才跳过（如补几天数据）
                    # 单日同步 (fetch_start_date == end_date_str) 不应跳过，即使 Baostock 返回空
                    _consecutive_timeout = 0
                    batch_skipped += 1
                    stocks_skipped += 1
                    logger.info(f"  ⏭️ {ticker} 增量拉取无新数据（{fetch_start_date}~{end_date_str}，疑似周末/节假日），跳过")
                else:
                    _consecutive_timeout = 0
                    batch_fail += 1
                    stocks_failed += 1
                
                total_processed += 1
                
                # 每10只股票打印进度
                if (j + 1) % 10 == 0:
                    logger.info(f"  进度: {j+1}/{len(batch)} (跳过:{batch_skipped} 成功:{batch_success} 失败:{batch_fail})")
            
            # 批量写入
            logger.info(f"  待写入: {len(batch_data)} 条")
            if batch_data:
                try:
                    written = upsert_daily_data(conn, batch_data)
                    logger.info(f"  ✅ 写入 {written} 条 (跳过:{batch_skipped} 成功:{batch_success} 失败:{batch_fail})")

                    # 累积 symbol，供最终 volume_ratio 统一计算（避免每批重复查历史）
                    all_updated_symbols.extend(d['symbol'] for d in batch_data)
                except Exception as e:
                    logger.error(f"  ❌ 写入失败: {e}")
                    # 即使写入失败，也继续处理下一批，不中断整个任务
            
            batch_count += 1
            
            # ========== 保存检查点 ==========
            if batch_count % SYNC_CONFIG['checkpoint_interval'] == 0:
                last_symbol = batch.iloc[-1]['ticker'] if len(batch) > 0 else ''
                save_checkpoint(conn, job_name, i + len(batch) - 1, last_symbol, 
                              total_processed, stocks_success, stocks_skipped, stocks_failed, biz_date)
                logger.info(f"  💾 检查点已保存 (index={i + len(batch) - 1})")
            
            time.sleep(1)

        # ========== 所有批次完成后，统一计算 volume_ratio（单条 SQL，不用额外调 Baostock）==========
        if all_updated_symbols:
            logger.info(f"📊 批量计算量比: {len(all_updated_symbols)} 只股票")
            update_volume_ratio(conn, biz_date, all_updated_symbols)
            logger.info("✅ 量比计算完成")

        # ========== 最终检查点 ==========
        save_checkpoint(conn, job_name, total_stocks - 1, '', 
                       total_processed, stocks_success, stocks_skipped, stocks_failed, biz_date)
        
        # 更新任务状态为完成（需要在关闭连接前调用）
        update_job_run(conn, job_id, status='COMPLETED',
                       rows_raw=total_processed,
                       rows_written=stocks_success)
        add_job_log(conn, job_id, "INFO", f"同步完成，成功:{stocks_success} 跳过:{stocks_skipped} 失败:{stocks_failed}")
        conn.close()
        conn = None  # 避免 finally 块重复关闭
        
        # ── Step3: 最终进度报告 ──
        elapsed_min = (time.time() - start_ts) / 60
        rate = total_processed / max(elapsed_min, 1) if elapsed_min > 0 else float('inf')
        logger.info(f"📊 最终进度报告: {total_processed}/{total_stocks} ({'100' if stocks_success + stocks_skipped + stocks_failed >= total_stocks else total_processed*100//max(total_stocks,1)}%), "
                    f"速率={rate:.1f}只/分钟, 总用时={elapsed_min:.0f}min")

        elapsed = (now() - start_time_dt).total_seconds()
        logger.info("\n" + "="*60)
        logger.info(f"✅ 同步完成!")
        logger.info(f"总耗时: {elapsed:.1f}秒 ({elapsed/60:.1f}分钟)")
        logger.info(f"总处理: {total_processed} 只")
        logger.info(f"成功: {stocks_success} 只, 跳过: {stocks_skipped} 只, 失败: {stocks_failed} 只")
        # 统计一致性校验
        actual_sum = stocks_success + stocks_skipped + stocks_failed
        if actual_sum != total_processed:
            logger.warning(f"⚠️ 统计不一致: total_processed={total_processed} vs sum(成功+跳过+失败)={actual_sum}")
        else:
            logger.info(f"✅ 统计校验通过: {total_processed} = {stocks_success} + {stocks_skipped} + {stocks_failed}")
        
        logger.info("="*60)
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️ 用户中断，保存检查点...")
        update_job_run(conn, job_id, status='FAILED', error_message='用户中断')
        if conn and current_batch_idx is not None:
            last_ticker = ''
            if current_batch is not None and len(current_batch) > 0:
                last_ticker = str(current_batch.iloc[-1]['ticker'])
            save_checkpoint(conn, job_name, current_batch_idx,
                           last_ticker,
                           total_processed, stocks_success, stocks_skipped, stocks_failed, biz_date)
        if conn:
            conn.close()
        logger.info("检查点已保存，下次运行将自动恢复")
        
    except Exception as e:
        logger.error(f"同步出错: {e}")
        add_job_log(conn, job_id, "ERROR", f"同步出错: {e}")
        update_job_run(conn, job_id, status='FAILED', error_message=str(e))
        if conn:
            conn.close()
        raise
    finally:
        logout_baostock()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='日线行情同步脚本')
    parser.add_argument('--force-restart', action='store_true', help='强制从头开始，忽略断点')
    parser.add_argument('--start-date', type=str, default=None,
                       help='起始日期 (YYYY-MM-DD)，默认使用 data_days 计算')
    parser.add_argument('--end-date', type=str, default=None,
                       help='结束日期 (YYYY-MM-DD)，默认使用今天')
    args = parser.parse_args()
    
    sync_stock_daily(force_restart=args.force_restart, 
                     start_date=args.start_date,
                     end_date=args.end_date)
