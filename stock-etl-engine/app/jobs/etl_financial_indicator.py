"""
ETL Script: 从 Baostock 回填 dwd_stock_financial_indicator 表

数据来源映射:
- query_profit_data:  roeAvg->roe, npMargin->net_margin, gpMargin->gross_margin,
                       netProfit->net_profit, MBRevenue->revenue, epsTTM->eps,
                       totalShare->total_share, liqaShare->liqa_share
- query_balance_data: liabilityToAsset->debt_to_asset, currentRatio->current_ratio, quickRatio->quick_ratio
- query_growth_data:  YOYNI->net_profit_yoy
- query_dupont_data:  dupontROE->roe, dupontROE/dupontAssetStoEquity->roa (更准确)
- query_cash_flow_data: CFOToOR->ocf_to_revenue

注意:
- Baostock 的 roeAvg, npMargin, gpMargin, liabilityToAsset 本身就是百分数格式 (如 8.45 表示 8.45%)
- currentRatio, quickRatio 是倍数，不是百分数
- YOYNI 是小数格式 (如 0.15 表示 15%)，需要转换
- ocf: Baostock 未提供经营活动现金流绝对值，保留为空
- revenue_yoy: Baostock 未提供营收同比，保留为空
- ocf_to_revenue: 使用 CFOToOR 映射（CFOToOR 已是小数格式，如 -0.017 表示 -1.7%）
"""

import baostock as bs
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, date
from app.core.timezone import now
import time
import os
import sys
import logging
import concurrent.futures


logger = logging.getLogger(__name__)

# ========== 配置（统一从 core.config 导入）==========
from app.core.config import DB_CONFIG

# ── Baostock API 超时保护 ──
BAOSTOCK_QUERY_TIMEOUT = 30  # 每个 Baostock API 调用的超时时间（秒）

# 模块级单例 Executor：避免每次调用创建/销毁线程池（全量同步 ~5000 只 × 5 张表 ≈ 25000 次）
_baostock_executor: concurrent.futures.ThreadPoolExecutor | None = None


def _get_baostock_executor() -> concurrent.futures.ThreadPoolExecutor:
    """懒初始化模块级单例 Executor（max_workers=1，调用串行化）。"""
    global _baostock_executor
    if _baostock_executor is None or _baostock_executor._shutdown:
        _baostock_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    return _baostock_executor


def _run_with_timeout(func, timeout, *args, **kwargs):
    """在线程中执行函数，超时则抛出 TimeoutError"""
    executor = _get_baostock_executor()
    future = executor.submit(func, *args, **kwargs)
    try:
        return future.result(timeout=timeout)
    except concurrent.futures.TimeoutError:
        raise TimeoutError(f"Baostock API 调用超时（{timeout}s）: {func.__name__}{args}")


def _shutdown_baostock_executor() -> None:
    """安全关闭 Executor，供 main() finally 块调用。"""
    global _baostock_executor
    if _baostock_executor is not None and not _baostock_executor._shutdown:
        _baostock_executor.shutdown(wait=False)
        _baostock_executor = None


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def convert_code_to_symbol(code):
    """baostock code 转换为标准 symbol 格式，如 sz.000001 -> 000001.SZ"""
    code = code.lower()
    if code.startswith('sh.'):
        return code[3:].zfill(6) + '.SH'
    elif code.startswith('sz.'):
        return code[3:].zfill(6) + '.SZ'
    elif code.startswith('bj.'):
        return code[3:].zfill(6) + '.BJ'
    return code

def get_report_type(stat_date_str):
    """根据 statDate 判断报告期类型

    注意：Baostock 返回的利润表数据是累计值：
    - Q1(3月): 一季度数据
    - Q2(6月): 半年累计数据
    - Q3(9月): 三季度累计数据
    - Q4(12月): 全年累计数据

    但财务报表习惯上：
    - 一季报(Q1): 截至3月底
    - 半年报(H1): 截至6月底（不是Q2）
    - 三季报(Q3): 截至9月底（不是Q3）
    - 年报(annual): 截至12月底

    我们需要正确区分 Q2 和 H1：Q2 单独表示二季度数据（4-6月），H1 是半年度累计
    """
    try:
        if len(stat_date_str) < 7:
            return None
        month = int(stat_date_str[5:7])
    except (ValueError, IndexError):
        return None
    if month == 3:
        return 'Q1'
    elif month == 6:
        return 'H1'
    elif month == 9:
        return 'Q3'
    elif month == 12:
        return 'annual'
    return 'annual'

def to_percent(val, max_val=None):
    """将小数转为百分数，如 0.15 -> 15.0
    如果值超过 max_val，返回 None（视为异常值）并记录警告日志。
    """
    if val is None:
        return None
    result = round(val * 100, 4)
    if max_val is not None and abs(result) > max_val:
        logger.warning("财务指标百分比超出阈值 (%.4f%% > %.0f%%)，置为空: value=%s", result, max_val, val)
        return None
    return result

def parse_float(val):
    """解析浮点数，空字符串或None返回None"""
    if val is None or val == '' or val == 'None':
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def _fetch_profit_data(code, year, quarter):
    """获取利润表数据（内部无超时版本）"""
    rs = bs.query_profit_data(code=code, year=year, quarter=quarter)
    if rs.error_code != '0':
        return [], []
    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    return data_list, rs.fields

def fetch_profit_data(code, year, quarter):
    """获取利润表数据（带超时保护）"""
    try:
        return _run_with_timeout(_fetch_profit_data, BAOSTOCK_QUERY_TIMEOUT, code, year, quarter)
    except TimeoutError as e:
        logger.warning("利润表查询超时: %s %d Q%d", code, year, quarter)
        return [], []

def _fetch_balance_data(code, year, quarter):
    """获取资产负债表数据（内部无超时版本）"""
    rs = bs.query_balance_data(code=code, year=year, quarter=quarter)
    if rs.error_code != '0':
        return [], []
    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    return data_list, rs.fields

def fetch_balance_data(code, year, quarter):
    """获取资产负债表数据（带超时保护）"""
    try:
        return _run_with_timeout(_fetch_balance_data, BAOSTOCK_QUERY_TIMEOUT, code, year, quarter)
    except TimeoutError as e:
        logger.warning("资产负债表查询超时: %s %d Q%d", code, year, quarter)
        return [], []

def _fetch_growth_data(code, year, quarter):
    """获取成长能力数据（内部无超时版本）"""
    rs = bs.query_growth_data(code=code, year=year, quarter=quarter)
    if rs.error_code != '0':
        return [], []
    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    return data_list, rs.fields

def fetch_growth_data(code, year, quarter):
    """获取成长能力数据（带超时保护）"""
    try:
        return _run_with_timeout(_fetch_growth_data, BAOSTOCK_QUERY_TIMEOUT, code, year, quarter)
    except TimeoutError as e:
        logger.warning("成长能力查询超时: %s %d Q%d", code, year, quarter)
        return [], []

def _fetch_dupont_data(code, year, quarter):
    """获取杜邦分析数据（内部无超时版本）"""
    rs = bs.query_dupont_data(code=code, year=year, quarter=quarter)
    if rs.error_code != '0':
        return [], []
    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    return data_list, rs.fields

def fetch_dupont_data(code, year, quarter):
    """获取杜邦分析数据（带超时保护）"""
    try:
        return _run_with_timeout(_fetch_dupont_data, BAOSTOCK_QUERY_TIMEOUT, code, year, quarter)
    except TimeoutError as e:
        logger.warning("杜邦分析查询超时: %s %d Q%d", code, year, quarter)
        return [], []

def _fetch_cash_flow_data(code, year, quarter):
    """获取现金流数据（内部无超时版本）"""
    rs = bs.query_cash_flow_data(code=code, year=year, quarter=quarter)
    if rs.error_code != '0':
        return [], []
    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    return data_list, rs.fields

def fetch_cash_flow_data(code, year, quarter):
    """获取现金流数据（带超时保护）"""
    try:
        return _run_with_timeout(_fetch_cash_flow_data, BAOSTOCK_QUERY_TIMEOUT, code, year, quarter)
    except TimeoutError as e:
        logger.warning("现金流查询超时: %s %d Q%d", code, year, quarter)
        return [], []

def fetch_all_financial_data(code, year, quarter):
    """获取某股票某季度所有财务数据并合并"""
    result = {
        'code': code,
        'statDate': None,
        'report_period': None,
        'report_type': None,
        'announce_date': None,
        'eps': None,
        'bps': None,
        'roe': None,
        'roa': None,
        'gross_margin': None,
        'net_margin': None,
        'debt_to_asset': None,
        'current_ratio': None,
        'quick_ratio': None,
        'total_share': None,
        'liqa_share': None,
        'revenue': None,
        'net_profit': None,
        'revenue_yoy': None,
        'net_profit_yoy': None,
        'ocf': None,
        'ocf_to_revenue': None,
        'source': 'baostock'
    }

    profit_data, profit_fields = fetch_profit_data(code, year, quarter)
    balance_data, balance_fields = fetch_balance_data(code, year, quarter)
    growth_data, growth_fields = fetch_growth_data(code, year, quarter)
    dupont_data, dupont_fields = fetch_dupont_data(code, year, quarter)
    cash_flow_data, cash_flow_fields = fetch_cash_flow_data(code, year, quarter)

    if profit_data:
        row = profit_data[0]
        result['statDate'] = row[profit_fields.index('statDate')]
        result['announce_date'] = row[profit_fields.index('pubDate')]
        result['report_type'] = get_report_type(result['statDate'])
        try:
            result['report_period'] = datetime.strptime(result['statDate'], '%Y-%m-%d').date()
        except ValueError:
            result['report_period'] = None

        # roeAvg, npMargin, gpMargin 是小数格式（如 0.067312 = 6.73%），需转百分数
        # 注意：Baostock 有时返回异常大的值（如 npMargin=-71560 表示 -71560%），
        # 超过 9999% 视为异常值，置为空
        result['roe'] = to_percent(parse_float(row[profit_fields.index('roeAvg')]), max_val=9999)
        result['net_margin'] = to_percent(parse_float(row[profit_fields.index('npMargin')]), max_val=9999)
        result['gross_margin'] = to_percent(parse_float(row[profit_fields.index('gpMargin')]), max_val=9999)
        result['net_profit'] = parse_float(row[profit_fields.index('netProfit')])
        result['revenue'] = parse_float(row[profit_fields.index('MBRevenue')])
        result['eps'] = parse_float(row[profit_fields.index('epsTTM')])
        result['total_share'] = parse_float(row[profit_fields.index('totalShare')])
        result['liqa_share'] = parse_float(row[profit_fields.index('liqaShare')])

    if balance_data:
        row = balance_data[0]
        # liabilityToAsset 是小数格式（如 0.05 = 5%），转百分数需 *100
        la = parse_float(row[balance_fields.index('liabilityToAsset')])
        # 负债率超过 100% 视为异常值（正常企业不会超过）
        if la is not None and la > 1.0:
            result['debt_to_asset'] = None
        else:
            result['debt_to_asset'] = to_percent(la, max_val=9999) if la is not None else None
        # currentRatio, quickRatio 是倍数，不是百分数
        result['current_ratio'] = parse_float(row[balance_fields.index('currentRatio')])
        result['quick_ratio'] = parse_float(row[balance_fields.index('quickRatio')])

    if growth_data:
        row = growth_data[0]
        # YOYNI 超过 1000% 视为异常值（Baostock 有时会返回极端值如 13500 表示 1350000%）
        result['net_profit_yoy'] = to_percent(parse_float(row[growth_fields.index('YOYNI')]), max_val=1000)
        # revenue_yoy: baostock query_growth_data 仅提供 YOYNI (净利润增速)
        # YOYEquity 是股东权益增速，不是收入增速，故不映射

    if dupont_data:
        row = dupont_data[0]
        dupont_roe = parse_float(row[dupont_fields.index('dupontROE')])
        if result['roe'] is None and dupont_roe is not None:
            # dupontROE 是小数格式，需转百分数
            result['roe'] = to_percent(dupont_roe)

        # roa 计算: 利用 DuPont 分解 ROE = ROA × 资产权益比
        # 因此 ROA = ROE / 资产权益比 = dupontROE / dupontAssetStoEquity
        dupont_asset_to_equity = parse_float(row[dupont_fields.index('dupontAssetStoEquity')])
        if result['roa'] is None and dupont_roe is not None and dupont_asset_to_equity is not None and dupont_asset_to_equity != 0:
            result['roa'] = to_percent(dupont_roe / dupont_asset_to_equity)

    if cash_flow_data:
        row = cash_flow_data[0]
        # CFOToOR 是经营活动现金流/营业收入，已是小数格式（如 -0.017 表示 -1.7%）
        cfo_to_or = parse_float(row[cash_flow_fields.index('CFOToOR')])
        if cfo_to_or is not None:
            result['ocf_to_revenue'] = to_percent(cfo_to_or, max_val=9999)

    return result

def get_all_symbols(conn):
    """获取所有股票symbol列表"""
    cur = conn.cursor()
    cur.execute("SELECT symbol FROM dwd_security_master WHERE status = 'LISTED' ORDER BY symbol")
    symbols = [row[0] for row in cur.fetchall()]
    cur.close()
    return symbols

def symbol_to_baostock_code(symbol):
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

def main(
    sync_year: int | None = None,
    sync_quarter: int | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> int:
    """
    财务指标 ETL 同步入口。

    Args:
        sync_year:     指定年份（与 sync_quarter 配对 → 单年单季度）
        sync_quarter:  指定季度（1-4）
        start_year:    区间起始年（与 end_year 配对 → 区间多季度）
        end_year:      区间结束年

    Returns:
        同步的记录数。
    """
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)

    # CLI fallback：若函数参数未提供，回退到 argparse + os.environ（兼容独立运行）
    if sync_year is None and sync_quarter is None and start_year is None and end_year is None:
        import argparse
        parser = argparse.ArgumentParser(description='财务指标 ETL 同步脚本')
        parser.add_argument('--year', type=int, help='指定年份 (YYYY)')
        parser.add_argument('--quarter', type=int, choices=[1,2,3,4], help='指定季度 (1-4)')
        parser.add_argument('--start-year', type=int, help='区间起始年')
        parser.add_argument('--end-year', type=int, help='区间结束年')
        args, _ = parser.parse_known_args()

        if args.year and args.quarter:
            sync_year = args.year
            sync_quarter = args.quarter
        elif args.start_year and args.end_year:
            start_year = args.start_year
            end_year = args.end_year

    # 参数优先级：
    # 1. sync_year + sync_quarter → 同步指定年/季度
    # 2. start_year + end_year → 同步区间内所有季度
    # 3. 都不设置 → 默认同步 2020~2026 全部季度
    if sync_year and sync_quarter:
        years = [int(sync_year)]
        quarters = [int(sync_quarter)]
        mode = f'增量同步: {sync_year}年第{sync_quarter}季度'
    elif start_year and end_year:
        start_y, end_y = int(start_year), int(end_year)
        years = list(range(start_y, end_y + 1))
        quarters = [1, 2, 3, 4]
        mode = f'区间同步: {start_y}~{end_y}年'
    else:
        years = [2026, 2025, 2024, 2023, 2022, 2021, 2020]
        quarters = [1, 2, 3, 4]
        mode = '全量同步: 2020-2026'

    bs.login()
    print('=' * 70)
    print('  财务指标 ETL 同步任务')
    print('  数据源: Baostock')
    print('  目标表: dwd_stock_financial_indicator')
    print(f'  同步模式: {mode}')
    print('=' * 70, flush=True)

    conn = get_db_connection()
    symbols = get_all_symbols(conn)
    total_stocks = len(symbols)

    # ── 批量加载已存在季度（一次性查询替代 N+1）──
    cur = conn.cursor()
    cur.execute(
        "SELECT symbol, report_period::text, report_type FROM dwd_stock_financial_indicator WHERE symbol = ANY(%s)",
        (symbols,),
    )
    existing_lookup: dict[str, set[tuple]] = {}
    for sym, rp, rt in cur.fetchall():
        existing_lookup.setdefault(sym, set()).add((rp, rt))
    cur.close()

    print(f'\n[配置]')
    print(f'  股票数量: {total_stocks}')
    print(f'  同步年份: {years}')
    print(f'  同步季度: {quarters}')
    print(f'  优化: 跳过已存在的季度数据（批量加载）')

    total_records = 0
    error_count = 0
    skip_count = 0
    start_time = now()

    # ── UPSERT SQL（参数化，供 executemany 使用）──
    upsert_sql = """
    INSERT INTO dwd_stock_financial_indicator
    (symbol, report_period, report_type, announce_date, eps, bps, roe, roa,
     gross_margin, net_margin, debt_to_asset, current_ratio, quick_ratio,
     total_share, liqa_share, revenue, net_profit, revenue_yoy, net_profit_yoy,
     ocf, ocf_to_revenue, source, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (symbol, report_period, report_type)
    DO UPDATE SET
        announce_date = EXCLUDED.announce_date,
        eps = EXCLUDED.eps,
        bps = EXCLUDED.bps,
        roe = EXCLUDED.roe,
        roa = EXCLUDED.roa,
        gross_margin = EXCLUDED.gross_margin,
        net_margin = EXCLUDED.net_margin,
        debt_to_asset = EXCLUDED.debt_to_asset,
        current_ratio = EXCLUDED.current_ratio,
        quick_ratio = EXCLUDED.quick_ratio,
        total_share = EXCLUDED.total_share,
        liqa_share = EXCLUDED.liqa_share,
        revenue = EXCLUDED.revenue,
        net_profit = EXCLUDED.net_profit,
        revenue_yoy = EXCLUDED.revenue_yoy,
        net_profit_yoy = EXCLUDED.net_profit_yoy,
        ocf = EXCLUDED.ocf,
        ocf_to_revenue = EXCLUDED.ocf_to_revenue,
        source = EXCLUDED.source,
        updated_at = NOW()
    """

    BATCH_SIZE = 100  # 每批提交行数，与 sync_stock_daily.py 保持一致

    def _flush_batch(batch: list) -> int:
        """批量执行 UPSERT 并 commit。"""
        if not batch:
            return 0
        cur = conn.cursor()
        try:
            execute_values(cur, upsert_sql, batch, template=None, page_size=BATCH_SIZE)
            conn.commit()
            return len(batch)
        except Exception as e:
            conn.rollback()
            logger.error("批量 UPSERT 失败 (%d 条): %s", len(batch), e)
            raise
        finally:
            cur.close()

    try:
        print(f'\n[开始同步]', flush=True)
        print('-' * 70, flush=True)

        batch_buffer: list[tuple] = []
        stock_records_count = 0  # per-stock counter reset not needed (tracked via stock_skip below)

        for idx, symbol in enumerate(symbols):
            bs_code = symbol_to_baostock_code(symbol)
            if not bs_code:
                continue

            stock_skip = 0
            existing_quarters = existing_lookup.get(symbol, set())

            for year in years:
                for quarter in quarters:
                    # 映射 quarter 到 report_type
                    report_type = {1: 'Q1', 2: 'H1', 3: 'Q3', 4: 'annual'}[quarter]

                    # 计算 report_period（用于判断是否已存在）
                    month_day_map = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}
                    m, d = month_day_map[quarter]
                    report_period_str = f"{year}-{m:02d}-{d:02d}"

                    # 跳过已存在的季度（existing_quarters 存的是字符串 key）
                    if (report_period_str, report_type) in existing_quarters:
                        skip_count += 1
                        stock_skip += 1
                        continue

                    try:
                        data = fetch_all_financial_data(bs_code, year, quarter)
                        if data['statDate'] is None or data['report_period'] is None:
                            continue

                        # 计算 bps: equity / total_share
                        # 由于 Baostock 未提供 equity 字段，通过 ROE 公式估算: equity ≈ net_profit / roe
                        # 注：此方法依赖 net_profit 和 roe 数据的一致性，可能存在微小误差
                        if data['bps'] is None:
                            np_ = data['net_profit']
                            r = data['roe']
                            ts = data['total_share']
                            if np_ is not None and r is not None and r != 0 and ts is not None and ts != 0:
                                equity = np_ / (r / 100)  # roe 是百分数，转为小数
                                data['bps'] = round(equity / ts, 4)

                        batch_buffer.append((
                            symbol,
                            data['report_period'],
                            data['report_type'],
                            data['announce_date'],
                            data['eps'],
                            data['bps'],
                            data['roe'],
                            data['roa'],
                            data['gross_margin'],
                            data['net_margin'],
                            data['debt_to_asset'],
                            data['current_ratio'],
                            data['quick_ratio'],
                            data['total_share'],
                            data['liqa_share'],
                            data['revenue'],
                            data['net_profit'],
                            data['revenue_yoy'],
                            data['net_profit_yoy'],
                            data['ocf'],
                            data['ocf_to_revenue'],
                            data['source']
                        ))

                        # 达到批次大小时提交
                        if len(batch_buffer) >= BATCH_SIZE:
                            count = _flush_batch(batch_buffer)
                            total_records += count
                            stock_records_count += count
                            batch_buffer.clear()

                    except Exception as e:
                        error_count += 1
                        print(f'  [错误] {symbol} {year}Q{quarter}: {e}', flush=True)

                    time.sleep(0.05)

            # 每只股票完成后的日志
            elapsed = (now() - start_time).total_seconds()
            rate = total_records / elapsed if elapsed > 0 else 0
            print(f'  [{idx+1:4d}/{total_stocks}] {symbol}: 新增{stock_records_count}条 跳过{stock_skip}条 | '
                  f'累计{total_records}条 错误{error_count}条 | {rate:.1f}条/秒', flush=True)
            stock_records_count = 0

        # 提交剩余缓冲数据
        if batch_buffer:
            count = _flush_batch(batch_buffer)
            total_records += count
            batch_buffer.clear()

    finally:
        try:
            conn.close()
        except Exception:
            pass
        try:
            bs.logout()
        except Exception:
            pass
        _shutdown_baostock_executor()

    elapsed = (now() - start_time).total_seconds()
    print('-' * 70)
    print('[同步完成]')
    print(f'  总耗时: {elapsed:.1f}秒 ({elapsed/60:.1f}分钟)')
    print(f'  总记录: {total_records}条')
    print(f'  跳过: {skip_count}条')
    print(f'  错误数: {error_count}条')
    print(f'  平均速度: {total_records/elapsed:.1f}条/秒')
    print('=' * 70)

    return total_records

if __name__ == '__main__':
    main()