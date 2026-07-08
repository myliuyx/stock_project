#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ETL Script: 增量同步新股板块数据

数据来源:
- efinance: ef.stock.get_belong_board()

目标表:
- dwd_board_master: 板块主数据
- dwd_board_relation: 股票-板块关系（无 trade_date 维度）

增量逻辑:
- 查询最近 N 天内上市的新股（list_date >= 今天 - N 天）
- 对每只新股调用 efinance 获取所属板块
- 写入 dwd_board_master + dwd_board_relation

用法:
    python sync_new_ipo_boards.py                    # 默认同步近7天新股
    python sync_new_ipo_boards.py --days 3         # 指定天数
"""
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from app.core.timezone import now
import re
import os
import sys
import logging
import argparse
import time


# ========== 配置 ==========
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', '192.168.3.16'),
    'port': int(os.environ.get('DB_PORT', '5432')),
    'database': os.environ.get('DB_NAME', 'stock_cache_system'),
    'user': os.environ.get('DB_USER', 'postgres'),
    'password': os.environ.get('DB_PASSWORD', ''),
}

RATE_LIMIT_DELAY = 0.3  # efinance 限速
BATCH_SIZE = 200
LOG_DIR = os.environ.get('SYNC_LOG_DIR', '/tmp')
if not os.path.exists(LOG_DIR):
    LOG_DIR = '/tmp'


# ========== 过滤规则（复用 board_sync_service.py） ==========
_EXCLUDE_PATTERNS = [
    r'融资融券', r'转融通', r'沪股通', r'深股通', r'陆股通',
    r'上证50', r'上证180', r'深证100', r'沪深300', r'HS300',
    r'富时罗素', r'标准普尔', r'MSCI', r'证金持股',
    r'百元股', r'大盘股', r'大盘成长', r'大盘价值', r'权重股',
    r'茅指数', r'宁组合', r'央国企改革', r'破净股', r'长期破净',
    r'周期股', r'红利股', r'IPO受益', r'^AH股',
]

_BOARD_TYPE_RULES = [
    (r'银行|白酒|食品饮料|医药|医疗|电子[^\w]|软件|计算机|电池|锂电|半导体|新能源[车]?|光伏|通信|传媒|房地产|建筑|汽车|机械设备|化工|有色金属|煤炭|钢铁|电力设备|军工|农业|零售|旅游|教育|金融|保险|证券|集成电路|存储芯片|国产芯片|芯片设计', 'INDUSTRY'),
    (r'概念|主题|AI|人工智能|云计算|大数据|物联网|5G|机器人|智能[驾驶家居穿戴]|新能源车|储能|固态电池|钠离子电池|液冷|CPO|铜缆高速|光通信|数据中心|虚拟现实|混合现实|消费电子|苹果概念|华为概念|小米汽车|特斯拉概念|宁德时代|宁组合|医美|养老|互联医疗|跨境支付|数字货币|区块链|信创|Kimi', 'CONCEPT'),
    (r'板块$|特区|成渝', 'AREA'),
]


def _is_excluded(name: str) -> bool:
    for p in _EXCLUDE_PATTERNS:
        if re.search(p, name):
            return True
    return False


def _infer_board_type(name: str) -> str:
    for pat, btype in _BOARD_TYPE_RULES:
        if re.search(pat, name):
            return btype
    return 'OTHER'


# ========== 日志 ==========
def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, f"sync_new_ipo_boards_{now().strftime('%Y%m%d')}.log")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger('sync_new_ipo_boards')


# ========== 任务状态管理（etl_job_run）==========
def init_job_run(conn, biz_date: str = None) -> int:
    """创建任务记录，返回 job_id"""
    cursor = conn.cursor()
    current_time = now()
    cursor.execute("""
        INSERT INTO etl_job_run (job_name, biz_date, status, start_time, rows_raw, rows_written, created_at)
        VALUES (%s, %s, 'RUNNING', %s, 0, 0, %s)
        RETURNING id
    """, ('new_ipo_board_sync', biz_date or current_time.strftime('%Y-%m-%d'), current_time, current_time))
    job_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    return job_id


def update_job_status(conn, job_id: int, status: str, rows_written: int = 0, error_message: str = None):
    """更新任务状态"""
    cur = conn.cursor()
    updates = ["status = %s", "end_time = NOW()", "rows_written = %s"]
    params = [status, rows_written]

    if status == 'COMPLETED':
        # 计算 duration（start_time -> end_time）需要子查询，直接设 duration_ms
        cur.execute("SELECT EXTRACT(EPOCH FROM (NOW() - start_time))::bigint * 1000 FROM etl_job_run WHERE id = %s", (job_id,))
        row = cur.fetchone()
        if row and row[0] is not None:
            updates.append("duration_ms = %s")
            params.append(row[0])

    if error_message:
        updates.append("error_message = %s")
        params.append(error_message)

    sql = f"UPDATE etl_job_run SET {', '.join(updates)} WHERE id = %s"
    params.append(job_id)
    cur.execute(sql, tuple(params))
    conn.commit()
    cur.close()


# ========== 数据获取 ==========
def get_new_ipo_symbols(conn, days: int = 7) -> list:
    """查询最近 days 天内上市的新股"""
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, name, list_date
        FROM dwd_security_master
        WHERE list_date >= CURRENT_DATE - (%(days)s || ' days')::interval
          AND status = 'LISTED'
        ORDER BY list_date DESC
    """, {'days': days})
    rows = cur.fetchall()
    cur.close()
    return [{'symbol': r[0], 'name': r[1], 'list_date': r[2]} for r in rows]


def get_belong_boards(symbol: str) -> list:
    """调用 efinance 获取单只股票的板块列表"""
    try:
        import efinance as ef
        clean = symbol.split('.')[0]
        df = ef.stock.get_belong_board(clean)
        if df is None or df.empty:
            return []
        name_col = next((c for c in df.columns if str(c) in {'板块名称', '板块', '所属板块', '板块名', 'name', 'industry'}), None)
        code_col = next((c for c in df.columns if str(c) in {'板块代码', '代码', 'code'}), None)
        if not name_col:
            return []
        dedupe = set()
        results = []
        for _, row in df.iterrows():
            import pandas as pd
            name_raw = row.get(name_col, '')
            if pd.isna(name_raw) or not str(name_raw).strip():
                continue
            name = str(name_raw).strip()
            if name in dedupe:
                continue
            dedupe.add(name)
            code = str(row.get(code_col)).strip() if code_col and not pd.isna(row.get(code_col)) else None
            board_type = _infer_board_type(name) if not _is_excluded(name) else None
            results.append({'name': name, 'code': code, 'type': board_type})
        return results
    except Exception:
        return []


# ========== 数据库写入 ==========
def upsert_board_master(conn, records: list) -> int:
    if not records:
        return 0
    cur = conn.cursor()
    sql = """
    INSERT INTO dwd_board_master (board_code, board_name, board_type, source, updated_at)
    VALUES %s
    ON CONFLICT (board_code) DO UPDATE SET
        board_name = EXCLUDED.board_name,
        board_type = COALESCE(EXCLUDED.board_type, dwd_board_master.board_type),
        source = EXCLUDED.source,
        updated_at = NOW()
    """
    current_time = now()
    values = [(r['code'], r['name'], r['type'], 'efinance', current_time) for r in records if r.get('code')]
    for i in range(0, len(values), BATCH_SIZE):
        execute_values(cur, sql, values[i:i + BATCH_SIZE])
        conn.commit()
    cur.close()
    return len(values)


def upsert_board_relation(conn, symbol: str, records: list) -> int:
    if not records:
        return 0
    # 删除旧关系（全量替换）
    cur = conn.cursor()
    cur.execute("DELETE FROM dwd_board_relation WHERE symbol = %s", (symbol,))
    conn.commit()
    # 插入新关系
    sql = """
    INSERT INTO dwd_board_relation (symbol, board_code, board_type, relation_source, updated_at)
    VALUES %s
    """
    current_time = now()
    values = [(symbol, r['code'], r['type'], 'efinance', current_time) for r in records if r.get('code') and r.get('type')]
    if not values:
        cur.close()
        return 0
    for i in range(0, len(values), BATCH_SIZE):
        execute_values(cur, sql, values[i:i + BATCH_SIZE])
        conn.commit()
    cur.close()
    return len(values)


# ========== 主逻辑 ==========
def sync_new_ipo_boards(days: int = 7) -> dict:
    logger = setup_logging()
    current_time = now()

    # Step 1: 创建任务记录（status=RUNNING）
    conn = psycopg2.connect(**DB_CONFIG)
    job_id = init_job_run(conn, biz_date=current_time.strftime('%Y-%m-%d'))
    logger.info(f"【新股板块增量同步】开始（近 {days} 天，job_id={job_id}）")

    new_stocks = get_new_ipo_symbols(conn, days)

    if not new_stocks:
        # 无新股：标记完成
        update_job_status(conn, job_id, "COMPLETED", rows_written=0)
        conn.close()
        logger.info(f"近 {days} 天无新股上市，跳过")
        return {"stocks": 0, "boards": 0, "job_id": job_id}

    logger.info(f"发现 {len(new_stocks)} 只新股: {[s['symbol'] for s in new_stocks]}")

    total_boards = 0
    success = 0
    fail = 0
    error_msg = None

    try:
        for stock in new_stocks:
            symbol = stock['symbol']
            boards = get_belong_boards(symbol)
            if not boards:
                fail += 1
                logger.warning(f"  {symbol} {stock['name']} 无板块数据")
                continue

            filtered = [b for b in boards if not _is_excluded(b['name'])]
            if not filtered:
                fail += 1
                logger.warning(f"  {symbol} {stock['name']} 过滤后无有效板块")
                continue

            stock_conn = psycopg2.connect(**DB_CONFIG)
            try:
                upsert_board_master(stock_conn, filtered)
                written = upsert_board_relation(stock_conn, symbol, filtered)
                total_boards += written
                success += 1
                logger.info(f"  {symbol} {stock['name']} -> {written} 个板块")
            except Exception as e:
                fail += 1
                error_msg = str(e)
                logger.error(f"  {symbol} 写入失败: {e}")
            finally:
                stock_conn.close()

            time.sleep(RATE_LIMIT_DELAY)

        # 确定最终状态
        if success > 0 and fail == 0:
            status = "COMPLETED"
        elif success > 0 and fail > 0:
            status = "COMPLETED"  # 部分成功也算完成
        else:
            status = "FAILED"

        update_job_status(conn, job_id, status, rows_written=total_boards, error_message=error_msg if status == "FAILED" else None)
    except Exception as e:
        logger.error(f"同步过程异常: {e}")
        try:
            update_job_status(conn, job_id, "FAILED", rows_written=0, error_message=str(e))
        except Exception:
            pass

    conn.close()

    logger.info(f"【新股板块增量同步】完成: {success} 只成功, {fail} 只失败, 共 {total_boards} 个关系")
    return {"stocks": len(new_stocks), "success": success, "fail": fail, "boards": total_boards, "job_id": job_id}


# ========== CLI ==========
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='新股板块增量同步')
    parser.add_argument('--days', type=int, default=7, help='查询近N天新股（默认7天）')
    args = parser.parse_args()

    try:
        result = sync_new_ipo_boards(days=args.days)
        print(f"\n完成: {result}")
    except Exception as e:
        print(f"\n失败: {e}")
        sys.exit(1)
