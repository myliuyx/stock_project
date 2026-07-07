#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""测试 baostock 连接是否正常"""

import sys
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def test_baostock_connect():
    import baostock as bs

    # 1. 登录
    log.info("Step 1: logging in to baostock...")
    lg = bs.login()
    if lg.error_code != "0":
        log.error(f"login failed, error_code={lg.error_code}, error_msg={lg.error_msg}")
        return False
    log.info("login success")

    # 2. 查询股票基础信息（以招商银行 sh.600036 为例）
    log.info("Step 2: querying security basic info (sh.600036)...")
    rs = bs.query_stock_basic(code="sh.600036")
    if rs.error_code != "0":
        log.error(f"query_stock_basic failed: {rs.error_msg}")
        bs.logout()
        return False

    rows = []
    while rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        log.error("query_stock_basic returned no rows")
        bs.logout()
        return False

    log.info(f"  columns: {rs.fields}")
    for r in rows[:3]:
        log.info(f"  row: {r}")

    # 3. 查询最近5个交易日的日线行情
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
    log.info(f"Step 3: querying daily K-line for sh.600036 ({start_date} ~ {end_date})...")
    rs2 = bs.query_history_k_data_plus(
        "sh.600036",
        "date,code,open,high,low,close,volume,amount",
        start_date=start_date,
        end_date=end_date,
        frequency="d",
    )
    if rs2.error_code != "0":
        log.error(f"query_history_k_data_plus failed: {rs2.error_msg}")
        bs.logout()
        return False

    count = 0
    while rs2.next():
        count += 1
        row = rs2.get_row_data()
        if count <= 5:
            log.info(f"  day[{count}]: {row}")

    log.info(f"  total rows returned: {count}")

    # 4. 登出
    bs.logout()
    log.info("Step 4: logged out")
    return True


if __name__ == "__main__":
    try:
        ok = test_baostock_connect()
        sys.exit(0 if ok else 1)
    except Exception as e:
        log.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)
