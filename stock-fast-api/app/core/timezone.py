"""统一时区工具 — 所有时间戳使用中国标准时间 (UTC+8)

所有 `datetime.now()` / `datetime.utcnow()` 调用替换为 `from app.core.timezone import now`。
"""

from datetime import datetime, timedelta, timezone

CST = timezone(timedelta(hours=8))
CHINA_TZNAME = "Asia/Shanghai"


def now() -> datetime:
    """返回当前 CST (UTC+8) 时间，带时区信息。"""
    return datetime.now(tz=CST)


def today_str() -> str:
    """返回今天的日期字符串 (YYYY-MM-DD)，使用中国时间。"""
    return now().strftime("%Y-%m-%d")
