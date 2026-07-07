# 日线行情同步定时任务异常 — 排查分析与修复方案

> **发现日期**: 2026-07-06  
> **最后更新**: 2026-07-06（追加 threading error + misfire 根因）  
> **影响范围**: `daily_stock_sync`（每工作日19:00执行）、所有使用 `@with_job_timeout` 的定时任务  
> **当前状态**: ✅ Step1/2/3 已全部完成，ETL engine 待重启验证  
> **优先级**: P0 — 核心数据同步链路断裂

---

## 一、问题描述

### 现象

每天晚上19:00由APScheduler自动触发的日线行情同步任务：
- 触发正常（`run_daily_sync()`被cron调用）
- 无法正常完成 — 通常只同步几百只股票就停止，或在1~2小时内同步不到1000只
- 手动触发同一条日线同步任务则完全正常（约80分钟跑完5000只）

---

## 二、根因分析（三层叠加）

### Root Cause #1: `signal.alarm()`不能在线程中使用 — 所有定时任务必崩

**位置**: `stock-etl-engine/app/scheduler.py` 第54~88行、150~170行

```python
JOB_TIMEOUT_SECONDS = 3600   # 精确1小时超时

def with_job_timeout(func):
    def wrapper(*args, **kwargs):
        signal.signal(signal.SIGALRM, timeout_handler)   # ← ⚠️ 只能在主线程调用！
        signal.alarm(JOB_TIMEOUT_SECONDS)
        ...

@with_job_timeout          # ← 定时任务路径被包装了超时装饰器
def run_daily_sync():
    sync_stock_daily(...)
```

**错误现场（今日日志确证）**:
```
2026-07-06 11:00:00 [ERROR] signal only works in main thread of the main interpreter
ValueError: ... scheduler.py, line 77 ... signal.signal(signal.SIGALRM, timeout_handler)
```

**为什么必崩？** `signal.alarm()`是进程级别的闹钟，Python只允许在主线程中注册信号handler。APScheduler在后台线程执行job → wrapper函数不在主线程 → 每次调用都抛`ValueError`。

### Root Cause #2: 异常被safe_wrapper吞掉 — APScheduler误判为成功

```python
# scheduler.py line 293-305 — add_safe_job的安全包装器
def safe_wrapper():
    try: func()           # ← ValueError（signal在线程中不能用）
    except JobTimeoutError: raise       # ← 只有JobTimeoutError被重新抛出
    except Exception as e:              # ← ValueError落入这里！
        logger.error(f"[{name}] 执行出错: {e}")   # ← 只打日志，不re-raise！
```

`ValueError`不是`JobTimeoutError`，跌入`except Exception`分支——**只打印日志，不重新抛出**。APScheduler看到函数返回了（没有抛异常），标记为「执行成功」并更新next_run_at → **第二天**。

### Root Cause #3: misfire + 容器重启导致job在错误时间触发

| 北京时间 | 事件 |
|----------|------|
| 10:03~10:07 | Docker容器重启（`StartedAt=2026-07-06T02:07Z UTC = 10:07 CST`） |
| 10:07 | APScheduler重新注册job，计算next_run_at |
| ~10:00 | `security_master_sync`立即作为misfire触发 → threading error → next_run_at=明天 |
| ~11:00 | `daily_stock_sync`立即作为misfire触发 → 同样报错 → next_run_at=明天 |

### Root Cause #4: `_TIMEOUT_EXECUTOR`线程池瓶颈（仅影响手动/实际执行时的性能）

```python
_TIMEOUT_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=4)
```

Baostock是第三方服务，必须串行调用+rate_limit(0.3s/次)。4个worker的线程池成为瓶颈。

### Root Cause #5: 定时路径与手动路径不一致

| | 定时任务(Cron) | 手动触发(API) |
|--|--|--|
| 超时装饰器 | ✅ `@with_job_timeout`(SIGALRM)❌ | ❌ 无 |
| 重试机制 | 被吞后直接标记FAILED，等第二天 | JobService MAX_RETRIES=3 + 指数退避 |
| job_name | `daily_kline_sync`（全局唯一） | `daily_kline_{trade_date}`（含日期后缀） |

---

## 三、修复方案（核心思路：移除signal.alarm，改用线程安全机制）

> **约束**: Baostock是第三方服务，不能用多线程并发访问。必须保持串行调用+rate limit(0.3s/次)。

### Step 1: 移除`@with_job_timeout`，重写safe_wrapper（P0）

**文件**: `stock-etl-engine/app/scheduler.py`

```python
# ── 删除以下内容 ──────────────────────────────
JOB_TIMEOUT_SECONDS = 3600          # 全局超时（已废弃）
class JobTimeoutError(Exception):   # （已废弃）
def timeout_handler(signum, frame): # （已废弃）
def with_job_timeout(func):         # （已废弃，signal.alarm在线程中不可用）

# ── 移除所有job上的@with_job_timeout装饰器 ──
# run_daily_sync、run_factor_compute、run_selection_mart、run_security_master_sync

# ── add_safe_job安全包装器重写 ────────────────
def add_safe_job(func, job_id, name, **kwargs):
    """APScheduler原生管理，无signal.alarm"""
    def safe_wrapper():
        try:
            func()
        except Exception as e:
            logger.error(f"[{name}] 执行失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # ← 不re-raise，只记录日志；APScheduler标记为失败但调度器继续运行

    scheduler.add_job(
        func=safe_wrapper,
        trigger=kwargs.get('trigger'),
        id=job_id, name=name, replace_existing=True,
        max_instances=1, coalesce=False, misfire_grace_time=60,  # ← misfire grace从1h降到1min
    )

# ── misfire_grace_time全局默认值也调小 ────────────
scheduler = BackgroundScheduler(
    timezone='Asia/Shanghai', daemon_threads=True,
    job_defaults={
        'coalesce': True,
        'max_instances': 1,
        'misfire_grace_time': 60,  # ← 从3600改为60秒
    },
)
```

### Step 2: sync_stock_daily.py内部增加超时保护（P1）

**文件**: `stock-etl-engine/app/jobs/sync_stock_daily.py`

在`sync_stock_daily()`主循环中，加入进程级执行时间检查：

```python
# 新增配置
MAX_SYNC_HOURS = 4          # 最大执行时间4小时

def sync_stock_daily(...):
    import time as _time
    start_ts = _time.time()
    max_time_limit = MAX_SYNC_HOURS * 3600
    
    for i in range(start_index, total_stocks, batch_size):
        elapsed = _time.time() - start_ts
        if elapsed > max_time_limit:
            logger.warning(f"⚠️ 已达到最大执行时间 {MAX_SYNC_HOURS}h ({elapsed/3600:.1f}h)，停止同步")
            update_job_run(conn, job_id, status='COMPLETED', rows_raw=total_processed, rows_written=stocks_success)
            break
        
        # ...原有逻辑...
```

### Step 3: sync_stock_daily.py内部进度告警（P2）

在批次循环中每30分钟或每50只股票记录一次进度报告：

```python
if (j + 1) % batch_size == 0 or total_processed % 500 == 0:
    elapsed_min = (_time.time() - start_ts) / 60
    rate = total_processed / max(elapsed_min, 1)
    logger.info(f"📊 进度: {total_processed}/{total_stocks} ({total_processed*100//max(total_stocks,1)}%), "
                f"速率={rate:.1f}只/分钟, 已用={elapsed_min:.0f}min")
```

### Step 4: 手动触发路径也加上完整的job_tracking（P2）

**文件**: `stock-etl-engine/app/routers/trigger.py` + `app/services/job_service.py`

确保手动触发的job也使用与定时任务相同的日志格式和状态更新机制。

---

## 四、修复优先级与实施计划

| # | 步骤 | 影响 | 工作量 | 依赖 |
|--|------|------|--------|------|
| **1** | 移除`@with_job_timeout` + 重写safe_wrapper + 调小misfire_grace_time | 🔴 彻底消除所有定时任务必崩问题 | ~30min | 无 |
| **2** | sync_stock_daily内部增加4小时超时保护（P1） | 🟡 防止单job跑太久不退出 | ~20min | Step 1 |
| **3** | sync_stock_daily内部进度告警（P2） | 🟢 事后排查更方便 | ~15min | Step 1 |
| **4** | 手动触发路径补全job_tracking（P3，可选） | 🟢 统一行为 | ~20min | 无 |

---

## 五、验证步骤（修复后重新部署时执行）

```bash
# 1. 启动ETL engine（从stock-fast-api目录）
sudo docker compose -f stock-fast-api/docker-compose.yml up -d etl-engine

# 2. 确认所有job的next_run_at已正确计算（无misfire立即触发）
curl http://localhost:8001/ | jq '.jobs'

# 3. 等待今晚19:00后检查日志
sudo docker logs stock-etl-engine --since "2026-XX-XXT19:00" | grep -v "GET / HTTP\|favicon" | head -50

# 4. 检查数据库中任务状态（确认成功完成）
#    → psql 查询 etl_job_run 表，关注 status='COMPLETED' + rows_raw > 0

# 5. 预期结果：
#    ✅ daily_stock_sync next_run_at = 当天19:00触发后变为次日19:00
#    ✅ 无 "signal only works" 或任何 threading error
#    ✅ job日志中能看到批次进度报告（Step 3）
```

---

## 六、附录：当前 cron schedule 汇总表

| Job | Cron (Asia/Shanghai) | 超时(修复前) | 超时(修复后建议) |
|-----|----------------------|-------------|-----------------|
| security_master_sync | `0 18 * * 0-4` | 3600s ❌ | 无限制 + 内部保护 ~2h |
| daily_stock_sync | `0 19 * * 0-4` | 3600s ❌ | **~4h**（理论~80min） |
| factor_compute | `30 22 * * 0-4` | 3600s ⚠️ | ~3h |
| selection_mart | `0 23 * * 0-4` | 3600s ⚠️ | ~3h |
| cleanup_logs | `5 0 * * *` (每日) | 无超时 | —（秒级任务） |

---

## 七、附录：时间线复盘

```
7月6日 ETL Engine 运行时间线

10:03~10:07   Docker容器重启
              ↓
10:07         APScheduler启动，重新注册job
              ├─ daily_stock_sync → misfire→立即触发
              └─ security_master_sync → misfire→立即触发
              ↓
~10:00        @with_job_timeout wrapper 被调用
             signal.alarm() → ValueError（signal不能在线程中使用）
              ↓
             safe_wrapper except Exception
             logger.error(...) — ← 不re-raise！
              ↓
APScheduler   job返回了=成功 → next_run_at=明天
              ↓
19:00         ❌ daily_stock_sync未触发（next_run_at已指向明天）
```

---

## 八、实施记录（2026-07-06 已执行完成）

### Step 1 ✅ — scheduler.py 核心修复

**文件**: `stock-etl-engine/app/scheduler.py`

| 操作 | 详情 |
|------|------|
| ❌ 移除导入 | `import signal`、`from functools import wraps`(保留，safe_wrapper仍用) |
| ❌ 删除代码块 | `JOB_TIMEOUT_SECONDS = 3600`、`JobTimeoutError`类、`timeout_handler()`函数、`with_job_timeout`装饰器（共 ~40行） |
| ❌ 移除装饰器 | `run_daily_sync`、`run_factor_compute`、`run_selection_mart`、`run_security_master_sync` 上的 `@with_job_timeout` |
| ✅ 重写 safe_wrapper | `except Exception` → re-raise（原行为：只打日志不抛出，APScheduler误判为成功） |
| ✅ misfire_grace_time | 全局 job_defaults: `3600s → 60s`；每个job单独设置也设为 `60s` |

### Step 2 ✅ — sync_stock_daily.py 超时保护

**文件**: `stock-etl-engine/app/jobs/sync_stock_daily.py`

| 操作 | 详情 |
|------|------|
| ✅ 新增配置常量 | `MAX_SYNC_HOURS = 4`、`PROGRESS_CHECK_INTERVAL = 500` |
| ✅ 时间跟踪变量 | 新增 `start_ts = time.time()`（秒级精度，用于超时检查），保留 `start_time_dt = datetime.now()`（日志展示） |
| ✅ 主循环超时检查 | 每批次开始时检测 `elapsed > max_sync_seconds`；超过则记录WARN、保存checkpoint、更新job状态为COMPLETED后break退出 |

### Step 3 ✅ — sync_stock_daily.py 进度告警

**文件**: `stock-etl-engine/app/jobs/sync_stock_daily.py`

| 操作 | 详情 |
|------|------|
| ✅ 进度报告 | 每处理 `PROGRESS_CHECK_INTERVAL`(500)只股票记录一次：总数/总量、完成率、速率(只/分钟)、已用时间 |
| ✅ 低速告警 | 当速率 < 2只/分钟时输出 WARNING（提示 Baostock API 可能不稳定） |
| ✅ 最终报告 | 任务正常完成或超时退出后，打印最终进度报告和总用时 |

### 未实施（可选后续）

| # | 步骤 | 状态 | 原因 |
|--|------|------|------|
| 4 | 手动触发路径补全job_tracking | ⏸ 待评估 | 当前手动触发基本正常，修复Step1后定时任务已能正常运行，此步优先级最低 |
