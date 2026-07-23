# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the ETL Engine service in this repository.

## Overview

Independent microservice for scheduled data synchronization, factor computation, and mart building. Powered by APScheduler, pulls from baostock/efinance APIs and writes to PostgreSQL using raw SQL via `psycopg2`.

**Tech stack**: FastAPI + APScheduler + psycopg2 + baostock/efinance SDK + Python 3.11

## Commands

```bash
# Install dependencies
./venv/bin/pip install -r requirements.txt

# Start dev server (internal :8082, Docker maps external :8001)
./venv/bin/uvicorn app.main:app --reload --port 8082

# Run single test
./venv/bin/pytest tests/test_example.py -v

# Docker build and run
docker build -t stock-etl-engine .
docker run -p 8001:8082 --env-file .env stock-etl-engine
```

## Architecture

```
app/main.py          → FastAPI entry, API key middleware, scheduler lifecycle hooks
app/scheduler.py     → APScheduler orchestration (495 lines): cron job registration,
                       job record tracking (etl_job_run), timeout protection, event listeners
app/routers/trigger  → HTTP trigger endpoints (/api/v1/trigger/*) called by backend service
app/core/config      → DB_CONFIG, ETL_API_PORT, ETL_API_KEY, LOG_DIR from .env
app/core/logger      → Structured logging to files in SYNC_LOG_DIR
app/services/        → Business logic for sync/factor/mart operations
app/jobs/            → 13 scripts (8 active + 5 paused/legacy)
```

### Key patterns in scheduler.py

- **Lock file**: Uses `fcntl.flock` on `/tmp/etl_engine_scheduler.lock` to prevent multiple workers from starting duplicate schedulers
- **Job tracking**: Every scheduled job creates a `RUNNING` record in `etl_job_run` via `_create_job_record()`, then updates status (COMPLETED/FAILED) + row counts via `_complete_job_record()`
- **Safety wrapper**: `_wrap_job_for_record(func, job_name)` catches exceptions, logs error, and marks job FAILED in DB — individual failures don't crash the scheduler
- **Trade day guard**: `is_trade_day()` checks `dwd_trade_calendar` before running daily syncs to avoid off-day errors
- **Log cleanup**: Runs every night at 00:05 via APScheduler simple trigger, removes `.log` files older than 3 days from LOG_DIR

## Scheduled Jobs (cron times in UTC+8)

| Job ID | Function | Cron | Script | Description | Status |
|--------|----------|------|--------|-------------|--------|
| sync_new_ipo_board | `run_new_ipo_board_sync` | 17:30 daily | `sync_new_ipo_boards.py` | Recent IPO stocks + board assignments | Active |
| security_master | `run_security_master_sync` | 18:00 daily | `sync_security_master.py` | Full market stock master data | Active |
| stock_daily | `run_daily_sync` | 19:00 daily | `sync_stock_daily.py` | Full OHLCV (open/high/low/close/volume) | Active |
| factor_compute | `run_factor_compute` | 23:00 daily | `compute_factor.py` | MA, RSI, MACD, BOLL indicators | Active |
| selection_mart | `run_selection_mart` | 23:30 daily | `build_selection_mart.py` | Stock screening wide table | Active |
| log_cleanup | cleanup_logs | 00:05 daily (independent) | scheduler.py internal | Remove logs >3 days old | Active |
| adjust_factor | run_adjust_factor_sync | 20:00 (was) | `sync_adjust_factor.py` | Adjustment factors for price adjustment | Paused |
| financial_indicator | run_financial_indicator_sync | 21:30 (was) | `etl_financial_indicator.py` | Financial indicators (ROE, revenue, profit) | Paused |

> Active jobs go through `_wrap_job_for_record()` for DB tracking. Log cleanup runs via APScheduler's simple trigger without job records.

## Key Files

| File | Purpose |
|------|---------|
| `app/main.py` | FastAPI app, API key middleware (`X-API-Key` header), scheduler lifecycle (startup/shutdown events) |
| `app/scheduler.py` | Core orchestrator: cron registration, job record tracking, timeout protection, trade day checks |
| `app/core/config.py` | Env vars → DB_CONFIG, ETL_API_PORT, ETL_API_KEY; validates required fields at startup |
| `app/routers/trigger.py` | HTTP endpoints for backend to trigger jobs manually |
| `app/core/logger.py` | File-based logging with rotation |

## Conventions

- **Timezone**: Always use `from app.core.timezone import now` — no bare `datetime.now()`
- **DB access**: Raw SQL via `psycopg2`, never SQLAlchemy ORM in ETL service
- **Error handling**: Jobs wrapped to prevent scheduler crash; individual failures don't stop other scheduled jobs
- **Response format**: Consistent with backend: `{"code": 0, "message": "...", "data": ...}`
- **Authentication**: Backend calls require `X-API-Key` header matching `ETL_ENGINE_API_KEY`; health check endpoints (`/health`, `/`) are exempt
