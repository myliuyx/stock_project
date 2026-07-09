# CLAUDE.md

This file provides guidance for Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the **A-Share Stock Information Caching System** — a local data analysis platform for China's A-share market. It synchronizes daily market data from baostock/efinance into PostgreSQL and provides a web dashboard for stock screening, individual stock analysis, sector analysis, and ETL job monitoring.

**Architecture**: Three-service monorepo. Frontend serves as a REST API consumer (~51 endpoints). Backend handles database queries and ETL orchestration via HTTP calls to the independent ETL engine. All services share one PostgreSQL database. Timezone is unified to CST (UTC+8) across all services.

**Service topology**:
```
stock-front_ui (:5173 dev / :4173 prod)
    ↓ proxies /api to backend
stock-fast-api (:8081)
    ↑ calls via HTTP
stock-etl-engine (:8001 Docker / :8082 internal)
    ↑ fetches data from baostock/efinance APIs
PostgreSQL (:5432) — shared by fast-api and etl-engine
```

## Quick Commands

### Backend (port 8081)
```bash
cd stock-fast-api
./venv/bin/pip install -r requirements.txt
./venv/bin/uvicorn app.main:app --reload --host 0.0.0.0 --port 8081
./venv/bin/python -m pytest tests/test_file.py -v    # single test file
```

### Frontend (port 5173, proxies /api to backend)
```bash
cd stock-front_ui
npm install
npm run dev                               # dev server on :5173
npm run build                             # vue-tsc type check + vite build
npm run preview                           # preview production build
```

### ETL Engine (port 8001 externally, 8082 internally)
```bash
cd stock-etl-engine
./venv/bin/pip install -r requirements.txt
./venv/bin/uvicorn app.main:app --reload --host 0.0.0.0 --port 8082
# Docker deployment (from repo root)
docker compose build etl-engine && docker compose up -d etl-engine
```

## Repository Structure

```
stock_project/
├── stock-fast-api/          # FastAPI backend (Router → Schema → Service → Repository)
│   ├── app/
│   │   ├── core/            # config, db (SQLAlchemy pool), exceptions, response
│   │   ├── routers/         # 12 API route modules (~51 endpoints)
│   │   ├── schemas/         # Pydantic DTOs (request/response models)
│   │   ├── services/        # 10 business services
│   │   ├── repositories/    # 10 data access repos (raw SQL via `text()`)
│   │   ├── middleware/      # Request/response middleware + rate limiter
│   │   └── utils/           # pagination, validation helpers
│   ├── tests/               # Backend tests (pytest)
│   └── docs/                # API registry, DB design docs
├── stock-front_ui/          # Vue 3 frontend
│   ├── src/
│   │   ├── api/             # Axios layer (request.ts + 12 domain modules)
│   │   ├── components/      # board/, dashboard/, job/, selection/, strategy/, stock/
│   │   ├── layouts/         # MainLayout.vue
│   │   ├── pages/           # 17 route page components
│   │   ├── router/          # Vue Router (history mode, lazy-loaded) + auth guard
│   │   ├── stores/          # Pinia stores (app, tradeDate, job, selectionTemplate)
│   │   └── types/           # TypeScript interfaces per domain
│   └── mock/                # mock data for development (VITE_USE_MOCK=true)
└── stock-etl-engine/        # Independent ETL engine (APScheduler + FastAPI, v1.0.8)
    ├── app/
    │   ├── core/            # config (port 8082 default), logger, exceptions, timezone
    │   ├── routers/         # trigger router (/api/v1/trigger/*)
    │   ├── services/        # job_service, backfill_service, board_sync_service
    │   ├── jobs/            # 7+ active ETL scripts (sync, compute, mart)
    │   └── scheduler.py     # APScheduler lifecycle + safe_wrapper + log cleanup
```

## Key Conventions

- **API prefix**: `/api/v1`
- **Response format**: `{"code": 0, "message": "success", "data": {...}}`
- **Pagination format**: `{"list": [...], "page": 1, "page_size": 20, "total": N}`
- **Auth**: Bearer token in `Authorization` header; BizException codes map to HTTP status (1xxx→401, 4xxx→400, 404x→404, 5xxx→500)
- **Job polling**: frontend polls `/api/v1/jobs?status=RUNNING` every 10s for task status
- **Backend DB access**: raw SQL via `sqlalchemy.text()`; connection pool: pool_size=10, max_overflow=20
- **ETL Engine API**: connects to ETL at `{ETL_ENGINE_URL}/api/v1/trigger/run`; triggers async job execution
- **Timezone**: all services use CST (UTC+8); import `from app.core.timezone import now` instead of `datetime.now()`

## Adding/Modifying Backend APIs

Follow the **Schema → Repository → Service → Router** order:

1. Define request/response Pydantic models in `schemas/`
2. Write the SQL query in `repositories/` using `text(sql)` + `db.execute(text(sql), params)`
3. Implement business logic in `services/`
4. Wire up the route in `routers/`
5. Self-test: syntax check → start server → `curl` the endpoint

## Job Trigger Flow

When user triggers a job from frontend:
1. Frontend calls `POST /api/v1/jobs/run` with `job_name`
2. Backend (JobService) inserts PENDING record into `etl_job_run` table
3. Backend calls ETL Engine HTTP API → async job execution
4. Frontend polls `/api/v1/jobs?status=RUNNING` for updates
5. ETL Engine completes and updates DB status

## Adding/Modifying ETL Jobs

All ETL jobs live in `stock-etl-engine/app/jobs/`. To add a new job:
1. Create script file (e.g., `jobs/my_sync.py`) — must have `def main():` entry point
2. Register in `scheduler.py` → `register_jobs()` function using `add_safe_job()`
3. If manual trigger needed, add endpoint in stock-fast-api `routers/jobs.py`

## ETL Job Schedules (cron, Mon-Fri)

| # | 任务名（job_name） | cron 时间 | 说明 |
|---|--------|----------|------|
| 1 | `new_ipo_board_sync` | **17:10** | 近7天上市新股及其所属板块 |
| 2 | `security_master_sync` | **17:30** | 全市场股票基础信息 |
| 3 | `daily_stock_sync` | **19:00** | 全市场行情 OHLCV 数据 |
| 4 | `factor_compute` | **23:00** | MA/RSI/MACD/BOLL 等指标 |
| 5 | `selection_mart` | **23:30** | 汇总行情+因子+财务 → 选股分析 |
| — | `cleanup_logs` | 每日 00:05（独立） | 清理超3天日志文件 |

> ⏸️ Paused (commented out in scheduler.py): Adjustment factor sync (`adjust_factor_sync`, was 20:00), Financial indicator sync (`financial_indicator_sync`, was 21:30) — still accessible via manual trigger API.

## API Modules

| Module | Router file | Endpoints | Key paths |
|--------|-------------|-----------|-----------|
| Auth | `auth.py` | 2 | `/auth/login`, `/auth/verify` |
| Dashboard | `dashboard.py` | 4 | `/dashboard/summary`, `/jobs`, `/coverage`, `/watchlist-analysis` |
| Selection | `selection.py` | 5 | `/selection/dates`, `/industries`, `/query`, `/export`, `/top` |
| Stocks | `stocks.py` | 9 | `/stocks/search`, `/profile`, `/daily`, `/factors`, `/finance`, `/boards`, `/latest-price` |
| Strategies | `strategy.py` | 4 | `/strategies`, `/strategies/<id>`, `/query`, `/analyze` (9 strategies) |
| Jobs | `jobs.py` | 13 | `/jobs` list, run, 7 dedicated triggers, detail, logs, cancel |
| Coverage | `coverage.py` | 3 | `/coverage` list/summary/detail by symbol |
| Boards | `boards.py` | 3 | `/boards` list, `<code>` detail, `<code>/members` |
| Backfill | `backfill.py` | 2 | `/backfill/run`, `/backfill/status/<id>` |
| System | `system.py` | 1 | `/system/meta` |
| Watchlist | `watchlist.py` | 4 | `/watchlist` CRUD + check |

## Database Tables (key tables)

| Table | Rows (approx.) | Description |
|-------|---------------|-------------|
| `dwd_security_master` | ~5,200 | Stock master data (symbol, name, exchange, industry) |
| `dwd_board_master` / `dwd_board_relation` | 83 / ~5,200 | Board definitions + stock-board relationships |
| `dwd_trade_calendar` | — | A-share trading calendar |
| `dwd_stock_daily` | ~4.87M | Daily OHLCV market data |
| `dwd_stock_factor_daily` | ~1.27M | Technical factors (MA/RSI/MACD/ATR etc.) |
| `dwd_stock_financial_indicator` | ~36K | Financial indicators (ROE, revenue, profit) |
| `mart_stock_selection_daily` | ~620K | Stock screening analysis wide table |
| `etl_job_run` / `etl_job_run_log` | — | ETL task execution records and logs |

## Documentation Index

All project documentation lives in `docs/` (root-level user/ops docs) and each sub-project's `docs/` folder (technical reference docs).

**Documentation hierarchy**:

```
Level 1 — Quick Start
├── docs/QUICK_START.md          First-time installation & deployment
└── docs/DEPLOYMENT.md           Docker deployment deep-dive

Level 2 — User Guides
├── docs/USER_GUIDE.md           Feature usage guide
├── docs/TROUBLESHOOTING.md      Common issues & solutions
└── docs/A股定时任务异常排查与修复方案.md ETL task troubleshooting

Level 3 — Reference Docs
├── stock-fast-api/docs/REGISTRY.md                       # Full API doc (51 endpoints) — authoritative source
├── stock-fast-api/docs/A股股票信息缓存系统数据库设计文档.md    # Database schema
├── stock-fast-api/docs/A股股票信息缓存系统表关系说明文档.md   # Table relationships
├── stock-fast-api/docs/A股股票信息缓存系统架构设计文档.md   # System architecture & data flow
├── stock-etl-engine/docs/定时任务拆分设计文档.md           # ETL engine design
└── docs/问股策略数据库支持分析报告.md                      # 9 strategy analysis

Level 4 — Ops Guide
├── docs/DDL_REFERENCE.md                                  # DDL script reference
└── stock-fast-api/docs/定时任务使用文档.md                  # ETL task manual trigger & management
```

## Task Types

| Type | Description | Approach |
|------|-------------|----------|
| A类 | Convert mock API to real SQL | Full flow (Schema → Repo → Service → Router) |
| B类 | New endpoint | Full flow from scratch |
| C类 | Bug fix | Skip design doc, quick fix |
| D类 | Database change | Database-first |
| E类 | ETL script modification | Independent process (`stock-etl-engine/`) |
| F类 | Documentation maintenance | Lightweight |
