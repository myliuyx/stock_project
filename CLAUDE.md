# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**A股股票信息缓存系统** — A local data analysis platform for China's A-share market that synchronizes daily market data from baostock/efinance into PostgreSQL and provides a web dashboard for stock screening, individual stock analysis, sector analysis, and ETL job monitoring.

### Service Topology (v1.6.x)

Three-service monorepo with the ETL engine decoupled as an independent microservice:

| Service | Directory | Docker Port | Local Dev Port | Role |
|---------|-----------|-------------|----------------|------|
| **Frontend** | `stock-front_ui/` | — | 5173 | Web dashboard | Vue 3, TypeScript, Vite, Element Plus |
| **Backend API** | `stock-fast-api/` | 8000 | 8081 | REST API & job orchestration | FastAPI, SQLAlchemy, Python |
| **ETL Engine** | `stock-etl-engine/` | 8001 | 8082 (local) | Data scraping & processing | FastAPI, APScheduler, Python |

### Architecture Flow

```
┌───────────────────────────────────────────────────────┐
│              stock-front_ui (:5173 dev / :80 prod)     │
│   Vue 3 | TS | Element Plus | ECharts | lightweight-charts
│   Dashboard | Selection | Stock Analysis | Jobs        │
└─────────────────────┬─────────────────────────────────┘
                      │ /api → http://localhost:8000/api/v1 (Docker)
                      │                    or :8081 (local dev)
┌─────────────────────▼─────────────────────────────────┐
│          stock-fast-api (:8000 Docker / :8081 local)   │
│   FastAPI | SQLAlchemy 12 routers | ~50 endpoints      │
│   Router → Schema → Service → Repository (raw SQL)     │
│   HTTP calls to ETL Engine for async job execution     │
└───────────┬──────────────────────┬──────────────────────┘
            │                      │ (shared database)
┌───────────▼──────────────────────▼──────────────────────┐
│         PostgreSQL (:5432)                               │
│      16 tables | ~6M+ rows                               │
└─────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────┐
│    stock-etl-engine (:8001 Docker / :8082 local)        │
│   APScheduler | 11 job scripts (7 active + 1 paused + 3 unregistered) │
│   Data sources: baostock, efinance                      │
└────────────────────────────────────────────────────────┘
```

## Global Commands

### Start All Services (Docker)

```bash
cd stock-fast-api
cp .env.example .env  # edit with DB and JWT settings
docker compose up -d
# PostgreSQL :5432, FastAPI :8000, ETL Engine :8001
```

### Individual Service Local Dev

```bash
# Frontend (port 5173)
cd stock-front_ui && npm install && npm run dev

# Backend (port 8081 local; production port is 8000 via Docker)
cd stock-fast-api && ./venv/bin/uvicorn app.main:app --reload --host 0.0.0.0 --port 8081

# ETL Engine (port 8082 local; Docker maps external :8001 → internal :8082)
cd stock-etl-engine && ./venv/bin/uvicorn app.main:app --reload --port 8082
```

### Testing & Verification

```bash
# Backend tests (pytest, no fixtures beyond venv)
cd stock-fast-api && ./venv/bin/pytest                  # all tests
./venv/bin/pytest tests/test_xxx.py -v                   # single file

# Syntax check
python -m py_compile app/repositories/xxx.py

# Verify API
curl http://localhost:8081/api/v1/system/meta             # backend health
curl http://localhost:8001/                                # ETL engine health

# Frontend build (includes vue-tsc type checking)
cd stock-front_ui && npm run build
```

## Architecture Patterns

### Backend API Flow (`stock-fast-api`)

**Router → Schema → Service → Repository**:

| Layer | Directory | Purpose |
|-------|-----------|---------|
| Router | `app/routers/` (~10 .py + extras) | Receive requests, call services, return unified responses |
| Schema | `app/schemas/` (~13 .py files) | Pydantic models for request/response validation |
| Service | `app/services/` (~10 .py files) | Business logic and data aggregation |
| Repository | `app/repositories/` (~11 .py files) | Raw SQL queries via SQLAlchemy `text()` — **no ORM** |

Key core files: `core/config.py` (env vars), `core/deps.py` (DI: get_db, get_current_user), `core/exceptions.py` (BizException), `core/response.py` (unified envelope), `middleware/rate_limit.py`.

### ETL Engine Architecture (`stock-etl-engine`)

- **Scheduler**: APScheduler manages cron jobs with DB tracking in `scheduler.py` (525 lines)
- **Job Tracking**: Each job creates RUNNING record in `etl_job_run`, updates status on completion/failure
- **Thread-level Timeout**: `_wrap_with_timeout(func, timeout_sec=28800)` — 8h default thread-level timeout to prevent hung tasks
- **Safety Wrapper**: `_wrap_job_for_record()` catches exceptions, logs error, marks FAILED — individual failures don't crash the scheduler
- **Trade Day Guard**: Checks `dwd_trade_calendar` before running daily syncs
- **Lock File**: Uses `fcntl.flock` to prevent duplicate schedulers
- **Authentication**: Backend-to-ETL requires `X-API-Key` header matching `ETL_ENGINE_API_KEY`; health check endpoints exempt

### Frontend Architecture (`stock-front_ui`)

- **API Layer**: Axios with unified error handling, Bearer token injection from localStorage, business error code handling
- **State Management**: Pinia stores — auth/app (token, user), job (10s polling for RUNNING jobs), selectionTemplate, tradeDate
- **Routing**: History mode, lazy-loaded components, auth guard checks `localStorage.getItem('token')`
- **Components**: Organized by domain (base, dashboard, job, selection, stock, strategy)
- **Charts**: lightweight-charts for K-line, ECharts for volume/regular charts
- **Virtual Scrolling**: vue-virtual-scroller for large tables (>500 rows) via VirtualTable

## Unified Conventions

### API Response Format
```json
{"code": 0, "message": "success", "data": {...}}
```

### Pagination Format
```json
{"list": [...], "page": 1, "page_size": 20, "total": N}
```

### Adding New Endpoints (Backend)
Follow: **Schema → Repository → Service → Router**. If target table has no data, keep Mock with `// TODO` comment for real SQL. Repository uses `self.db.execute(text(sql), params)` — never ORM.

### Timezone
Always use `from app.core.timezone import now` — never bare `datetime.now()`.

### Port Reference (Quick)

| Service | Docker | Local Dev | Notes |
|---------|--------|-----------|-------|
| Frontend | 80 (Nginx) | 5173 (Vite HMR) | — |
| Backend API | 8000 | 8081 | Docker-compose maps :8000; local dev uses :8081 |
| ETL Engine | 8001 (ext) / 8082 (int) | 8082 | App listens on 8082 internally |
| PostgreSQL | 5432 | 5432 | — |

## Scheduled Jobs (ETL Engine, Beijing Time UTC+8)

| Job | Cron | Script | Status |
|-----|------|--------|--------|
| New IPO Board Sync | 17:10 Mon-Fri | `sync_new_ipo_boards.py` | Active |
| Daily OHLCV Sync | 19:00 Mon-Fri | `sync_stock_daily.py` | Active |
| Adjustment Factor Sync | 20:00 (paused) | `sync_adjust_factor.py` | Paused |
| Financial Indicator Sync | 21:30 (paused) | `etl_financial_indicator.py` | Paused |
| Technical Factor Compute | 23:00 Mon-Fri | `compute_factor.py` | Active |
| Selection Mart Build | 23:30 Mon-Fri | `build_selection_mart.py` | Active |
| Stock Master Data Sync | 18:00 daily | `sync_security_master.py` | Active |
| Log Cleanup | 00:05 daily | scheduler.py internal | Active |

> Paused jobs are still accessible via manual trigger API from the backend.

## Task Classification (Backend Development)

| Type | Scenario | Process |
|------|----------|---------|
| A-class | Mock → real SQL | Full flow (Schema→Repo→Service→Router) |
| B-class | New endpoint | Full flow |
| C-class | Bug fixes | Skip design, locate and fix quickly |
| D-class | Database changes | DDL first |
| E-class | ETL script modifications | Independent flow |
| F-class | Documentation maintenance | Lightweight |

## Self-Testing Checklist (Backend)

```bash
# 1. Syntax check → python -m py_compile app/repositories/xxx.py
# 2. Start test server → ./venv/bin/uvicorn app.main:app --reload --port 8081
# 3. Verify with curl → curl http://localhost:8081/api/v1/stocks/search?keyword=茅台
```

## Documentation Index

| Document | Location | Description |
|----------|----------|-------------|
| **Sub-project CLAUDE.md** | `stock-fast-api/CLAUDE.md`, `stock-front_ui/CLAUDE.md`, `stock-etl-engine/CLAUDE.md` | Service-specific details, file indexes |
| [Quick Start](docs/QUICK_START.md) | Root `docs/` | Setup and deployment guide (bilingual) |
| [User Guide](docs/USER_GUIDE.md) | Root `docs/` | Feature usage instructions |
| [Troubleshooting](docs/TROUBLESHOOTING.md) | Root `docs/` | Common issues and fixes |
| [API Registry](stock-fast-api/docs/REGISTRY.md) | Backend `docs/` | **Authoritative API docs** (~50 endpoints) |
| [Database Design](stock-fast-api/docs/A股股票信息缓存系统数据库设计文档.md) | Backend `docs/` | Table structures and relationships |
| [Architecture Design](stock-fast-api/docs/A股股票信息缓存系统架构设计文档.md) | Backend `docs/` | System architecture details |
| [DDL Script](docs/09_postgresql_ddl.sql) | Root `docs/` | PostgreSQL DDL (16 tables) |
