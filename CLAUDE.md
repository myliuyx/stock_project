# CLAUDE.md

This file provides guidance for Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a monorepo containing the **A股股票信息缓存系统** (A-Stock Information Caching System):

- `stock-fast-api/` — FastAPI backend REST API (PostgreSQL + SQLAlchemy, raw SQL via `text()`)
- `stock-front_ui/` — Vue 3 + TypeScript frontend (Element Plus, ECharts, lightweight-charts)
- `stock-etl-engine/` — Independent ETL engine with APScheduler (FastAPI-based, v1.0.8)

**Architecture**: Backend serves a REST API (`/api/v1/*`) consumed by the frontend (~51 endpoints). The backend handles database queries and job orchestration; the frontend provides admin dashboard, stock screening console, and strategy analysis UI. Data flows: baostock/efinance → ETL Engine (APScheduler) → PostgreSQL (16 tables, ~6M+ rows) → FastAPI → Vue 3 SPA.

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
./venv/bin/pytest tests/test_file.py -v    # single test file
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

> **Port note**: ETL engine defaults to port 8082 internally (`ETL_API_PORT` in config). Docker maps `8001:8001` for external access; the API service connects via `http://etl-engine:8001` inside the Docker network.

## Repository Structure

```
stock_project/
├── stock-fast-api/          # FastAPI backend (Router → Schema → Service → Repository)
│   ├── app/
│   │   ├── core/            # config, db (SQLAlchemy pool), exceptions, response, deps
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
    │   ├── core/            # config (port 8082 default), logger, exceptions, response
    │   ├── routers/         # trigger router (/api/v1/trigger/*)
    │   ├── services/        # job_service, backfill_service, board_sync_service
    │   ├── jobs/            # 10 active ETL scripts (sync, compute, mart)
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
- **Error codes**: defined in `app/core/exceptions.py`

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

| # | 任务名 | cron 时间 | 说明 |
|---|--------|----------|------|
| 1 | 新股板块增量同步 | 17:30 | 盘后同步新股相关板块 |
| 2 | 股票主数据同步 | 18:00 | 同步新增/退市股票信息 |
| 3 | 日线行情同步 | 19:00 | 拉取当日日K线数据 |
| 4 | 技术因子计算 | **23:00** | 计算MA/MACD/BOLL等技术指标 |
| 5 | 选股宽表构建 | **23:30** | 基于因子数据构建选股分析宽表 |
| - | 复权因子同步 | (已暂停) | 原 20:00，暂未启用 |
| - | 财务指标同步 | (已暂停) | 原 21:30，暂未启用 |
| — | 日志清理 | 次日 00:05 | 定时清理过期 ETL 日志文件 |

## Documentation Index

All project documentation lives in `docs/` (root-level user/ops docs) and each sub-project's `docs/` folder (technical reference docs).

**文档层级结构**：

```
一级 — 快速开始
├── docs/QUICK_START.md          首次安装部署
└── docs/DEPLOYMENT.md           Docker 部署详解

二级 — 用户指南
├── docs/USER_GUIDE.md           功能使用方法
├── docs/TROUBLESHOOTING.md      常见问题与解决方案
└── docs/A股定时任务异常排查与修复方案.md   ETL 任务问题排查

三级 — 参考文档
├── stock-fast-api/docs/REGISTRY.md                             # API 完整文档（51端点）— 权威来源
├── stock-fast-api/docs/A股股票信息缓存系统数据库设计文档.md    # 数据库表结构
├── stock-fast-api/docs/A股股票信息缓存系统表关系说明文档.md    # 表之间关联
├── stock-fast-api/docs/A股股票信息缓存系统架构设计文档.md      # 系统架构与数据流（需更新）
├── stock-etl-engine/docs/定时任务拆分设计文档.md               # ETL 引擎拆分设计
└── docs/问股策略数据库支持分析报告.md                          # 9种选股策略分析

四级 — 运维指南
├── docs/DDL_REFERENCE.md                                       # 数据库 DDL 脚本说明
└── stock-fast-api/docs/定时任务使用文档.md                     # ETL 任务手动触发与管理
```

**文档导航入口**：`docs/README.md` — 包含所有文档的层级索引和快速链接。

## Task Types

| Type | Description | Approach |
|------|-------------|----------|
| A类 | Mock 接口改为真实 SQL | Full flow (Schema → Repo → Service → Router) |
| B类 | 新增接口 | Full flow from scratch |
| C类 | Bug 修复 | Skip design doc, quick fix |
| D类 | 数据库变更 | Database-first |
| E类 | ETL 脚本修改 | Independent process (stock-etl-engine/) |
| F类 | 文档维护 | Lightweight |
