# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a monorepo containing the **A股股票信息缓存系统** (A-Stock Information Caching System):

- `stock-fast-api/` — FastAPI backend (PostgreSQL + SQLAlchemy, raw SQL via `text()`)
- `stock-front_ui/` — Vue 3 + TypeScript frontend (Element Plus, ECharts, lightweight-charts)
- `stock-etl-engine/` — Independent ETL engine (APScheduler + FastAPI, v1.0.1)

**Architecture**: Backend serves a REST API (`/api/v1/*`) consumed by the frontend. The backend handles database queries; the frontend provides the admin dashboard, stock screening console, and operations monitoring UI. Data flows: baostock/efinance → PostgreSQL (16 tables, ~6M rows) → FastAPI → Vue 3 SPA.

## Quick Commands

### Backend (port 8081)
```bash
cd stock-fast-api
./venv/bin/pip install -r requirements.txt
./venv/bin/uvicorn app.main:app --reload --host 0.0.0.0 --port 8081
./venv/bin/pytest                          # run all tests (3 test files)
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
# Docker deployment (from stock-fast-api/)
docker compose build etl-engine && docker compose up -d etl-engine
```

> **Port note**: ETL engine defaults to port 8082 internally (`ETL_API_PORT` in config). Docker maps `8001:8001` for external access; the API service connects via `http://etl-engine:8001` inside the Docker network.

## Repository Structure

```
stock_project/
├── stock-fast-api/          # FastAPI backend (Router → Schema → Service → Repository)
│   ├── app/
│   │   ├── core/            # config, db (SQLAlchemy pool), exceptions, response, deps
│   │   ├── routers/         # 11 API route modules (~45 endpoints)
│   │   ├── schemas/         # Pydantic DTOs (request/response models)
│   │   ├── services/        # 12 business services
│   │   ├── repositories/    # 10 data access repositories (raw SQL via `text()`)
│   │   ├── middleware/      # Request/response middleware + rate limiter
│   │   └── utils/           # pagination, validation helpers
│   ├── tests/               # Backend tests (pytest)
│   └── docs/                # API registry, DB design docs
├── stock-front_ui/          # Vue 3 frontend
│   ├── src/
│   │   ├── api/             # Axios layer (request.ts + 12 domain modules)
│   │   ├── components/      # base/, board/, dashboard/, job/, selection/, stock/, strategy/
│   │   ├── layouts/         # MainLayout.vue
│   │   ├── pages/           # 16 route page components
│   │   ├── router/          # Vue Router (history mode, lazy-loaded) + auth guard
│   │   ├── stores/          # Pinia stores (app, tradeDate, job, selectionTemplate)
│   │   └── types/           # TypeScript interfaces per domain
│   └── mock/                # mock data for development (VITE_USE_MOCK=true)
└── stock-etl-engine/        # Independent ETL engine (APScheduler + FastAPI, v1.0.1)
    ├── app/
    │   ├── core/            # config (port 8082 default), logger
    │   ├── routers/         # trigger router (/api/v1/trigger/*)
    │   ├── services/        # job_service, backfill_service, board_sync_service
    │   ├── jobs/            # 12 ETL scripts (sync, compute, ETL)
    │   └── scheduler.py     # APScheduler lifecycle + daemon threads + job timeout
```

## Key Conventions

- **API prefix**: `/api/v1`
- **Response format**: `{"code": 0, "message": "success", "data": {...}}`
- **Pagination format**: `{"list": [...], "page": 1, "page_size": 20, "total": N}`
- **Auth**: Bearer token in `Authorization` header; BizException codes map to HTTP status (1xxx→401, 4xxx→400, 404x→404, 5xxx→500)
- **Job polling**: frontend polls `/api/v1/jobs?status=RUNNING` every 10s for task status
- **Backend DB access**: raw SQL via `sqlalchemy.text()`; connection pool: pool_size=10, max_overflow=20
- **ETL Engine**: default port 8082 internally (Docker: 8001), health check at `/` returns scheduler status + job next-runs
- **Error codes**: defined in `app/core/exceptions.py`

## Development Workflow

### Task Types

| Type | Description | Approach |
|------|-------------|----------|
| A类 | Mock 接口改为真实 SQL | Full flow (Schema → Repo → Service → Router) |
| B类 | 新增接口 | Full flow from scratch |
| C类 | Bug 修复 | Skip design doc, quick fix |
| D类 | 数据库变更 | Database-first |
| E类 | ETL 脚本修改 | Independent process |
| F类 | 文档维护 | Lightweight |

### Backend Self-Test Three-Step

```bash
# 1. Syntax check
python -m py_compile app/repositories/xxx.py

# 2. Start test server
./venv/bin/uvicorn app.main:app --reload --host 0.0.0.0 --port 8081

# 3. curl the endpoint
curl http://localhost:8081/api/v1/stocks/search?keyword=茅台
```

## Adding/Modifying Backend APIs

Follow the **Schema → Repository → Service → Router** order:

1. Define request/response Pydantic models in `schemas/`
2. Write the SQL query in `repositories/` using `text(sql)` + `db.execute(text(sql), params)`
3. Implement business logic in `services/`
4. Wire up the route in `routers/`
5. Self-test: syntax check → start server → `curl` the endpoint

## Documentation Index

All project documentation lives in `docs/` (root-level user/ops docs) and each sub-project's `docs/` folder (technical reference docs).

**文档层级结构**：

```
一级 — 快速开始
├── docs/QUICK_START.md          首次安装部署
└── docs/DEPLOYMENT.md           Docker 部署详解

二级 — 用户指南
├── docs/USER_GUIDE.md           功能使用方法
└── docs/TROUBLESHOOTING.md      常见问题与解决方案

三级 — 参考文档
├── stock-fast-api/docs/REGISTRY.md                             # API 完整文档（42+端点）— 权威来源
├── stock-fast-api/docs/A股股票信息缓存系统数据库设计文档.md  # 数据库表结构
├── stock-fast-api/docs/A股股票信息缓存系统表关系说明文档.md  # 表之间关联
├── stock-fast-api/docs/A股股票信息缓存系统架构设计文档.md    # 系统架构与数据流
├── stock-front_ui/docs/A股股票信息缓存系统前后端API接口设计文档.md  # 前端视角 API 速查
└── stock-etl-engine/docs/定时任务拆分设计文档.md              # ETL 引擎拆分设计

四级 — 运维指南
├── docs/DDL_REFERENCE.md                                        # 数据库 DDL 脚本说明
├── stock-fast-api/docs/定时任务使用文档.md                      # ETL 任务配置管理
└── stock-etl-engine/app/scheduler.py                            # APScheduler 守护线程 + 超时机制 (v1.0.1)
```

**文档导航入口**：`docs/README.md` — 包含所有文档的层级索引和快速链接。
