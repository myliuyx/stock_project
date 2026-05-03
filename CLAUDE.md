# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a monorepo containing the **A股股票信息缓存系统** (A-Stock Information Caching System):

- `stock-fast-api/` — FastAPI backend (PostgreSQL + SQLAlchemy)
- `stock-front_ui/` — Vue 3 + TypeScript frontend (Element Plus, ECharts)

**Architecture**: Backend serves a REST API (`/api/v1/*`) consumed by the frontend. The backend handles database queries; the frontend provides the admin dashboard, stock screening console, and operations monitoring UI.

## Quick Commands

### Backend
```bash
cd stock-fast-api
./venv/bin/pip install -r requirements.txt
./venv/bin/uvicorn app.main:app --reload --host 0.0.0.0 --port 8081
./venv/bin/pytest                          # run tests
./venv/bin/pytest tests/ -v               # run single test file
```

### Frontend
```bash
cd stock-front_ui
npm install
npm run dev                               # dev server on :5173 (proxies /api to 192.168.3.18:8000)
npm run build
```

## Repository Structure

```
stock_project/
├── stock-fast-api/          # FastAPI backend (Router → Schema → Service → Repository)
│   ├── app/
│   │   ├── core/            # config, db, exceptions, response
│   │   ├── routers/         # 9 API route modules (31 endpoints)
│   │   ├── schemas/         # Pydantic DTOs
│   │   ├── services/        # 8 business services
│   │   ├── repositories/    # 7 data access repositories
│   │   ├── utils/           # pagination, validation
│   │   └── scheduler.py     # scheduled task runner
│   └── docs/                # API registry, DB design docs
└── stock-front_ui/           # Vue 3 frontend
    ├── src/
    │   ├── api/             # Axios layer (request.ts + domain modules)
    │   ├── components/      # base/, stock/, selection/, job/, board/
    │   ├── pages/           # route page components
    │   ├── stores/          # Pinia stores (app, tradeDate, job, selectionTemplate)
    │   └── layouts/         # MainLayout.vue
    └── mock/                # mock data for development
```

## Key Conventions

- **API prefix**: `/api/v1`
- **Response format**: `{"code": 0, "message": "success", "data": {...}}`
- **Pagination format**: `{"list": [...], "page": 1, "page_size": 20, "total": N}`
- **Auth**: Bearer token in `Authorization` header; 401 → redirect to `/login`
- **Job polling**: frontend polls `/api/v1/jobs?status=RUNNING` every 10s for task status

## Detailed Documentation

Each sub-project has its own CLAUDE.md with complete details:
- Backend specifics → `stock-fast-api/CLAUDE.md`
- Frontend specifics → `stock-front_ui/CLAUDE.md`
