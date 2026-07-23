# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**A股股票信息缓存系统** — A local data analysis platform for China's A-share market that synchronizes daily market data from baostock/efinance into PostgreSQL and provides a web dashboard for stock screening, individual stock analysis, sector analysis, and ETL job monitoring.

### Service Topology (v0.5.0)

The project is a three-service monorepo with the ETL engine decoupled as an independent microservice:

| Service | Directory | Port | Role | Tech Stack |
|---------|-----------|------|------|------------|
| **Frontend** | `stock-front_ui/` | 5173 | Web dashboard | Vue 3, TypeScript, Vite, Element Plus |
| **Backend API** | `stock-fast-api/` | 8000 | REST API & job orchestration | FastAPI, SQLAlchemy, Python |
| **ETL Engine** | `stock-etl-engine/` | 8002* | Data scraping & processing | FastAPI, APScheduler, Python |

*\*Internal port: 8082. Docker mapped port: 8001.*

### Architecture Flow

```
┌──────────────────────────────────────────────────────────┐
│                  stock-front_ui (:5173)                   │
│    Vue 3 | TypeScript | Element Plus | ECharts            │
│   Dashboard | Selection | Stock Analysis | Job Monitor    │
└─────────────────────┬────────────────────────────────────┘
                      │ /api → http://localhost:8000/api/v1
┌─────────────────────▼────────────────────────────────────┐
│              stock-fast-api (:8000)                       │
│   FastAPI | SQLAlchemy 11 Routers | ~50 endpoints          │
│   Router → Schema → Service → Repository                  │
│   HTTP calls to ETL Engine for async job execution        │
└───────────┬──────────────────────┬────────────────────────┘
            │                      │ (shared database)
┌───────────▼──────────────────────▼────────────────────────┐
│              PostgreSQL (:5432)                           │
│     16 tables | ~6M+ rows                                 │
└───────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────┐
│           stock-etl-engine (:8001 Docker / :8082)         │
│     APScheduler | 7 active scheduled jobs                 │
│     Data sources: baostock, efinance                      │
└───────────────────────────────────────────────────────────┘
```

## Global Commands

```bash
# Start all services with Docker (recommended)
cd stock-fast-api && docker compose up -d

# Individual service startup
cd stock-front_ui && npm install && npm run dev          # Frontend (:5173)
cd stock-fast-api && ./venv/bin/uvicorn app.main:app --reload  # Backend (:8000)
cd stock-etl-engine && ./venv/bin/uvicorn app.main:app --reload  # ETL Engine (:8082)

# Database initialization (manual setup only)
psql -h <host> -U <user> -d <dbname> -f docs/09_postgresql_ddl.sql
```

## Testing & Linting

### Backend (stock-fast-api)
```bash
# Run all tests
./venv/bin/pytest

# Run specific test file
./venv/bin/pytest tests/test_example.py -v

# Syntax check
python -m py_compile app/repositories/example.py
```

### Frontend (stock-front_ui)
- No dedicated test framework; TypeScript strict mode via `vue-tsc` in build process
- Build command: `npm run build` (includes type checking)

## Key Architecture Patterns

### Backend API Flow
1. **Router Layer** (`app/routers/`) → Receives requests, calls services, returns unified responses
2. **Schema Layer** (`app/schemas/`) → Pydantic models for request/response validation  
3. **Service Layer** (`app/services/`) → Business logic and data aggregation
4. **Repository Layer** (`app/repositories/`) → Raw SQL queries via SQLAlchemy `text()` (no ORM)

### ETL Engine Architecture
- **Scheduler Orchestration**: APScheduler manages 7 active cron jobs with DB tracking
- **Job Tracking**: Each job creates RUNNING record in `etl_job_run`, updates status on completion/failure
- **Safety Wrapper**: Individual job failures don't crash the scheduler via `_wrap_job_for_record()`
- **Trade Day Guard**: Checks `dwd_trade_calendar` before running daily syncs

### Frontend Architecture  
- **API Layer**: Axios with unified error handling and Bearer token injection
- **State Management**: Pinia stores for auth, jobs (with 10s polling), selection templates
- **Routing**: History mode with lazy-loaded components and auth guard
- **Components**: Organized by domain (charts, tables, forms) with virtual scrolling for large datasets

## Unified Conventions

### API Response Format
```json
{"code": 0, "message": "success", "data": {...}}
```

### Error Handling
- Backend: Business exceptions defined in `app/core/exceptions.py`
- Frontend: HTTP errors (401→login redirect, 403/5xx→user-friendly messages)
- ETL Engine: Job failures logged and marked as FAILED without stopping scheduler

### Database Access Patterns
- **Backend**: SQLAlchemy `text()` for raw SQL execution via `self.db.execute(text(sql), params)`
- **ETL Engine**: Direct psycopg2 connections (no SQLAlchemy ORM)
- **Timezone**: Always use `from app.core.timezone import now` — never bare `datetime.now()`

### Authentication & Security
- JWT tokens stored in localStorage, injected as `Authorization: Bearer` header
- Backend-EtL communication requires `X-API-Key` header matching `ETL_ENGINE_API_KEY`
- Health check endpoints exempt from API key validation

## Scheduled Jobs (Beijing Time UTC+8)

| Job | Cron | Description | Status |
|-----|------|-------------|--------|
| New IPO Board Sync | 17:10 Mon-Fri | Recent IPO stocks + board assignments | Active |
| Adjustment Factor Sync | 17:30 Mon-Fri | Price adjustment factors for OHLCV | Active |
| Stock Master Data Sync | 23:50 Mon-Fri | Full market stock basic info | Active |
| Daily OHLCV Sync | 19:00 Mon-Fri | Complete market daily OHLCV data | Active |
| Technical Factor Compute | 23:00 Mon-Fri | MA/RSI/MACD/BOLL indicators | Active |
| Selection Mart Build | 23:30 Mon-Fri | Stock screening wide table aggregation | Active |
| Log Cleanup | 00:05 daily | Remove logs >3 days old | Active |

> **Paused**: Financial indicator sync (was 21:30) — still accessible via manual trigger API.

## Documentation Index

For detailed information about specific services, refer to their sub-project CLAUDE.md files:
- **Backend Development**: [`stock-fast-api/CLAUDE.md`](./stock-fast-api/CLAUDE.md)
- **Frontend Development**: [`stock-front_ui/CLAUDE.md`](./stock-front_ui/CLAUDE.md)  
- **ETL Engine**: [`stock-etl-engine/CLAUDE.md`](./stock-etl-engine/CLAUDE.md)

### Core Documentation
- [Quick Start Guide](docs/QUICK_START.md) — Setup and deployment instructions
- [API Registry](stock-fast-api/docs/REGISTRY.md) — Complete API documentation (50 endpoints)
- [Database Design](stock-fast-api/docs/A股股票信息缓存系统数据库设计文档.md) — Table structures and relationships
- [Architecture Design](stock-fast-api/docs/A股股票信息缓存系统架构设计文档.md) — System architecture details

## Development Workflow

### Task Classification
| Type | Description | Approach |
|------|-------------|----------|
| A-class | Mock interface to real SQL | Full development flow |
| B-class | New endpoint (after frontend/backend agreement) | Full development flow |  
| C-class | Bug fixes | Quick locate and fix, skip technical design |
| D-class | Database changes (new table/field/index) | Database first approach |
| E-class | ETL script modifications | Independent flow |
| F-class | Documentation maintenance | Lightweight process |

### Self-Testing Checklist
1. **Syntax check**: `python -m py_compile app/repositories/example.py`
2. **Start services**: Run backend and frontend locally
3. **API verification**: Test endpoints with curl or API client
4. **Database validation**: Verify data integrity in PostgreSQL