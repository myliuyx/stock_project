# A股股票信息缓存系统 
# A-Stock Information Caching System

[中文](#中文) | [English](#english)

---

## 中文

### 项目介绍

A 股股票信息缓存系统是一个 A 股本地化数据分析平台，通过 [baostock](http://www.baostock.com/) 和 efinance 将每日市场数据同步到本地 PostgreSQL 数据库，并提供功能完整的 Web 管理后台。

**v0.5.0 更新**：ETL 引擎（stock-etl-engine）已从后端独立为单独的微服务，负责所有定时任务调度；新增 9 种选股策略和问股分析功能。

### 核心功能

- **盘后数据同步** — 独立 ETL 引擎自动同步全市场行情、财务指标、技术因子（周一至周五）
- **9种选股策略** — 底部放量/箱体震荡/多头趋势/缠论/均线金叉/一阳夹三阴/缩量回踩/放量突破/波浪理论
- **问股分析** — 输入股票代码，自动用全部9种策略扫描并给出评分和信号
- **个股分析** — K线图、历史行情、技术指标、财务数据、所属板块
- **板块分析** — 行业板块、概念板块、成分股查询
- **自选股管理** — 添加/移除自选股、技术面实时监控
- **ETL 任务监控** — 实时任务状态、手动触发、执行日志（通过 HTTP 调用独立 ETL 引擎服务）
- **数据回补** — 支持个股历史数据补充

### 系统架构（v0.5.0）

ETL Engine 已从 v0.5.0 起独立为单独的微服务，负责所有定时任务调度：

```
┌──────────────────────────────────────────────────────────┐
│                  stock-front_ui (:5173)                   │
│    Vue 3 | TypeScript | Element Plus | ECharts            │
│   控制台 │ 选股 │ 个股 │ 板块 │ 策略分析 │ 任务监控        │
└─────────────────────┬────────────────────────────────────┘
                      │ /api → http://localhost:8000/api/v1
┌─────────────────────▼────────────────────────────────────┐
│              stock-fast-api (:8000)                       │
│   FastAPI | SQLAlchemy 11个Router | ~50端点               │
│   Router → Schema → Service → Repository                  │
│   HTTP 调用 ETL Engine 执行任务                           │
└───────────┬──────────────────────┬────────────────────────┘
            │                      │ (共享数据库)
┌───────────▼──────────────────────▼────────────────────────┐
│              PostgreSQL (:5432)                           │
│     16张表 | ~600万+行数据                                │
└───────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────┐
│           stock-etl-engine (:8001 Docker / :8082)         │
│     APScheduler | 7个活跃定时任务 + 自动调度               │
│     数据来源: baostock, efinance                          │
└───────────────────────────────────────────────────────────┘
```

### 技术栈

| 层级 | 技术 |
|------|------|
| 前端 | Vue 3, TypeScript, Vite, Element Plus, ECharts, lightweight-charts |
| 后端 API | FastAPI, SQLAlchemy, Pydantic |
| ETL Engine | APScheduler, baostock/efinance SDK, FastAPI |
| 数据库 | PostgreSQL 15+ |

### 快速开始

#### 方式一：Docker Compose（推荐）

```bash
git clone https://github.com/myliuyx/stock_project.git
cd stock_project

# 配置环境变量
cp stock-fast-api/.env.example stock-fast-api/.env
# 编辑 .env 填入数据库和 JWT 配置

# 一键启动所有服务（PostgreSQL + FastAPI + ETL Engine）
cd stock-fast-api && docker compose up -d

# 前端开发模式
cd stock-front_ui
npm install && npm run dev
```

> **端口说明**：FastAPI `:8000`，ETL Engine Docker `:8001`，前端 `:5173`（开发），PostgreSQL `:5432`

#### 方式二：手动部署

**后端：**

```bash
cd stock-fast-api

# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 安装依赖
pip install -r requirements.txt

# 配置环境变量
cp .env.example .env
# 编辑 .env 填入数据库和 JWT 配置

# 初始化数据库
psql -h <host> -U <user> -d <dbname> -f docs/09_postgresql_ddl.sql

# 启动服务（端口 :8000）
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

**ETL Engine：**

```bash
cd stock-etl-engine

python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 启动 ETL 引擎（内部 :8082，Docker 映射外部 :8001）
uvicorn app.main:app --reload --host 0.0.0.0 --port 8082
```

**前端：**

```bash
cd stock-front_ui

npm install
npm run dev
# 访问 http://localhost:5173（/api 代理到 :8000）
```

### 环境变量说明

**后端 (`stock-fast-api/.env`)：**

| 变量名 | 必填 | 默认值 | 说明 |
|--------|------|--------|------|
| `DB_HOST` | 是 | - | PostgreSQL 主机地址 |
| `DB_PORT` | 否 | 5432 | PostgreSQL 端口 |
| `DB_NAME` | 是 | - | 数据库名 |
| `DB_USER` | 是 | - | 数据库用户 |
| `DB_PASSWORD` | 是 | - | 数据库密码 |
| `JWT_SECRET_KEY` | 是 | - | JWT 签名密钥（至少32字符） |
| `CORS_ORIGINS` | 否 | - | 允许的跨域地址（逗号分隔） |

### 项目结构

```
stock_project/
├── stock-fast-api/          # FastAPI 后端 REST API（:8000）
│   ├── app/
│   │   ├── core/            # 配置、数据库连接、异常、响应封装
│   │   ├── routers/         # API 路由模块（12个文件，~50端点）
│   │   ├── schemas/         # Pydantic 数据模型
│   │   ├── services/        # 业务逻辑（10个服务）
│   │   ├── repositories/    # 数据访问层（10个仓库）
│   │   └── middleware/      # 请求处理中间件
│   ├── docs/                # API 文档、数据库设计、架构文档
│   ├── tests/               # 后端测试
│   └── docker-compose.yml   # Docker 部署配置
├── stock-front_ui/          # Vue 3 前端（:5173 开发）
│   ├── src/
│   │   ├── api/             # Axios 请求层（12个模块）
│   │   ├── components/      # 按模块分类的 Vue 组件
│   │   ├── views/           # 路由页面组件（16个）
│   │   ├── stores/          # Pinia 状态管理
│   │   └── layouts/         # 主布局组件
├── stock-etl-engine/        # ETL 引擎独立微服务（Docker :8001 / 内网 :8082）
│   ├── app/
│   │   ├── core/            # 配置、日志、异常处理
│   │   ├── routers/         # 触发接口 (/api/v1/trigger/*)
│   │   ├── services/        # ETL 任务服务
│   │   ├── jobs/            # 7个活跃定时脚本（同步、计算、构建）
│   │   └── scheduler.py     # APScheduler 调度 + 超时保护
│   ├── docs/                # ETL 引擎设计文档
│   └── Dockerfile           # ETL 容器镜像
└── docs/                    # 项目级文档（快速入门、部署指南等）
```

### 数据库表

| 层级 | 表名 | 记录数（约） | 说明 |
|------|------|-------------|------|
| **维度** | `dwd_security_master` | 5,198 | 股票主数据（代码、名称、交易所、行业） |
|  | `dwd_board_master` + `dwd_board_relation` | 83 / 5,199 | 板块定义与股票-板块关系 |
| **事实** | `dwd_trade_calendar` | — | A股交易日历 |
|  | `dwd_stock_daily` | ~487万 | 日线行情数据（OHLCV） |
|  | `dwd_stock_factor_daily` | ~127万 | 技术指标（MA/RSI/MACD/ATR等） |
|  | `dwd_stock_financial_indicator` | ~36,000 | 财务指标（ROE/营收/利润等） |
| **宽表** | `mart_stock_selection_daily` | ~62万 | 选股分析宽表 |
|  | `etl_job_run` + `etl_job_run_log` | — | ETL 任务执行记录与日志 |

### API 文档

API 前缀：`/api/v1`（后端端口 :8000）

| 模块 | 端点数 | 说明 |
|------|--------|------|
| Auth | 2 | 登录、Token 验证 |
| Dashboard | 4 | 系统概览、自选股分析 |
| Selection | 5 | 选股结果查询/导出 |
| Stocks | 9 | 行情搜索、个股资料、日线、因子、财务等 |
| **Strategies** | **4** | **9种策略列表、详情、查询、问股分析（v0.5.0 新增）** |
| Jobs | **13** | **任务列表、7个专用触发接口 + 通用触发、日志（ETL Engine HTTP调用）** |
| Coverage | 3 | 数据覆盖概览 |
| Boards | 3 | 板块列表、详情、成分股 |
| Backfill | 2 | 历史数据回补 |
| System | 1 | 系统元信息 |
| Watchlist | 4 | 自选股管理 |

完整 API 文档：[`stock-fast-api/docs/REGISTRY.md`](stock-fast-api/docs/REGISTRY.md)

### ETL 定时任务（stock-etl-engine）

> 详细配置见 [定时任务使用文档](stock-fast-api/docs/定时任务使用文档.md)。
> 调度逻辑已迁移至独立服务 `stock-etl-engine`，由 APScheduler 管理。

时间均为北京时间（UTC+8），周一至周五自动执行：

| # | 任务名 | Cron 时间 | 说明 |
|---|--------|----------|------|
| 1 | 新股板块增量同步 | **17:10** | 近7天上市新股及其所属板块 |
| 2 | 复权因子同步 | **17:30** | OHLCV 价格调整因子 |
| 3 | 股票主数据同步 | **23:50** | 全市场股票基础信息 |
| 4 | 日线行情同步 | **19:00** | 全市场行情 OHLCV 数据 |
| 5 | 技术因子计算 | **23:00** | MA/RSI/MACD/BOLL 等指标 |
| 6 | 选股宽表构建 | **23:30** | 汇总行情+因子+财务 → 选股分析 |
| — | 日志清理 | 每日 00:05（独立） | 清理超3天日志文件 |

> ⏸️ 已暂停：财务指标同步 (原21:30) —— 仍可通过手动触发接口调用。

### 相关文档

- [快速入门指南](docs/QUICK_START.md)
- [用户使用指南](docs/USER_GUIDE.md)
- [Docker 部署详解](docs/DEPLOYMENT.md)
- [API 接口文档（50端点）](stock-fast-api/docs/REGISTRY.md)
- [数据库设计文档](stock-fast-api/docs/A股股票信息缓存系统数据库设计文档.md)
- [架构设计文档](stock-fast-api/docs/A股股票信息缓存系统架构设计文档.md)

### 许可证

MIT License

---

## English

### Overview

A-Stock Information Caching System is a local data analysis platform for China's A-share market (A股). It synchronizes daily market data from [baostock](http://www.baostock.com/) into a local PostgreSQL database and provides a full-featured web dashboard for stock screening, individual stock analysis, sector analysis, and ETL job monitoring.

### Features

- **Daily Data Sync** — Automatic synchronization of full-market OHLCV data, financial indicators, and technical factors after market close
- **Smart Stock Screening** — Filter stocks by technical indicators (MA, RSI, MACD, ATR) and financial metrics (ROE, revenue, P/E)
- **Individual Stock Analysis** — K-line charts, historical prices, technical factors, financial data, and board membership
- **Sector/Board Analysis** — Industry boards, concept boards, and member stocks
- **ETL Job Monitoring** — Real-time task status, manual triggering, execution logs
- **Historical Data Backfill** — Supplement missing data for individual stocks
- **Data Coverage Tracking** — Monitor historical data completeness per stock

### Architecture (v0.5.0)

ETL Engine was decoupled into an independent microservice in v0.5.0 for all scheduled task orchestration:

```
┌───────────────────────────────────────────────────────┐
│              stock-front_ui (:5173)                    │
│   Vue 3 | TypeScript | Element Plus | ECharts          │
│   Dashboard │ Selection │ Stock │ Strategies │ Jobs    │
└─────────────────────┬─────────────────────────────────┘
                      │ /api → http://localhost:8000/api/v1
┌─────────────────────▼─────────────────────────────────┐
│          stock-fast-api (:8000)                        │
│   FastAPI | SQLAlchemy | 11 Routers | ~50 endpoints    │
│   Router → Schema → Service → Repository               │
│   HTTP calls to ETL Engine for async job execution     │
└───────────┬─────────────────────┬──────────────────────┘
            │                     │ (shared database)
┌───────────▼─────────────────────▼──────────────────────┐
│         PostgreSQL (:5432)                              │
│      16 tables | ~6M+ rows                              │
└────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────┐
│    stock-etl-engine (:8001 Docker / :8082 internal)     │
│   APScheduler | 7 active scheduled jobs                 │
│   Data sources: baostock, efinance                      │
└────────────────────────────────────────────────────────┘
```

### Tech Stack

| Layer | Technology |
|-------|------------|
| Frontend | Vue 3, TypeScript, Vite, Element Plus, ECharts, lightweight-charts |
| Backend API | FastAPI, SQLAlchemy, Pydantic |
| ETL Engine | APScheduler, baostock/efinance SDK, FastAPI |
| Database | PostgreSQL 15+ |

### Quick Start

#### Option 1: Docker Compose (Recommended)

```bash
git clone https://github.com/myliuyx/stock_project.git
cd stock_project

# Configure environment
cp stock-fast-api/.env.example stock-fast-api/.env
# Edit .env with your database and JWT settings

# Start all services (PostgreSQL + FastAPI + ETL Engine)
cd stock-fast-api && docker compose up -d

# Frontend dev mode
cd stock-front_ui
npm install && npm run dev
```

> **Ports**: FastAPI `:8000`, ETL Engine Docker `:8001`, Frontend `:5173` (dev), PostgreSQL `:5432`

#### Option 2: Manual Setup

**Backend:**

```bash
cd stock-fast-api

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your database and JWT settings

# Initialize database
psql -h <host> -U <user> -d <dbname> -f docs/09_postgresql_ddl.sql

# Start server (port :8000)
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

**ETL Engine:**

```bash
cd stock-etl-engine

python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Start ETL engine (internal :8082, Docker maps external :8001)
uvicorn app.main:app --reload --host 0.0.0.0 --port 8082
```

**Frontend:**

```bash
cd stock-front_ui

npm install
npm run dev
# Visit http://localhost:5173 (/api proxied to :8000)
```

### Environment Variables

**Backend (`stock-fast-api/.env`):**

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DB_HOST` | Yes | - | PostgreSQL host |
| `DB_PORT` | No | 5432 | PostgreSQL port |
| `DB_NAME` | Yes | - | Database name |
| `DB_USER` | Yes | - | Database user |
| `DB_PASSWORD` | Yes | - | Database password |
| `JWT_SECRET_KEY` | Yes | - | JWT signing key (min 32 chars) |
| `CORS_ORIGINS` | No | - | Allowed CORS origins (comma-separated) |

### Project Structure

```
stock_project/
├── stock-fast-api/          # FastAPI REST API backend (:8000)
│   ├── app/
│   │   ├── core/            # Config, DB connection, exceptions, response
│   │   ├── routers/         # API route modules (11 files, ~50 endpoints)
│   │   ├── schemas/         # Pydantic data models
│   │   ├── services/        # Business logic (10 services)
│   │   ├── repositories/    # Data access layer (10 repos)
│   │   └── middleware/      # Request processing middleware
│   ├── docs/                # API registry, DB design, architecture docs
│   ├── tests/               # Backend tests
│   └── docker-compose.yml   # Docker deployment config
├── stock-front_ui/          # Vue 3 frontend (:5173 dev)
│   ├── src/
│   │   ├── api/             # Axios API layer (11 modules)
│   │   ├── components/      # Vue components by module
│   │   ├── views/           # Route page components (16 pages)
│   │   ├── stores/          # Pinia state management
│   │   └── layouts/         # Main layout component
├── stock-etl-engine/        # ETL engine microservice (:8001 Docker / :8082 internal)
│   ├── app/
│   │   ├── core/            # Config, logging, exceptions
│   │   ├── routers/         # Trigger API (/api/v1/trigger/*)
│   │   ├── services/        # ETL task services
│   │   ├── jobs/            # 7 active scheduled scripts (sync, compute, build)
│   │   └── scheduler.py     # APScheduler orchestrator + timeout protection
│   ├── docs/                # ETL engine design documentation
│   └── Dockerfile           # ETL container image
└── docs/                    # Project-level docs (quick start, deployment guide, etc.)
```

### Database Tables

| Layer | Table | Rows (approx.) | Description |
|-------|-------|---------------|-------------|
| **Dimension** | `dwd_security_master` | 5,198 | Stock master data (symbol, name, exchange, industry) |
|  | `dwd_board_master` + `dwd_board_relation` | 83 / 5,199 | Board definitions and stock-board relationships |
| **Fact** | `dwd_trade_calendar` | — | A-share trading calendar |
|  | `dwd_stock_daily` | ~4.87M | Daily OHLCV market data |
|  | `dwd_stock_factor_daily` | ~1.27M | Technical factors (MA/RSI/MACD/ATR etc.) |
|  | `dwd_stock_financial_indicator` | ~36K | Financial indicators (ROE/revenue/profit) |
| **Mart** | `mart_stock_selection_daily` | ~620K | Stock screening analysis wide table |
|  | `etl_job_run` + `etl_job_run_log` | — | ETL task execution records and logs |

### API Documentation

API base URL: `/api/v1` (backend port :8000)

| Module | Endpoints | Description |
|--------|-----------|-------------|
| Auth | 2 | Login, token verification |
| Dashboard | 4 | System overview, watchlist analysis |
| Selection | 5 | Stock screening results query/export |
| Stocks | 9 | Quote search, profile, daily, factors, finance etc. |
| **Strategies** | **4** | **9 strategy lists/detail/query/stock analysis (v0.5.0 new)** |
| Jobs | **13** | **Task list, 7 dedicated trigger APIs + generic trigger, logs (ETL Engine HTTP calls)** |
| Coverage | 3 | Data coverage overview |
| Boards | 3 | Board list, detail, members |
| Backfill | 2 | Historical data backfill |
| System | 1 | System metadata |
| Watchlist | 4 | User watchlist management |

Full API documentation: [`stock-fast-api/docs/REGISTRY.md`](stock-fast-api/docs/REGISTRY.md)

### ETL Scheduled Tasks (stock-etl-engine)

> Detailed config at [定时任务使用文档](stock-fast-api/docs/定时任务使用文档.md).
> Scheduling logic moved to independent service `stock-etl-engine`, managed by APScheduler.

All times in Beijing time (UTC+8), auto-executes Mon-Fri:

| # | Task Name | Cron Time | Description |
|---|-----------|----------|-------------|
| 1 | New IPO Board Sync | **17:10** | Recent 7-day IPO stocks and their boards |
| 2 | Adjustment Factor Sync | **17:30** | Price adjustment factors for OHLCV |
| 3 | Stock Master Data Sync | **23:50** | Full market stock basic info |
| 4 | Daily OHLCV Sync | **19:00** | Full market daily OHLCV data |
| 5 | Technical Factor Compute | **23:00** | MA/RSI/MACD/BOLL etc. indicators |
| 6 | Selection Mart Build | **23:30** | Aggregate OHLCV+factors+finance → screening analysis |
| — | Log Cleanup | Daily 00:05 (independent) | Clean logs >3 days old |

> ⏸️ Paused: Financial indicator sync (was 21:30) — still accessible via manual trigger API.

### Related Documents

- [Quick Start Guide](docs/QUICK_START.md)
- [User Guide](docs/USER_GUIDE.md)
- [Deployment Guide](docs/DEPLOYMENT.md)
- [API Registry (50 endpoints)](stock-fast-api/docs/REGISTRY.md)
- [Database Design](stock-fast-api/docs/A股股票信息缓存系统数据库设计文档.md)
- [Architecture Design](stock-fast-api/docs/A股股票信息缓存系统架构设计文档.md)

### License

MIT License