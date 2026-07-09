# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述 (Project Overview)

**A股股票信息缓存系统** — 一个用于中国 A 股市场的本地数据分析平台。该系统通过 baostock/efinance 同步每日行情数据到 PostgreSQL，并提供 Web 端仪表盘进行选股、个股分析、板块分析及 ETL 任务监控。

### 服务拓扑 (Service Topology)
本项目是一个三服务单体仓库 (Monorepo):

| 服务名称 | 目录 | 端口 | 角色 | 技术栈 |
|---------|------|------|------|------------|
| **Frontend** | `stock-front_ui/` | 5173 | Web 仪表盘 | Vue 3, TS, Vite, Element Plus |
| **Backend** | `stock-fast-api/` | 8081 | REST API & 任务编排 | FastAPI, SQLAlchemy, Python |
| **ETL Engine** | `stock-etl-engine/` | 8002* | 数据抓取与清洗 | FastAPI, APScheduler, Python |

*\*内部端口: 8082. Docker 映射端口: 8001.*

## 全局开发命令 (Global Commands)

```bash
# 使用 Docker 一键启动全栈环境 (推荐)
docker compose up -d

# 分别启动各服务
# 前端 (Frontend)
cd stock-front_ui && npm install && npm run dev

# 后端 (Backend)
cd stock-fast-api && ./venv/bin/pip install -r requirements.txt && ./venv/bin/uvicorn app.main:app --reload --port 8081

# ETL 引擎 (ETL Engine)
cd stock-etl-engine && ./venv/bin/pip install -r requirements.txt && ./venv/bin/uvicorn app.main:app --reload --port 8082
```

## 开发导航指南 (Navigation Guide)

对于具体的业务逻辑、API 定义或 UI 组件开发，请查阅各服务目录下专门的 `CLAUDE.md` 文件以获取深度上下文：

*   **后端开发** (API 设计, SQL, 业务逻辑): [`stock-fast-api/CLAUDE.md`](./stock-fast-api/CLAUDE.md)
*   **前端开发** (Vue 组件, Pinia 状态管理, UI/UX): [`stock-front_ui/CLAUDE.md`](./stock-front_ui/CLAUDE.md)
*   **ETL & 数据流水线** (任务调度, 数据同步): [`stock-etl-engine/CLAUDE.md`](./stock-etl-engine/CLAUDE.md)

## 核心原则 (Core Principles)

*   **时区 (Timezone)**: 全局统一使用 **CST (UTC+8)**。Python 中请始终使用 `from app.core.timezone import now` 而非 `datetime.now()`。
*   **数据库 (Database)**: 共享 PostgreSQL 实例。后端通过 SQLAlchemy `text()` 执行原生 SQL 以保证性能和控制力。
*   **API 规范**: 统一响应格式 `{"code": 0, "message": "success", "data": {...}}`。
*   **任务触发**: 前端发起请求 $\rightarrow$ 后端 (JobService) 记录任务 $\rightarrow$ 调用 ETL Engine HTTP API $\rightarrow$ 异步执行并更新数据库状态。

## 文档索引 (Documentation Index)

*   **快速入门 & 部署**: `docs/QUICK_START.md`, `docs/DEPLOYMENT.md`
*   **问题排查**: `docs/TROUBLESHOOTING.md`, `docs/A股定时任务异常排查与修复方案.md`
*   **技术参考**: 各子项目目录下的 `docs/` 文件夹。
