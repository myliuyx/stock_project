# Docker 部署指南 / Docker Deployment Guide

[English](#english) | [中文](#中文)

---

## English

### Overview

This guide covers Docker-based deployment for both backend and frontend.

### Prerequisites

- Docker 20.10+
- Docker Compose 2.0+
- PostgreSQL 15+ (running and accessible)

### Backend Deployment

#### 1. Create Environment File

In `stock-fast-api/`, create `.env` file:

```bash
# Database
DB_HOST=your_postgres_host
DB_PORT=5432
DB_NAME=stock_db
DB_USER=stock_user
DB_PASSWORD=your_secure_password

# JWT Authentication
JWT_SECRET_KEY=your_secret_key_at_least_32_characters

# CORS (comma-separated)
CORS_ORIGINS=http://localhost:5173,http://your-domain.com
```

#### 2. Initialize Database

```bash
# Create database
psql -h <host> -U <user> -c "CREATE DATABASE stock_db;"

# Create tables
psql -h <host> -U <user> -d stock_db -f docs/09_postgresql_ddl.sql
```

#### 3. Build and Run

```bash
cd stock-fast-api

# Build image
docker build -t stock-api:latest .

# Run container
docker-compose up -d
```

The API will be available at `http://localhost:8081`.

### Architecture Overview (v0.5.0+)

Starting from v0.5.0, the system uses a **microservice architecture** with ETL Engine as a separate service:

```
baostock/efinance → ETL Engine (:8001 ext / :8082 int) → PostgreSQL ← FastAPI (:8081) ← Frontend (:5173)
```

| Service | Container Port | External Port | Description |
|---------|---------------|---------------|-------------|
| etl-engine | 8001 (internal HTTP server) | 8001 | ETL data sync engine, independent Docker image in `stock-etl-engine/` |
| stock-api | 8081 (FastAPI) | 8081 | REST API gateway, reads from PostgreSQL |
| stock-frontend | 80 (Nginx) | 5173 | Vue 3 admin dashboard |

**Key changes in v0.5.0:**
- ETL Engine moved to `stock-etl-engine/` as a standalone microservice with its own Docker image and scheduler (APScheduler)
- FastAPI port changed from :8000 → :8081 externally
- Data flow: ETL Engine fetches from baostock/efinance sources, writes to PostgreSQL; FastAPI only reads data

### Frontend Deployment

#### Build Image

```bash
cd stock-front_ui

# Build Docker image
docker build -t stock-frontend .
```

#### Run Container

```bash
# Run with port mapping
docker run -d -p 5173:80 --name stock-frontend stock-frontend
```

The frontend will be available at `http://localhost:5173`.

#### Nginx Configuration

The Docker image uses Nginx to serve the built static files. Key configuration:

```nginx
server {
    listen 80;
    server_name _;

    # Serve static files
    location / {
        root /usr/share/nginx/html;
        try_files $uri $uri/ /index.html;
    }

    # Proxy API requests to backend
    location /api/ {
        proxy_pass http://stock-api:8081/api/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### Docker Compose (Full Stack)

Create a `docker-compose.yml` at project root:

```yaml
services:
  postgres:
    image: postgres:15-alpine
    container_name: stock-postgres
    restart: always
    ports:
      - "5432:5432"
    environment:
      - POSTGRES_DB=${DB_NAME:-stock_db}
      - POSTGRES_USER=${DB_USER:-stock_user}
      - POSTGRES_PASSWORD=${DB_PASSWORD}
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${DB_USER:-stock_user}"]
      interval: 10s
      timeout: 5s
      retries: 5

  etl-engine:
    build: ../stock-etl-engine
    container_name: stock-etl-engine
    restart: always
    ports:
      - "8001:8001"
    environment:
      - DB_HOST=${DB_HOST:?Please set DB_HOST}
      - DB_PORT=${DB_PORT:-5432}
      - DB_NAME=${DB_NAME:?Please set DB_NAME}
      - DB_USER=${DB_USER:?Please set DB_USER}
      - DB_PASSWORD=${DB_PASSWORD:?Please set DB_PASSWORD}
      - ETL_ENGINE_API_KEY=${ETL_ENGINE_API_KEY:-etl_secret_key_2026}
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8001/')"]
      interval: 30s
      timeout: 10s
      retries: 3

  stock-api:
    build: ./stock-fast-api
    container_name: stock-api
    restart: always
    ports:
      - "8081:8081"
    environment:
      - DB_HOST=${DB_HOST}
      - DB_PORT=${DB_PORT:-5432}
      - DB_NAME=${DB_NAME}
      - DB_USER=${DB_USER}
      - DB_PASSWORD=${DB_PASSWORD}
      - JWT_SECRET_KEY=${JWT_SECRET_KEY}
      - CORS_ORIGINS=${CORS_ORIGINS:-}
      - ETL_ENGINE_URL=http://etl-engine:8001/api/v1/trigger
      - ETL_ENGINE_API_KEY=${ETL_ENGINE_API_KEY:-etl_secret_key_2026}
    depends_on:
      postgres:
        condition: service_healthy
      etl-engine:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8081/')"]
      interval: 30s
      timeout: 10s
      retries: 3

  stock-frontend:
    build: ../stock-front_ui
    container_name: stock-frontend
    restart: always
    ports:
      - "5173:80"
    depends_on:
      - stock-api

volumes:
  pgdata:
```

Run from `stock-fast-api/`:

```bash
cd stock-fast-api
docker-compose up -d
```

### Environment Variables Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DB_HOST` | Yes | - | PostgreSQL host |
| `DB_PORT` | No | 5432 | PostgreSQL port |
| `DB_NAME` | Yes | - | Database name |
| `DB_USER` | Yes | - | Database user |
| `DB_PASSWORD` | Yes | - | Database password |
| `JWT_SECRET_KEY` | Yes | - | JWT signing key (min 32 chars) |
| `CORS_ORIGINS` | No | - | Allowed CORS origins |

### Troubleshooting

#### Backend won't start

```bash
# Check logs
docker logs stock-api

# Common issues:
# - Missing environment variables
# - Database connection failed
# - Port 8081 already in use (FastAPI), port 8001 already in use (ETL Engine)
```

#### Frontend returns 502

```bash
# Check if backend is running
docker logs stock-api

# Check nginx logs
docker exec stock-frontend cat /var/log/nginx/error.log
```

#### Database migration

```bash
# Connect to database
psql -h <host> -U <user> -d stock_db

# Check tables
\dt

# Run specific migration
psql -h <host> -U <user> -d stock_db -f docs/09_postgresql_ddl.sql
```

---

## 中文

### 概述

本指南介绍如何使用 Docker 部署后端和前端服务。

### 环境要求

- Docker 20.10+
- Docker Compose 2.0+
- PostgreSQL 15+（已启动并可访问）

### 后端部署

#### 1. 创建环境变量文件

在 `stock-fast-api/` 目录创建 `.env` 文件：

```bash
# 数据库配置
DB_HOST=你的PostgreSQL主机
DB_PORT=5432
DB_NAME=stock_db
DB_USER=stock_user
DB_PASSWORD=你的安全密码

# JWT 认证
JWT_SECRET_KEY=你的密钥至少32字符

# 跨域配置（逗号分隔）
CORS_ORIGINS=http://localhost:5173,http://你的域名.com
```

#### 2. 初始化数据库

```bash
# 创建数据库
psql -h <主机> -U <用户> -c "CREATE DATABASE stock_db;"

# 创建表
psql -h <主机> -U <用户> -d stock_db -f docs/09_postgresql_ddl.sql
```

#### 3. 构建并运行

```bash
cd stock-fast-api

# 构建镜像
docker build -t stock-api:latest .

# 运行容器
docker-compose up -d
```

API 地址：`http://localhost:8081`

### 架构概览 (v0.5.0+)

从 v0.5.0 开始，系统采用**微服务架构**，ETL Engine 作为独立服务运行：

```
baostock/efinance → ETL Engine (:8001 外网 / :8082 内网) → PostgreSQL ← FastAPI (:8081) ← 前端 (:5173)
```

| 服务 | 容器端口 | 外部端口 | 说明 |
|------|---------|---------|------|
| etl-engine | 8001（内部 HTTP 服务器） | 8001 | ETL 数据同步引擎，独立 Docker 镜像位于 `stock-etl-engine/` |
| stock-api | 8081 (FastAPI) | 8081 | REST API 网关，只读 PostgreSQL |
| stock-frontend | 80 (Nginx) | 5173 | Vue 3 管理后台 |

**v0.5.0 主要变更：**
- ETL Engine 迁移至 `stock-etl-engine/` 作为独立微服务，拥有独立的 Docker 镜像和调度器（APScheduler）
- FastAPI 外部端口从 :8000 → :8081
- 数据流向：ETL Engine 从 baostock/efinance 获取数据写入 PostgreSQL；FastAPI 仅负责读取数据

### 前端部署

#### 构建镜像

```bash
cd stock-front_ui

# 构建 Docker 镜像
docker build -t stock-frontend .
```

#### 运行容器

```bash
# 运行并映射端口
docker run -d -p 5173:80 --name stock-frontend stock-frontend
```

前端地址：`http://localhost:5173`

#### Nginx 配置

Docker 镜像使用 Nginx 提供静态文件服务，关键配置：

```nginx
server {
    listen 80;
    server_name _;

    # 静态文件
    location / {
        root /usr/share/nginx/html;
        try_files $uri $uri/ /index.html;
    }

    # API 请求代理到后端
    location /api/ {
        proxy_pass http://stock-api:8081/api/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### Docker Compose（完整部署）

在项目根目录创建 `docker-compose.yml`：

```yaml
services:
  postgres:
    image: postgres:15-alpine
    container_name: stock-postgres
    restart: always
    ports:
      - "5432:5432"
    environment:
      - POSTGRES_DB=${DB_NAME:-stock_db}
      - POSTGRES_USER=${DB_USER:-stock_user}
      - POSTGRES_PASSWORD=${DB_PASSWORD}
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${DB_USER:-stock_user}"]
      interval: 10s
      timeout: 5s
      retries: 5

  etl-engine:
    build: ../stock-etl-engine
    container_name: stock-etl-engine
    restart: always
    ports:
      - "8001:8001"
    environment:
      - DB_HOST=${DB_HOST:?请设置 DB_HOST}
      - DB_PORT=${DB_PORT:-5432}
      - DB_NAME=${DB_NAME:?请设置 DB_NAME}
      - DB_USER=${DB_USER:?请设置 DB_USER}
      - DB_PASSWORD=${DB_PASSWORD:?请设置 DB_PASSWORD}
      - ETL_ENGINE_API_KEY=${ETL_ENGINE_API_KEY:-etl_secret_key_2026}
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8001/')"]
      interval: 30s
      timeout: 10s
      retries: 3

  stock-api:
    build: ./stock-fast-api
    container_name: stock-api
    restart: always
    ports:
      - "8081:8081"
    environment:
      - DB_HOST=${DB_HOST}
      - DB_PORT=${DB_PORT:-5432}
      - DB_NAME=${DB_NAME}
      - DB_USER=${DB_USER}
      - DB_PASSWORD=${DB_PASSWORD}
      - JWT_SECRET_KEY=${JWT_SECRET_KEY}
      - CORS_ORIGINS=${CORS_ORIGINS:-}
      - ETL_ENGINE_URL=http://etl-engine:8001/api/v1/trigger
      - ETL_ENGINE_API_KEY=${ETL_ENGINE_API_KEY:-etl_secret_key_2026}
    depends_on:
      postgres:
        condition: service_healthy
      etl-engine:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8081/')"]
      interval: 30s
      timeout: 10s
      retries: 3

  stock-frontend:
    build: ../stock-front_ui
    container_name: stock-frontend
    restart: always
    ports:
      - "5173:80"
    depends_on:
      - stock-api

volumes:
  pgdata:
```

从 `stock-fast-api/` 目录启动：

```bash
cd stock-fast-api
docker-compose up -d
```

### 环境变量说明

| 变量名 | 必填 | 默认值 | 说明 |
|--------|------|--------|------|
| `DB_HOST` | 是 | - | PostgreSQL 主机地址 |
| `DB_PORT` | 否 | 5432 | PostgreSQL 端口 |
| `DB_NAME` | 是 | - | 数据库名 |
| `DB_USER` | 是 | - | 数据库用户 |
| `DB_PASSWORD` | 是 | - | 数据库密码 |
| `JWT_SECRET_KEY` | 是 | - | JWT 签名密钥（至少32字符） |
| `CORS_ORIGINS` | 否 | - | 允许的跨域地址（逗号分隔） |

### 常见问题排查

#### 后端无法启动

```bash
# 查看日志
docker logs stock-api

# 常见问题：
# - 环境变量未设置
# - 数据库连接失败
# - 端口 8081 被占用（FastAPI），端口 8001 被占用（ETL Engine）
```

#### 前端返回 502

```bash
# 检查后端是否正常运行
docker logs stock-api

# 查看 nginx 日志
docker exec stock-frontend cat /var/log/nginx/error.log
```

#### 数据库迁移

```bash
# 连接数据库
psql -h <主机> -U <用户> -d stock_db

# 查看表
\dt

# 执行特定迁移
psql -h <主机> -U <用户> -d stock_db -f docs/09_postgresql_ddl.sql
```

---

## 相关文档

- [快速入门指南](QUICK_START.md)
- [用户使用指南](USER_GUIDE.md)
- [故障排查指南](TROUBLESHOOTING.md)
- [DDL 脚本参考](DDL_REFERENCE.md)
- [API 接口文档](../stock-fast-api/docs/REGISTRY.md)