# ASIN Performances

前端 React + 后端 FastAPI (MVC) + MySQL，使用 Docker Compose 容器化运行。代码通过卷挂载同步到容器：前端开发服务器可热更新；后端在 Docker 内默认不启用 `--reload`，详见下文「启动方式」。

## 结构说明

- **frontend/**：React (Vite + TypeScript) 前端
- **backend/**：FastAPI 后端，MVC 结构
  - `app/models/`：数据模型（表 `asin_performances` 在此定义，启动时自动建表）
  - `app/controllers/`：路由与业务逻辑
  - `app/views/`：请求/响应 Schema（Pydantic）
- **docker-compose.yml**：MySQL + 后端 + 前端，后端/前端目录挂载到容器，实现代码变动自动同步

## 数据库

- **数据库名**：由 `backend/.env` 中的 `MYSQL_DATABASE` 配置（示例可为 `rug`）
- **表名**：`asin_performances`
- **字段**：id（自增主键）、parent_asin、child_asin、parent_order_total、order_num、week_no、child_impression_count、child_session_count、search_query、search_query_volume、search_query_impression_count、search_query_purchase_count

表结构由 SQLAlchemy 在 `backend/app/models/asin_performance.py` 中定义，应用首次启动时自动创建。

## 配置（数据库账号口令）

- **应用**：复制 `backend/.env.example` 为 `backend/.env`，填写 **MYSQL_***（连 Docker 内 MySQL 时 `MYSQL_HOST=mysql`）、**online_db_*** 等。`config.py` 只读此文件。
- **仅 Docker 中的 MySQL 容器**：复制 `backend/docker-mysql.env.example` 为 `backend/docker-mysql.env`，填写 **MYSQL_ROOT_PASSWORD**、**MYSQL_DATABASE**、**MYSQL_USER**、**MYSQL_PASSWORD**。该文件不含线上库口令，避免整份 `.env` 注入 DB 容器。
- **两处对齐**：`docker-mysql.env` 中的 `MYSQL_DATABASE` / `MYSQL_USER` / `MYSQL_PASSWORD` 须与 `backend/.env` 中对应项一致（镜像创建的用户库与应用连接一致）。
- 本机直连数据库（不用 compose 的 mysql）时，`MYSQL_HOST` 可改为 `127.0.0.1` 等。

## 启动方式

在项目根目录执行：

```bash
docker compose up --build
```

- 前端：http://localhost:5173  
- 后端 API：http://localhost:9090  
- API 文档：http://localhost:9090/docs  

## 持续集成

推送到 `main` / `master` 或针对这两支的 Pull Request 会运行 [GitHub Actions](.github/workflows/ci.yml)：前端 `npm run build`（必过）、`npm run lint`（非阻塞提醒）；后端安装 `backend/requirements.txt` 并对 `app` 做 `compileall`。

修改 `frontend/` 下代码并保存后，前端会热更新。Docker 中的后端默认**未**开启 uvicorn `--reload`（避免长请求被进程重启打断）；修改后端代码后请重启 `backend` 容器，或在本机对 uvicorn 使用 `--reload` 进行开发。

## Docker 启动顺序与自动重启

- 三个服务均设置 **`restart: unless-stopped`**，进程异常退出后由 Docker 自动拉起。
- **不再**使用 Docker 内置的周期性 `healthcheck`（避免对 **`/health`** 的轮询写满访问日志）。**backend** 镜像通过 **`backend/docker-entrypoint.sh`** 在**启动 uvicorn 前**一次性等待 **MySQL TCP** 可连（最多约 120 秒）。**frontend** 在 **backend 容器已启动**后再启动（`depends_on`）。
- 需要人工或编排探活时，仍可直接请求 **`GET /health`**（JSON `{"status":"ok"}`）；开发环境下 **frontend** 的 `/health` 由 Vite 中间件提供（`vite.config.ts`）。
- 日志使用 **json-file** 并限制单文件大小与份数，避免占满磁盘。
- 查看重启次数：`docker inspect asinperformances_v2-backend-1 --format '{{.RestartCount}}'`（容器名以 `docker compose ps` 为准）。
- 崩溃原因请看退出前日志：`docker compose logs backend --tail=200`（或 `mysql` / `frontend`）。
