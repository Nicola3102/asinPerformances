# ASIN Performances

前端 React + 后端 FastAPI (MVC) + MySQL，使用 Docker Compose 容器化运行。代码通过卷挂载实现**随脚本动态同步**：修改后端或前端代码后，容器内服务会自动重载，无需重建镜像。

## 结构说明

- **frontend/**：React (Vite + TypeScript) 前端
- **backend/**：FastAPI 后端，MVC 结构
  - `app/models/`：数据模型（表 `asin_performances` 在此定义，启动时自动建表）
  - `app/controllers/`：路由与业务逻辑
  - `app/views/`：请求/响应 Schema（Pydantic）
- **docker-compose.yml**：MySQL + 后端 + 前端，后端/前端目录挂载到容器，实现代码变动自动同步

## 数据库

- **数据库名**：`rug`
- **表名**：`asin_performances`
- **字段**：id（自增主键）、parent_asin、child_asin、parent_order_total、order_num、week_no、child_impression_count、child_session_count、search_query、search_query_volume、search_query_impression_count、search_query_purchase_count

表结构由 SQLAlchemy 在 `backend/app/models/asin_performance.py` 中定义，应用首次启动时自动创建。

## 启动方式

在项目根目录执行：

```bash
docker compose up --build
```

- 前端：http://localhost:5173  
- 后端 API：http://localhost:9090  
- API 文档：http://localhost:9090/docs  

修改 `frontend/` 或 `backend/` 下代码并保存后，前端会热更新，后端会由 uvicorn `--reload` 自动重启，无需重新构建或重启容器。
