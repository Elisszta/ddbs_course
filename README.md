## 分布式抢课系统运行指南

基于 FastAPI (Python) + React (TypeScript) + MySQL (Docker Cluster) 构建的高并发、高可用分布式选课系统。

## 1. 环境准备

在开始之前，请确保你的机器上安装了以下工具：

1. **Docker & Docker Compose**: 用于运行数据库集群。
2. **Python 3.10+**: 建议使用 Conda 管理环境。
3. **Node.js (v16+)**: 用于运行前端项目。

## 2. 数据库集群搭建 (核心步骤)

本系统包含 1 主 2 从 (用于存用户档案) 和 3 个分片库 (用于存课程数据)。

请务必按照以下顺序执行，以确保主从复制关系建立正确。

### 2.1 清理旧环境 (防止冲突)

```
# -v 参数至关重要，它会清除旧的数据库卷，防止 UUID 冲突
docker-compose down -v
```

### 2.2 启动容器

```
docker-compose up -d
```

> 等待 15-20 秒，确保 MySQL 容器完全启动并就绪。可以使用 `docker ps` 查看状态。

### 2.3 配置主从复制

```
./setup_replication.sh
```

> 检查点：脚本结束时，必须看到 `Slave_IO_Running: Yes` 和 `Slave_SQL_Running: Yes`。如果不是双 Yes，请勿进行下一步。

### 2.4 初始化表结构与数据

```
./init_db.sh
```

> 检查点：脚本最后会输出 `Slave1 tables`，确保其中包含 `student`, `teacher` 等表。

## 3. 启动后端服务

我们以 A 校区 (Master 节点) 为例。

### 3.1 检查配置

打开 `.env.a` 文件，确保端口配置正确：

- `DB_MASTER_SLAVE_URL`: 端口应为 3310 (Master)
- `DB_SHARD_URL`: 端口应为 3320 (Shard A)

### 3.2 安装依赖与启动服务

```
# 激活 Python 环境 (如果使用 Conda)
conda activate ddbs_course

# 安装 Python 依赖
pip install -r requirements.txt

# 运行启动脚本 (参数：校区 端口)
./start_campus.sh a 8000
```

- API 文档: https://www.google.com/search?q=http://127.0.0.1:8000/docs
- OpenAPI JSON: https://www.google.com/search?q=http://127.0.0.1:8000/openapi.json

## 4. 启动前端服务

前端代码位于 `school-web/` 目录下。

### 4.1 安装依赖

```
cd school-web
npm install
```

### 4.2 同步后端 API 定义 (关键)

如果后端代码有更新，运行此命令自动生成前端 TypeScript 类型和请求函数：

```
npm run gen-api
```

### 4.3 启动开发服务器

```
npm run dev
```

- 前端地址: http://localhost:3000

## 5. 常见问题排查

Q: 启动后端时报错 Table 'student' doesn't exist？

A: 说明数据库初始化失败或连接到了错误的端口（如 3312）。请执行步骤 2 中的完全重置流程，并检查 .env.a 是否连接的是 3310。

Q: 前端提示 CORS error？

A: 检查后端 main.py 是否配置了 CORSMiddleware，以及前端 vite.config.ts 中的 proxy 配置是否正确指向了后端端口。

Q: Slave 状态显示 Connecting？

A: Docker 内部网络问题或 UUID 冲突。请务必使用 docker-compose down -v 清理旧卷。

Q: 只有 3312 有数据，3310 没有？

A: 你的 Python 程序连接错端口了。请执行 `unset DB_MASTER_SLAVE_31