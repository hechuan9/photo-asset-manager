# NAS Docker 部署

日常 NAS 使用[原生 SPK](../spk/README.md)。本目录保留 Docker 构建和独立部署入口；不要与原生套件同时访问同一状态目录。

## 配置与启动

```sh
cd deploy/nas
cp .env.example .env
chmod 600 .env
# 填写路径、客户端可达地址和随机 KEEPS_ACCESS_TOKEN
# 新空库使用 CONTROL_PLANE_AUTO_CREATE_SCHEMA=1 初始化
docker compose build
docker compose up -d control-plane
curl --fail http://localhost:2283/healthz
```

原片保持 `/volume2/photo`；`KEEPS_ROOT` 单独保存 `db/`、`previews/` 和维护状态，不存放原片或标准照片。Docker 示例将 `/volume2/docker/keeps/data` 挂载到 `/keeps`。数据库为 `control_plane.sqlite` 和 `jobs.sqlite`，同一状态目录只允许一个服务管理。

`KEEPS_LIBRARY_ID`、token 和 `TZ` 在迁移时保持一致，无时区 EXIF 按 `TZ` 解析。照片目录可写用于显式导入、新增标准图及身份 metadata 补写；禁止覆盖或永久删除已有照片。软件回收站和停止追踪只修改数据库。用户输入同名确认后的文件夹删除使用 NAS 共享回收站，详见服务端 README；该功能需要真实 DSM 共享配置，默认 Docker 部署不提供该配置。

业务 API 使用 Bearer token；健康检查和有时效签名的预览下载除外。Compose 将 2283 绑定至回环地址，客户端入口需配置 HTTPS 反向代理或 VPN，保留 Authorization、路径和查询参数。`CONTROL_PLANE_PUBLIC_BASE_URL` 必须与客户端入口一致；令牌和含签名 URL 的日志不提交 Git。

## 升级与恢复

先停止服务及独立维护进程，用 SQLite backup API 备份两个数据库，并保留镜像及 `.env`。已有库使用新镜像显式迁移：

```sh
docker compose stop control-plane
# 在此完成双库备份并检查备份可读
docker compose run --rm --no-deps control-plane migrate
docker compose up -d control-plane
```

迁移后检查健康、鉴权、资产计数、预览下载和任务恢复。回退必须使用匹配的数据库、镜像及配置，先保留最新状态，不能只切回旧镜像或恢复单个数据库。原片和新增标准图均不得删除。

历史路径迁移使用 `scripts/migrate_nas_paths.py`；存储迁移使用 `scripts/migrate_nas_storage.py`。先查看 `--help` 并预演，正式应用必须停止服务。旧数据仅含 ledger、没有业务投影时，应先完成历史导入；运行时不再回放 ledger。

## 后台处理

文件事件与增量状态驱动扫描，不定时重复全库扫描；启动或事件丢失时保留补漏。日常本地编码开启、远端派发关闭。每批最多 20 项、并发 1、轮后休息 60 秒；Linux 仅用于明确安排的[一次性批量处理](../linux-worker/README.md)。

`GET /libraries/{library}/jobs` 查看扫描任务，`GET /libraries/{library}/cache-status` 查看编码与缓存清理。`cache-retry` / `cache-rebuild` 每次最多处理 20 项。GC 只清理已登记且无引用的旧预览，等待 20 分钟；不删除原片、标准图或未知文件。服务健康不表示全库处理完成。
