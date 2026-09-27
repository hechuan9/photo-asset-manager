# NAS 部署

NAS 上的 Rust 服务是 Keeps 唯一业务后端；macOS/iOS 通过 HTTP API 浏览和整理照片。客户端不运行扫描器，不维护业务 SQLite，也不上传 ledger。

## 存储与运行

Compose 使用 `server/Dockerfile`。SQLite 数据、任务队列和可重建预览存于 `KEEPS_ROOT`：

- `db/control_plane.sqlite`：资产查询投影、元数据及历史事件。
- `db/jobs.sqlite`：追踪目录、后台任务、增量文件状态。
- `previews/`：1200px 上限的 HEIC 预览。
- `backups/`：升级前备份。

四个原片目录按 NAS 真实绝对路径同路径只读挂入容器；宿主目录必须已经存在。容器的 `/volume2` 只包含这些原片挂载及其父目录，服务数据另挂 `/myphoto/keeps`。服务不会删除、移动或覆盖照片，回收站和停止追踪只改数据库。

导航、文件索引和任务均使用 NAS 路径，例如 `/volume2/photo`，不再使用 `/originals` 或客户端 `/Volumes` 别名。已有部署须停服，运行 `scripts/migrate_nas_paths.py`（先 dry-run，apply 时自动备份两个数据库），再用新 Compose 重建容器。历史清单恢复只验证存在、大小和既有资产关联，内容哈希由正常扫描继续核对。

`KEEPS_LIBRARY_ID` 指定初始资料库。首次运行自动追踪 `/volume2`；停止追踪后重启不会重新启用。默认每 300 秒安排扫描，单 worker 顺序执行，已有运行任务不会重复排队。进程重启后恢复未完成任务；未变化的文件跳过处理。扫描读取 SHA-256 和 EXIF，优先复用已有资产与预览，缺少预览时生成 HEIC。失败保留错误链，可从 macOS 的 NAS 任务面板重试。

无时区 EXIF 按 `TZ` 解析；迁移时应使用原有 Mac 的解释时区，目前配置 `America/New_York`。RAW 支持范围取决于 LibRaw；无法解码的文件保留原片并将任务标记失败。

## 配置与升级

```bash
cd deploy/nas
cp .env.example .env
chmod 600 .env
# 填写真实宿主路径、客户端可达 URL 和随机 KEEPS_ACCESS_TOKEN
# 不要把令牌放到聊天、命令参数或 Git 中
docker compose build
```

新空库可设 `CONTROL_PLANE_AUTO_CREATE_SCHEMA=1` 初始化。已有库升级必须停止旧服务，使用 SQLite backup API 保存一致备份，随后显式迁移：

```bash
docker compose stop control-plane
# 先备份 KEEPS_ROOT/db/control_plane.sqlite，并检查备份可读
docker compose run --rm --no-deps control-plane migrate
# 已有库保持 CONTROL_PLANE_AUTO_CREATE_SCHEMA=0
docker compose up -d control-plane
curl --fail http://localhost:2283/healthz
```

迁移以事务回放旧事件，保留原表、资产 ID、评分、标签与回收站状态，建立 NAS 查询投影。新客户端仅调用 assets/counts/directories/folders/jobs 等 API；旧客户端的 ledger 上传、心跳、预览上传接口已关闭。

业务 API 要求 `Authorization: Bearer <KEEPS_ACCESS_TOKEN>`；健康检查及有时效签名的预览下载除外。HTTP 地址用于可信局域网，外部访问需 HTTPS 或 VPN。令牌存在 NAS Compose 目录 `.env`，权限 root:0600；群晖继承 ACL 可能放宽初始权限，必须显式 chmod 并回读。SSH 密码独立保存在本机 `codex-secret` 加密库。

## 当前目标

- NAS：`192.168.0.50:2283`，DSM 7.3.2 / x86_64，Docker 24.0.2。
- 资料库：`local-library`；容器：`keeps-control-plane`；重启策略：`unless-stopped`。
- Compose 目录：`/volume2/docker/keeps/deploy/nas`。
- 服务数据：`/volume2/myphoto/keeps`。
- 原片：`/volume2/photo`、`/volume2/myphoto/未处理Raw`、`/volume2/myphoto/已处理Raw`、`/volume2/myphoto/和川专属`。
- 上版备份：`/volume2/myphoto/keeps/backups/before-rust-20260926-102248.sqlite`。

首次目录扫描可能耗时较长，资产 API 可立即读取迁移后的历史库；目录位置随扫描补全。服务健康不等于全库扫描结束，查看 `/libraries/local-library/jobs` 的状态和计数。

## 2026-09-26 NAS 核心版本验证

- 部署镜像 `keeps-server:nas-core-20260926`，Compose 使用其 `keeps-server:local` 标签。
- 升级备份 `/volume2/myphoto/keeps/backups/before-nas-core-20260926-181636.sqlite`，SQLite 完整性检查通过。
- 成功回放并保留 744,409 条历史事件；116,872 条快照对应 116,745 个唯一资产，迁移后的资产数一致。
- 业务鉴权、资产查询、600,194 字节现有预览下载及内容 SHA-256 验证通过；真实响应由共享 Swift DTO 成功解码。
- 四个原片 mount 的 `RW=false`；容器重启后保留同一未完成任务，并从增量记录跳过已完成文件。
- Rust 34 项自动测试、Clippy、真实容器迁移/扫描/预览/修改/重启测试通过；共享 Swift 7 项、macOS 6 项测试及 iOS Simulator 构建通过。
- 首次全库扫描仍在运行，不能把服务部署完成当成全库处理完成。iOS 模拟器卡在系统启动，未完成界面实测；未安装到 iOS 真机。

补充现场验证见 [NAS 服务验证报告](../../docs/validation/nas-service-20260926.md)：在 NAS 本机使用生产镜像完成坏文件、幂等性、SIGKILL 恢复与完整历史数据比对；生产服务持续运行。

后续目录导航版本已部署为 `keeps-server:navigation-20260926`：新增服务端真实目录按层查询与本地接入状态，macOS 已完成两区域界面实测。备份、挂载与验收证据见 [导航验收](../../docs/validation/nas-navigation-20260926.md)。

最新统一资料库版本为 `keeps-server:unified-http-20260926`，运行镜像 `e527e706e61c`，容器 healthy。导航 API 已移除未实现的 `location/local` 字段；Mac 统一资料库与来源刷新通过。备份、回退镜像和线上验证见 [统一 HTTP 资料库验证](../../docs/validation/2026-09-26-unified-http-library.md)。

当前隐藏目录版本为 `keeps-server:hidden-directories-20260926`，镜像 `81af7a9a9fd7`，schema 2；容器 healthy。新增目录隐藏配置与资产/计数过滤，迁移保留全部 117,344 个资产。双库备份、逐行数据比对、真实隐藏规则及原片只读验证见 [隐藏目录部署验收](../../docs/validation/2026-09-26-hidden-directories-nas.md)。回退必须同时恢复升级前 schema 1 数据库，不能只切回旧镜像。
