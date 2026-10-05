# NAS 部署

2026-10-05 后续已完成[正式图库迁移至原生 SPK](../../docs/validation/2026-10-05-spk-production-migration.md)。Mac 当前连接 `http://192.168.0.50:2283` 的原生服务；旧 Docker 停止并保留，本页 Compose 内容作为构建与回退参考。

2026-10-05 已部署 Mac 文件夹导入服务 `keeps-server:import-20261005`；隔离导入、数据库备份及生产接口验证通过。当前公网 443 返回路由器证书，Mac 旧连接地址也不可达，不能把服务部署完成视为客户端网络入口已恢复。详见[导入部署验证与访问限制](../../docs/validation/2026-10-05-macos-import-deployment.md)。

NAS 上的 Rust 服务是 Keeps 唯一业务后端；macOS/iOS 通过 HTTP API 浏览和整理照片。客户端不运行扫描器，不维护业务 SQLite，也不上传 ledger。

## 存储与运行

Compose 使用 `server/Dockerfile`。SQLite 数据、任务队列和可重建预览存于 `KEEPS_ROOT`：

- `db/control_plane.sqlite`：权威资产、元数据、版本关联及缓存状态。
- `db/jobs.sqlite`：追踪目录、后台任务、增量文件状态。
- `previews/`：512px HEIC 小预览与尚未替换的旧预览。
- 标准照片：RAW 同目录下的新 HEIF 文件；已有 JPEG/HEIF 直接复用，同一资产登记多个文件版本。3FR 标准转换暂缓。
- `backups/`：升级前备份。

唯一照片目录 `/volume2/photo` 按原路径挂载；系统数据 `/volume2/docker/keeps/data` 挂载到 `/keeps`。照片目录可写用于新增标准照片、显式导入照片及文档规定的身份 metadata 回填，禁止覆盖已有照片内容。`myphoto` 完全解除挂载。服务不会删除、移动或以其他照片覆盖已有照片，回收站和停止追踪只改数据库。

导航、文件索引和任务均使用 NAS 路径，例如 `/volume2/photo`，不再使用 `/originals` 或客户端 `/Volumes` 别名。已有部署须停服，运行 `scripts/migrate_nas_paths.py`（先 dry-run，apply 时自动备份两个数据库），再用新 Compose 重建容器。历史清单恢复只验证存在、大小和既有资产关联，内容哈希由正常扫描继续核对。

`KEEPS_LIBRARY_ID` 指定初始资料库。首次运行自动追踪 `/volume2/photo`；停止追踪后重启不会重新启用。默认每 300 秒安排扫描，单 worker 顺序执行，已有运行任务不会重复排队。进程重启后恢复未完成任务；未变化的文件跳过处理。扫描读取 SHA-256 和 EXIF 并登记资产；独立持久化缓存状态驱动同一 worker 生成小图。每批最多 20 个媒体资产、全局并发 1、批次结束后休息 60 秒。新入库、历史重建、源变化及缓存缺失走同一流程。缓存错误由 cache-status 返回，显式 cache-retry 每次最多重试 20 个有错误的待重试或失败媒体项和 20 个清理项。

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

schema 7 迁移以已有业务表为准，保留资产 ID、评分、标签、版本与回收站状态，移除运行时 ledger 及多源同步表。不会重放旧事件；仅含旧 ledger 的未投影数据库须先完成显式历史导入。新客户端仅调用 assets/counts/directories/folders/jobs 等 API；旧客户端的 ledger 上传、心跳、预览上传接口已关闭。

业务 API 要求 `Authorization: Bearer <KEEPS_ACCESS_TOKEN>`；健康检查及有时效签名的预览下载除外。HTTP 地址用于可信局域网，外部访问需 HTTPS 或 VPN。令牌存在 NAS Compose 目录 `.env`，权限 root:0600；群晖继承 ACL 可能放宽初始权限，必须显式 chmod 并回读。SSH 密码独立保存在本机 `codex-secret` 加密库。

## 当前目标

- NAS：`192.168.0.50:2283`，DSM 7.3.2 / x86_64，Docker 24.0.2。
- 资料库：`local-library`；容器：`keeps-control-plane`；重启策略：`unless-stopped`。
- Compose 目录：`/volume2/docker/keeps/deploy/nas`。
- 服务数据：`/volume2/docker/keeps/data`。
- 照片：`/volume2/photo`（已有原片及新增标准图）。
- 上版备份：`/volume2/myphoto/keeps/backups/before-rust-20260926-102248.sqlite`。

首次目录扫描可能耗时较长，资产 API 可立即读取迁移后的历史库；目录位置随扫描补全。服务健康不等于全库扫描结束，查看 `/libraries/local-library/jobs` 的状态和计数。

## 中央数据库与存储迁移

本次升级使用 `scripts/migrate_nas_storage.py`。默认只读输出计划；`--precopy` 在线复制稳定的 `previews/cache`，保存文件校验清单；`--apply` 必须先停止服务，使用 SQLite backup API 复制数据库，并将已确认来源的旧生成标准图复制到对应 RAW 同目录。该 schema 7 迁移工具使用 RAW 完整文件名和源哈希作为历史中间命名；后续命名回填已改为同主文件名 `.heic`，冲突用 `.1`、`.2`；已有文件冲突即停止，绝不覆盖。群晖 `@eaDir` 不属于 Keeps 缓存，不要求复制。

系统数据目标是 `/volume2/docker/keeps/data`，旧树完整保留为回退副本，切换后的服务不再挂载它。schema 7 从当前业务表迁移并删除 ledger、多设备状态、归档回执和冲突表；旧事件保留在旧库备份，不再参与恢复或同步。已有照片目录仅 `/volume2/photo`，因新增标准图需要可写；普通浏览和缓存生成不会改写已有照片；schema 9 身份回填可补写 metadata，不改变图像像素或 RAW 成像数据。

验收应覆盖迁移前后业务表摘要相同、服务 healthy、仅两个预期挂载、同资产标准图及缩略图下载哈希、重扫不重复入库、双核/2GiB限制。缓存重建进度与部署完成是两个独立状态。回退必须恢复匹配的数据库、镜像和配置，并给迁移中已新增的标准图保留同资产关联；不得删除这些照片。

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

当前缓存契约版本为 `keeps-server:cache-contract-20260927`，镜像 `2003e93c989a`，schema 2 不变，容器 healthy。已过期的有效签名预览链接返回 HTTP 403 / `preview_token_expired`，篡改令牌仍返回 HTTP 400，换链响应包含顶层 `width`、`height`、`version`；双库备份、原片只读挂载与任务恢复验证见 [缓存 NAS 部署验证](../../docs/validation/cache-nas-20260927.md)。

当前版本与后台监听机制已部署为 `keeps-server:versions-watch-20260927`，镜像 `17950d7cfe47`，schema 3，容器 healthy。原生目录监听、持久化目录任务队列、版本/default API 已上线；NAS 临时样本验证通过，425 个生产目录监听及队列推进已核验。双库备份、历史表完整保留与回退说明见 [部署验收](../../docs/validation/2026-09-27-versions-watch-nas.md)。历史版本证据尚未全量回填，数据库整理候选已完成首批实盘核验，未执行合并。

2026-09-27 14:03 UTC 已升级为 `keeps-server:folder-merge-20260927`（`7855aff06325`），schema 3 不变，服务 healthy。新文件归组限制为同直接父目录。用户已授权的全库同目录整理已在 NAS 独立进程启动，先在线预演，之后自动停服刷新计划、备份合并、恢复并验收；不表示全库整理已完成。[执行状态与报告位置](../../docs/validation/2026-09-27-folder-merge-execution.md)。

## HTTPS 反向代理部署

仓库 Compose 将 `2283` 仅绑定到 NAS 的 `127.0.0.1`，由 DSM 反向代理提供客户端入口。此配置在域名、证书和反向代理验收前不要直接应用到现有局域网部署；历史线上地址不代表反向代理已经上线。

1. 确认宽带具备可入站的公网连接，DDNS 域名解析到实际公网地址；DDNS 不能解决运营商 CGNAT。
2. DSM 为 Keeps 的完整域名配置有效证书及续期，证书 SAN 必须覆盖该域名。
3. 在控制面板 → 登录门户 → 高级 → 反向代理，建立独立 Keeps 主机规则：来源 HTTPS、Keeps 域名、8443；目标 HTTP、127.0.0.1、2283。将对应证书关联到这条服务。保留 Authorization 头、路径和查询参数；不为 API 配置公共缓存，保持访问日志关闭；DSM 全局错误日志可能带请求 URI，不应导出包含完整签名 URL 的错误日志。
4. 家庭路由器仅将 TCP 443 转发到 NAS 的专用 8443。不要转发 2283、SSH 或 DSM 管理端口；保留既有无关规则。
5. 维护任务结束后，备份现有 Compose 和 `.env`（保存在 NAS 私有部署目录，权限 0600），将 `CONTROL_PLANE_PUBLIC_BASE_URL` 改为 Keeps 的 HTTPS 地址，再应用 Compose。不要重建数据库或变更原片挂载。
6. iOS/macOS 服务地址改为同一 HTTPS 地址，图库 ID 和访问令牌保持不变。公网地址同时用于 API 和服务端生成的签名预览链接。
7. 家庭网络使用 NAT 回环；当前路由器已验证支持回环。不要直接将该域名内网解析到 NAS：NAS 的 443 是 DSM 共用入口，Keeps 专用入口是 8443。

验收：NAS 本机健康检查通过；局域网直接访问 2283 不通；通过 Keeps HTTPS 域名验证健康、鉴权、正确/错误图库、分页和预览下载。另用手机蜂窝网络验证相同步骤，回到 Wi-Fi 后再次验证；仅在 NAS 本机或同一局域网访问成功不算公网验收。正常 TLS 校验必须通过，不使用跳过证书检查作为验收结果。

回滚：恢复私有备份中的 Compose 和 `.env` 并重新创建服务，客户端恢复旧局域网地址。只撤销本次新增的 Keeps 反向代理和端口转发，保留 DSM、其它应用及证书的既有配置。

## 2026-09-28 目录修订与修改窗口

已部署 `keeps-server:revision-updating-20260928`（`9bd99b3bc73ce`），schema 5，生产容器 healthy。新增目录修订树、isUpdating 和 15 分钟静默收尾；13 张原有业务表逐行摘要一致，双库备份与任务恢复、HTTPS 接口和预览下载均验证通过。回退必须恢复匹配的数据库，不能只切旧镜像。详见[部署验收与回退记录](../../docs/validation/2026-09-28-revision-updating-nas.md)。

## 2026-09-29 小预览与标准照片流水线

配置 `STANDARD_PHOTOS_ROOT` 为 Keeps 状态目录外的输出位置，当前为 `/volume2/myphoto/standard-photos`。原片四个挂载仍只读。默认 `KEEPS_THUMBNAIL_QUALITY=50`，规格为最长边 512px HEIC；JPEG/HEIF 标准照片保持原编码，RAW-only 生成完整尺寸 HEIF。

当前 NAS 不支持 Docker CFS 配额，使用 `KEEPS_CPUSET=2-3` 允许两个 CPU 核，`KEEPS_MEMORY_LIMIT=2g` 限制容器内存；这些约束同时覆盖扫描、编码和 API，不能通过增加另一个 worker 绕过。批量 20、并发 1、休息 60 秒目前为代码常量。标准照片编码允许最多 900 秒，小图/旧预览为 180 秒；停止宽限 930 秒覆盖最长单个编码阶段，若整个资产尚未完成则重启后恢复。

启用 `KEEPS_CACHE_GC_ENABLED=1` 后，生成并验证新小图时切换当前预览引用，将已替换旧缓存登记到持久清理队列。等待 20 分钟后，每轮最多回收 20 个无引用对象。只清理已登记的旧 preview/thumbnail，不扫描删除未知孤立文件，不删除标准照片或原片。

只读检查：

```sh
codex-secret run chuan-nas -- python3 scripts/nas_cache_status.py
```

状态接口为 `GET /libraries/{library}/cache-status`。`POST /libraries/{library}/cache-rebuild` 每次仅将最多 20 个已就绪项重置为待生成；它不是一键清空全库命令。全库规格变更由源/规格核对自动入队。可用服务不代表全库已完成，须分别检查生成失败、待盘点资产及 GC 队列。

本次镜像、备份、隔离验证与生产进度见[流水线执行记录](../../docs/validation/2026-09-29-thumbnail-pipeline-nas.md)。schema 6 回退须恢复配套双库和配置，不能只回退镜像。

## 临时 Linux 计算节点

需要远端生成时，先部署 schema 8 的 NAS 镜像，再启动 [Linux worker](../linux-worker/README.md)。NAS 使用双核/2GiB、本地每轮20项及60秒休息；Linux有独立的16并发限制，通过同一数据库租约领取任务。内部接口继续经既有DSM HTTPS反向代理访问，不新增NAS端口或照片挂载。数据库升级前应停服并备份两份SQLite；全库生成进度与部署验收分开记录。

`KEEPS_LOCAL_CACHE_ENCODING_ENABLED=0` 为协调模式：NAS不领取本地编码任务，但继续扫描、盘点、审计、GC和远端结果发布。设为1恢复本地编码；不涉及数据库迁移。Linux的线程数由它自身控制，NAS通过现有media_cache索引和事务直接领取一条任务，不建立第二层队列。
