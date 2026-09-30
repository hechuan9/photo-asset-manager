# 标准图命名与存量回填

2026-09-29，用户授权修正标准图命名，并回填现有 Keeps 生成图。

## 规则

- `DSC02514.ARW` 对应 `DSC02514.heic`；已有文件或其他资产占用目标时使用 `DSC02514.1.heic`、`.2.heic`。
- 写入 `Software=Keeps` 和 `XMP-xmp:CreatorTool=Keeps`，来源关联保留在数据库。
- NAS 本地编码路径与 Linux 远程发布共用同一命名选择逻辑；发布仍拒绝覆盖已有文件。
- 已确认的同资产 JPEG/HEIF 继续复用；3FR 仍仅生成缩略图。

## 验证

- Rust library：79 passed，1 ignored（本机真实媒体测试）；Clippy all-targets `-D warnings`、fmt 通过。
- 回填脚本 5 项测试通过，包括真实 schema、重复内容分属不同资产、已占用名称、无覆盖发布和跨文件系统回退。
- Linux 真实 RAW/JPEG/HEIF/3FR smoke PASS，覆盖新名称、Keeps metadata、下载 hash、重复完成和重新扫描。
- 隔离旧命名数据库完成单张回填，下载 hash 与重新扫描后资产数/ready 状态验证通过。

## 生产执行

生产回填和部署验收已完成，NAS 与 Linux 均已恢复运行。NAS 于 21:17:12 UTC 停服，Linux 在此前排空；NAS 健康检查于 22:11:24 UTC 通过，Linux 于 22:11:50 UTC 恢复。此次维护窗口不得用于吞吐下降或工期外推。

- 预检：3,033 个有数据库生成来源证据的旧命名 HEIC，共 51,294,150,420 bytes；无 `.N` 命名冲突。
- 其中 7 个文件属于 3 组相同内容。预检曾主动停止；脚本已支持相同内容保留各自路径、资产和来源关系。
- 仅对上述生成标准图补写 metadata、调整名称和同步数据库，不重新编码；原始照片、RAW、现有用户 JPEG/HEIF 不修改。
- 迁移备份和清单：`/volume2/docker/keeps/releases/standard-names-20260929/backfill`。
- NAS release：`/volume2/docker/keeps/releases/standard-names-20260929`。
- Linux release：`/home/hechuan/keeps-worker/releases/standard-names-20260929`。

## 最终验收

- 回填 3,033 张，0 个名称冲突，全部采用原主文件名 `.heic`；修改后合计 51,294,335,433 bytes。
- 全量 metadata 读回和新旧文件/备份 SHA-256 校验通过。旧长文件名均已撤下，独立备份与暂存保留。
- 两个 SQLite 数据库 quick_check 均为 ok；90,491 个资产、112,773 条路径、6,786 个版本、211,147 条文件记录保持不变；media_cache 的全部状态与缩略图逐行对比不变。
- NAS 镜像 `sha256:f01b492cfab03d7d8e6780e8bb89f97605417d39041cc0b009dd841369155c01`；Linux 镜像 `sha256:efc56151ab267b25cc5554b4056fc6f79f1e5dd9b7e5e65242fc924e49df59e7`。来自同一导出镜像，RootFS 六层逐一相同，两端镜像存储表示不同。
- NAS deployment.json、Linux worker-deployment.json 均 PASS；NAS 双核 cpuset=2-3、2 GiB、localEncodingEnabled=false；Linux 16 并发、16 CPU、24 GiB、每进程 4 GiB 地址空间均保持。
- 三张迁移后样本的标准图 SHA、Keeps metadata、资产归属和对应原始 RAW SHA 验证通过。
- 恢复后已有 16 张新生成标准图；其中三张 (`DSC00295.heic`、`DSC05319.heic`、`DSC01337.heic`) 的短名称、SHA 和 Keeps metadata 验证通过。
- 22:14:00 UTC：ready 5,559，pending 84,916，processing 16，failed 0；NAS healthy、restart 0、OOM false，CPU 186.31% / 200%、内存 1.009 GiB / 2 GiB。
- Linux 22:13:24 UTC：恢复后完成 38 项，新增错误 0，restart 0，OOM 及 memory.events 均为 0；瞬时 CPU 1598.16% / 1600%、内存 10.55 GiB / 24 GiB。
- 资产 `0f96059e-c7ea-46ee-a767-069f0617b673` 有一条既存 `No such file or directory` 待重试记录（pending/attempts=1）；已与迁移前数据库备份核对，不是本次新增错误，交给后续扫描/重试观察。
- nas-2 保持每 15 分钟汇报，已更新新镜像和维护窗口；监控快照保留历史并建立恢复后新基线，不用维护期间速率计算 ETA。

结构化证据：`docs/validation/2026-09-29-standard-names.json`；NAS release 下另有 backfill-summary.json、sample-verification.json、fresh-task-verification.json 与完整 manifest。
