# NAS 数据库派发与 Linux 编码验证

2026-09-29：用户要求数据库直接取符合条件任务，不排序、不维护内存候选队列。

## 实现

- 同一 SQL immediate transaction 中按 library/status/available_at 筛选，LIMIT 1；无 JOIN、CASE、ORDER BY，无额外队列或 schema 变更。
- NAS 设置 KEEPS_LOCAL_CACHE_ENCODING_ENABLED=0，保留扫描、核对、GC、API 和数据库管理。Linux 保持 4 并发。
- 上传保留体积与 HEIC 头检查，完整解析/hash/尺寸验证在 complete 发布前执行。
- 不删除、移动或覆盖原始照片；标准图发布保护保持不变。

## 验证

- 78 library tests、17 HTTP tests、1 CLI test PASS；1 real-media test 本机忽略；Clippy all-targets -D warnings、fmt、diff-check PASS。
- Linux 真实 RAW/JPEG/HEIF/3FR 四类 fixture smoke PASS。
- NAS release /volume2/docker/keeps/releases/coordinator-20260929/deployment.json PASS。镜像 sha256:243fc6bae016d50737041842b3bb9cf1fa43335b12ea7ae2785abdc01cacc37f，schema 8。
- 已备份数据库和部署配置，等待运行任务完成后切换；Linux 于 17:31:30 UTC 恢复。
- 数据库领取查询的只读基准从约 0.701 秒降至 0.0038 秒；不代表端到端吞吐提升同样倍数。
- 17:34:11 UTC：ready 444（恢复前434），processing 4，pending 89625，failed 0，pendingInventory 0。NAS healthy/restart 0/OOM false，localEncodingEnabled false，GC pending 335（之前394），failed 0；磁盘空余约35 TB。NAS 瞬时 CPU 80.75%，内存772.4 MiB/2 GiB。
- Linux 30秒平均 CPU 350.7%（约3.5核）；running/restart 0/OOM false。17:31:30–17:34:19 完成12项，任务耗时38–57秒，输入约34–37 MB，输出约10–24 MB；约4.3项/分钟，仅是短窗口，不能与此前优先小图的21.4项/分钟等价比较或外推全库工期。
- 已断开 Mac 到 Linux 的 SSH，任务由常驻 Docker 独立执行。nas-2 自动化保持暂停。

原始部署证据、完整测试日志及 smoke 报告保存在 /tmp/keeps-coordinator-20260929 与对应 NAS/Linux release 目录。
