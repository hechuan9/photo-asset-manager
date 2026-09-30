# 文件位置同步优先级

用户授权文件位置变化优先于元数据刷新与缩略图生成，并修复已移动目录的旧数据库引用。

## 实现

- watcher 的创建、删除、重命名、新子目录及定向协调扫描在已有 SQLite jobs 表标记 `path_sync`。优先级为路径同步、元数据刷新、普通扫描；低优先级扫描在文件边界让出执行权。同为路径同步时忽略附带元数据标记的优先级差异，不互相抢占，按先后顺序完成。
- 缩略图远程领取仍是数据库 `LIMIT 1`，没有新增全库排序、内存任务队列或线程锁。不存在的源路径若仍在等待路径同步，则退回 pending、延后 30 秒，不消耗重试次数。有效源路径照常处理。
- 跨目录移动仅在内容哈希唯一对应一个既有资产、历史同内容路径全部明确不存在、新文件存在时复用原资产 ID。先登记新路径，再撤销旧引用，保留默认版本、整理状态及有效缩略图；真实副本和歧义不合并。
- 候选包括旧 Mac 导入的历史来源；中央数据库中的历史 holder 不限制移动匹配。

## 验证

- Rust 87 个库测试、17 个 HTTP 测试、1 个 CLI 测试通过；1 个本地真实媒体测试忽略，由 Linux 真实文件 smoke 覆盖。
- Clippy all-targets `-D warnings`、fmt、diff check 通过。
- Linux RAW/JPEG/HEIF/3FR smoke 六项 PASS，覆盖旧 Mac 来源的真实目录移动、同资产 ID、旧路径撤销、原始 hash 不变及无新增资产。

## 部署与恢复

- Release：NAS `/volume2/docker/keeps/releases/path-sync-20260929`；Linux `/home/hechuan/keeps-worker/releases/path-sync-20260929`。
- NAS 最终镜像 `sha256:85b164e92c54bb43389c5426992638f7c8948257059ff7b53b6f8ead09224b58`；schema8，jobs 增加 path_sync，双核 cpuset=2-3 / 2GiB，localEncodingEnabled=false。
- Linux 保留原镜像 `sha256:efc56151ab267b25cc5554b4056fc6f79f1e5dd9b7e5e65242fc924e49df59e7`，16 并发 / 16 CPU / 24GiB，编码逻辑未改。
- Linux 于 23:53:03 UTC 排空；NAS 23:53:21 UTC 停服，首轮修正 23:57:57 UTC healthy，Linux 23:58:13 UTC 恢复。之后发现同级路径任务受附带元数据标记抢占，补充回归测试并再次部署：00:02:54 UTC Linux 排空、00:03:35 UTC NAS 停服、00:04:20 UTC 健康，Linux 00:04:28 UTC 恢复。两次维护窗口均不能用于吞吐下降或 ETA 外推；最终恢复后建立新基线。
- 首次部署验收发现旧 Mac holder 被匹配条件排除，立即停服；保留首次状态后从本次备份恢复两个数据库，补充历史来源测试并重新部署。最终证据以 deployment.json 和 path-verification.json 为准，不能把 first-attempt 当成功记录。
- 备份 control_plane.sqlite.before / jobs.sqlite.before 及首次状态均保留，不自动清理。没有删除、移动或覆盖原始照片。

## 生产路径验收

旧目录 `/volume2/photo/照片/2025/婚礼拍立得`，新目录 `/volume2/photo/照片/2025/白墨水婚礼/婚礼拍立得`。147/147 路径与原资产 ID、内容哈希一致，旧目录 catalog_paths 为 0，新目录 147 条；原有 8 张 ready 缩略图逐项保持，SQLite quick_check=ok。扫描最终 completed，后半段处理 51、跳过已同步 96、失败 0。

- 三张原始文件 SHA-256 与旧数据库一致，包含 B0002535.3FR，三张默认路径均更新为新目录。
- 9 项旧路径错误通过 cache-retry 重新入队，2026-09-30 00:06:46 UTC 全部 ready、last_error=NULL；3FR 只生成缩略图，保留 deferred RAW 描述，不生成标准 HEIC。
- 恢复后 NAS healthy，restart 0 / OOM false；Linux restart 0、OOM 和 memory.events=0、无新增错误，16 路任务正常推进。
- nas-2 已更新最终镜像与维护窗口，继续每 15 分钟汇报；历史监控数据保留，重新建立有效速率基线。
- 结构化证据：[2026-09-29-path-sync-priority.json](2026-09-29-path-sync-priority.json)。
