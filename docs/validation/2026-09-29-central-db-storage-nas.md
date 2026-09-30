# 中央数据库与存储重构验收

状态：正式部署与最终健康验收均 PASS。全库缓存生成仍在后台继续，尚未完成。

发布目录：`/volume2/docker/keeps/releases/central-db-20260929`。受管访问使用 secrets-management / codex-secret chuan-nas，不记录凭据。

## 最终结构

- 系统数据：`/volume2/docker/keeps/data` → 容器 `/keeps`。
- 唯一照片挂载：`/volume2/photo` → 同路径，可写只用于新增标准图；任何已有照片均不得覆盖、移动或删除。
- myphoto 不参与运行挂载或追踪。旧系统树与旧标准图保留为未挂载回退资料；本次没有清理它们。
- 标准图放 RAW 同目录，名称 `<RAW完整文件名>.keeps-<RAW SHA256>.heic`，登记同一资产的版本及 generatedFrom 来源。已确认 JPEG/HEIF 直接复用，3FR 暂缓标准转换但仍生成小图。
- 缩略图保存在系统数据的 previews 中，可以重建；与长期照片数据分开。

## 数据库与客户端

schema 7 从现有业务表升级，不重放历史事件。运行时删除 ledger_events、ledger_sequence_counters、device_states、archive_receipts、sync_conflicts 和 derivative_objects.declared_event_seq；事件协议模块移除。评分、标签、回收站、入库、文件状态及缩略图引用直接在数据库事务内修改，保留资料库／目录修订号与 SQLite WAL。

客户端仍使用原有 HTTP API，不需要新增本地业务数据库或离线事件同步。历史 Python 迁移工具和测试素材不属于活跃后端；旧 ledger 留在迁移前数据库，不作为新系统恢复真源。

## 已完成验证

- Rust 71 个库测试、17 个 HTTP 测试、1 个 CLI 测试通过；1 个依赖 Linux 媒体环境的测试由 NAS 真实样本验证补充。Clippy all-targets -D warnings 与格式检查通过。
- 迁移工具 5 项测试通过：不覆盖、可重试、同资产登记与用户默认保留、缓存清单复用、CoW 克隆／完整复制回退。覆盖历史 RAW 无版本／默认记录、错误自动默认修复，保留 ready 缓存来源匹配。
- NAS 隔离真实 RAW、JPEG、HEIF、3FR 四样本全部 ready；RAW 标准同目录同资产，JPEG/HEIF 原文件复用，3FR 不返回 RAW 标准图链接。缩略图最长边 ≤512，签名下载逐个 SHA256 相符。
- 隔离重扫后仍 4 个资产，原 fixture 和生产来源哈希均未变。单核与 2GiB 限额有效，无 OOM。证据 `smoke/20260929-084603-d80edf/report.json`。
- 镜像源包 SHA256：`3328dc9f8719f00c068840e894123fb74c323248f63fe3435bb099249d6b0b20`。镜像 `0c622b9f9424` 与隔离验证镜像仅来源标签不同；服务端二进制 SHA256 已核对相同。

## 迁移与回退边界

`migrate_nas_storage.py --precopy` 在缓存暂停时预复制；Btrfs 支持时创建独立 CoW inode，否则完整复制及哈希核对。已有副本由大小／mtime 清单复核；群晖 @eaDir 不是 Keeps 对象，排除后续复制。旧树不变。停服后 `--apply` 用 SQLite backup API 复制数据库，校验完整性，再复制已确认来源的 5 张标准图。

切换前后的 catalog_assets、catalog_files、catalog_paths、catalog_versions、catalog_version_paths、catalog_defaults、隐藏目录、photos、videos、directories、media_cache 按表比较行数和 SHA256。标准图登记属于独立存储迁移步骤，schema 升级不改这些业务表。全部资产 snapshot 与原数据库一致。

首次正式尝试发现历史资产没有默认版本时，迁移登记生成图会改变缓存有效来源。下载验收因此失败，自动回退服务健康；保存证据 `deployment-attempt1-rolledback.json`。已恢复这 5 个资产的原有效来源，并修复迁移程序，补 RAW 版本路径和来源默认、保留用户选择；再次切换前直接验证缓存来源匹配。该事件不代表原片丢失或新 schema 数据损坏。

回退时保留已经新增的标准图，为实际存在且哈希匹配的文件登记同资产及增量记录，保持旧缓存描述。回退标记使下次迁移重新读取最新数据库，避免复用过期状态。旧系统根完整保留，不能自动清理。

## 后台处理

生成沿用每轮最多20项、并发1、休60秒、单核/2GiB；RAW标准图编码上限900秒。部署完成与全库生成完成必须分开报告。全库完成须 pendingInventory/pending/processing/failed 和 gc.pending/gc.failed 均为0，并有实际文件抽样证据；3FR 的 standard=null 是预期。

只读状态：`codex-secret run chuan-nas -- python3 scripts/nas_cache_status.py`。人工暂停 nextBatchAt=253402300799 不能报告停滞。未知孤立旧缓存与旧回退副本均不在本次自动清理范围。

## 最终线上状态

- 观察时间：2026-09-29T16:12:53Z。镜像 `0c622b9f9424`，healthy、重启0、无OOM，单核/2GiB。
- 90,073 个资产。相较前次目录清理后的89,628，切换前在线扫描新增445个索引；迁移时全部资产snapshot一致，生成标准图没有新增独立资产。
- 仅系统数据 `/volume2/docker/keeps/data:/keeps` 和照片 `/volume2/photo:/volume2/photo` 两个挂载；myphoto路径引用为0；schema7旧同步表为0。
- 5张历史标准图迁移并经生产HTTPS下载验证，原文件保留。旧myphoto系统树只作为回退资料，未挂载、未清理。
- 缓存状态：{"failed": 0, "pending": 90060, "processing": 1, "ready": 12}；待盘点0；GC已恢复开启，20分钟宽限/每轮20项。生成暂停已解除。
- `nas-2` 已建立当前线程每15分钟只读监控，仅异常、完成或需操作时通知；基线 `/tmp/keeps-thumbnail-monitor-latest.json` 已更新为有效状态。
- [机器可读验收](2026-09-29-central-db-storage-nas.json)。
