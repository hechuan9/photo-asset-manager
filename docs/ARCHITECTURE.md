# Photo Asset Manager 架构

## 范围

本仓库现在是一个小型 monorepo，分成三个已经落地的目录：

- `macos/`：SwiftPM macOS app、本地 SQLite projection/cache、测试和 app 打包脚本。
- `ios/`：Xcode iOS app，复用同一套 ledger/SQLite core，当前只实现自动同步回放验证与瀑布流图库。
- `control_plane/`：FastAPI + SQLAlchemy 的 Aurora authoritative event store control-plane API 和测试。
- `infra/terraform/`：AWS control plane 与 S3 衍生图层的 Terraform 入口，已经开始定义 remote state bootstrap、Aurora、derivative bucket 和 runtime skeleton。

control_plane 已经实现第一版 dev/test HTTP 服务；AWS control plane 仍然会继续通过 Terraform 承载部署边界，但设备不能直连 Aurora，所有读写都必须经过 control-plane API。生产认证、迁移编排和 Aurora 部署参数仍未完成，当前服务不应把 dev/test 授权 stub 当成生产边界。

Terraform 当前已经明确了这些边界：

- remote state 使用 S3 bucket + DynamoDB lock table 的 bootstrap 资源，但 backend 仍需先用本地 state 启动后再切换。
- Aurora PostgreSQL 采用 Serverless v2，连接材料放在 Secrets Manager，Lambda 通过 VPC 直连 PostgreSQL。
- derivative 只落 S3 thumbnail/preview bucket，开启 encryption、versioning、Block Public Access，并为 presigned upload 保留受控 CORS。
- control-plane runtime 采用 API Gateway + Lambda skeleton，默认入口使用 `AWS_IAM`，CloudWatch logs 作为最小可观测面。
- Lambda 访问 AWS API 走 VPC endpoints：Secrets Manager interface endpoint 和 S3 gateway endpoint，避免依赖公网出口。
- 仍未定义真实迁移、健康检查、smoke gate 和 production deployment 流程。

## 数据所有权

系统把本地投影、同步事实、衍生图和原片分开：

- 每台 iOS/macOS 设备拥有自己的本地 SQLite，用来保存本地 replicated event cache、outbox、UI 投影、迁移状态和可丢弃 cache metadata；本地 SQLite 不是跨设备权威 ledger。
- 跨设备同步的真源是 append-only 业务事件 ledger，不是本地 SQL update。
- 缩略图和中等预览图是衍生对象，目标是长期放在 S3；ledger 只保存声明和指针，不保存图片 bytes。
- 原片、RAW、sidecar canonical 和 canonical export 不进入 S3，仍归单一 original server/NAS 管。

## 客户端 App

当前仓库已经有两个客户端运行时：

- macOS app：完整资料库管理器，负责扫描、导入、归档、文件夹浏览、本地文件操作，以及把可同步变化自动写入 ledger 并上传。
- iOS app：最小只读回放端，负责把 control-plane ledger 自动拉到本地 SQLite 投影，并用瀑布流查看图库；当前不承担扫描、导入、归档或任何原片文件操作。

主要入口：

- `macos/Package.swift`
- `macos/Sources/PhotoAssetManager/PhotoAssetManagerApp.swift`
- `macos/Sources/PhotoAssetManager/ContentView.swift`
- `macos/scripts/pre_merge_gate.sh`
- `macos/scripts/package_app.sh`
- `ios/PhotoAssetManagerIOS.xcodeproj`
- `ios/Sources/PhotoAssetManagerIOS/PhotoAssetManagerIOSApp.swift`
- `ios/Sources/PhotoAssetManagerIOS/IOSLibraryStore.swift`
- `ios/Sources/PhotoAssetManagerIOS/WaterfallGalleryView.swift`

根目录 `scripts/` 只是兼容 wrapper，会转发到 `macos/scripts/`。

app 可以直接读写自己的本地 SQLite，但跨设备可同步的修改必须先变成 ledger 业务事件。云端数据库不应暴露给 app 直连。

## Ledger 同步实现

当前 ledger 代码已经在 macOS 端落地，核心文件是：

- `macos/Sources/PhotoAssetManager/SyncLedger.swift`
- `macos/Sources/PhotoAssetManager/SyncControlPlane.swift`
- `macos/Sources/PhotoAssetManager/SyncBootstrapper.swift`
- `macos/Sources/PhotoAssetManager/SQLiteDatabase.swift`
- `macos/Sources/PhotoAssetManager/LibraryStore.swift`

### 事件模型

每条同步事实是 `OperationLedgerEntry`，字段包括：

- `opID`
- `libraryID`
- `deviceID`
- `deviceSequence`
- `hybridLogicalTime`
- `actorID`
- `entityType`
- `entityID`
- `opType`
- `payload`
- `baseVersion`
- `createdAt`

目前支持的业务事件类型包括：

- `asset_snapshot_declared`
- `file_placement_snapshot_declared`
- `metadata_set`
- `tags_updated`
- `move_to_trash`
- `restore_from_trash`
- `imported_original_declared`
- `archive_requested`
- `original_archive_receipt_recorded`
- `derivative_declared`

这些事件是同步协议。SQLite 表结构只是本机存储和查询实现，不能把 SQL schema 当作跨设备协议。

### 本地表

SQLite 里与同步相关的表包括：

- `operation_ledger`：本地 replicated event cache + outbox。本机 pending 事件和服务端 committed 事件统一落表，但跨设备权威顺序来自服务端 `global_seq`。
- `sync_upload_queue`：本机待上传事件队列。
- `sync_cursors`：每个远端 peer/control-plane 游标。
- `sync_hlc_state`：每个 library/device 的 HLC 状态。
- `sync_migration_state`：现有 Mac 库 bootstrap ledger 化的水位。
- `file_objects`：content hash、size、role 表示的文件对象。
- `file_placements`：某个 file object 当前在哪个设备、NAS/server 或云端位置。
- `derivative_objects`：asset 级 thumbnail/preview 的 S3 指针和尺寸。
- `asset_trash_states`：共享回收站投影状态。

本地 `assets`、`file_instances` 等表仍用于 UI 查询和现有 macOS 功能，但跨设备语义应从 ledger 事件产生，而不是直接同步这些表。

### 写入路径

用户操作不应该直接暴露为任意 SQL update。当前写入路径是：

```text
SwiftUI / LibraryStore
  -> SyncCommandLayer
  -> SQLiteDatabase.recordLedgerOperation / updateAssetMetadataAndAppendLedger
  -> operation_ledger + sync_upload_queue + 本地 side table
```

已经走 command/ledger 的操作包括：

- 评分：`setRating`
- 旗标：`setFlag`
- 颜色标签：`setColorLabel`
- 标签增删：`updateTags`
- 移入共享回收站：`moveToTrash` / `moveAssetsToTrash`
- 从共享回收站恢复：`restoreFromTrash` / `restoreAssetsFromTrash`
- 新导入原片声明：`declareImportedOriginal`
- 请求归档：`requestArchive`
- 原片 server 归档 receipt：`recordArchiveReceipt`
- S3 thumbnail/preview 衍生图声明：`declareDerivative`
- 扫描/导入路径产生的资产快照与 placement 快照：`upsertScannedFile(..., ledgerContext:)`

`LibraryStore` 对评分、flag、标签等现有 UI 操作会先更新本地 projection，再在同一个事务中 append ledger。这样 UI 立即可见，同时同步队列不会丢事件。

macOS 当前不会等用户手动点同步才上传。`LibraryStore` 会在扫描、导入、元数据回填、评分/标签修改和回收站状态变化后做一次 debounce，然后自动执行：

1. 如有需要先把现有资料库 bootstrap 成 ledger 快照。
2. 上传待同步的缩略图衍生对象。
3. 再执行 `SyncService.sync()` 上传/拉取 ledger。

这样 iOS 端只要前台在线，就能自动回放最新 ledger，并按需拉取缩略图，不需要任何手动导出或手动触发同步流程。

### 时间、顺序和幂等

每台设备写事件时会保留两个顺序维度：

- `deviceSequence`：同一个 `libraryID + deviceID` 下递增。
- `hybridLogicalTime`：`wallTimeMilliseconds + counter + nodeID`。

`SQLiteDatabase.reserveLedgerClock` 会读取 `sync_hlc_state`，如果系统时间倒退，就保留旧 wall time 并递增 counter。这样离线写入也能得到单调 HLC。

事件落表时以 `op_id` 作为 primary key。重复收到同一个 `op_id` 时：

- 只有在 `op_id`、`library_id`、`device_id`、`device_seq`、HLC、entity/op identity 都一致且 payload 完全一致时，才视为幂等。
- payload 不一致：快速失败，报 `operation_ledger.op_id 冲突`。

这条规则同时用于本地 append、远端 pull 和 bootstrap 重跑。

### 上传和拉取

`SyncService.sync()` 当前分两步：

```text
uploadPendingOperations()
pullRemoteOperations()
```

上传流程：

1. 从 `sync_upload_queue` claim `pending` 事件。
2. 状态改为 `uploading`，避免同一进程重复上传。
3. `POST /libraries/{libraryID}/ops`。
4. 成功后标记 `acknowledged` 并从 upload queue 删除，同时记录每条 accepted op 的服务端 `global_seq`。
5. 失败时恢复为 `pending`，增加 attempt count，并保留完整错误字符串。

如果 app 上传到一半崩溃，`recoverStaleLedgerUploads` 会把过期的 `uploading` claim 恢复成 `pending`，允许重试。

拉取流程：

1. 从 `sync_cursors` 读取 peer cursor。
2. `GET /libraries/{libraryID}/ops?after=<cursor>`；如果响应 `hasMore == true`，继续按新 cursor 拉到最后一页。
3. 把每一页返回的远端 committed events append 到本地 `operation_ledger`，状态为 `acknowledged`，并保存每条事件的服务端 `global_seq`。
4. 同一事务里更新 cursor；如果远端返回的 `op_id` 已经是本机 pending op，则校验 payload 一致后标记为 `acknowledged`、删除 outbox 项并记录 `global_seq`。

注意：upload response 里的 `cursor` 只表示服务端当前最新 committed watermark，不能直接覆盖本地 pull cursor。`sync_cursors` 只在成功 replay `GET /ops` 返回页后推进；upload ack 只更新已接受 op 自身的 `global_seq` / `remote_cursor`。

本地 projection replay 优先使用服务端 `global_seq` 排序。没有 `global_seq` 的本机 pending op 只表示本机乐观状态，不改变 authoritative committed events 的 replay 顺序。

目前 control-plane HTTP client 已实现请求构造、JSON 编解码、路径/query percent-encoding、保留 header 防覆盖、archive receipt、derivative upload metadata 和 derivative metadata 获取。真正的云端 API server 还没有实现。

### 新设备冷启动投影

新设备第一次同步时，不能假设本地已经存在 `assets`/`file_instances` SQL 行。当前实现里：

- `asset_snapshot_declared` 会直接 materialize 到本地 `assets` 表；
- 后续 `metadata_set` / `tags_updated` / trash restore 会继续更新本地投影；
- `derivative_declared` 继续写 `derivative_objects` 和云端 placement，iOS 端再通过 control-plane `GET /derivatives/{assetID}` 取 signed download URL 做缩略图加载。

这样 iOS 新设备即使没有任何本地原片路径，也能先看到资产元数据和远端缩略图。iOS 瀑布流不会回退去读原图路径；没有缩略图时只显示占位或等待远端 derivative。

### Projection 和冲突处理

`SyncLedgerProjector.project(_:)` 用 ledger 重建当前状态。排序规则是：

```text
global_seq ASC when present
hybridLogicalTime ASC
deviceID ASC
opID ASC
```

合并规则：

- rating、flag、color label、caption 使用 per-field register。
- tags 使用 add/remove set register，不用最后写入覆盖整个标签数组。
- trash/restore 是 asset lifecycle 状态，投影到 `asset_trash_states`。
- imported original / archive requested 会把 asset 标成 `pending_original_upload`。
- original archive receipt 会把 asset 标成 `archived` 并记录 server placement。
- derivative declaration 会记录 `derivative_objects`，并把 S3 bucket 作为 cloud preview placement。

当 rating 或 flag 在同一个 HLC 下出现不同值时，projector 会记录 `SyncConflict`，同时仍按确定性 tie-break 选出投影值。当前 conflict 还只是内存 projection 结果，没有持久化 conflict queue 或 UI 解决流程。

### Bootstrap 现有 Mac 库

`SyncBootstrapper.bootstrapExistingLibraryToLedger()` 用一次快照把已有 SQLite 库 ledger 化：

1. 扫描当前 assets 和 file instances。
2. 计算 source database fingerprint。
3. 写入 `sync_migration_state.started`。
4. 生成稳定 `opID` 的 snapshot 事件：
   - asset snapshot
   - file placement snapshot
5. append 到 `operation_ledger`，状态为 `pending`。
6. 用 `SyncLedgerProjector` replay 并校验 projection。
7. 写入 `sync_migration_state.completed`、ledger high watermark 和 projection verification。

bootstrap 的 `opID` 来自稳定 key 的 SHA-256 派生 UUID。重复运行时同一事实不会重复写入；如果同一 stable `opID` 对应的 payload 变化，会快速失败。

bootstrap 不会伪造 thumbnail/preview derivative declaration。真实衍生图必须等本地已经生成缩略图文件，并且上传成功后，再由 macOS append `derivative_declared`。这样 ledger 始终只声明真实存在、iOS 可取到的缩略图对象。

### 文件内容和 S3 衍生图

文件内容不直接塞进 ledger。当前模型是：

```text
Asset
  -> FileObject(contentHash, sizeBytes, role)
  -> FilePlacement(holderID, storageKind, authorityRole, availability)
```

thumbnail/preview 通过 `DerivativeObject` 表达：

```text
assetID + role + fileObject + s3Object(bucket/key/eTag) + pixelSize
```

当前第一阶段只优先同步 thumbnail；preview 可以沿用同一套对象模型，但还不是跨设备浏览成立的前置条件。未来正式后端应由 control-plane 签发上传/下载 URL，客户端不应靠猜 key 作为权限边界。

## AWS Control Plane 边界

预期 AWS control plane 位于客户端和云端持久化之间。客户端不直接连云端数据库。

第一版 API 面建议保持为：

- `POST /libraries/{libraryID}/ops`
- `GET /libraries/{libraryID}/ops?after=<cursor>`
- `POST /devices/{deviceID}/heartbeat`
- `POST /derivatives/uploads`
- `GET /derivatives/{assetID}?role=thumbnail|preview`
- `POST /archive/receipts`

control plane 负责校验 operation payload、执行 library/device 访问控制、检查幂等、签发 S3 URL、保存 audit metadata。

## Terraform

Terraform 文件位于 `infra/terraform/`。

当前已经实现：

- AWS provider 配置和默认 tags。
- 基于 project/environment/account 的命名策略与共享变量。
- remote state bootstrap 资源：S3 bucket + DynamoDB lock table。
- 默认 VPC 下的 Aurora PostgreSQL subnet group、security group、Serverless v2 cluster、Secrets Manager connection secret，以及 Lambda VPC 直连所需的 subnet 绑定。
- 只用于 thumbnail/preview 的 S3 derivative bucket，启用 encryption、versioning 和 Block Public Access。
- API Gateway + Lambda control-plane runtime skeleton、CloudWatch logs 和最小 IAM 边界。
- 关键 outputs：Aurora endpoint/database/secret ARN、derivative bucket name、runtime API URL。

Terraform definitions 已经落地，但尚未完成真实 AWS apply/deployment/remote backend activation/migration/auth/smoke。当前仓库中的 Terraform 仍然需要先用本地 backend bootstrap，再切换到共享 backend，不能把 local state 的结果误当成生产环境已部署。

Terraform state 不提交。实际 AWS 资源是否已创建，以后续 apply 和部署结果为准，不应从配置文件本身推断。

## 当前未实现

这些生产级部分还没有完成：

- 生产认证边界和权限门控。
- schema 迁移、版本门禁和发布编排。
- 真实 AWS apply 和生产环境 deployment。
- 远程 Terraform backend activation。
- S3 derivative bucket 的 signed upload/download policy 收敛。
- API Gateway、Lambda、ECS、App Runner 或等价生产运行时的生产化配置。
- 用于衍生图 signed upload/download 的 IAM role 和 policy。
- conflict queue 持久化和 UI 冲突解决。
- dev/test 之外的 control_plane 生产化包装和部署脚本。

## 架构清单

```json
{
  "repo_type": "photo_asset_manager_monorepo",
  "implemented_runtimes": ["macos_app", "ios_app", "control_plane_devtest"],
  "planned_runtimes": ["aws_control_plane"],
  "key_directories": {
    "macos_app": "macos",
    "ios_app": "ios",
    "control_plane": "control_plane",
    "terraform": "infra/terraform",
    "root_wrappers": "scripts"
  },
  "macos_entrypoints": [
    "macos/Package.swift",
    "macos/Sources/PhotoAssetManager/PhotoAssetManagerApp.swift",
    "macos/scripts/pre_merge_gate.sh",
    "macos/scripts/package_app.sh"
  ],
  "ios_entrypoints": [
    "ios/PhotoAssetManagerIOS.xcodeproj",
    "ios/Sources/PhotoAssetManagerIOS/PhotoAssetManagerIOSApp.swift",
    "ios/Sources/PhotoAssetManagerIOS/IOSLibraryStore.swift",
    "ios/Sources/PhotoAssetManagerIOS/WaterfallGalleryView.swift"
  ],
  "ledger_core_files": [
    "macos/Sources/PhotoAssetManager/SyncLedger.swift",
    "macos/Sources/PhotoAssetManager/SyncControlPlane.swift",
    "macos/Sources/PhotoAssetManager/SyncBootstrapper.swift",
    "macos/Sources/PhotoAssetManager/SQLiteDatabase.swift",
    "macos/Sources/PhotoAssetManager/LibraryStore.swift"
  ],
  "terraform_entrypoints": [
    "infra/terraform/main.tf",
    "infra/terraform/variables.tf",
    "infra/terraform/outputs.tf",
    "infra/terraform/versions.tf",
    "infra/terraform/networking.tf",
    "infra/terraform/endpoints.tf",
    "infra/terraform/aurora.tf",
    "infra/terraform/runtime.tf",
    "infra/terraform/s3.tf",
    "infra/terraform/state.tf"
  ],
  "cloud_resources_currently_defined": [
    "terraform_remote_state_bootstrap",
    "aurora_postgresql_serverless_v2",
    "s3_derivative_bucket",
    "apigw_lambda_control_plane_runtime"
  ],
  "cloud_resources_currently_applied": [],
  "local_state_files_committed": false,
  "original_file_policy": "当前仓库流程不得删除、移动、覆盖用户照片原片，也不得把 RAW、sidecar 或 canonical export 存入 S3。",
  "control_plane_status": {
    "dev_test_http_api": "implemented",
    "production_auth": "not_implemented",
    "migration_version_gate": "not_implemented",
    "aurora_deployment": "terraform_defined_not_applied",
    "remote_backend_activation": "not_implemented",
    "smoke_gate": "not_implemented"
  }
}
```
