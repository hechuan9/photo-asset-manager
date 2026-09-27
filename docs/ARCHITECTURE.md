# Keeps 架构

产品导航与能力边界见 [统一服务端资料库](UX_DESIGN.md)。统一采用 HTTP/HTTPS API；本地上传与逻辑相册尚未实现。

## 唯一运行模式

Keeps 采用一个 NAS 核心服务和两个原生薄客户端。NAS 是照片索引、整理结果、任务状态和预览对象的唯一真源；macOS 与 iOS 只负责用户交互、HTTP 请求及可丢弃的图片缓存。

客户端已切换到共享 HTTP API；Rust 服务已在 `chuan_nas` 部署并完成历史数据迁移，首次全库扫描仍在后台运行。本文描述职责和代码边界，实际部署步骤与验证结果以 [NAS 部署说明](../deploy/nas/README.md) 为准。

```mermaid
flowchart LR
    Mac[macOS SwiftUI] --> API[KeepsAPI 共享 Swift 包]
    iOS[iOS SwiftUI] --> API
    API --> HTTP[NAS Rust / Axum]
    HTTP --> Catalog[资产查询与整理]
    HTTP --> Jobs[目录追踪与持久化任务]
    Catalog --> DB[NAS SQLite 与内部审计事件]
    Jobs --> Media[扫描与媒体处理]
    Media --> Originals[只读原片目录]
    Media --> Preview[服务端预览对象]
    Media --> Catalog
```

## 代码入口与职责

| 目录 | 职责 |
| --- | --- |
| `server/` | 唯一活跃后端：Rust HTTP API、权威查询模型、SQLite、任务和媒体处理。 |
| `shared/` | `KeepsAPI` Swift 包：HTTP 请求、DTO、连接偏好及 Keychain 凭据；无 SQL、事件回放或照片处理。 |
| `macos/` | 统一资料库浏览、选择、筛选、整理表单及服务器来源/任务界面；`LibraryStore` 仅保存 UI 内存状态。 |
| `ios/` | 原生图库、详情与整理交互；`IOSLibraryStore` 通过同一个 `KeepsClient` 查询 NAS。 |
| `deploy/nas/` | Docker Compose、环境配置及 NAS 部署说明。 |
| `control_plane/` | 旧 Python 协议、迁移 seed 工具与对照测试；不是第二套活跃后端。 |
| `scripts/` | 客户端打包入口和显式执行的迁移工具。 |

Rust 主要模块：

- `main.rs`：配置、状态装配、监听与停机。
- `api.rs`：HTTP 认证和路由。
- `catalog.rs`：资产查询、修改及与服务器内部审计事件的衔接。
- `jobs.rs`：目录追踪、持久化扫描任务与文件处理记录。
- `media.rs`：原片元数据读取、指纹与预览生成。
- `store.rs` / `protocol.rs`：历史事件存储、协议验证、幂等和审计兼容。
- `previews.rs`：服务端预览对象及签名 URL。

后台任务的启动、轮询和恢复属于 NAS 进程。客户端关闭、离线或更换设备不得决定任务生命周期。

## 客户端边界

macOS 使用标准 Settings scene（⌘,）配置 Keeps Server 的服务地址、资料库和访问令牌；先验证服务鉴权与资产查询，成功后才持久化并切换连接。设置不管理 NAS 设备、SMB 挂载或 SSH 登录。

两端的查询与修改经过 `shared/Sources/KeepsAPI/KeepsClient.swift`。客户端不创建业务 SQLite，不生成、上传或回放 ledger，不扫描目录、不解析 EXIF，也不从本地原片生成预览。

客户端不存在“NAS / 本地”双资料库模式。默认浏览统一资料库；服务器来源目录属于按需展开的辅助视图和服务端管理配置。主图库查询不依赖目录导航成功。客户端无需 SMB 挂载，不能把服务器路径作为本机文件 URL 打开；预览和业务交互均走 HTTP。

未来本地文件仅作为显式上传来源。上传尚未实现，不恢复客户端监听、扫描或本地业务数据库。协议选择保持 HTTP JSON，当前共用手写 KeepsAPI；尚未引入 OpenAPI 客户端生成器。
客户端允许保留：

- 当前筛选、选择、已加载分页及表单状态。
- 服务地址与资料库名称等 UserDefaults 偏好。
- Keychain 中的访问凭据；旧 `ios.sync.*` 偏好继续读取，明文凭据成功迁移后移除。
- 网络图片缓存。缓存不是业务真源，丢失后可重新请求 NAS。

界面上的“修改评分”“保存标签”“移入回收站”直接提交资产 API，并使用服务器返回结果。请求失败显示错误，不切换到本地写入模式，也不排队产生离线业务事件。

预览由 KeepsAPI 中的共享图片加载器加载，两端使用同一缓存策略。iOS/iPadOS 与 macOS 每个应用各有 5 GiB 磁盘缓存，位于系统 Caches/KeepsPreviews；按文件字节统计，超限时按最近访问时间淘汰到 4 GiB，启动读取和新增缓存时检查容量。写入前检查可用空间；不足 1 GiB 时回收磁盘图片缓存。若写入仍因磁盘满而失败，记录错误并显示已下载图片，其他写入错误正常上报。无固定 TTL，系统清除或文件损坏后重新下载；缓存写入与清理只访问该专用目录。

磁盘身份由服务器 URL、资料库、资产 UUID 和预览版本组成，不包含签名下载 URL；刷新链接不会重复缓存同一版本。内存使用 NSCache，iOS 64 MiB、macOS 256 MiB 的成本目标，按解码图片 bytesPerRow × height 计费，淘汰由系统控制；按显示尺寸和屏幕倍率下采样，内存身份另含解码尺寸。同一磁盘身份的并发下载合并；使用无 URLCache 的图片会话，避免签名链接产生另一份 HTTP 磁盘缓存。

签名下载 URL 有效期为 15 分钟。服务端对验签成功但过期的下载令牌返回 HTTP 403、detail.code=preview_token_expired；客户端仅对这个明确错误自动刷新并重试一次，其他错误显示完整详情并允许手动重试。刷新响应包含预览版本，缓存必须写入实际返回版本，不能把新图片记为旧版本。NAS 当前有效预览对象长期保留，不参与客户端 LRU。没有预览时显示占位，客户端不回退读取 NAS 路径或本地原片。

交互 API 使用独立的长期 URLSession，与共享会话中的图片下载分离，避免目录请求和预览争用同一会话的连接调度。目录展开只返回当前层；服务端为每个目录探测是否存在可见直接子目录，遇到第一个即停止，`hasChildren` 返回真实布尔值，叶子目录不显示展开箭头。

macOS 目录树使用 `NSViewRepresentable` 包装原生 `NSOutlineView`，由 AppKit 负责树形行复用、展开折叠及键盘导航；SwiftUI 保留其它界面。目录节点按路径保持稳定身份，数据和展开状态由 `LibraryStore` 管理，Coordinator 仅桥接视图。数据源同步读取内存缓存，异步响应仅更新变化节点；收起再展开不重复请求，刷新保留有效展开、选择和滚动位置，切换服务器才重置。`local.keeps` 的 `navigation` 日志记录目录请求耗时与返回数量，不记录路径或凭据。

路径统一以 NAS 真实绝对路径为身份，例如 `/volume2/photo/照片`。原片以宿主同路径只读挂入容器；导航、查询、追踪配置、扫描状态和 catalog_paths 使用同一路径，不再使用 `/originals/library` 别名或 Mac `/Volumes` 路径。根 `/volume2` 下按真实层级显示 `photo`、`myphoto`。历史目录关联可通过 `scripts/migrate_nas_paths.py` 离线恢复，核对文件存在/大小及既有资产记录；恢复不代表重新校验内容哈希。

导航条目包含数据库 `photoCount`，按 `(library_id,path)` 索引范围查询后代路径并对未回收资产去重；不枚举文件系统统计照片、不逐项请求图库分页。目录行在子目录或所选目录内容读取期间显示原生旋转指示，完成后恢复文件夹图标。计数独立于图库筛选条件。

## API 契约

业务路由需要共享访问凭据。首期使用单一 NAS 的 Bearer 认证，不声称具备多用户或租户隔离能力。预览下载使用服务端签名 URL。

| API | 用途 |
| --- | --- |
| `GET /libraries/{libraryID}/assets` | 分页查询、目录范围、搜索、评分、旗标、颜色、标签、回收站及排序。 |
| `GET /libraries/{libraryID}/assets/{assetID}` | 资产详情。 |
| `PATCH /libraries/{libraryID}/assets/{assetID}` | 修改评分、旗标、颜色和标签。 |
| `POST .../assets/{assetID}/trash` / `restore` | 修改共享回收站状态。 |
| `GET` / `PUT /libraries/{libraryID}/hidden-directories` | 读取和修改目录隐藏标记（path、hidden），返回 paths；仅改 NAS 数据库。 |
| `GET /libraries/{libraryID}/counts` | 全部、精选和回收站计数。 |
| `GET /libraries/{libraryID}/directories` | 服务器索引中的目录及数量。 |
| `GET /libraries/{libraryID}/navigation?path=...` | 服务器真实目录导航；省略路径返回追踪范围内的根入口，指定路径返回直接子目录，包含空目录；仅返回 `path`、`directories`，不再返回位置分区或本地暂存状态。 |
| `GET` / `POST /libraries/{libraryID}/folders` | 查看追踪目录与原片根路径、添加相对目录。 |
| `DELETE /libraries/{libraryID}/folders/{folderID}` | 停止追踪，保留原片。 |
| `POST .../folders/{folderID}/scan` | 提交扫描任务。 |
| `GET /libraries/{libraryID}/jobs` | 任务状态和错误。 |
| `POST .../jobs/{jobID}/retry` | 重试失败任务。 |
| `GET /derivatives/{assetID}?role=preview&libraryID=...` | 刷新预览下载 URL。 |

分页响应为 `items`、`total`、`nextCursor`；游标由服务端解释。`PATCH` 中省略字段表示不修改，显式 `colorLabel: null` 表示清除颜色。

旧 `/ops`、心跳、归档回执和客户端预览上传路由已关闭；事件格式仅用于 NAS 内部审计与历史数据迁移。客户端直接提交业务命令，不接触事件协议。

## 数据与照片安全

- `KEEPS_ROOT` 保存服务数据库、任务数据、预览及其他服务工件；当前宿主机路径 `/volume2/myphoto/keeps`。
- Compose 将四个既有原片目录按 NAS 绝对路径只读挂载，服务端 `ORIGINAL_ROOT=/volume2`；容器仅暴露这些原片目录，服务数据另挂 `/myphoto/keeps`。
- Compose 将原片目录以只读方式挂载。任何照片、RAW、sidecar 原始文件都不得删除、移动或覆盖。
- 停止目录追踪和移入回收站只改变服务器记录，不能对应磁盘删除。
- 预览写入 `KEEPS_ROOT/previews`。维护生成的预览和缓存不能触碰原片。
- SQLite 与 ledger 只属于 NAS 内部。历史 Python 数据库与事件用于迁移、兼容和审计；同一状态目录不能同时运行新旧后端。

旧客户端的本地数据库不是新架构中的活跃数据源。源码移除不删除用户已有数据库或照片；需要导入历史资料时，使用显式迁移工具和验证步骤，不能在新客户端启动时恢复旧扫描、复制或同步任务。

## 客户端发布

客户端通过 TestFlight 内部测试分发，团队为 ClimaMind LLC（`3TZ6RCL8NE`）。`scripts/testflight.sh` 统一执行两端 Xcode 归档与 App Store Connect 上传；macOS 发布工程为 `macos/Keeps.xcodeproj`，继续复用现有 Swift 源文件与 KeepsAPI，本地 SwiftPM 测试与开发打包保持可用。iOS 使用既有工程，企业导出配置已移除。

macOS 发布构建启用 App Sandbox，只授权出站网络访问，不请求读取用户照片目录。连接凭据继续使用应用私有 Keychain；签名沙盒版本的配置读取与连接需实际安装验收。上传、Apple 处理、分配内部测试组和设备可安装是不同阶段，不能以归档成功代替发布完成。

## 验证边界

- `swift test --package-path shared`：共享 HTTP 路由、编码、分页、错误与服务器返回数据契约。
- `swift test --package-path macos`：客户端 UI 状态、远端交互和品牌资源。
- iOS Simulator 构建：共享包集成和原生客户端编译。
- `cargo test --manifest-path server/Cargo.toml`：服务端协议、存储、查询与任务测试。
- 真机/NAS 验收需要另外验证服务重启、原片只读挂载、任务持续执行、数据迁移及客户端到 NAS 的完整流程；编译或单元测试通过不能替代这些证据。

## Architecture Inventory

```json
{
  "repo_type": "photo_asset_manager_monorepo",
  "active_backend": "rust_nas_server",
  "client_role": "ux_only",
  "business_source_of_truth": "nas",
  "client_business_database": false,
  "client_event_replay": false,
  "client_original_processing": false,
  "client_nas_mount_required": false,
  "transport": "http_json",
  "key_directories": {
    "rust_server": "server",
    "shared_client_api": "shared",
    "macos_app": "macos",
    "ios_app": "ios",
    "legacy_migration_reference": "control_plane",
    "nas_deploy": "deploy/nas"
  },
  "entrypoints": [
    "server/src/main.rs",
    "server/src/api.rs",
    "shared/Package.swift",
    "shared/Sources/KeepsAPI/KeepsClient.swift",
    "shared/Sources/KeepsAPI/KeepsSettings.swift",
    "macos/Package.swift",
    "macos/Sources/PhotoAssetManager/PhotoAssetManagerApp.swift",
    "ios/KeepsIOS.xcodeproj/project.pbxproj",
    "ios/Sources/KeepsIOS/KeepsIOSApp.swift",
    "deploy/nas/docker-compose.yml"
  ],
  "server_modules": [
    "server/src/catalog.rs",
    "server/src/jobs.rs",
    "server/src/media.rs",
    "server/src/store.rs",
    "server/src/protocol.rs",
    "server/src/previews.rs"
  ],
  "original_file_policy": "read_only_no_delete_move_or_overwrite",
  "nas_production_acceptance": "requires_live_verification"
}
```

## 隐藏目录

Catalog schema 2 新增按资料库隔离的隐藏目录标记。升级 schema 1 时需停服备份并显式执行 migrate；保留资产与历史事件。assets 与 counts 默认过滤隐藏内容，showHidden=true 关闭过滤；过滤先于分页和 total 计算。选中隐藏根或其后代时解除该根的过滤，独立隐藏的更深层后代仍需进入后查看。资产有多个路径时，只要包含仍被过滤的隐藏路径就不显示。导航入口仍保留。Mac 提供目录右键标记与菜单开关，开关为本机显示偏好，标记保存在 NAS。该功能属于浏览过滤，不是文件加密或访问控制。

### 持续集成

GitHub Actions 在 main push、PR 和手动触发时验证 Rust 服务、Apple 客户端、Python 控制平面与迁移工具，执行发布脚本测试和密钥扫描。CI 不持有发布凭据，不自动上传。发布脚本从本机 `codex-secret run app-store-connect` 接收认证环境变量，支持只读构建状态查询；归档和上传使用权限 0600 的临时私钥文件并在退出时清理。
