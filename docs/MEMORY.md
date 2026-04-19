# 仓库记忆

## 发布与保密
- 公开仓库不得提交 `.build/`、本地 SQLite 库、生成的 `.app` 包、`AGENTS.md` 或任何本地凭据文件。
- 发布前必须运行 `scripts/pre_merge_gate.sh`，至少验证 Swift 构建和常见密钥模式扫描。

## 资产索引语义
- 停止追踪来源目录只影响后续扫描调度，不删除资产、不删除文件位置，也不把原片视为用户主动删除。
- 当前阶段不为了“预览”额外创建 JPEG；已有 JPG 应作为独立文件位置入库并可用于浏览。
- RAW 和同名 JPG 可以作为同一资产的两个原片位置存在，但 `thumbnail` 是资产级派生物；每个资产最多只能有一条缩略图记录。
- 派生文件迁移只能复制、校验并更新数据库路径，不能删除旧派生文件或任何照片文件。

## 文件夹组织
- 适用范围：文件夹树、来源目录移动、扫描清单整理。
- 问题模式：把“移动文件夹”误实现成移动磁盘目录或批量改写照片路径。
- 根因：UI 组织关系和真实文件系统位置没有分清。
- 预防动作：文件夹移动只能更新 `source_directories.parent_source_directory_id`，不得调用文件系统移动/删除，也不得批量改写 `file_instances.path`。
- 合并前验证：检查 diff 中移动逻辑只更新来源目录父级，并运行 `swift test` 与 `scripts/pre_merge_gate.sh`。

## POV 收尾
- 适用范围：完工、合并、push、交付可打开 app。
- 问题模式：改动在隔离 worktree 里验证通过，但用户打开老目录 app 看不到变化。
- 根因：开发路径和用户固定打开路径不同，打包产物没有回写到固定仓库目录。
- 预防动作：最终合并、验证、打包必须在 `/Users/hechuan/workspace/photo-asset-manager` 执行；`.build/` 和 `.app` 只生成不提交。
- 合并前验证：运行 `swift test`、`swift build`、`scripts/pre_merge_gate.sh`、`./scripts/package_app.sh`，并确认 app 路径是 `.build/app/PhotoAssetManager.app`。

## 文件夹树展开
- 适用范围：侧边栏文件夹树、展开/收缩、来源目录分组、文件夹浏览。
- 问题模式：侧边栏实时枚举 NAS/大目录导致卡顿，或选中未索引目录时创建空 browse 节点导致没有照片。
- 根因：把实时文件系统目录树和已索引浏览图混成同一个浏览真源。
- 预防动作：文件夹浏览树必须来自数据库里的 `indexedBrowseFolders` / `browse_nodes`；展开只投影已索引 browse graph，不在 SwiftUI render 路径调用 `contentsOfDirectory`。
- 合并前验证：运行覆盖 `indexedBrowseFolders`、禁止 `contentsOfDirectory`、字符串节点 ID、非纯黑 sidebar 颜色的 sidebar 测试，并执行 `swift test` 与 `scripts/pre_merge_gate.sh`。

## 浏览图
- 适用范围：文件夹浏览、未来相册/标签/日期等访问入口、资产查询筛选。
- 问题模式：把文件夹选择实现成路径字符串前缀筛选，导致直属/递归语义混乱，也难以扩展到非文件夹访问方式。
- 根因：没有把“文件实例的真实位置”和“用户访问资产的浏览入口”分成两个模型。
- 预防动作：新增访问入口必须优先接入 `browse_nodes`、`browse_edges`、`browse_file_instances`；文件夹浏览使用 `BrowseSelection` 和 `BrowseScope`，不要绕过浏览图直接拼路径筛选；选择文件夹只能读取已有 browse node，不能为了选择而写入空节点。
- 合并前验证：运行覆盖 `browse_nodes`、`browse_edges`、`browse_file_instances`、`WITH RECURSIVE selected_browse_nodes` 和菜单范围切换的测试，并执行 `swift test` 与 `scripts/pre_merge_gate.sh`。

## 扫描来源恢复
- 适用范围：启动迁移、import batch 恢复、来源目录清单。
- 问题模式：旧的 interrupted 根目录扫描把过宽来源（例如 `/Volumes/photo`）重新加入 `source_directories`，绕过用户后来收窄到子目录的意图，并把 `#recycle` 重新暴露到浏览树。
- 根因：迁移从所有 `import_batches` 回填来源目录，没有按完成状态过滤。
- 预防动作：只允许 `finished`、`finished_with_errors`、`resumed` 批次恢复来源目录；扫描跳过目录必须按 path component 识别 `#recycle`、`.trashes`、`.fseventsd`、`.spotlight-v100`。
- 合并前验证：运行覆盖 interrupted batch 不恢复 source、recycle path component 跳过、数据库无 `/Volumes/photo/#recycle%` 记录的检查，并执行 `swift test` 与 `scripts/pre_merge_gate.sh`。

## 启动索引整理
- 适用范围：应用启动、来源目录扫描、文件夹浏览树、后台可用性校验。
- 问题模式：数据库里仍有 `last_scanned_at IS NULL` 的来源时，应用直接展示部分文件夹树并启动后台校验，用户一打开就看到缺照片或空目录。
- 根因：启动路径只做普通刷新，没有先判定库索引是否处于未完成状态；单纯重扫已入库文件时，未变化文件可能跳过 browse graph membership 写入。
- 预防动作：启动时必须先检查未完成来源和已有文件但缺 browse graph 的来源；存在问题时进入阻塞式“系统整理中”任务，先从 `file_instances` 回填 browse graph，再补扫未完成来源，完成后才启动后台文件状态校验；重扫命中 unchanged 文件时也要修复 direct membership。
- 合并前验证：运行覆盖 `startStartupLibraryOrganizationIfNeeded`、`lastScannedAt == nil`、`sourceDirectoryPathsNeedingBrowseGraphRepair`、`backfillBrowseGraphFromFileInstances`、`unchangedFileInstanceID`、`BlockingTaskReport(title: "系统整理中"`、启动整理后刷新 `indexedBrowseFolders` 的测试，并执行 `swift test` 与 `scripts/pre_merge_gate.sh`。

## 启动后台任务
- 适用范围：启动后的文件可用性校验、后台状态条、SQLite 批量写回。
- 问题模式：启动后后台任务对全库照片逐条检查并逐条写库，导致大库侧边栏和选择交互卡顿。
- 根因：后台校验需要遍历大量 `file_instances`，如果查询缺少针对 `file_role,path` 的索引，或每条结果单独 UPDATE，主 actor 写库窗口会被拉长。
- 预防动作：可用性目标查询必须有 `idx_file_instances_role_path` 支撑；批量写回按 availability 分组，用 `WHERE id IN (...)` 更新，避免每条文件单独 prepare/step；启动时全量校验必须按最近完成时间节流，旧库首次升级先建立校验水位，手动入口才强制全量校验。
- 合并前验证：运行覆盖 `idx_file_instances_role_path`、`Dictionary(grouping: updates, by: \.availability)`、`WHERE id IN`、`last_availability_refresh_at`、`INSERT OR IGNORE INTO app_settings`、`startAvailabilityRefreshInBackground(force:)` 的测试，并执行 `swift test` 与 `scripts/pre_merge_gate.sh`。

## 文件夹浏览性能
- 适用范围：文件夹切换、资产网格分页、缩略图/预览渲染。
- 问题模式：打开大目录或子文件夹时，首屏一次加载过多资产，或 SwiftUI `body` 同步读取 NAS 缩略图/原片，导致 UI 卡顿。
- 根因：查询分页粒度过大，滚动续页依赖手动操作；预览加载和图片解码落在主线程 render 路径上；目录切换的 SQL/解码边界如果没有可持久查询的耗时日志，会继续靠体感猜测。
- 预防动作：文件夹切换只加载小页资产，滚动接近末尾自动续页；目录打开必须先给 blocking/选中反馈，资产读取走后台只读 SQLite 连接，过滤查询先分页再聚合；缩略图和原片 fallback 必须可取消、异步加载并通过内存缓存复用，`AssetPreviewImage.body` 不得直接调用 `NSImage(contentsOfFile:)` 或 `ImageRenderer.renderableImage`；性能诊断日志使用可查的 `notice` 级别并覆盖 click/load/decode 边界。
- 合并前验证：运行覆盖 `assetPageSize = 96`、`loadMoreAssetsIfNeeded`、自动 `.onAppear` 续页、`ImagePreviewCache`、可取消图片 decode、目录选择后台只读查询、`PerformanceLog` notice 日志、以及预览 body 禁止同步图片读取的测试，并执行 `swift test`、`scripts/pre_merge_gate.sh`、`./scripts/package_app.sh`。

## 启动 NAS 挂载
- 适用范围：应用启动、NAS 来源目录、文件状态校验、索引整理。
- 问题模式：NAS 还未挂到 `/Volumes/<share>` 时就开始扫描或可用性校验，导致原片被误判为 `Missing Original`。
- 根因：把 SMB/Finder 挂载状态当成文件缺失证据，且挂载尝试晚于启动整理和文件状态校验。
- 预防动作：启动时必须先用 blocking task 挂载已登记的 NAS 卷根目录；挂载失败时停止扫描和 availability check，并保留明确的 `NAS 挂载未完成` 状态。
- 合并前验证：运行覆盖 `mountNASRootsAtStartup`、`startupNASMountSucceeded` gate、`NASMountManager` 和 `smb://` 挂载入口的测试，并执行 `swift test`、`scripts/pre_merge_gate.sh`、`./scripts/package_app.sh`。
