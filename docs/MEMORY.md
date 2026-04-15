# 仓库记忆

## 发布与保密
- 公开仓库不得提交 `.build/`、本地 SQLite 库、生成的 `.app` 包、`AGENTS.md` 或任何本地凭据文件。
- 发布前必须运行 `scripts/pre_merge_gate.sh`，至少验证 Swift 构建和常见密钥模式扫描。

## 资产索引语义
- 停止追踪来源目录只影响后续扫描调度，不删除资产、不删除文件位置，也不把原片视为用户主动删除。
- 当前阶段不为了“预览”额外创建 JPEG；已有 JPG 应作为独立文件位置入库并可用于浏览。
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
- 根因：启动路径只做普通刷新，没有先判定库索引是否处于未完成状态。
- 预防动作：启动时必须先检查未完成来源；存在未扫描来源时进入阻塞式“系统整理中”任务，补齐 browse graph 后再启动后台文件状态校验。
- 合并前验证：运行覆盖 `startStartupLibraryOrganizationIfNeeded`、`lastScannedAt == nil`、`BlockingTaskReport(title: "系统整理中"`、启动整理后刷新 `indexedBrowseFolders` 的测试，并执行 `swift test` 与 `scripts/pre_merge_gate.sh`。
