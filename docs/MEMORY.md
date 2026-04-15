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
