# 仓库记忆

## 发布与保密
- 公开仓库不得提交 `.build/`、本地 SQLite 库、生成的 `.app` 包、`AGENTS.md` 或任何本地凭据文件。
- 发布前必须运行 `scripts/pre_merge_gate.sh`，至少验证 Swift 构建和常见密钥模式扫描。
