# 同目录整理生产执行

用户已授权执行；2026-09-27 14:01:29 UTC 在 NAS 启动独立进程，PID 12370。此文档记录启动验收，不表示全库整理完成。

## 已完成

- 停服备份 catalog/jobs 双库，SQLite 完整性检查通过；备份位于 `/volume2/myphoto/keeps/backups/before-folder-merge-deploy-20260927-070130`。
- 保留旧镜像别名 `keeps-server:before-folder-merge-20260927`，源 image `17950d7cfe475d279a602b8263f343ccc38d63c9bdfc6b7d3c4803d5f8e9b0ed`。
- 部署新镜像 `keeps-server:folder-merge-20260927`（`7855aff063257f09f22bd797e4994c6232aa6bd2d2bdd4c22b3f1c64424a48cc`），Compose 别名为 `keeps-server:local`。schema 保持 3。
- 新容器 healthy；原片挂载全部只读，业务 API 和预览 SHA-256 检查通过。NAS 活跃服务源码已同步到该镜像对应归档。

## 持续执行流程

NAS 独立进程在 SSH 连接结束后继续运行，不依赖客户端在线。

1. 在线全库 plan，工作目录 `/volume2/myphoto/keeps/maintenance/folder-merge-20260927`，检查器复用服务同样的 `TZ=America/New_York`。
2. 在线 plan 完整后停止服务，使用证据缓存重新 plan，使计划匹配静止数据库。
3. apply 自动另做合并前双库备份；只应用精确证据、同直接父目录、无字段冲突且完整核验的组。
4. 核对资产数减去合并数，路径数、历史事件数与隐藏目录数保持不变；重新核对参与合并原片 SHA-256。
5. 恢复服务并验收健康、只读挂载、业务 API 与预览，最终状态写入 `completed`。异常写入完整 trace；若已停服则尝试恢复服务并记录结果。

执行器：`/volume2/docker/keeps/releases/folder-merge-20260927/run-execution.py`。

权威实时状态：`/volume2/docker/keeps/releases/folder-merge-20260927/execution-state.json`；完整日志：同目录 `execution.log`。后续查询应先读取此状态与进程，不要重复启动作业。运行锁防止并发启动。

计划完成时生成 `plan.json`；未完成报告是 `plan.partial.json`。应用结果和原子合并映射见工作目录的 `applied.json` / `applied.sqlite`，备份位置见 `apply-started.json`。字段冲突、元数据候选与未完整索引目录会留在报告中，原片不删、不移动、不覆盖。

本地复制的状态 JSON 仅是启动阶段快照，最新阶段与计数以 NAS 文件为准。

启动后已确认实际文件检查正在推进：证据缓存 20 个文件，已完成 2 个目录的报告，目录错误和文件问题均为 0；进程存活，生产容器 healthy。该计数只代表本次观察时刻，不是最终结果。
