# 来源设置与扫描任务展示

来源配置移至 macOS 设置的“来源”页，保留添加追踪、停止追踪和手动扫描。侧栏“任务追踪”独立显示扫描进度、当前文件和失败重试，运行任务优先显示，pending 改为“排队中”并解释 NAS 单扫描 worker 调度。

## 实时诊断

只读查询当前 SPK 的 `/volume2/@appdata/KeepsNativeProbe/production-state/db/jobs.sqlite`，两次间隔约 10.5 秒：

- 238 pending、1 running、207 completed。
- 正在运行 `/volume2/photo`：processed 从 5367 到 5376，skipped 从 9822 到 9903，failed=0，当前文件变化。
- 238 个 pending 的 available_at 均已到期，error 均为空；正在等待单 worker。
- 排队中包括 214 个路径同步、23 个元数据刷新和 1 个普通扫描；运行根扫描的 rerun=1。
- 相同目录去重，但祖先和后代扫描不合并，产生大量重叠范围。此次没有取消任务或改调度策略。
- 原接口按创建时间倒序取最近 100 条，可能完全漏掉旧的运行任务；现已在 SQL LIMIT 前按运行、排队、其他状态排序，组内仍按创建时间倒序。

## 客户端验证与发布边界

`swift test --package-path macos`：37 项通过；本地 app 打包和签名校验通过。本轮生成 `macos/.build/app/Keeps.app`，未替换正在运行的 `/Applications/Keeps.app`，未发布 TestFlight。服务端查询顺序变更需要随下一次 NAS 服务更新生效；本轮未重启 NAS 或改写线上队列。

服务端新增回归测试 `library_job_limit_keeps_old_running_job_before_newer_queue_and_history`：1 PASS，覆盖小 LIMIT 下的运行任务保留、状态分组、同组顺序及图库隔离；`cargo fmt --check` 与 `git diff --check` 通过。

后续用户授权部署已完成：参见 [0010 服务端更新](2026-10-05-task-settings-deployment.md) 与 [Mac 更新](2026-10-05-task-app-update.md)。上述未部署边界仅描述首次实现时点。
