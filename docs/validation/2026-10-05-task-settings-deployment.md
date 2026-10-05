# 2026-10-05 任务展示服务端部署

用户明确授权重新部署服务端并更新本机 App。本轮原生 SPK 从 0009 升级至 **0.1.0-0010**。

## 变更与构建

- 上传的 server 源码与线上 0009 构建源逐文件比较，仅 `src/jobs.rs` 有变化：任务列表在 LIMIT 前优先返回 running、pending，保留同组原排序；实际扫描调度不变。
- NAS 复用已有 Rust 构建依赖，release 构建通过；沿用 0009 媒体运行时，仅替换服务程序。
- 套件测试 4 项通过。DSM 不支持 Docker CPU CFS 配额，构建改用已支持的 CPU 0、1 亲和性；Docker 仅承担构建。

## 升级与备份

NAS 目录 `/volume2/docker/keeps/releases/task-settings-20261005/` 保存源码、构建脚本、0010 SPK、`preflight.json`、`upgrade.json`、`verification.json`。

停止原生套件后，以 Btrfs reflink 复制 `production-state/db` 至上述目录的 `db-before`；随后使用 synopkg 安装并启动 0010。0009 回退包保留于 `releases/import-light-20261005/KeepsNativeProbe-0.1.0-0009.spk`。未恢复旧数据库，也未启动旧 Docker。

- 已安装 INFO 回读为 0.1.0-0010；程序 SHA256 与构建产物一致：`95ea27bf3667bc159441fb899547a65389eaedd62fc96e59e963bb914a2365d6`。
- access-token、server-url、original-root、server-overrides 升级前后内容指纹完全一致。
- 健康检查 ok，照片计数升级前后均为正常 124871、回收站 2、精选 0。
- 任务接口返回 100 条，首条为 `/volume2/photo` running；扫描重新领取后重置本轮计数，10 秒间隔观察 skipped 从 1065 增至 1355、failed=0，后台继续推进。
- 3 张真实缩略图均 HTTP 200，响应 17822、17531、10794 字节。
- 仅一个原生服务进程，PID 30544，UID 164747；旧 keeps-control-plane 容器保持 exited。

Mac 已更新并正常启动，设置来源与任务追踪分离；部署后实际窗口显示全根扫描置顶、其他任务排队。详见 [Mac 更新记录](2026-10-05-task-app-update.md)。照片原片未因本次部署删除、移动或覆盖；未发布 TestFlight。
