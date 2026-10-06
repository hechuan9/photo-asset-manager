# 2026-10-05 NAS 默认处理

日常扫描和媒体处理默认由 NAS 承担，Linux 仅用于用户明确安排的一次性大规模处理。

## 实现与上线

- `KEEPS_REMOTE_WORKER_ENABLED` 未设置、为 `0` 或 `false` 时，远端 claim 返回 `{"enabled":false,"task":null}`，不领取或修改任务。显式 `1` / `true` 才派发；已有任务的心跳、上传、完成接口仍可用于正常收尾。
- NAS 示例配置及生产配置均为本地编码 `1`、远端派发 `0`。Linux Compose 使用显式 `bulk` profile 和 `restart: "no"`，结束后关闭 NAS 远端开关并停止 worker。
- 原生套件升级至 `0.1.0-0014`，从含 0013 稳定分页和 HEIC PNG 中间格式修复的当前源码构建，替换 keeps-server、keeps-render 和 keeps-inspect。
- 升级时暂停西雅图金松修复进程组，使用 SQLite backup API 保存两份数据库。在线备份受持续入库写入影响，先停止服务后完成备份，再升级；升级后恢复批次进程。
- 只回收 `linux-*` 的 4 个未完成租约，将对应 processing 项放回 pending，不消耗失败重试次数。未取消批次维护租约，未全库重排缓存。
- 已安装 keeps-server SHA256：`bdd94900b5df213eb3417b1dfe032acfa17a0b08c60214cc0f0270b6c59f4e13`，与构建产物一致。

## 验证

- Linux release 构建、10 项远端任务测试、4 项 SPK 测试、Rust 格式及 diff 检查通过。
- DSM 安装成功，健康接口 `ok`；token、服务地址和原片根配置指纹不变。
- 线上远端 claim 返回禁用状态，本地编码为 true，活跃 Linux 租约为 0。回读发现 1 个无远端租约的本地 processing 项，确认 NAS 已实际领取任务。仅一个原生服务进程，UID 164747。
- 新安装渲染器生成 10 位 HEIC 测试图的缩略图，RGB 均值 `[0.847059, 0.196078, 0.0941176]`，符合预期 `[0.85, 0.2, 0.1]` 的每通道 0.06 容差，测试源哈希不变。
- 两份数据库备份 `quick_check=ok`；已修复的 B0018360.HEIC 缩略图下载 HTTP 200，SHA256 与修复结果一致。旧 Docker 服务保持 exited。
- 批次恢复后的检查点为 41/1501，失败 0，状态 running。
- 正式资产数在持续入库中从 125848 增至 125868，精选 0、回收站 2；此变化不是本次修复删除或移动照片。

## 运行边界

切换前 NAS 观察到 `linux-1` 租约持续续期，证明 worker 当时仍在运行。其注册 SSH 目标从本机不可达，因此未修改该主机现有容器的 restart policy，也未确认容器停止；生产 NAS 已拒绝新领取并使旧租约失效，Linux 无法继续发布这些过期任务。仓库中的手动启用和不自动重启设置适用于下次显式部署。

西雅图金松整批修复仍在 NAS 运行，完成情况以维护 journal/status 为准，不把局部检查点称为整批完成。未修改原片像素、移动或删除原片。

证据保存在 NAS `/volume2/docker/keeps/releases/nas-default-20261005/`：源码、安装包、数据库备份、回收租约清单、`upgrade.json`、`verification.json`、`media-validation.json`。0013 安装包和旧状态备份保留，回退时应先保存最新数据库，不直接用旧备份覆盖后续入库数据。

此前分支 CI 的 Python 迁移测试因未构建 `server/target/debug/keeps-server` 失败；该问题与此开关无关，本次不声明全套 CI 通过。
