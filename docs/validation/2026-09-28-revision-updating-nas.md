# NAS 目录修订与修改窗口部署验收

部署完成：2026-09-28T22:24:31Z。生产容器 `keeps-control-plane` 健康，镜像 `keeps-server:revision-updating-20260928`（`sha256:9bd99b3bc73ce19769626d13425c74dc9d040e274d2a7d87206b022649f33247`），catalog 从 schema 3 升级到 5。

## 数据与运行验证

- 停服后使用 SQLite backup API 备份 catalog、jobs，两库完整性检查通过。备份目录：`/volume2/myphoto/keeps/backups/before-revision-updating-20260928-222142`，目录权限 0700，备份及配置文件权限 0600。
- 迁移前后逐行计算 13 张原有业务表的摘要，全部一致；保留 123,177 个资产和 825,088 条历史事件。新增 411 个目录版本节点。
- 四个原片目录保持只读挂载，2283 仍仅绑定 127.0.0.1，既有 HTTPS 地址和配置保留。
- 内部 HTTP 和既有 HTTPS 入口的健康、鉴权、revision/isUpdating、资产列表通过；真实预览下载 334,544 字节，SHA-256 一致，TLS 正常校验。
- 11 个未完成任务 ID 全部保留；20 秒观察期间后台任务推进 66 项。任务尚未全部结束，线上 isUpdating=true 符合当前修改状态。
- NAS 本机 20 次版本请求中位数 1.532 ms，最大 3.297 ms；查询计划使用主键索引。这是现场样本，不是负载性能保证。
- NAS 隔离容器通过迁移、入库、预览、无变化重扫、失败重试、SIGKILL 恢复及原片摘要不变测试。仅在隔离库中加速活动时间，重启后静默窗口正确收尾，版本从 6 增至 10，isUpdating=false。

## 发布与回退

发布目录：`/volume2/docker/keeps/releases/revision-updating-20260928`，包含源码归档校验、构建、测试、部署日志和完整证据。当前服务器源码已同步到 NAS Compose 构建目录，旧源码保存为发布目录中的 `before-server-source.tar.gz`。

回退镜像：`sha256:cf58d25d23ae65ce7a79cc9222a86e43ba0d929b9378ed8ed67d614e0c51a14b`，保留标签 `keeps-server:before-revision-updating-20260928`。旧服务器不支持 schema 5，不能只切镜像；如需回退，先停服并额外备份升级后的两个数据库，再恢复本次升级前的 catalog、jobs 和私有 Compose 配置，切回旧镜像并重建验证。恢复旧库会丢失升级后新增的数据库变化，应先核对差异；原片不受影响。

[完整机器可读证据](2026-09-28-revision-updating-nas.json)。HTTPS 检查从 NAS 发起，未将其表述为蜂窝网络独立验收。本次未发布客户端。
