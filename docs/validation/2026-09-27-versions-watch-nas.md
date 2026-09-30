# 版本与后台监听 NAS 部署验收

2026-09-27 13:12:59 UTC 现场验收通过。详细证据见 [JSON](2026-09-27-versions-watch-nas.json)。

## 部署结果

- NAS 镜像：`keeps-server:versions-watch-20260927`，SHA `17950d7cfe475d279a602b8263f343ccc38d63c9bdfc6b7d3c4803d5f8e9b0ed`；Compose 别名 `keeps-server:local`。
- 容器 `keeps-control-plane` running / healthy，catalog schema 3。
- 源码归档 SHA-256：`c05c159eb1d22227794a17c5dcc3c8966f2904623409b5f76cf315a4400552af`。归档逐文件核对与本地源码一致；NAS 原生 amd64 构建成功。本地 Docker VM 空间不足，未清理其他项目镜像。
- 发布目录：`/volume2/docker/keeps/releases/versions-watch-20260927`，保留源码归档、构建日志、临时样本测试日志、部署日志和证据。
- 四个原片挂载全部 `RW=false`，只有 `/myphoto/keeps` 数据挂载可写。未删除、移动或覆盖用户照片。

## 数据保留与回退

停服后使用 SQLite backup API 备份两个数据库，分别通过 `PRAGMA integrity_check`：

`/volume2/myphoto/keeps/backups/before-versions-watch-20260927-060648/`

包含 `control_plane.sqlite`、`jobs.sqlite` 及旧服务源码/Compose 归档。目录名采用 NAS 宿主本地时间；证据时间使用 UTC。

schema 2→3 新增版本表，不回填或重组历史资产。迁移前后对全部 10 张旧业务表按 rowid 顺序读取全部列、使用相同 JSON 序列化后 SHA-256 比对，行数和摘要完全一致，包含 **121,967 个资产、148,516 个路径、819,579 条历史事件**、评分标签快照、隐藏配置及预览声明。新版本表迁移后为空；启动后的正常处理才逐步加入记录。

回退镜像保留为 `keeps-server:before-versions-watch-20260927`（`2003e93c989adc7583833c8719348ea462e5b057b56cc53c8620f04750039c48`）。若需回退，先停服务，同时从上述一致备份恢复两个数据库，再切旧镜像并启动；旧程序不能直接读取 schema 3。恢复会撤回备份时间之后的数据库修改，需先另存当前数据库。不要触碰原片目录。

## NAS 本机临时样本验证

新生产镜像在隔离容器及临时 Docker volume 上通过：文件事件入队、改名后的 metadata-only JPEG 同图归组、相同拍摄元数据不同图像保持分开、默认版本选择与预览、目录移动、源文件外部消失后的默认回退和预览重建、重启恢复、停机期间 sidecar 修改补扫。样本原始字节保持一致，预览位于原片目录之外。删除/移动场景只操作合成临时样本，未用于用户照片。

## 生产现场验证

- 健康接口成功；未授权业务请求返回 401。
- 资产、revision、versions、version-candidates API 可读；已产生版本的样本包含一个默认版本。
- 下载既有 500,737 字节预览并核对 SHA-256 与 API version 一致。
- 425 个原生 inotify 目录监听，观察期间监听警告/错误为 0。
- 旧任务 ID 保留；全量任务累计跳过 606 个已处理文件后，让目录任务优先执行。
- 连续读取确认目录任务 processed 3→6、catalog_versions 4→7、revision 819594→819606，failed=0。队列仍在运行，不代表全库处理结束。

## 数据库整理起点

[部署前审计与实盘重复核验](2026-09-27-catalog-audit.md) 已完成：16 组共享 JPEG 哈希经 32 个实际文件重新核验一致，全部属于单 JPEG 资产与既有双版本资产的子集关系，用户字段无冲突；已记录建议保留的资产 ID。另一版本的视觉关系未重新确认，元数据候选不能批量合并。

本次部署未执行历史资产合并、拆分或全量强制版本回填。已有未变化文件可能被增量扫描跳过，版本证据完整覆盖需要后续受控回填。服务端机制已上线；macOS/iOS 新客户端代码未在本次重新发布到 TestFlight。
