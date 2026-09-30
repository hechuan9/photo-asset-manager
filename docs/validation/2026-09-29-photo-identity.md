# 照片根身份 metadata 与 NAS 回填

## 约定与边界

用户明确允许修改原片 metadata；不得改变图像像素或 RAW 成像数据，不删除、移动或以其他照片替换原始文件。

- 字段为 XMP `xmpMM:OriginalDocumentID`，写入值 `xmp.did:<UUID>`。
- 合法非空 UUID 直接信任，不用文件哈希认证根身份。缺少时沿用已有资产 UUID；新资产生成 UUID。
- 支持的格式直接内嵌。ExifTool 不支持写入的 3FR 等格式使用 `原文件名.扩展名.xmp`，保留已有旁车字段。
- 标准 HEIC 在 NAS 发布前写入相同根 ID；文件名沿用主文件名，不加哈希。
- schema 9 的根映射和别名支持同根多个版本/位置。移动路径保留资产；文件字节哈希继续用于具体版本和传输校验。
- 历史回填由 NAS 自动执行，每轮最多 20 个，轮后至少 60 秒；路径同步优先，远端 processing 资产跳过。文件写入产生监听事件时可能提前结束当前轮以先核对路径。
- metadata-only 写入同步更新数据库版本、路径、默认图、标准描述及已完成远端任务，保留已生成缩略图。没有重新引入 ledger。
- `cache-status.identityBackfill` 返回 pending/ready/failed/missing 与错误。missing 是旧路径不存在的记录，不触发删除。

## 验证

- 本地 Rust 测试：93 个 lib、17 个 HTTP、1 个 CLI 通过；2 个依赖媒体工具的测试保持 ignored。
- `cargo clippy --all-targets -- -D warnings` 通过。
- Linux 隔离真实媒体验证：ARW、JPEG、HEIF 写入前后解码像素完全相同；3FR 原始字节不变且 XMP 身份正确；标准图继承根 ID；目录移动保留资产 ID；远端完成接口可重复提交。
- 证据：Linux `/home/hechuan/keeps-worker/releases/identity-20260929/smoke/20260929-203939-a451b9/report.json`。
- schema 8 → 9 的真实旧库回填验证及最终部署状态见同 release 的 `migration.json`、NAS `deployment.json`，仅 PASS 为最终证据。

## 部署状态

- NAS 镜像 `sha256:5f3cc37084d36c482cfffdb4105b9f18ccffb79fd83bf8a21e20074122eef503`，schema 9，健康，双核/2GiB，仍仅协调远端编码。
- 迁移前后资产数均为 91,838，根映射 91,838；Linux 保留原镜像、16 并发。
- 维护：00:51:25Z 开始排空，00:52:29Z NAS 停服，00:54:57Z healthy，00:55:11Z Linux 恢复。该窗口不得用于吞吐/ETA。生产最初被显式迁移保护阻止启动，随后运行 `keeps-server migrate` 并重新创建容器；最终 deployment.json PASS 为准。
- 真实旧库测试：5 个文件身份 ready，4 张缩略图内容及引用保持不变，4 项已完成远端任务可幂等重提。
- 00:57:33Z 线上抽样：3 张 RAW 已有旁车 UUID 被规范化并直接信任，3 张新 HEIC 已内嵌根 UUID，全部与数据库一致。
- 最新观测 2026-09-30T00:57:56Z：缓存 {'failed': 0, 'pending': 81933, 'processing': 16, 'ready': 9889}；身份 {'batchLimit': 20, 'errors': [], 'failed': 0, 'pending': 5683, 'ready': 4}。
- 身份 pending/ready 是 jobs 已登记文件的计数，不等于全库资产总量；目录扫描继续发现文件，计数会增长。当前路径同步积压优先，历史补写已启用但尚未完成全库。
- Linux 恢复后 67 项成功，0 新错误、0 OOM；NAS 新容器 restart=0/OOM=false。部署前 DNG 解码错误仍为历史状态，未在本次扩展范围内改动解码器。
- 自动化 nas-2 已更新，每 15 分钟报告身份计数与原有处理状态；维护后重新建立速率基线。
- 数据库备份、回填验证样本与暂存均保留，不自动清理。
