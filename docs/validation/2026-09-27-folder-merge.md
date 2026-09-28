# 逐目录批量整理脚本验收

实现入口：[merge_tracked_folders.py](../../scripts/merge_tracked_folders.py)，运行说明：[按文件夹整理](../folder-merge.md)。

## 本地验证

- Python 合并脚本 16 项测试通过：追踪配置与挂载交集、父子目录及同前缀目录隔离、跨目录资产桥接排除、字段与默认冲突、元数据候选不合并、双库原子回滚、原片不变、默认版本与扫描状态同步、计划失效、XMP-only 修改、缓存续跑、缺失的高优先级版本排除、幂等。
- 既有路径迁移测试 1 项通过。
- Rust lib 47 项通过；依赖 Linux 媒体工具的既有 roundtrip 在 macOS 按原标记忽略。HTTP 9 项与新媒体检查 CLI 1 项通过。
- Rust fmt、Clippy all-targets `-D warnings`、git diff whitespace、架构入口路径检查通过。

## NAS 真实媒体隔离验证

2026-09-27 13:41:47 UTC，新镜像 `keeps-server:folder-merge-20260927` 原生 amd64 构建成功，SHA `7855aff063257f09f22bd797e4994c6232aa6bd2d2bdd4c22b3f1c64424a48cc`。未替换生产容器。

在发布目录临时样本中运行真实 ImageMagick/ExifTool 和 keeps-inspect，使用 NAS Python 3.8 与真实 SQLite 执行 plan、apply 和重复 apply：

- 父目录 3 个 JPEG：两个图像内容相同但元数据不同，另一个拍摄信息相同但画面不同。
- 子目录 2 个相同 JPEG，与父目录的图像内容也相同。
- 只形成 2 个目录内合并组，资产 5→3，全部 5 条原片路径保留。
- 不同画面不合并；父子目录的同图不合并。
- 元数据表明已编辑的成片作为默认，默认 priority=3。
- 5 个原片 SHA-256 在应用后完全一致，重复 apply 返回已完成。

同一新镜像另外通过 NAS 原生事件集成：新增事件入队、JPEG 版本归组、同目录改名保留关联、子目录同图独立、默认缺失回退与预览重建、重启、离线 sidecar 修改补扫以及原片不变。

机器证据：[NAS 样本 JSON](2026-09-27-folder-merge-nas.json)。样本容器与临时文件已由测试清理，没有使用用户原片目录。检查镜像、脚本及测试副本位于 NAS `/volume2/docker/keeps/releases/folder-merge-20260927`。

## 生产边界

本轮没有对生产数据库执行全库预演或 apply，没有停服或替换当前生产服务。新服务端“仅同直接父目录匹配”的修复随源码与检查镜像交付；全库应用前须先按部署流程升级该服务端，避免旧服务继续全库归组。

脚本只自动处理精确内容证据。RAW/成片及编辑后的视觉相似版本仍是候选，不代表已完成全部人眼意义的照片合并。旧 ledger 原封不动保存；合并状态恢复需要双库备份和维护目录，单独回放旧 ledger 不足以恢复离线整理结果。
