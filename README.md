# Photo Asset Manager

本地优先的 macOS 照片资产管理器原型。

## 当前能力

- 扫描本地目录和 NAS 目录。
- 解析基础 EXIF：拍摄时间、机身、镜头。
- 生成本地缩略图和预览图缓存。
- 用 SHA-256 内容 hash 和元数据指纹识别重复位置。
- 使用 SQLite 建立资产、文件实例、版本、导入批次和操作日志。
- 区分本地工作副本、NAS 权威副本、导入来源和缓存。
- 提供 `Inbox`、`Working`、`Needs Archive`、`Needs Sync`、`Archived`、`Missing Original` 视图。
- 支持按文件名、路径、相机、扩展名、评分、标签、目录和状态筛选。
- 支持评分、精选、标签编辑。
- 支持一键归档到 NAS、同步导出或 sidecar 类变更、记录导出文件。
- 归档和同步时拒绝覆盖目标文件，复制前后校验 hash，并写入操作日志。
- 支持多个本地或 NAS 来源目录，并可停止或恢复追踪单个目录。
- 扫描时会尽量读取嵌入 XMP/EXIF 或同名 `.xmp` sidecar 中的评分。

## 构建

```bash
swift build
```

## 打包 macOS App

```bash
./scripts/package_app.sh
open .build/app/PhotoAssetManager.app
```

应用数据库和缓存默认存放在：

```text
~/Library/Application Support/PhotoAssetManager
```

## 使用建议

第一步先添加一个或多个 NAS 来源目录，让已有权威原片进入索引；第二步扫描本地工作目录。归档到 NAS 前需要先在工具栏设置 NAS 根目录。

停止追踪目录只会让该目录不再参与后续批量扫描，不会删除已入库资产或文件位置。
