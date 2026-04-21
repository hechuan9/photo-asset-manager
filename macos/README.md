# Photo Asset Manager

本地优先的 macOS 照片资产管理器原型。

## 当前能力

- 添加多个照片文件夹，并通过刷新扫描清单。
- 从库外照片文件夹导入到已登记资料库目标；导入只复制并校验，不删除来源照片。
- 解析基础 EXIF：拍摄时间、机身、镜头。
- 可迁移缩略图保存位置；不会为了生成预览而额外创建预览 JPEG。
- 用 SHA-256 内容 hash 和元数据指纹识别重复位置。
- 使用 SQLite 建立资产、文件实例、版本、导入批次和操作日志。
- 区分本地工作副本、NAS 权威副本、导入来源和缓存。
- 支持按文件名、路径、相机、扩展名、评分、标签和目录筛选。
- 支持评分、精选、标签编辑。
- 支持完整文件夹浏览模式：每个文件夹和子文件夹都可选中查看照片，并可在菜单栏切换“仅当前文件夹”或“包含子文件夹”。
- 支持一键归档到 NAS、同步导出或 sidecar 类变更、记录导出文件。
- 归档和同步时拒绝覆盖目标文件，复制前后校验 hash，并写入操作日志。
- 支持多个文件夹；文件夹列表就是扫描清单，可刷新或移除单个文件夹记录。
- 扫描时会尽量读取嵌入 XMP/EXIF 或同名 `.xmp` sidecar 中的评分。
- RAW 和同名 JPG 会尽量归到同一个资产；已有 JPG 会直接作为可浏览图使用。
- RAW 和同名 JPG 可同时作为原片位置存在，但每个资产只保留一个缩略图记录。
- 支持从照片右键菜单把选中资产移入共享回收站；该操作只写入同步 ledger 和本地投影状态，不删除、移动或覆盖磁盘照片。
- 支持通过菜单栏从共享回收站恢复选中资产。
- 新增同步 ledger 基础层：本地评分、精选、标签、共享回收站和归档 receipt 走 append-only 业务事件，SQLite 只作为本机 UI 投影。
- 支持把现有 Mac SQLite 库 bootstrap 成幂等 ledger 初始快照，迁移只表达当前事实，不 replay 历史 SQL。
- 衍生图采用 S3 authoritative / 本地 cache 模型：缩略图和中等预览图作为长期 S3 derivative，原片、RAW 和 sidecar canonical 不进入 S3。

## 构建

```bash
swift build
```

## 打包 macOS App

```bash
./scripts/package_app.sh
open .build/app/PhotoAssetManager.app
```

应用数据库默认存放在：

```text
~/Library/Application Support/PhotoAssetManager
```

## 使用建议

先添加一个或多个照片文件夹，让已有原片进入索引。`/Volumes` 下的文件夹会自动视为 NAS 存储，归档和同步会使用对应的卷根目录。

启动后应用会在后台尝试挂载已登记的 NAS 卷根目录，例如已有来源 `/Volumes/photo/照片` 时会尝试挂载 `smb://chuan_nas.local/photo`。如果 NAS 主机名不同，可设置：

```bash
defaults write PhotoAssetManager nasSMBHost "your-nas-host.local"
```

NAS 挂载未完成时，后台文件状态校验会跳过这一轮，避免把离线 NAS 上的原片误标成缺失。文件状态全量校验有 24 小时启动节流；旧库首次升级会先建立校验水位，避免第一次打开就遍历全库。工具栏里的“校验文件状态”可手动强制刷新。

左侧文件夹区可直接选中文件夹查看照片；菜单栏的“文件夹”菜单可切换只看当前文件夹直属照片，或包含所有子文件夹照片。“全部资产”会退出文件夹浏览并回到全库。

移除文件夹只会改变数据库里的扫描配置，不会删除已入库资产、文件位置或磁盘照片。缩略图位置未设置时，新扫描不会生成额外缩略图；有 JPG 的资产会直接用 JPG 浏览。迁移缩略图只复制、校验并更新数据库路径，不删除旧文件。

删除照片当前不是物理删除操作，不等同于“移除文件夹”或停止追踪。必须先在资产网格右键选择删除，再在确认弹窗中确认；确认后资产进入共享回收站并从默认视图隐藏，磁盘上的照片文件保持原样。第一版不自动执行原片物理清除。

多设备同步基础层使用业务事件 ledger，而不是同步本地 SQL。评分、精选、标签、回收站、导入原片声明、归档请求和原片归档 receipt 都记录为 append-only operation；`assets`、`file_instances`、共享回收站和归档状态是 replay 后的本地投影。云端控制面保存 ledger、设备游标、缩略图/预览和归档 receipt，原片仍以单一 NAS/server 作为 canonical store。

已有 Mac 库迁移到同步架构时，会生成一次 `system:migration` actor 的 bootstrap ledger 快照，包括资产 metadata、标签、文件位置以及已有 thumbnail/preview derivative 指针。bootstrap operation 使用稳定 ID，重复运行必须幂等；如果同一稳定 operation ID 的 payload 变化，迁移会失败并要求人工处理。迁移状态记录在本地 watermark 中，只有 ledger replay 校验通过后才标记完成。

如果希望在接入 remote control-plane 前先手动完成这一步，可在应用里使用“工具 -> 补齐同步 Ledger”或同步状态面板里的“补齐 ledger”。它只会把当前 SQLite 事实补成初始 ledger 快照，不会删除、移动或覆盖任何照片文件；后续配置好自动同步后，历史缩略图也会继续按数据库状态补传。

缩略图和预览图的二进制内容不写入 ledger。ledger 只记录 `DerivativeDeclared` 业务事件和 S3 指针；本地路径只表示 cache 命中，cache miss 时按控制面返回的 derivative metadata 下载到本地 cache。cache 可清理和重建，清理 cache 不写 ledger，也不得触碰原片目录。
