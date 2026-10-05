# 导入内存修复与可选去重

## 已验证原因

原 `FileHandle.read(upToCount:)` 循环缺少块级 autoreleasepool，独立 512 MiB 文件复现峰值常驻内存 545308672 字节；同样循环及时释放临时对象后为 8470528 字节。原小样本功能测试没有覆盖这一内存问题。

## 当前行为

- 默认 `deduplicate=false`。Mac 只枚举文件并保存名称、大小、修改时间，不预读源文件内容；按来源父目录和 stem 保持 RAW/HEIF/XMP 配对，目标平铺，同名冲突整组改名。
- 上传前后以及恢复批次时检查源文件大小与修改时间。NAS 流式接收、检查长度并持久化接收摘要；提供源 SHA256 时额外比较。
- 可选去重仅检查本次目标目录已有文件的字节内容，不进行全库查重、相似图片比较或来源批次内部查重。只在找到相同原片且重叠配套内容均相同时跳过已有项，补齐缺失项；配套内容冲突时整组另名。
- 可选源哈希按 1 MiB 分块，并在每块结束时释放临时对象；NAS 去重读取文件不持有任务数据库锁。提交前重新核验跳过文件。

## 验证

- Rust HTTP：23 项通过；导入相关单元/HTTP筛选：9 项通过。
- Swift 共享包：33 项通过；Mac 导入：9 项通过，含默认省略摘要、可选开启、恢复时源文件变化失败。
- `python3 macos/scripts/test_import_memory.py`：实际 ImportSource 处理 2 GiB 稀疏文件，默认峰值常驻内存 7290880 字节，可选哈希 9617408 字节。回归阈值为 128 MiB。
- 使用实际 ImportSource、ImportStore、KeepsClient 和 URLSession，对本机 HTTP 测试接收器完整上传 2 GiB 文件，峰值常驻内存 32489472 字节。该数字是独立导入进程，不含应用图片网格缓存。
- NAS 隔离原生运行时：真实 HEIC 无源摘要上传、恢复同一 manifest、提交并索引完成；默认同内容另名；开启去重跳过目标已有文件，全部通过。测试使用独立 originals/state，未导入正式照片库。

## 部署

NAS 原生套件已升级为 `KeepsNativeProbe 0.1.0-0009`；Mac 已安装 `/Applications/Keeps.app` 并通过签名验证。升级前后正式照片 124871、回收站 2，配置文件及令牌哈希不变。正式接口与 3 张缩略图读取成功，缓存 ready 124871、pending/processing/failed 均为 0。

NAS 证据位于 `/volume2/docker/keeps/releases/import-light-20261005/`：`upgrade.json`、`validation/result.json` 和 `KeepsNativeProbe-0.1.0-0009.spk`。
