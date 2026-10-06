# 高位深 HEIF 缩略图诊断

## 已确认的原因

用户报告的花屏文件实际是 `/volume2/photo/照片/2026/西雅图金松/B0018360.HEIC`，而非同名 3FR。相机为 Hasselblad X2D II 100C，原片 Software 为 `1.3.16.2`，没有 Keeps 生成标记。Apple ImageIO 解码原 HEIC 正常；同名 3FR 的正式缩略图也正常。

NAS libheif 1.23.4 的 `heif-convert` 输出 TIFF，再交 ImageMagick 缩小，可稳定复现彩色噪声。改为 PNG 中间格式后，同一原片显示正常。PNG 设置压缩级别 0，避免对一次性中间文件做耗时压缩。没有改动原片。

上游 `heifio/encoder_tiff.cc` 将高位深交错色彩缓冲直接传入 TIFF scanline writer，`encoder_tiff.h` 要求大端高位深色彩。诊断确认 TIFF 输出路径有问题；尚未对上游具体端序/位深缺陷做补丁。Keeps 使用 PNG 中间格式绕开已复现的损坏路径，不增加多套解码 fallback。

## 证据

- 原片 SHA-256、catalog content hash、cache source hash、standard version 一致：`0ba150e1cc58b31db995911f2cf7c62db8549692224e0438391822e1b18bfcbb`。
- 坏缩略图 SHA-256 与 objectRef/version 一致：`f6fe3cd8038289cf3d63fa6e2bec6fa1043936f36c908e1d345932d70ee92dd2`。
- 指定 HEIC asset：`37056bc3-6b8e-40a7-a044-a49e12defb25`。
- 本地诊断图在 `.build/3fr-diagnosis/`：`b360-apple.png`、`b360-tiff-small.png`、`b360-png-small.png`、`b360-heic-cached.png`、`b360-raw-cached.png`。
- NAS 诊断输出在 `/volume2/docker/keeps/releases/incremental-20261005/3fr-diagnosis/`。
- 100 MP 无压缩 PNG 中间文件为 612,390,859 bytes；正式 renderer 的资源限制验证应以随后构建运行记录为准。

## 回归验证

新增 Linux 媒体运行时测试 `media::tests::high_bit_depth_heif_preserves_colors`：生成明确 10 bit 的 HEIC，渲染缩略图并比较 RGB 均值，同时验证原文件哈希未变。

NAS 同构命令对比：目标 RGB 为 0.85 / 0.20 / 0.10；旧 TIFF 路径得到 0.337 / 0.331 / 0.176，新 PNG 路径得到 0.847 / 0.196 / 0.094。新结果通过各通道误差小于 0.06 的断言。

本地媒体测试 6 项通过，2 项依赖 Linux 媒体运行时的测试按约定忽略。Linux runtime 与指定原片完整 `keeps-render` 的后续验证见下节。

## 修复范围

只改 MediaProcessor 的 HEIF 解码中间文件，NAS 与 Linux renderer 共用。不提高全局 cache spec，不触发全库重编码。只读 catalog 当时筛出 X2D II 100C HEIC 候选 1514 项，其中指定目录 105 项；这些数量是候选范围，不代表已逐张确认损坏。已有坏缓存需要定点重建才能消除；Linux worker 当时不可达，不能声称线上 worker 已更新。

## 完整渲染与定点发布

NAS 构建修正后的 keeps-render 成功。新增 `high_bit_depth_heif_preserves_colors` Linux 回归实际执行通过；同一 100MP 原片经完整 renderer 在默认子进程限制下 34.996 秒完成，输出 512×384、37,315 bytes，原片 SHA256 未变。

仅修复 asset `37056bc3-6b8e-40a7-a044-a49e12defb25`：保存原 media_cache 行与旧缩略图后，为该资产建立单个维护 worker 租约，再经过既有 thumbnail 上传和 complete API 发布，复用服务端源哈希检查、派生对象写入、版本更新和 GC。未直接覆盖原片或旧缓存，未全局修改 cache spec、未全库重排队。新缩略图 SHA256 为 `c8fe786a856b19d905cc7e8e8b2c18176627ac81f7b1d76a8c4e32953d374719`。备份、验证与发布结果在 NAS `releases/incremental-20261005/3fr-diagnosis/`。

既有 builder 命令附带 watcher 测试，其中目录快速重建测试此次等待新事件超时（4 PASS / 1 FAIL）；该模块没有在本修复中修改。没有重复执行来掩盖失败，不把本次验证称为全套通过。

Linux worker SSH 返回 No route to host，尚不能更新其运行镜像；此处仅确认修复代码、NAS 独立 renderer 与上述单张缓存已处理。其余候选照片尚未逐张验证或重建。

正式 API 回读指定资产并下载缩略图 HTTP 200，返回字节 SHA256 与新缩略图一致，确认不是只改本地诊断图。
