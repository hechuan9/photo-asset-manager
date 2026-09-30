# 两张 JPEG XL DNG 的 Photoshop 标准图

用户选择直接用 Photoshop 导出标准图，不集成额外 DNG 解码器。

## 输出

Photoshop 2026 / Camera Raw 18.6 打开两个 DNG 副本，保留嵌入的 Lightroom 调整及裁剪，导出 16-bit Display P3 TIFF。Photoshop 保存列表无 HEIF，因此用 macOS sips 将 TIFF 转为质量 90 的 HEIC，保留 Display P3，未缩小尺寸。

| 原文件 | 同目录标准图 | 尺寸 | 字节 |
| --- | --- | --- | --- |
| `/volume2/photo/照片/2023/西藏/DSC04420-Pano.dng` | `DSC04420-Pano.heic` | 17533 × 3876 | 29257406 |
| `/volume2/photo/照片/2025/Dolemites/DSC07423-Pano.dng` | `DSC07423-Pano.heic` | 22917 × 3946 | 46001966 |

HEIC 内嵌原资产根 UUID，Software/CreatorTool 为 Keeps，Instructions 记录 Photoshop 渲染来源。输出采用独占创建，没有覆盖已有用户文件。两个缩小的 HEIC 解码预览已人工查看，画面正常。

## 原文件与数据库

原 DNG 仅补写根 UUID 和 XMP-photoshop:Instructions：不解码原 DNG，显示及缩略图使用对应 HEIF。写入前后逐一比较 TIFF 各 IFD 的 strip/tile/JPEG 图像数据块 SHA-256，全部一致。未删除、移动或替换原照片。

HEIC 作为原资产的版本登记，随后通过 default-version API 固定为用户选择的默认版本。沿用现有默认版本及标准图选择机制，正常处理输入为 HEIC；没有新增通用 decode-disabled 字段，Instructions 是可读说明而不是解码器识别的开关。若用户将默认版本改回 DNG 且移走 HEIC，现有系统仍可能再次尝试解码。

本次未修改或部署生产代码、未接入 DNGLab/Adobe SDK；NAS/Linux 镜像和并发不变。仅将两个未完成的缓存任务更新为 HEIC 来源并重新排队。

## 证据

NAS `/volume2/docker/keeps/releases/dng-jxl-20260929/`：

- `fixtures.json` 和两个 DNG 副本：修改 metadata 前的原文件备份。
- `photoshop-published.json`：标准图路径、尺寸、根 ID、SHA-256。
- `original-annotations.json`：原片说明及逐数据块不变证据。
- `before-photoshop-register.sqlite`：登记前一致性数据库备份。
- `photoshop-registration.json`：版本登记。
- `photoshop-final-state.json`：最新默认版本、缓存状态和实际 worker 输入。

## 最终结果

2026-09-30T02:24:49Z，两资产均 `ready`、`last_error=null`；最新远端任务 `input_path` 为对应 HEIC、`completed=1`。缩略图分别为 512×114 / 19530 字节、512×88 / 15747 字节。标准图路径均为上表同名 HEIC，原资产 UUID 未变。监控 nas-2 已追加解决记录，不再重复报告这两项历史 DNG 错误。
