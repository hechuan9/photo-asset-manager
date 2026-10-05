# 2026-10-05 缓存收尾修复

用户授权修复残余失败、路径索引和视频首帧缓存；照片原始文件不得删除、移动、覆盖或改变图像数据。nas-2 继续保持用户要求的暂停状态。

## 机制与修复

- 远端并发领取可能同时清理同一个过期任务目录，第二次删除返回 ENOENT。复用现有 COMMIT 锁串行清理。
- 7 个大视频原先完整下载后才取首帧，输入超过每任务 4 GiB 下载限额或租约超时。认证源接口支持 HTTP Range，Linux 用仅监听 loopback 的认证代理供 ffmpeg 按需读首帧；凭据不进入 ffmpeg 参数，保持 TLS 校验及禁止重定向。领取及发布时仍完整校验来源 SHA。
- 8 个扩展名为 ARW 的来源实际为 JPEG。解码分支识别 JPEG 文件头并跳过 LibRaw；实测发现 ImageMagick 仍按扩展名选择 DNG coder，已补上显式 JPEG coder，原文件保持不变。
- 婚礼迁移遗留 315 条旧目录索引。真实新路径和内容 SHA 逐条匹配后同步索引，保留资产 ID；5 个失败 JPEG 已重试成功。
- 2 个资产来源仅存在 NAS 回收站，使用现有 trash API退出活动集合，保留历史及回收站文件。缓存盘点、状态统计和领取排除已回收资产，不伪造 ready。
- 4 个 DNG 使用 Photoshop 2026 / Camera Raw 18.7 保留嵌入调整和裁剪，导出 16-bit Display P3 TIFF，再以 sips quality 90 转为 HEIC。原片只允许身份和说明 metadata 更新，需验证图像数据块不变。
- 1 个 HEIF 在 Linux libheif 解码尺寸异常，Mac ImageIO 完整解码为 1616×1080，已人工查看画面，重新编码为同尺寸 HEIC。

## 验证与生产证据

Rust remote_worker 9 项、cache_pipeline 10 项及 JPEG 文件头回归测试通过；Python worker 5 项通过。统一部署源来自生产 identity release，加上本次目标文件补丁，不包含工作区未部署的 Mac 导入功能。

NAS `releases/path-repair-20261005/` 保存路径修复前数据库备份、`path-verification.json`、`full-path-verification.json`、`trash-verification.json`。315 条旧路径引用为 0，5 个 JPEG 缓存 ready / error=null。

最终生产镜像 config ID：`sha256:109cec315137604c802b53e75c385a321b11218925223e97b49cedd96183c6f0`。二次维护 Linux drain 14:15:54Z 至 resume 14:17:51Z；NAS 停服 14:16:45Z 至 healthy 14:17:31Z。首次维护从 Linux 14:03:17Z 排空开始，NAS 14:07:07Z healthy，Linux约14:07:20Z恢复；跨这些窗口不得计算吞吐。

身份回填待优先路径扫描结束；2026-10-05 14:02 UTC 香港扫描 processed=1602，updated_at 实时，无停止推进证据。不得清空任务队列或把等待当成失败。

## Photoshop 与 HEIF 最终验收

2026-10-05 14:12:16 UTC，5 项全部 ready / last_error=null，Linux 实际输入均为对应 HEIC 且任务 completed=1，默认版本 user_selected=1。五个原 DNG（含 DSC01814-Pano-2）逐 TIFF 图像数据块 SHA 不变，原 HEIF 全文件 SHA 不变；5 个缩略图存在、字节数、解码尺寸全部 PASS。合法根 UUID 与资产 ID 保留，标准图独占创建，无覆盖。证据 NAS `/volume2/docker/keeps/releases/dng-photoshop-20261005/` 的 `photoshop-final-state.json`、`photoshop-published.json`、`original-annotations.json` 与 `photoshop-registration.json`。

首次视频部署实测：NAS claim 完整 SHA 校验超过反向代理超时，HTTP 504；领取后的租约需要安全恢复，不能据此认定 Range 首帧功能已经通过生产验收。仅 Keeps HTTPS 反向代理规则 read/send timeout 从 60 改为 1800 秒，connect 保留 60 秒；持久化 ReverseProxy.json 精确备份，原生 nginx -t 和 reload 通过。Worker claim/complete 等待时间相应为 1800 秒，HTTPS/认证与完整 SHA 校验保留，无新增监听端口。最终15项目标全部实际成功。

## JPEG 与视频生产验证

14:21:14 UTC，8 个 JPEG 内容的 ARW 全部 ready/error=null，8 个源 SHA 全部不变。旧诊断文件的 asset ID 已过时，使用当前 catalog_paths 的相同路径与 SHA 映射到现有身份 ID 后定点重试；映射保存在 `arw-current.json`。

视频首三项实际读取 12,320,768 / 10,223,616 / 11,534,336 字节并成功 ready；最终7项视频全ready，无新增错误。Linux 空闲 CPU/34.79MiB，正在等待 NAS 完整源 SHA 校验和发布，不能据此认定 worker 停滞。两容器 restart policy=unless-stopped，Linux Docker enabled，worker 主动 HTTPS 连接 NAS；本机工作目录与凭据挂载，无 Mac 依赖。

## 最终全库状态

2026-10-05 14:26:17 UTC（美东10:26:17），活动缓存 **124841 / 124841 ready（100%）**，pendingInventory / pending / processing / failed 均0，errors为空；GC pending/failed均0。身份回填 ready=129282、pending=5285、failed=0、errors为空，仍等待优先路径扫描，不能将缓存完成表述为身份回填也完成。身份任务计数为登记文件数，不是活动资产分母。

7视频+8JPEG在14:25:46 UTC定点验收全部ready/error=null，最后视频14:25:25 UTC完成。7视频总源50,512,979,819字节，Linux实际读取81,526,784字节（约77.75MiB，0.1614%）；源全量SHA校验仍由NAS执行。证据 NAS `/volume2/docker/keeps/releases/video-range-20261005/retry-verification.json`，包含实际Range处理与JPEG源不变校验。

NAS healthy/restart=0/OOM=false，CPU65.45%（双核上限200%）、内存156MiB/2GiB，磁盘可用33,396,697,460,736字节（33.40TB）。Linux14:26:17 UTC最终检查restart/OOM=0、memory.events全部0、ERROR=0、磁盘可用约441GB，CPU0%、内存29.75MiB/24GiB；编码并发16、16CPU配额、128GiB临时预算与每进程4GiB地址限制保留。两端长期运行不依赖Mac，nas-2保持PAUSED。

保留边界：原片/RAW/旁车、原HEIF、NAS回收站、修复前数据库/原片备份与发布证据均保留；本次没有删除、移动或覆盖用户照片。缓存GC仅处理Keeps派生状态，不清理照片或myphoto回退副本。

Linux最终证据：`/home/hechuan/keeps-worker/releases/video-range-20261005/final-worker-verification.json` 与 `worker-deployment-final.json`；实现细节及测试见 `2026-10-05-video-range.md`。
