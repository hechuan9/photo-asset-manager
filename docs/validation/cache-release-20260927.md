# 图片缓存发布验证（2026-09-27 UTC）

## 发布范围

macOS 与 iOS/iPadOS 共用预览加载器：每个应用 5 GiB 磁盘缓存，超限 LRU 回收到 4 GiB，无固定 TTL；内存成本目标分别 256/64 MiB，按显示尺寸下采样。缓存按服务器、资料库、资产及预览版本隔离，同版本不受签名 URL 变化影响。并发下载合并；系统清除、损坏缓存可重新获取；磁盘满时保留已下载图片的显示并记录错误。

服务端正确验签但过期的下载令牌返回 `403 / preview_token_expired`，客户端仅对此刷新并重试一次。NAS 预览与原片不参与客户端 LRU。

## 验证

- KeepsAPI：16 项测试通过，覆盖版本/命名空间隔离、并发请求合并、重启后磁盘复用、换链、过期重试一次、错误分类、LRU、文件/目录丢失、损坏缓存、磁盘满与其他写入错误。
- macOS：23 项测试通过。
- 两端 Release 签名归档成功；Mac 包含 arm64/x86_64，iOS 为 arm64。
- 两端归档代码签名验证通过；Mac 沙盒与出站网络权限保持开启。
- macOS 发布门禁与 Gitleaks 通过。
- 本机以当前 KeepsAPI 直接下载真实 NAS 预览：1,697,508 字节，下采样为 170×256。新缓存实例使用不可达下载 URL，仍从磁盘成功解码为 342×512；仅一份磁盘文件，容量设置为 5,368,709,120 字节。探针不改业务数据，临时缓存退出后清理。

## 分发

版本为 macOS 0.3.0（4）、iOS 0.3.0（2）。API Key 的云端分发签名权限不足；归档可用，但凭该 Key 导出失败。随后使用现有 Xcode 登录账号成功完成分发签名；两端 xcodebuild 均返回 `Upload succeeded / EXPORT SUCCEEDED`。Apple API 回读确认两端均为 `VALID / IN_BETA_TESTING`，均已关联“Keeps 内部测试”组。

原始日志、App Store Connect 状态 JSON、凭据及签名 URL 不提交到仓库。共享加载器真实网络验证不替代 TestFlight 真机安装与界面验收。

- macos 构建 4：`19b01ddf-76e6-4250-af66-47dbd4b56f83`，上传时间 2026-09-26T17:52:41-07:00。

- ios 构建 2：`55b13822-2ea8-4327-8328-952f121f6629`，上传时间 2026-09-26T17:52:36-07:00。

## 发布中发现并修复的问题

首次真实过期链接的客户端验证暴露刷新响应契约缺口：API 只返回 downloadURL 和嵌套 derivative，而新加载器需要同资产预览一致的顶层 width、height、version。服务端已补齐三个字段，保留既有 derivative 字段，并在真实 HTTP 路由回归中逐项与资产 preview 比较。不能用此前仅验证 403 状态码代替换链成功。

首次 CI 同时发现 Rust 格式差异与 CI 旧版 Swift 对跨 actor 默认写入闭包的 Sendable 检查；已执行 rustfmt 并显式标记闭包 @Sendable，本地共享 16 项测试及真实 HTTP 路由回归再次通过。客户端 TestFlight 构建基于 3fe48cb，后续 @Sendable 为静态并发标注，不改变缓存运行行为；刷新响应修复由 NAS 部署提供。

修复提交 `5d6136e` 的 GitHub Actions 三组检查全部通过：Rust server、Apple clients and release scripts、Control plane and migration tools。CI 运行：<https://github.com/hechuan9/photo-asset-manager/actions/runs/36284061452>。

## 最终端到端结果

NAS 切换到 `keeps-server:cache-contract-20260927` 后，输入真实资产对应的正确签名、已经过期的下载 URL，由当前 KeepsAPI 的 PreviewCache 完整执行请求、403识别、刷新元数据、再次下载及下采样；成功输出170×256。随后创建新缓存实例，并将该版本下载URL改为不可达地址，仍成功输出342×512；磁盘仅一份1,697,508字节图片，配置上限5 GiB。该流程没有绕过共享加载器或预先换成有效URL。

NAS 镜像、双库备份、原片只读挂载与任务恢复证据见 [NAS部署验证](cache-nas-20260927.md)。未操作用户iPhone/iPad安装，也未把TestFlight状态或共享加载器验证当作真机界面验收。
