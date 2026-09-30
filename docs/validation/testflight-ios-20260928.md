# iOS TestFlight 0.3.0 (3)

## 本次改动

- 连接设置先验证服务鉴权、计数和一条资产查询，成功后才持久化到 UserDefaults / Keychain；验证期间防止重复提交和编辑。
- 无法连接与空图库分开呈现，提供重试和连接设置入口。
- 同一筛选刷新时保留现有照片，成功后替换；切换搜索、回收站或排序时清空旧结果，防止串数据。
- 工具栏补充辅助功能标签，设置页说明本地网络授权和 NAS 网络要求。
- 包含工作区已有的照片版本详情与前台修订轮询集成，未修改原片。

## 验证

- 共享 Swift 包：17 项测试通过。
- iOS arm64 Simulator 构建通过。当前 Xcode 下默认双架构构建出现主应用 x86_64 与仅 arm64 的共享包产物不匹配；使用 arm64 / ONLY_ACTIVE_ARCH=YES 验证通过。
- Release 签名归档成功，归档内版本为 0.3.0 (3)。
- 归档应用 codesign --verify --deep --strict 通过；git diff --check 通过。
- API Key 上传因缺少 cloud-managed distribution certificate 权限失败；改用脚本支持的本机 Xcode 账号上传。
- 模拟器未启动，尚未进行界面交互或手机端 NAS 验收。

## 发布状态

Xcode 账号上传成功（2026-09-28 07:39 America/New_York，EXPORT SUCCEEDED）。App Store Connect API 已确认构建 3 为 VALID / IN_BETA_TESTING，并分配到“Keeps 内部测试”组，自动通知已开启。Build ID：8411b819-9c51-435c-9f87-52be69675b46。原始回读见 testflight-ios-20260928.json。
