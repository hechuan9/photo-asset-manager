# TestFlight 迁移验证（2026-09-26）

## 配置与范围

复用同日 Piano Lab 验证过的 ClimaMind LLC Apple Developer Program 团队 `3TZ6RCL8NE` 和 Xcode 登录，替代历史企业发行流程。统一脚本 `scripts/testflight.sh` 显式区分 archive/upload；仅 TestFlight internal testing，不提交正式上架。

- iOS: `com.hechuan.Keeps`，KeepsIOS scheme，0.3.0 (1)。
- macOS: `local.keeps`，Keeps scheme，0.3.0 (1)，arm64 + x86_64。启用 App Sandbox 和出站网络。
- 原片与 NAS 状态均不修改；未自动替换当前本地安装。
- 功能范围不单独维护 features.md，产品与架构分别在 UX_DESIGN.md / ARCHITECTURE.md。

## 已验证

- `swift test --package-path shared`：8 项通过。
- `swift test --package-path macos`：19 项通过。
- iOS Release signed archive：成功。
- macOS Release signed archive：成功；`codesign --verify --deep --strict` 通过。
- macOS 本地开发打包：通过，共用 Version.xcconfig，版本正确。
- 发布脚本 bash 语法、ExportOptions 和 Info.plist 语法检查通过。

## 发布状态

两端 App Store Connect 记录已创建：iOS `6816541067`，macOS `6816541220`。两端 0.3.0 (1) 均已 `EXPORT SUCCEEDED`，Apple 已接收并开始处理。每端创建“Keeps 内部测试”自动分发组，仅加入账号本人。Apple 回读：两端 Build Uploads 均为 Complete，0.3.0 Build 1 Internal 均显示 Testing、Expires in 90 days，已分配 Keeps 内部测试组。设备安装与 NAS 业务验收尚未执行。首次 TestFlight 安装还需验证 NAS 连接设置、Keychain 凭据、局域网权限、图库加载与整理。

现场证据：iOS：`testflight-ios-20260926.png`（本地现场记录，未入库）、macOS：`testflight-macos-20260926.png`（本地现场记录，未入库）。
