# 缓存与 iOS 底部上拉刷新 TestFlight 发布

两端正式签名归档、codesign 严格校验和上传成功。App Store Connect 回读确认：

- iOS 0.3.0（11）：VALID、IN_BETA_TESTING，已关联 Keeps 内部测试。
- macOS 0.3.0（5）：VALID、IN_BETA_TESTING，已关联 Keeps 内部测试。

iOS 包含目录查询缓存，并仅由底部上拉松手刷新；Mac 包含目录版本缓存，维持自动刷新行为。没有提交 App Store 正式上架，也未做真机安装后的手势验收。

发布采用 scripts/testflight.sh。API 密钥用于查询及归档；带 API 密钥导出时 Apple 返回 cloud-managed distribution certificate 权限错误。改用已登录 Xcode 团队账户导出上传后成功，无需新增证书或更改权限。

证据：[iOS](testflight-ios-build11-cache-20260928.json)、[macOS](testflight-macos-build5-cache-20260928.json)。
