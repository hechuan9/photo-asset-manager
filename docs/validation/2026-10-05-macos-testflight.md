# macOS TestFlight 0.3.0 (8)

发布来源为提交 `5f8e963` 加 macOS 构建号递增；本轮不归档或上传 iOS。

- 使用 `scripts/testflight.sh macos archive` 成功归档，Bundle ID `local.keeps`，版本 `0.3.0`，构建号 `8`。
- 归档包含 arm64 / x86_64，`codesign --verify --deep --strict` 通过。
- App Store Connect API Key 缺少云端分发证书权限，首次导出失败；沿用已验证的本机 Xcode 登录账号分发流程，返回 `Upload succeeded / EXPORT SUCCEEDED`。
- Apple API 回读确认 `VALID / IN_BETA_TESTING`，已关联“Keeps 内部测试”组；构建 ID `02cb510e-b1fe-4dae-ba7e-e648dc3d91ea`。

原始上传日志、临时签名凭据和构建产物仅保存在被忽略的本地目录，不提交。
