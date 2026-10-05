# iOS 0.3.0.14 发布验证

本次修复顶部更早照片分页的重复触发，移除加载完成后自动继续分页；增加 iOS BGProcessingTask，复用全库缩略图缓存及分页断点。系统后台到期取消，后续继续；不保证后台立即运行或一次完成全库。

发布由当前暂存范围的独立源码快照构建，排除工作区内其他 NAS、服务端、Mac 和共享导入 API 改动。

- 独立快照 `swift test --package-path ios`：14 项通过。
- 独立快照 `swift test --package-path shared`：30 项通过。
- `scripts/testflight.sh ios archive`：签名 Release 归档成功。
- 归档 Info.plist：0.3.0 / 14，UIBackgroundModes=processing，任务标识 local.keeps.thumbnail-prefetch。
- `scripts/testflight.sh ios upload`：EXPORT SUCCEEDED。
- App Store Connect API 回读：VALID / IN_BETA_TESTING，自动通知开启，关联 Keeps 内部测试组。
- Build ID：30f0a4f1-9ba5-44d2-9692-831c1a4695cf

没有执行用户手机安装或真机后台调度验收。原片未改动。
