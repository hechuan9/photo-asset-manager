# iOS 0.3.0.15 固定分行发布

本次发布浏览期间固定行和槽位、删除留空和整行移除、缩略图原位更新、新照片独立成行追加、历史补录在明确刷新时再排版。分页发现变化自动同步，不要求回到底部。服务端稳定游标发布见同日 keyset NAS 记录。

发布快照从已提交基础加本次 iOS、共享布局及测试文件构建，排除其他未提交 Mac 界面修改。快照与工作区本次源文件 SHA256 一致。

- 共享 Swift 测试 38 项、iOS 状态测试 17 项通过；iOS Simulator 构建通过。
- 独立发布快照的 macOS 回归 37 项通过。
- 签名 Release 归档及 TestFlight 上传成功，版本 0.3.0 / 15。
- App Store Connect 回读 VALID / IN_BETA_TESTING，关联 Keeps 内部测试，自动通知开启。
- Build ID：d4a4d355-512e-4532-a4c9-b6d18e9d0703
- 归档可执行文件 SHA256：c4f14018bec9a4e5f16944b3160a9742650c688a3faba05c0929b61b5a224f0f。

本轮未在用户 iPhone 安装或实测滑动体验、后台任务调度。Apple 处理完成代表可在 TestFlight 更新，不代表已安装到设备。
