# Keeps macOS

Keeps 是 NAS 照片服务的原生 SwiftUI 客户端。NAS 承担照片扫描、索引、元数据修改、预览生成和任务持久化；macOS 仅保留界面状态和网络图片缓存。

在 Keeps → 设置（⌘,）的 Server 页面填写 Keeps Server 的 HTTP/HTTPS 地址、资料库名称和服务访问令牌。通过服务器鉴权和资料库查询验证后才保存并切换连接；失败时保留原来的连接。地址与资料库保留在 UserDefaults，凭据保存在 Keychain；旧连接偏好会自动迁移。NAS 不可用时显示请求错误，不切换本地数据库。

- 统一 HTTP 资料库：默认全部照片，支持精选与回收站；不依赖 SMB 挂载，也没有本地暂存资料库。
- 目录与图库之间可拖动调整宽度；目录滚动条采用自动隐藏的浮层样式。
- 图库滚动到底部自动加载下一页，加载过程中到达底部也会在当前请求完成后继续。
- 服务器来源目录按需展开浏览，原生 NSOutlineView 目录树按需展开，包括空目录，支持键盘导航及保留展开状态的刷新；搜索、评分、颜色、标签筛选和排序。
- 多选照片，修改评分、精选、颜色和标签。
- 共享回收站与恢复，只改变服务器资产状态，不删除原片。
- 服务端目录追踪、扫描、任务状态与失败重试；路径相对于服务器原片根目录。
- 使用 NAS 生成的预览图，不在客户端扫描、解析或生成照片衍生图。

目录导航依赖服务端 `/libraries/{libraryID}/navigation` API；来源管理与扫描通过“管理来源与任务”访问。Mac 不枚举本机文件，目录导航失败不阻止已索引照片查询。上传尚未实现。

```sh
swift test --package-path ../shared
swift test
bash scripts/package_app.sh
```

共享 HTTP 契约和 DTO 位于 `../shared` 的 KeepsAPI 包，macOS 与 iOS 使用同一份实现。

## TestFlight 分发

正式测试分发使用 `Keeps.xcodeproj` 的共享 `Keeps` scheme，自动签名团队为 `3TZ6RCL8NE`，Bundle ID 保持 `local.keeps`。工程直接编译现有源文件并引用 `../shared` 的 KeepsAPI，不维护第二份客户端实现。版本号和构建号统一在 `Version.xcconfig` 修改，每次上传递增构建号。

在仓库根目录执行：

```sh
bash scripts/testflight.sh macos archive
bash scripts/testflight.sh macos upload
```

归档保存在 `macos/.build/testflight/Keeps.xcarchive`，包含 Apple Silicon 与 Intel 架构。上传需要 Xcode 已登录具有团队发布权限的账号，以及 App Store Connect 中对应的 macOS App 记录。归档成功不代表 TestFlight 已处理完成。

发行版开启 App Sandbox、Hardened Runtime 和出站网络权限；客户端只通过 HTTP/HTTPS 访问 NAS，不需要本机照片访问权限。Keychain 仅保存当前应用凭据，不启用跨应用 Keychain sharing。首次切换到沙盒版本需在设置中确认服务地址、资料库与令牌；本地开发版的偏好和旧签名凭据不保证自动迁移。

`bash scripts/package_app.sh` 继续用于本机调试，生成 ad hoc 签名应用，不用于 TestFlight。SwiftPM 测试入口保持不变。

隐藏目录：目录右键选择“设为隐藏目录”或“取消隐藏目录”；子目录继承父目录的隐藏设置。“显示 → 过滤隐藏目录内容”默认开启，全库、精选、回收站和搜索均由服务端排除隐藏照片。进入隐藏目录或其子目录后可查看，独立标记的隐藏后代仍需进入后查看。目录入口保留，菜单可关闭过滤。此功能不加密原片、不提供密码锁。
