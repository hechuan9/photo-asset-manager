# 原生目录树实现

按用户确认的标准文件浏览器设计，macOS 目录面板改用 `NSViewRepresentable` 包装的 `NSOutlineView`。旧递归 SwiftUI 目录树及后续扁平模拟目录行均已移除。

- AppKit 负责树形行、原生展开箭头、键盘导航、行复用与选中；节点按路径保持 NSObject 身份。
- LibraryStore 保留服务器数据缓存与展开状态。数据源同步读取缓存，异步请求完成后更新对应分支。
- 收起重展不重复请求；手动刷新保留有效展开、选择和滚动位置；切换服务器重置状态并拒绝旧连接迟到响应。
- 未加载节点可展开，服务端只枚举当前层，返回 `hasChildren: null`；加载后按子目录结果确定是否为空。
- 加载失败可重试；刷新失败保留已有子目录，并提供错误重试入口。
- NAS 与本地暂存、服务连接设置及照片操作的职责边界保持不变。原片仍只读。

自动验证：macOS 15 项（包含真实 NSOutlineView 行数、展开、选择、稳定节点及刷新保持）、共享 API 8 项、Rust HTTP 6 项通过。已打包并安装到 `/Applications/PhotoAssetManager.app`，签名检查通过。

现场验证已确认原生 outline 展开和目录选择。原有全局左右照片快捷键与原生树冲突，已移至图库焦点范围；追加原生 NSWindow first responder 的键盘右方向键展开验证，确认不会选中照片。修订后的 15 项 Mac 测试通过。

NAS 已部署 `keeps-server:outline-20260926`（构建 ID `e04c86e9b03d`），备份前缀 `/volume2/myphoto/keeps/backups/before-outline-20260926-141738-` 的两个数据库完整性检查通过。真实导航返回四个正式目录名及未知子目录标记；四个原片挂载仍只读，后台扫描恢复运行。见 现场结果：`nas-outline-20260926.json`（本地现场记录，未入库）。

最终安装版现场验证通过：目录树键盘 Right 展开、Down 移动、Left 折叠；刷新保留选中目录与两级展开，收起后再次展开正常。图库点击 `B0014327.HEIC` 后 Right 切换到 `B0014325.HEIC`，Left 返回；再点击目录后 Right 只展开目录，照片选择保持为空。图库使用限定焦点的 `.onKeyPress`，不注册全局方向键快捷键。
