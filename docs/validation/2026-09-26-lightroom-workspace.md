# macOS Lightroom 风格工作区验证

参考：本机正在运行的 Adobe Lightroom（com.adobe.lightroomCC）照片网格窗口；非 Classic。采用顶栏搜索与侧栏开关、268 pt 目录面板、深灰无圆角照片网格、底部查看与整理工具和右侧信息面板。保留 Keeps 品牌与 NAS / 本地语义。

- `swift test --package-path macos`：16 项通过。
- `bash macos/scripts/package_app.sh`：编译、应用打包与签名检查通过。
- `git diff --check`：通过。
- 运行工作区构建，验证目录面板、筛选面板、信息面板可展开和收起。
- 离线错误提示不再撑大内容的最小尺寸；完整错误可通过“查看错误详情”读取。

限制：应用访问 NAS 时收到 NSURLErrorDomain -1009，底层原因为 `Local network prohibited`。未修改系统网络权限或服务器设置。真实照片网格、单张查看及底部评分操作尚未完成运行时验收；不将本轮认定为像素级一致性验收通过。

后续验证：统一 HTTP 资料库改动完成后，真实 NAS 查询与单张照片信息读取成功，前述网络阻碍已解除。详见 [统一 HTTP 资料库验证](2026-09-26-unified-http-library.md)。这不构成像素级一致性验收。
