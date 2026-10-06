# macOS 任务窗口简化

- 任务追踪从 sheet 改为单实例原生 Window，使用系统关闭按钮，隐藏最小化和缩放按钮；保留 Command-W 关闭。
- 移除“刷新”“完成”按钮，任务状态在窗口显示期间每 5 秒自动更新；失败任务仍可重试。
- 任务、来源、图库和照片信息滚动区使用自动隐藏的 overlay 滚动条；目录树沿用既有 overlay 样式。
- SwiftPM macOS 构建和本地打包签名检查通过。实际 UI 检查确认任务窗口 AX 仅含系统关闭按钮，无刷新/完成/最小化/缩放按钮；Command-W 返回主窗口，任务计数自动更新。
- macOS 独立递增为 0.3.1（9），Release 通用架构归档和签名检查通过；Apple API 已确认 VALID / IN_BETA_TESTING，关联“Keeps 内部测试”；build ID `5d14a554-acf5-4fd0-a2a9-84adeef700cb`。
