# 2026-10-05 来源设置与任务追踪 App 更新

用户授权重新部署服务端并更新 App；本记录只覆盖本机 App，服务端由独立部署记录覆盖。

- 更新前通过 Keeps 导入面板确认来源未选择、开始导入禁用，无当前导入。关闭面板并正常退出应用。
- 执行 `bash macos/scripts/package_app.sh`，Swift 增量构建通过，打包签名验证通过。
- 旧应用备份：`/Users/hechuan/Library/Application Support/Keeps/AppBackups/Keeps-before-task-settings-20261005-182512.app`。
- 安装位置：`/Applications/Keeps.app`；正常启动后 PID `17385`。
- 可执行文件 SHA256：`cb5666c67baa838c70377b688c8aca930f68175caf40e13495b6fed09b0d1609`。
- `codesign --verify --deep --strict` 通过，Bundle ID `local.keeps`，arm64，ad hoc 本机签名。
- 新版主窗口可查询照片，显示 124871 张；设置工具栏包含“服务器”“来源”。“来源”页显示 `/volume2/photo` 追踪中及来源管理按钮。
- 侧栏独立“任务追踪”入口；任务页显示“NAS 每次执行一个扫描任务；排队任务等待后台调度，目录变化会自动加入扫描。关闭此窗口不影响执行。”，不再包含来源编辑。
- 本次没有修改连接设置、触发扫描或导入，没有发布 TestFlight，也没有改动照片文件。

## 服务端部署后的联动验证

服务端 0010 部署后，只读检查现有任务窗口及截图确认：顶部摘要为“正在扫描：/volume2/photo”；扫描列表首项为 `/volume2/photo`，状态“扫描中”，显示当前文件，已跳过 256、失败 0；后续目录明确标为“排队中”。页面无来源输入框、添加追踪或停止追踪按钮。没有手动触发扫描。
