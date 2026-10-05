# Mac 导入目标新建文件夹

## 行为

导入面板的 NAS 目标目录旁增加“新建文件夹…”。输入名称后在当前目标目录创建单层子目录，并自动进入新目录作为导入目标。上传仍将递归来源平铺到所选目标。

服务端新增 `POST /libraries/{library}/directories`，请求 `{parentPath,name}`，返回 `201 {path}`。限制在本资料库活跃追踪范围；拒绝隐藏、系统、多层和控制字符名称；同名文件或目录返回 409，保留现有内容。

## 本地验证

- Rust HTTP 测试：21 项通过，覆盖创建、导航、重名保留原文件、非法名称、资料库隔离、越界及符号链接。
- Swift 共享包：32 项通过，含新 API 请求与响应契约。
- Mac 导入测试：7 项通过；应用构建与签名验证通过。

## 部署

- Mac 已安装至 `/Applications/Keeps.app`。NAS 原生套件已升级至 `KeepsNativeProbe 0.1.0-0008`，配置文件与令牌哈希保持一致。
- 在 Mac 导入面板输入测试目录名称并创建，实际显示新目录路径作为目标；返回父目录后新目录可见。
- 现场 API 重名请求返回 409，路径穿越名称返回 422。仅清理了本次创建且确认为空的测试目录。
- 升级前后普通照片 124871、回收站 2；缓存 ready 124871，pending/processing/failed 均 0。旧 Docker 服务仍停止。
- NAS 构建镜像层缓存损坏，因此使用现有编译镜像容器直接编译并提取二进制，保留原生运行环境；Docker 仅用于构建。
- NAS 证据：`/volume2/docker/keeps/releases/newfolder-20261005/upgrade.json`、`verification.json`，套件位于同目录。
