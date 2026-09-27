# NAS / 本地导航验收

2026-09-26 完成服务端部署和已安装 macOS 应用实测。本轮是目录导航与区域切换，不包含视频完整索引或本地文件接入。

- 服务镜像：`keeps-server:navigation-20260926`，构建 ID `cabe3d7e5162`；保留 `keeps-server:before-navigation-20260926` 回退标签。
- 更新前备份：`/volume2/myphoto/keeps/backups/before-navigation-20260926-130017-control_plane.sqlite` 和同前缀 `jobs.sqlite`，SQLite integrity_check 均为 ok。
- 真实导航 API 返回 library、personal、raw-processed、raw-unprocessed 四个目录；raw-unprocessed 子目录请求成功；未认证请求返回 401。
- 服务返回本地状态 `not_configured`；四个原片挂载仍为只读；扫描任务重启后继续 running。
- Mac 应用安装到 `/Applications/PhotoAssetManager.app`，签名验证通过。默认 NAS 页面加载首批 100 张，总数当时为 117,229。
- 实际点击本地暂存后显示服务器管理的空态，NAS 网格和详情清空；点击“浏览 NAS 资料库”成功恢复图库。
- 展开 library 显示真实子目录；选择 library 显示 NAS / library 路径和递归范围；刷新后树折叠，没有遗留空白分支。
- 自动验证：共享 Swift API 8 项、macOS 13 项、Rust HTTP 6 项均通过；差异检查通过。

API 现场结果见 JSON：`nas-navigation-20260926.json`（本地现场记录，未入库）。根目录可见不代表扫描已完成；历史资产的位置仍在扫描中补全，目录范围查询可能暂时没有已索引照片。
