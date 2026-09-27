# 目录名称与展开延迟修复

## 已确认原因与修改

- `/volume2/photo` 以 `/originals/library` 挂载，旧导航取容器路径 basename，因而错误呈现 `library`。Compose 现在传入原片源路径映射，服务启动时校验并提取显示名；保留内部路径和资产身份。
- 旧 `hasChildren` 完整读取并排序下一层目录。现在遇首个可见子目录即停止，测试用尾部错误迭代器证明短路有效，隐藏目录和符号链接排除规则不变。
- macOS API 与 AsyncImage 原先共用 shared URLSession；现改为长期独立 API 会话。是否曾发生实际连接排队未取得 URLSession metrics，不能认定为唯一延迟来源。

## 验证

- Rust 38 项通过，1 项 Linux 媒体运行时测试按既定标记跳过；共享 Swift 8 项、macOS 13 项通过。
- 部署 `keeps-server:navigation-fix-20260926`，镜像构建 ID `a074e58b5794`。
- 数据库备份前缀 `/volume2/myphoto/keeps/backups/before-navigation-fix-20260926-131722-`，control_plane 和 jobs 完整性检查通过。
- 真实 API 返回 photo、和川专属、已处理Raw、未处理Raw；四个原片挂载仍只读，扫描任务继续 running。见 现场结果：`nas-navigation-fix-20260926.json`（本地现场记录，未入库）。
- 新 Mac 应用已安装并通过签名验证，但启动仍等待用户处理系统钥匙串授权，尚未完成这一版的界面复验。

## 性能观测的边界

从 NAS 本机顺序请求根入口及两层目录：修改前热缓存样本为 2–31 ms；部署重启后的样本为 81–200 ms。后台扫描与缓存状态不同，这不是受控前后对比，不能据此宣称速度提升，也不能据此确定回归。已消除代码中的多余工作；用户实际交互延迟仍待授权后复验。
