# 2026-10-05 正式图库迁移至原生 SPK

用户明确确认将 Docker 正式图库、数据库与配置迁至原生套件。迁移完成，Mac 已显示完整正式图库；当前按局域网开发方式运行。

## 当前运行配置

| 项目 | 值 |
|---|---|
| 套件 | KeepsNativeProbe 0.1.0-0007（沿用原型包 ID） |
| Mac 服务地址 | `http://192.168.0.50:2283` |
| 资料库 | `local-library` |
| 原片根目录 | `/volume2/photo`，原位保留 |
| 新状态目录 | `/volume2/@appdata/KeepsNativeProbe/production-state` |
| 运行账号 | KeepsNativeProbe，UID/GID 164747 |
| 本地编码 | 0，保持原 Docker 设置 |
| 缩略图质量 / 自动建表 | 50 / 0，保持原设置 |
| 旧容器 | keeps-control-plane，exited、ExitCode=0、restart=no |
| 回退状态副本 | `/volume2/docker/keeps/data`，保留原目录 |

`var/original-root`、`var/server-url`、`var/access-token` 和 `var/server-overrides` 由套件账号持有，权限 0600。新增 overrides 只接受已知的逐行环境设置，不执行 shell 内容。服务程序 SHA256 与正式 Docker 镜像内程序一致。

## 迁移与验证

- 切换前确认 remote_cache_tasks 无未完成租约，361 个已记录照片父目录可由套件账号读取、写入和遍历；未修改原片目录权限。
- 原生测试服务先停止，Docker 随后正常退出。停机期间复制完整状态树，原始 Docker 状态目录保留。
- 用 Btrfs reflink 复制约 52 GB 状态；新旧文件独立写入。只调整新状态副本属主，不移动、复制或删除原片。
- 源与目标的 control_plane.sqlite、jobs.sqlite 均执行 integrity_check=ok；两个数据库全部表的记录数一致。
- catalog_assets 共 124873 条；API 显示正常照片 124871、回收站 2、精选 0，与迁移前一致。
- 完整保留 239416 个预览目录文件；20 个数据库引用样本对应文件存在。切换后 124871 个缓存 ready，pending/processing/failed 均为 0。
- 原照片路径保持 `/volume2/photo`，无需改写 DB；正式 library ID、令牌、质量、缓存 GC 和本地编码开关均保留。
- Mac 从新服务实际下载 3 张原图库缩略图，HTTP 200；已安装 Keeps 窗口显示 124871 张正式照片及回收站 2 张。
- 最终确认仅一个原生 Keeps 服务进程，普通账号运行，CPU 亲和性 0-1；旧 Docker 已停止且关闭自动重启，避免双服务。
- 0007 打包测试 4 项通过，安装包与当前启动脚本/配置源逐字一致。安装包 SHA256：`c57a57bd5bc466373ce498d67345bdb82b9170e23776ed07ee84a472c8a2a960`。

## 保留的证据与回退边界

NAS：`/volume2/docker/keeps/releases/spk-migration-20261005/`，包括 preflight.json、result.json、final-verification.json、迁移前 Docker 配置及套件配置备份。目录权限 0700；含凭据的 Docker inspect 备份权限 0600，不能外发。

Mac：`.build/spk/mac-connection-before-production.json` 保存此前测试连接，不含凭据；`.build/spk/KeepsNativeProbe-0.1.0-0007.spk` 为本轮安装包。

恢复迁移前 Docker 服务的管理员命令：

```sh
/usr/syno/bin/synopkg stop KeepsNativeProbe
/usr/local/bin/docker update --restart=unless-stopped keeps-control-plane
/usr/local/bin/docker start keeps-control-plane
```

这是恢复迁移时点的旧状态。回退前应先备份并评估原生切换后的新增数据库变更，不能直接覆盖新状态。旧 Docker 仍使用原来的回环端口映射和 HTTPS 基址，客户端访问须按其原网络入口处理。

本轮只验证局域网入口。未修改外网 DNS、路由器或 DSM 反向代理；原公网证书/转发问题不在本轮解决范围。原生硬内存总量限制及 NAS 整机重启尚未验证，沿用此前开发环境约定。后台已存在的扫描/身份补写任务随新服务继续运行，不把迁移称为重新完成全库扫描。
