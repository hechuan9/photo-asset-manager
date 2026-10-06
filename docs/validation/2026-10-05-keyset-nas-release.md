# 2026-10-05 稳定分页 NAS 发布

用户授权同步发布图库稳定分行所需服务端，原生套件已升级至 **KeepsNativeProbe 0.1.0-0013**。生产地址和资料库沿用既有配置。

## 发布范围与验证

- 发布前固定源码快照，与 0012 构建源码逐字比较：Cargo.toml、Cargo.lock 及运行时源码仅 `server/src/catalog.rs` 变化。之后其他会话的媒体解码修改未纳入本包。
- NAS Linux amd64 release 构建通过；12 项 catalog 和 23 项 HTTP 回归通过，包含新增、删除游标边界和同拍摄时间的分页稳定性。
- 本机 SPK 打包测试 4 项通过。随包媒体运行时复用 0012，仅替换 keeps-server；未迁移数据库 schema。
- DSM 安装成功，回读 INFO 为 `0.1.0-0013`，健康接口为 `ok`。
- `access-token`、`server-url`、`original-root`、`server-overrides` 四项配置指纹前后一致。
- 生产普通照片数升级前后均为 125125，精选 0，回收站 2。
- 生产 `capture_desc` 返回 `cd1.` 游标；按游标读取相邻两页，每页 40 项，80 个 ID 无重复。
- 3 张真实缩略图下载均 HTTP 200，字节数分别为 39173、72671、40301。
- 已安装二进制与构建产物 SHA256 一致：`4e4cc09fb613954950d64c87187e4a79a74ad4d306e9ad426cd5142c0d4ca145`。
- SPK SHA256：`ddf80ff9ef2609de76de8f58cd1543e403c414b4395d60d740f67ccdecc8b248`。
- 仅一个原生 keeps-server 进程，UID 164747；旧 Docker 服务保持 exited。
- 停服备份的 control_plane.sqlite 与 jobs.sqlite 均通过完整 integrity_check=ok；本次未修改、移动或删除任何原片。

## 证据与回退

NAS 证据目录：`/volume2/docker/keeps/releases/keyset-20261005/`，包含固定源代码、build.log、0013 安装包、release.py、verification.json 和停服后的 `db-before` 数据库副本。目录权限 0700。

旧 0012 安装包与构建产物保留在 `/volume2/docker/keeps/releases/incremental-20261005/`。如需回退，应先停服并保留回退时的最新数据库，不直接覆盖本次上线后新增状态；数据库 schema 未变化。照片与缩略图状态保持原位，本轮不恢复旧 Docker。

手机侧稳定分行的实际手势体验仍需真机验收；本记录只证明服务端部署与生产分页、缩略图链路。
