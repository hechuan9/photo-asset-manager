# Mac 文件夹导入部署验证（2026-10-05）

用户授权部署此前实现的 Mac 文件夹导入。来源递归枚举 RAW、HEIF 和关联 XMP，目标始终平铺在一个已有 NAS 目录内；原片不删除、不移动、不覆盖。

## 发布范围与备份

以 NAS 正在运行的 `video-range-final-20261005` 对应 `source-final.tar.gz` 为基线，仅加入 `server/src/imports.rs`，及 `api.rs`、`jobs.rs`、`lib.rs` 的导入注册与持久化表补丁。保留已上线的视频 Range、JPEG 内容识别和缓存修复。工作区其他未提交代码未整体打包部署。

新镜像：`keeps-server:import-20261005`，config ID `sha256:6d051a030aba5093f25905b821f32f3b68ddca7581f24c87e69bf4a3e63fdc42`。
旧镜像：`sha256:109cec315137604c802b53e75c385a321b11218925223e97b49cedd96183c6f0`，保留 `keeps-server:before-import-20261005` 标签。

发布与证据目录：NAS `/volume2/docker/keeps/releases/import-20261005/`。包含源码归档、逐文件 SHA256 provenance、构建日志、隔离测试、部署及上线验证脚本与 JSON 结果。

2026-10-05 11:52:12 美东时间停止旧容器，11:53:11 恢复 healthy，停服与重建约 59 秒。停服后通过 SQLite backup API 保存 `control_plane.sqlite.before`（1,656,360,960 字节）和 `jobs.sqlite.before`（52,301,824 字节），两份 `PRAGMA quick_check` 均为 `ok`。Compose 和 `.env` 备份权限 0600。catalog schema 仍为 9；jobs 数据库自动新增 imports 表。

## 验证结果

- 精确发布源码的导入相关 Rust 测试：6 PASS（含 2 项 HTTP 测试）。
- NAS 隔离容器实际导入：两张来自不同子目录、同名的 1,707,503 字节 HEIF 和关联 XMP 平铺发布；目标文件名不冲突，入库 2 个资产，扫描 job completed。
- 错误哈希/大小拒收；finish 前仅隐藏暂存；同 manifest 重试跳过已上传项；finish 重试不重复发布；生成的测试来源文件 SHA256 不变。
- 测试容器已停止，独立目录与数据库留存证据，没有将测试照片导入生产资料库。
- 11:53:26 美东时间生产 readback：healthy、restart=0、OOM=false；活动资产及 ready 缓存均 124841，pendingInventory/pending/processing/failed 均 0；本地编码仍禁用。后台目录任务继续由原队列管理，不能把缓存 ready 表述为所有扫描或身份回填完成。
- CPU `2-3`、内存 2 GiB、原 `/volume2/photo` 和 `/volume2/docker/keeps/data` 两个挂载保留。
- 生产接口鉴权 401、未知图库 404、导入请求校验 422 均正确，imports 表可读。生产中未创建测试导入批次。
- 经 NAS 专用 HTTPS 8443 反向代理、显式解析到 192.168.0.50，正常 TLS 校验通过；1.7 MB PUT 到不存在批次返回应用层 422，证明上传路由与反向代理可达。nginx `client_max_body_size 0`、`proxy_request_buffering off`；未改代理配置。

## Mac 安装及访问限制

本机 `/Applications/Keeps.app` 已更新并启动，签名完整性验证通过；二进制 SHA256 `b00a881e18d8a098776e76a42c2e2d0183722aca524816f4b0b645c325f398bb`。旧应用保存在 `/Users/hechuan/Library/Application Support/Keeps/AppBackups/Keeps-before-import-20261005.app`。未发布 TestFlight。

当前 Mac 旧配置仍指向 `http://192.168.0.50:2283`，该端口仅 NAS loopback 可访问，因此此旧连接不可用。配置的公网入口 `https://keeps.hechuannas.synology.me`（DNS 38.42.191.70）实际返回 TP-Link `tplinkdeco.net` 自签名证书，正常 TLS 请求失败；公网 8443 也不可连接。直接连接 NAS 192.168.0.50:8443、保持 Keeps 域名 SNI 时返回有效 Let's Encrypt 泛域证书并正常通过 TLS 校验。

此为现有公网转发/回环入口问题；当前验证不能证明公网可用，也不能声称已通过 Mac 界面完成生产导入。未关闭 TLS 校验、未开放 2283、未修改路由器或本机 DNS。要正常从 Mac 使用，仍需恢复正确的 Keeps 网络入口，再将 Mac 的旧地址更新为已验证入口。

## 回退

恢复发布目录中的 `docker-compose.yml.before` 并使用同目录私有 `.env.before` 对照原配置，重新创建 `control-plane`，随后验健康与鉴权。此次 catalog 无 schema 升级，jobs 的 imports 表为新增表；不要盲目恢复旧数据库而丢失切换后的用户操作。双库备份用于必要的数据恢复，已发布照片永远保留。Mac 可用上述应用备份恢复旧可执行文件。
