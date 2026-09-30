# Linux 计算 worker 部署验收

状态：部署及真实文件闭环 PASS，正式图库仍在后台生成。

## 运行位置

- Linux：`chuan-server`，有线 `192.168.0.9`，9950X3D 16核32线程、32GB RAM。容器 `keeps-worker-worker-1`，部署目录 `/home/hechuan/keeps-worker`。
- Worker：4并发、8 CPU、12GiB RAM、32GiB临时预算；无对外端口，仅挂载自己的临时目录与只读凭据文件。此次未使用 GPU。
- 通过 `extra_hosts` 将 NAS 域名指向 `192.168.0.50`，使用 `https://keeps.hechuannas.synology.me:8443`，保留 TLS 证书校验，全程走局域网。
- NAS：schema 8，运行镜像 `d4be507c65f1`；保留单核、2GiB、原有两个挂载与本地生成能力。90,073个资产迁移前后数量一致。
- Linux 镜像的 manifest ID 为 `2eacdd985809`，NAS Docker 导入后的 image ID 为 `d4be507c65f1`；两个环境内 server/render 二进制 SHA256 已逐个核对一致。
- NAS 发布证据：`/volume2/docker/keeps/releases/linux-worker-20260929/deployment.json`。Linux 发布目录：`/home/hechuan/keeps-worker/releases/worker-20260929`。

## 验证

- 75个 Rust 单元测试、17个HTTP测试、1个CLI测试、4个Python worker测试通过；Clippy与格式检查通过。
- 真实 RAW、JPEG、HEIF、3FR 四类副本均经隔离 API→下载→生成→上传→发布完成，重复complete成功；签名下载哈希一致，重扫仍4个资产，原始副本哈希不变。
- RAW标准图与源处于同目录、同一资产，已有JPEG/HEIF复用，3FR只提供小图。隔离测试53秒完成。
- 单张RAW样本完整任务36.42秒；同一RAW的4任务纯计算并发测试37.61秒完成，无OOM。这是约24.7MB RAW样本，不是全库速度保证。
- 17:10 UTC正式核验：Linux已完成20个任务，4个worker槽位均领取了任务，3个任务处理中。抽样3个资产的新小图下载哈希正确，原片哈希与索引一致。
- NAS状态17:09:57 UTC：healthy，重启0、无OOM，ready49、processing4、pending90020、failed0；错误列表为空。状态中的concurrency1表示NAS本地并发，Linux另有4个槽位。
- Linux状态17:11 UTC：running、重启0、无OOM；只读token挂载与资源上限均已核验。未挂载照片共享。

## 已解决的部署问题

第一次切换脚本将Compose目录误设为部署根，启动失败。已恢复两份数据库备份及旧容器，修正为容器labels确认的 `/volume2/docker/keeps/deploy/nas`，增加Compose预检后重试成功。首次记录以 `attempt1-*` 保留；未删除、移动或覆盖原片。

## 运行边界

NAS仍是业务和任务状态唯一真源，无ledger、额外业务数据库或消息队列。1800秒租约由worker续期，过期有限重试；完成凭证与缓存ready同一事务提交。Linux停止后NAS仍能自行处理。

领取优先已有标准图、3FR和视频；缺标准RAW仍在一个任务中先生成标准图再生成小图，尚未做RAW两阶段拆队列。标准图格式仍为HEIF。

定时监控 `nas-2` 保持PAUSED，没有自动恢复。停止Linux worker可在该机 `/home/hechuan/keeps-worker` 执行 `docker compose stop`，停止领取并等待当前任务结束。旧myphoto回退树继续保留且不挂载，不在任何清理范围。

[机器可读证据](2026-09-29-linux-worker-nas.json) · [部署及运行说明](../../deploy/linux-worker/README.md)

## 无 Mac 依赖核验

17:13 UTC 核验：Linux Docker 为 enabled/active，两端容器均为 unless-stopped；worker 内 NAS 域名解析为192.168.0.50。Linux累计完成83个正式任务，全库ready122，failed0。已关闭部署使用的交互SSH连接，运行只依赖Linux和NAS；Mac不转发网络、不传递运行任务、不承担定时调度。重启由Docker恢复（人为stop的容器遵循停止意图）；断线靠续租、过期回收和worker轮询恢复。Codex定时监控仍暂停。
