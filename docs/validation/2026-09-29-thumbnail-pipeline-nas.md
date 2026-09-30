# NAS 缩略图流水线执行记录

状态：2026-09-29 14:30:51 UTC 最终修复版已部署，NAS 自主重建运行中；全库尚未完成。首个完整生产批次由 NAS 只读观察进程继续记录。

- 发布目录：`/volume2/docker/keeps/releases/thumbnail-pipeline-20260929`。
- 受管访问：`codex-secret` 的 `chuan-nas`；不记录凭据。
- 机器实测：J4125，4核，约8GB RAM，约32TB可用空间。DSM内核不支持Docker CFS/NanoCPUs；采用cpuset单核绑定及2GiB内存上限。
- 服务端99项测试通过，1项Linux媒体测试由NAS隔离验证补充；客户端shared30/macOS28/iOS状态13项通过。
- TestFlight最终版本：iOS 0.3.0(13)、macOS 0.3.0(7)，ASC回读VALID/IN_BETA_TESTING且均关联内部测试组；未声称已在用户iPhone安装。
- 心跳监控`nas-2`每15分钟只读检查；正常推进安静，完成/失败/持续停滞/资源不足时通知。

## 监控命令

运行：

```sh
codex-secret run chuan-nas -- python3 scripts/nas_cache_status.py
```

最终发布证据包括 `standard-deadline-build.exit=0`、`large-raw-check.json`、`deadline-deployment.json` 和 `verification.json`；迁移证据为 `migration-check.json` 与 `deployment.json`；首个完整生产批次观测见 `batch-observation-final.json`。不要重启或重复启动已有作业。

## 保留边界

生产已启用 GC：每项成功后切换为新小图，20分钟后由NAS每轮最多回收20个无引用旧对象；生成失败的资产保留旧预览。原片、JPEG/HEIF标准照片、RAW、sidecar和业务数据库不删除。新小图从源生成，现有JPEG/HEIF不重新编码。照片/视频/目录已新增独立实体表；旧catalog继续承载公共资产查询与业务状态，不能声称旧表已删除。

[机器可读证据](2026-09-29-thumbnail-pipeline-nas.json)

## 实测编码

28张跨JPEG/HEIF/RAW源的尺寸对比完成。另16张JPEG按最终编码参数对比：512px质量50平均28,130字节、P95为59,211字节、平均2.22秒（单核隔离容器）；这是样本结果，不是全库保证。已检查部分512px样图，尚未实体iPhone验收。

旧缓存格式已实盘核对：约11.5万条为64位hex-1200.heic，其余约9千条为七段路径；GC两种格式均有严格白名单及回归。

## 正式部署验证

- 最终镜像 `keeps-server:thumbnail-pipeline-20260929`，ID `2de9d31675f719423b98d75fe7b7a50cf336f6a210496940f824d99a0973491f`，schema 6；初始迁移镜像为 `84b7b8ed7938`。
- 一致双库与配置备份：`/volume2/myphoto/keeps/backups/before-thumbnail-pipeline-20260929-140723`；旧镜像 `keeps-server:before-thumbnail-pipeline-20260929`。NAS 原服务端源码也已存档，部署源码与镜像构建包同步。
- 首次升级发现 schema 5 修订表重复创建，事务中止并自动恢复旧服务。已补 `version < 5` 条件与已填充 schema 5 的回归测试；随后在真实备份副本及正式停服迁移中验证全部 18 张旧表逐行摘要一致。
- 新照片表 123,877 条、视频表 0 条、目录表 419 条（迁移基线；扫描继续更新）。所有历史资产均入缓存队列，`pendingInventory=0`。
- NAS 27 资产隔离验证通过：真实 JPEG/HEIC/RAW、20 项批次和休息、签名下载、空目录、原片哈希、重启恢复，以及旧 flat key 20分钟保留和隔离时钟推进后的回收。真实编解码在迁移条件修复前完成；修复仅变更 schema gate 和回归测试。
- 正式 HTTPS 健康、鉴权、3 张照片的小图与标准图共 6 次下载/哈希核验通过；小图大小 14,842 / 15,091 / 34,685 字节。TLS 正常验证；未声称完成手机蜂窝网络验收。
- 四个原片挂载逐项比对保持只读；仅新增 `/volume2/myphoto/standard-photos` 写挂载。端口绑定与现有配置不变。初始观测容器 healthy、重启 0、OOM false；编码期间约 776MiB / 2GiB，CPU约一个核。
- 外部监控命令已实测成功；DSM sudo 的非登录 PATH 不含 Docker，脚本使用实测 `/usr/local/bin/docker`。

## 完成判定与回退

生成完成须待盘点、pending、processing、failed 全为零，并抽样验证实际文件。旧缓存替换回收还须 GC pending/failed 为零；未知未登记孤立文件不在本次删除范围。仅服务 healthy 或全库已入队不代表完成。慢速 RAW 和不支持的媒体会保留有限重试与完整错误，不伪报成功。

回退前先停止新服务，保存当前状态，再恢复匹配的两份数据库、旧 Compose/.env 及旧镜像。新生成标准照片是长期照片产物，回退时也不删除。监控只读运行，不自动执行回退或修改数据。

## 大 RAW 实测与吞吐边界

生产发现完整 RAW 标准图编码超过初始 180 秒 watchdog，未发生 OOM。仅标准图编码增加至 900 秒，小图与旧预览仍为 180 秒；显式 retry 也覆盖有错误的 pending 项，以免修复后排在全部新项之后。

同一张源文件在 NAS 单核、2GiB 隔离容器中已生成标准图和小图。源标注 7040px，但 LibRaw 有效图像为 7028×4688；验收按实际完整解码尺寸核对，标准图保持 7028×4688，小图 512×342。下载哈希及原片哈希/大小/mtime 全部一致。首次服务启动至生成就绪约267秒，其中包含首次空队列后的60秒休息；这是单张样本，不是全库平均。

索引扩展名基线：123,876 个有路径资产中，60,724 个有已有 JPEG/HEIF，63,050 个有常见 RAW 而无同资产标准图（明确版本选择或缺失路径会改变实际工作量）。按该单张样本量级，单核全库标准图转换可能以月计；已告知用户，保持授权的低负载限额，未把小图编码速率用作整体预计耗时。

## 最终切换及监控基线

- 14:30:51 UTC 最终镜像 healthy，原有 9 个 ready 项保留；通过正式 `cache-retry` API 将 5 个暂时超时项重新排队。未修改生产数据库来强制伪造就绪。
- 14:31:26 UTC：总计123,877，ready9、pending123,868、processing0、failed0、errors空；当时处于重启后的轮间/扫描窗口。GC待回收9、失败0。后续状态以每15分钟外部监控及NAS观察记录为准；空错误代表已重试入队，不代表这5个已全部重新生成。
- 最终镜像HTTPS再次核验通过，单核/2GiB约束、原片只读挂载与端口保持；未发生OOM。停止宽限改为930秒，中断的资产仍由持久队列恢复。
- 停机校验曾错误依赖Docker挂载数组返回顺序并自动回退；改为按挂载目标比较完整记录后验证通过，挂载内容未改变。
- 27资产隔离库已验证完整20项批次及休息；生产首个完整20项批次尚未结束，`observe-batch-final.py` 仅只读观察，最长2小时后自行退出，主生成任务不依赖它。长期监控为当前聊天的 `nas-2` 心跳。

最终只读快照（2026-09-29T14:36:51Z）：ready 10，processing 1，pending 123866，failed 0，错误列表为空。原超时 RAW 已在正式环境变为 ready，生成的7028×4688标准图及512×342小图经HTTPS下载哈希验证通过。抽查3个已替换旧预览文件均不存在，新小图均存在，确认生产延迟GC已实际开始。全库及首个完整生产批次仍未完成。
