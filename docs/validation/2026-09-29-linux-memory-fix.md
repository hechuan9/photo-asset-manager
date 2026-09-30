# Linux RAW 编码地址空间限制修复

## 原因与改动

`media::run_with_timeout` 固定通过 `prlimit --as=1610612736` 将图像子进程虚拟地址空间限制为 1.5 GiB。真实 Sony ARW 样本在独立容器、无其他任务时仍复现 `std::bad_alloc` / SIGABRT；Docker cgroup 内存事件计数均为0。

增加 `KEEPS_MEDIA_ADDRESS_SPACE_BYTES` 配置，默认仍为 NAS 的 1.5 GiB；Linux Compose 设置为 4 GiB。保留单线程编码、超时、ImageMagick memory/map/disk 限制、4任务并发、Docker 12 GiB 总内存。配置非数字或0快速报错。NAS 未重启或调整限额。

## 对照验证

Linux release: `/home/hechuan/keeps-worker/releases/memory-fix-20260929`。

- 同一失败样本 SHA256: `071719a77c6dfbf126c73a829b302506f2aba4d6905657c1404ea46190f54d93`。只复制原片读取测试，原片未修改。
- `old.exit=1`，old.log 保留完整 bad_alloc trace。
- `new.exit=0`；标准图 7968×5320，缩略图512×342。
- 本地 media 测试5通过、真实Linux测试1本机忽略；真实样本已在Linux验证。Clippy all-targets -D warnings、fmt、diff-check通过。

## 部署

2026-09-29 17:43:18 UTC 启动新 worker，镜像 `sha256:fa68ca574743182d4d08df3bc03c8d6a6bbfe820680bc4e88925ba02671b2808`。部署前已优雅等待旧任务结束，备份Compose和env（后者含凭据，勿输出）。运行时读回4并发、4 GiB地址空间、12 GiB容器内存；restart=0/OOM=false。

针对7个已确认失败、pending且attempts=1的任务，仅调整数据库available_at以提前重试，保留attempts和历史错误，不改原始文件、不改调度查询或排序。

## 线上闭环

7个历史失败资产均已第二次尝试成功，状态ready、last_error=null。新worker观测期间无新增任务失败和bad_alloc；4并发时cgroup内存峰值约4.3 GiB，oom/oom_kill/max均0。任务恢复自动处理，Mac不在运行链路。
