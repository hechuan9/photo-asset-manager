# Linux 一次性批量处理 worker

日常扫描、缩略图和标准图处理默认在 NAS。Linux 仅在用户明确安排的一次性大规模处理时启用，任务结束后停止，不常驻、不自动重启。

NAS 保存数据库、租约与文件发布权。Linux 主动通过 HTTPS 领取任务、下载输入、生成 HEIF，再上传 NAS；无需挂载照片目录或开放端口。已有 JPEG/HEIF 只生成缩略图，3FR 不生成标准图。编解码复用 `keeps-render` 与服务端相同的 MediaProcessor。

先显式将 NAS 的 `KEEPS_REMOTE_WORKER_ENABLED` 设为 `1` 并重启服务；未设置或为 `0` 时，服务器不向远端派发新任务。NAS 本地编码保持默认开启，两端通过同一租约避免重复处理。

准备只读 token 文件（权限 0600），设置 `KEEPS_TOKEN_PATH`、`KEEPS_LIBRARY_ID`，运行：

```sh
docker compose -f deploy/linux-worker/docker-compose.yml --profile bulk up -d --build
docker compose -f deploy/linux-worker/docker-compose.yml --profile bulk logs --tail 100 -f
```

默认 NAS 域名通过 `extra_hosts` 访问局域网 192.168.0.50:8443，仍正常验证 TLS 证书。地址变动可设置 `KEEPS_NAS_IP`。默认 16 并发、16 CPU、24GiB RAM、每任务 8GiB 临时空间、总预算 128GiB。临时盘须另留至少 2GiB 空余；大输入、超时或超额任务停止并回报失败，不无限消耗本地磁盘。编码器单线程，按任务并行。

完成本次批量处理后，将 NAS 的 `KEEPS_REMOTE_WORKER_ENABLED` 恢复为 `0`，重启服务，随后停止 worker。

停止：`docker compose -f deploy/linux-worker/docker-compose.yml --profile bulk down`。worker 停止接新任务，等待已领取任务完成（最长 30 分钟停止宽限）；强制结束后未回报任务由 NAS 租约过期后重新调度。任务目录正常退出自动清除；启动会检查崩溃残留占用，预算不足则拒绝启动；残留仅位于专用 scratch 中，可在停止容器后人工清理。不得指向原片或其他用户数据目录。

NAS 须先部署包含 worker API 的对应镜像。下载验证 SHA256；生成标准图保留元数据；上传和完成允许重试；领取响应丢失则等租约过期，避免主动重复领同项。

仅在本次批量处理需要独占编码时，可临时将 NAS 的 `KEEPS_LOCAL_CACHE_ENCODING_ENABLED` 设为 `0`；结束后必须恢复为 `1`。日常默认本地编码开启、远端派发关闭。不要用人工暂停 next_batch_at 代替配置开关。

图像子进程通过 `KEEPS_MEDIA_ADDRESS_SPACE_BYTES` 限制虚拟地址空间：Linux worker 配置为 4 GiB，未设置时维持 NAS 的 1.5 GiB 默认值。虚拟地址空间不等于实际物理内存；worker 总实际内存仍由 Docker 的 24 GiB 限额控制。
