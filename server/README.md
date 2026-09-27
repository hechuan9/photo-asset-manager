# Keeps Rust 服务

NAS 常驻服务使用 Rust、Axum 和 SQLite，负责资产查询、评分/标签/软回收站、目录追踪、增量扫描、EXIF、HEIC 预览及持久化任务。macOS/iOS 只调用业务 API。旧事件表保留为内部审计和历史迁移来源；客户端事件上传、心跳和预览上传路由已关闭。

```bash
cargo test --manifest-path server/Cargo.toml
cargo clippy --manifest-path server/Cargo.toml --all-targets -- -D warnings
docker build -f server/Dockerfile -t keeps-server:nas-core .
KEEPS_TEST_IMAGE=keeps-server:nas-core python3 server/tests/docker_smoke.py
```

配置：

- `KEEPS_ROOT`：服务状态目录；`db/control_plane.sqlite` 保存资产，`db/jobs.sqlite` 保存扫描任务。
- `ORIGINAL_ROOT`：容器内已存在的只读原片根目录，与服务状态目录分离。
- `CONTROL_PLANE_PUBLIC_BASE_URL`：客户端可达的服务地址。
- `KEEPS_ACCESS_TOKEN`：至少 32 字节的共享访问凭据。
- `CONTROL_PLANE_AUTO_CREATE_SCHEMA=1`：新空库初始化；现有库设 `0`，升级前备份并执行 `keeps-server migrate`。
- `KEEPS_LIBRARY_ID`：可选，首次启动时为该资料库追踪原片根目录；已停用目录不会因重启重新启用。
- `KEEPS_SCAN_INTERVAL_SECONDS`：默认 300，单 worker 定期安排扫描。
- `TZ`：无时区 EXIF 的解释时区，迁移时与旧客户端保持一致。
- `KEEPS_LISTEN_ADDR`：默认 `0.0.0.0:2283`。

业务 API 使用 Bearer token，预览下载 URL 有效期 15 分钟。共享令牌适用于受信任家庭资料库，不提供独立用户权限体系。

SQLite 使用 WAL。后台扫描只读原片，哈希和 EXIF 匹配已有资产，复用已有预览；未变化文件通过 size/mtime 跳过。媒体解码在 Linux 容器执行，依赖 ExifTool、LibRaw、ImageMagick 与 libheif；每个外部进程有时限和内存限制。错误保留完整上下文，单文件失败不阻止处理其他文件。

SIGTERM 停止领取任务并等待当前处理阶段结束。未完成的运行任务在下次启动恢复；停止追踪仅修改数据库。数据库和文件 I/O 在阻塞线程执行，查询接口不会承担媒体解码。

部署路径、迁移备份和现场验证见 [NAS 部署说明](../deploy/nas/README.md)。同一资料库只能有一个服务进程管理。
