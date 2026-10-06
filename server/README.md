# Keeps Rust 服务

NAS 常驻服务使用 Rust、Axum 和 SQLite，负责资产查询、评分/标签/软回收站、目录追踪、增量扫描、EXIF、HEIC 预览及持久化任务。macOS/iOS 只调用业务 API。旧事件表保留为内部审计和历史迁移来源；客户端事件上传、心跳和预览上传路由已关闭。

```bash
cargo test --manifest-path server/Cargo.toml
cargo clippy --manifest-path server/Cargo.toml --all-targets -- -D warnings
docker build -f server/Dockerfile -t keeps-server:nas-core .
KEEPS_TEST_IMAGE=keeps-server:nas-core python3 server/tests/docker_smoke.py
KEEPS_TEST_IMAGE=keeps-server:nas-core python3 server/tests/mechanisms_smoke.py
```

配置：

- `KEEPS_ROOT`：服务状态目录；`db/control_plane.sqlite` 保存资产，`db/jobs.sqlite` 保存扫描任务。
- `ORIGINAL_ROOT`：容器内已存在的原片根目录，与服务状态目录分离；显式导入需要目标目录可写，扫描继续只读原片。
- `CONTROL_PLANE_PUBLIC_BASE_URL`：客户端可达的服务地址。
- `KEEPS_ACCESS_TOKEN`：至少 32 字节的共享访问凭据。
- `CONTROL_PLANE_AUTO_CREATE_SCHEMA=1`：新空库初始化；现有库设 `0`，升级前备份并执行 `keeps-server migrate`。
- `KEEPS_LIBRARY_ID`：可选，首次启动时为该资料库追踪原片根目录；已停用目录不会因重启重新启用。
- `TZ`：无时区 EXIF 的解释时区，迁移时与旧客户端保持一致。
- `KEEPS_LISTEN_ADDR`：默认 `0.0.0.0:2283`。

业务 API 使用 Bearer token，预览下载 URL 有效期 15 分钟。共享令牌适用于受信任家庭资料库，不提供独立用户权限体系。

SQLite 使用 WAL。后台扫描只读原片，文件哈希或精确 JPEG 图像指纹可以自动归组；EXIF 匹配仅作为待视觉确认的候选。普通核对通过原片 size/mtime 与 XMP 状态签名跳过未变文件。文件事件精确入队，XMP 和导入仅核对当前目录层，目录结构变化才处理子树；不再定时全库扫描。启动/事件丢失保留一次补漏，独立 worker 持久化扫描断点，精确任务优先，运行中再次变化不会被吞掉。默认版本切换会重建预览；版本及监听机制详见 [架构文档](../docs/ARCHITECTURE.md)。日常媒体解码在 NAS 原生套件执行；Linux 远端 worker 仅用于显式启用的一次性批量处理（`KEEPS_REMOTE_WORKER_ENABLED=1`，默认关闭）。解码依赖 ExifTool、LibRaw、ImageMagick 与 libheif；每个外部进程有时限和内存限制。错误保留完整上下文，单文件失败不阻止处理其他文件。

SIGTERM 停止领取任务并等待当前处理阶段结束。未完成的运行任务在下次启动恢复；停止追踪仅修改数据库。数据库和文件 I/O 在阻塞线程执行，查询接口不会承担媒体解码。

部署路径、迁移备份和现场验证见 [NAS 部署说明](../deploy/nas/README.md)。同一资料库只能有一个服务进程管理。

版本机制需要 catalog schema 3，迁移只增加表，不回填或重组历史照片。升级前备份两个 SQLite 数据库。`mechanisms_smoke.py` 使用一次性 Linux Docker volume 和临时样本，不访问 NAS 照片；Docker Desktop 的宿主共享目录事件不等同于 NAS 本地文件系统，因此监听验收使用原生 Linux volume。

离线逐文件夹精确重复整理使用 [批量整理脚本](../docs/folder-merge.md)。镜像包含只读 NDJSON 媒体检查命令 `keeps-inspect`；新文件归组仅匹配同一直接父目录内的原片证据，已有路径关联保留。

## Mac 显式导入

`POST /libraries/{library}/directories` 接收 `{parentPath,name}`，在当前资料库已追踪的父目录中创建单层子目录，返回 `201 {path}`。名称不能是隐藏或系统目录、多层路径或包含控制字符；重名返回 409，不覆盖现有文件或目录。

`POST /libraries/{library}/imports` 接收 `{id,targetPath,deduplicate,files:[{id,relativePath,size,sha256?}]}`。`deduplicate` 默认 false，此时不要求源文件 SHA256，不按内容跳过文件。批次和文件 ID 为 UUID；提供 SHA256 时使用小写十六进制。目标必须是活跃追踪范围内的已存在目录；来源路径仅用于识别同源配对，所有文件平铺在目标目录。每批最多 10000 项，单文件 1 字节至 8 GiB。允许 RAW、HEIF/HEIC/HIF 及关联 XMP；不接受独立 XMP。响应包含每项 `fileName`、`uploaded`，以及批次 `finished`、`jobId`。

启用 `deduplicate` 时每项必须提供 SHA256；仅比较目标目录这一层已有文件，不比较画面相似度，也不处理来源批次内部的重复组。以内容一致的 RAW/HEIF 为配对依据；所有重叠配套文件都一致时跳过已有项并补齐缺失项，响应 `skipped:true`。已有 XMP 或其他配套文件冲突时整组另名。目标摘要计算不持有任务数据库锁，提交时再次验证跳过的文件。

服务端按来源父目录和不区分大小写的 stem 为 RAW/HEIF/XMP 分组。目标已有相同 stem（包括不同扩展）时，全组使用 `IMG (1)` 等共同后缀，保留配对。大小写不同的 XMP 以原片 stem 和扩展规范化命名。分配名称持久化在 jobs.sqlite，同一批次 ID 和 manifest 可重复提交恢复状态；不同批次即使内容相同也作为一次新导入。

`PUT /libraries/{library}/imports/{batch}/files/{file}` 流式上传二进制，不在内存保存完整文件。流式检查大小并计算接收摘要；如提供了源 SHA256 则额外比较。验证成功后保存到目标目录内隐藏的 `.keeps-import-*` 暂存文件，响应 `{uploaded:true}`。扫描和监听跳过暂存文件。单文件失败可以重新上传；成功项重试不会重复写入。

`POST /libraries/{library}/imports/{batch}/finish` 要求全部文件已上传，先检查目标冲突，再以不覆盖的硬链接发布全部 XMP 和原片，最后复用扫描队列处理身份、版本与预览，响应 `{job:...}`。完成原片发布后移除的是隐藏暂存链接；现有原片、来源照片不删除、不移动、不覆盖。发布中失败可重试，已发布项保留；批次不是目录级原子事务。若准备之后目标出现外部同名文件，返回 409 并保留该文件。

批次及完成上传的暂存文件跨服务重启保留，首版没有自动过期或取消清理；未完成导入需要通过原批次继续。服务端必须运行在支持硬链接的本地 NAS 文件系统，部署及真实照片整理效果仍需 NAS 验收。
