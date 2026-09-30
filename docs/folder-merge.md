# 按文件夹批量整理资产

入口是 `scripts/merge_tracked_folders.py`。它读取当前启用的追踪配置，按广度优先顺序逐层遍历，**只在同一个直接父目录内合并数据库资产**。父子目录、兄弟目录、不同追踪根之间都不去重。所有照片、RAW、sidecar 原件保持原路径和内容，预览文件也不会被此脚本删除或移动。

## 规则

- 对实际原片重新提取 SHA-256、拍摄元数据和精确 JPEG 图像指纹，复用 Rust `keeps-inspect`，不另写识别算法。JPEG 指纹排除元数据，但保留图像编码、方向和颜色解释；同内容哈希或同精确图像指纹才能自动合并。
- 时间、相机、镜头均非空且相同只形成待视觉确认的候选；不同机身序列号另行标注。RAW 与导出成片、调色、裁剪、重新压缩后的相似照片不会仅凭元数据强行合并。
- 既有多版本关系保留，不重新推断旧版本的视觉关系。后台同目录改名仍可保持关联；schema 9 后台扫描优先通过合法根 ID 识别跨目录移动，无根 ID 时仅在内容哈希唯一且旧位置均缺失时保留原 ID。此批量合并工具本身不执行跨目录合并。历史资产有任何其他目录路径（包括缺失路径和扫描状态路径），整组合并跳过，防止间接跨目录。
- 评分、旗标、颜色、说明、标签或回收站状态冲突则跳过；不同手动默认版本冲突也跳过。保留有手动默认选择的资产，其次原始版本较多、有预览的资产，最后按 ID 稳定选择。
- 任一被索引的原片或标记可用的版本未经本轮检查，整组跳过。存在未入库、内容已变化、读取失败照片的目录，先报告并等待正常索引补全。
- 符号链接、隐藏项、`@eaDir`、`#recycle` 和 Keeps 数据目录不遍历。目录隐藏标记是 UI 配置，不影响实际已追踪原片的核对。非照片文件不参与归组；sidecar 只检查状态，不改写。

## 范围与运行条件

脚本从指定 Docker 容器读取原片只读挂载，只访问这些挂载与 active 追踪配置的交集。例如服务追踪 `/volume2` 时，不会在 NAS 宿主扫描整个 volume 的其他共享目录。原片要求宿主同路径挂载，与现有部署一致。NAS 宿主使用 Python 3.8+，不需要安装 Python 第三方包；媒体检查在 Docker 镜像中完成。

先构建包含 `keeps-inspect` 的镜像，并按 NAS 升级流程部署这一版服务端。同目录扫描匹配边界也在这一版代码中；此前的 `versions-watch-20260927` 镜像没有检查命令，而且新文件仍可能全库匹配。不要以检查镜像已构建代替生产服务已升级。

```bash
cd /volume2/docker/keeps
docker build -f server/Dockerfile -t keeps-server:folder-merge-20260927 .
```

### 1. 在线只读预演

生产数据库通过 SQLite backup API 读取到维护目录的快照；检查缓存和报告均放在 `KEEPS_ROOT/maintenance`，不写原片目录或生产数据库。一个运行目录对应一次整理作业。

```bash
python3 scripts/merge_tracked_folders.py plan \
  --container keeps-control-plane \
  --run-dir /volume2/myphoto/keeps/maintenance/folder-merge-20260927 \
  --inspector docker run --rm -i \
    --volumes-from keeps-control-plane:ro \
    --env TZ=America/New_York \
    --entrypoint keeps-inspect keeps-server:folder-merge-20260927
```

检查容器的 `TZ` 必须与服务端一致（当前 NAS 为 `America/New_York`），使没有时区的 EXIF 得到相同解释。更换检查镜像或时区后使用新的运行目录，不复用旧证据缓存。

`--inspector` 必须是最后一个选项，之后的参数直接组成子进程参数数组，不经过 shell。大目录每检查 100 张照片输出一次进度。首次运行需读取所有照片内容，耗时取决于文件总量和磁盘速度；没有全库完成的固定时间承诺。

`plan.json` 包含每个目录的可合并组、保留资产 ID、跳过原因、元数据候选与文件错误。存在目录读取错误时 `complete=false`，禁止 apply。未入库等文件级问题会阻止对应目录归组，其他完整目录仍可整理；应结合 issues 判断覆盖范围。

中断后用同一命令增加 `--resume`（放在 `--inspector` 之前）。每次续跑重新读取数据库快照，复用 stat 未变化且没有 sidecar 的原片证据；有 sidecar 的照片重新检查。未完成的计划只保存为 `plan.partial.json`，不会被 apply 使用。

### 2. 停服刷新计划并应用

在线预演期间扫描器可能继续改变数据库。停服后续跑一次，让最终计划对应静止数据库；缓存会避免重新读取大多数不变照片。

```bash
cd /volume2/docker/keeps/deploy/nas
docker compose stop control-plane
# 重复上面的 plan 命令，并增加 --resume；完整报告成功后再执行：
python3 /volume2/docker/keeps/scripts/merge_tracked_folders.py apply \
  --container keeps-control-plane \
  --run-dir /volume2/myphoto/keeps/maintenance/folder-merge-20260927
docker compose up -d --no-build control-plane
```

apply 会确认容器已停止且挂载未改变，备份 catalog/jobs 双库并检查完整性，再检查追踪配置、资产全部当前字段、实际候选 SHA-256、stat 和 sidecar 是否仍与计划一致。任何变化都会中止，要求重新预演。

合并同步修改 catalog_paths、catalog_files、versions、version_paths、default、扫描 files 的资产 ID、更新时间和 revision。默认优先级沿用服务端；默认变化时只使预览数据库引用失效，服务恢复后由已有 worker 重建。被合并的源资产当前投影移除，原件路径全部保留，保留资产 ID 稳定。

catalog、jobs 与维护目录 `applied.sqlite` 使用 SQLite DELETE journal + FULL synchronous 跨库原子事务提交；正常结束恢复原 journal 模式。进程中断后 SQLite 恢复整个事务，不会出现 catalog 已合并而 jobs 仍引用旧 ID 的半完成状态。已提交的计划 ID 重复 apply 直接返回已完成；新预演生成新 ID，并重新评估当前状态。备份位置见 `apply-started.json` / `applied.json`。

历史 ledger、archive receipts 和冲突审计不重写。完整映射与计划保留在维护目录，映射随生产修改原子写入 `applied.sqlite`。**单独回放旧 ledger 不能恢复本次离线合并结果**；灾难恢复需要数据库备份与维护作业记录，不能丢弃它们。

### 3. 验收与恢复

恢复服务后确认健康、目录计数、保留资产的版本和默认图、扫描任务推进。全库扫描结束前不要把预览尚未补齐当作原片丢失。

需要恢复时先停服务并另备份当前 catalog/jobs，再同时从该次 `backups/folder-merge-*` 恢复两个数据库，保留维护目录供追溯。不能仅恢复一个库。恢复会撤回备份之后的用户数据库修改；原片无需恢复。若整库已恢复到合并前状态，使用新的维护作业目录重新预演，勿沿用旧的“已提交”日志。

## 验证

```bash
python3 -m unittest discover -s scripts -p 'test_merge_tracked_folders.py'
cargo test --manifest-path server/Cargo.toml --test inspect_cli
```

自动测试覆盖目录边界、真实 SQLite 双库事务、原片不变、用户冲突、默认版本、计划失效、sidecar 变化、幂等及中断续跑。NAS 合成样本验收需另外使用真实 ExifTool 与新检查镜像；生产全库 apply 是独立操作。

## 入库日期补全

拍摄日期必须有值。有效拍摄日期优先；缺失或无效时尝试 EXIF/XMP/QuickTime 创建日期，最后使用文件修改时间。仅写入数据库，不改写原片；不保存推测来源字段。用户可之后手动修正。元数据日期相同仍只形成候选，不单凭日期自动合并。
