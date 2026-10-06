# 离线文件夹整理

`scripts/merge_tracked_folders.py` 只合并同一直接父目录内的数据库资产，不删除、移动或改写照片、RAW、sidecar。仅相同内容哈希或精确 JPEG 图像指纹自动合并；元数据相似只形成候选，评分、标签或手动默认版本冲突时跳过。

此工具依赖 Docker 容器的挂载与停止状态检查，不能直接用于当前原生 SPK。使用前运行 `--help` 确认参数和部署条件，不得为使用该工具而启动指向旧数据库的历史容器。

## 操作顺序

1. 使用包含 `keeps-inspect` 的镜像，对 active 追踪根和只读原片挂载的交集执行 `plan`。检查器 `TZ` 必须与服务端一致；`--inspector` 是最后一个参数，之后直接传检查命令参数数组。
2. 作业目录放在状态根的 `maintenance/`，保存数据库快照、检查缓存和计划。中断后用同一目录加 `--resume`；更换镜像或时区则新建作业目录。
3. 检查计划的错误和覆盖范围。未入库、读取失败或内容变化会阻止对应目录合并；`complete=false` 禁止应用。
4. 停止服务及其他数据库写入者，再 `plan --resume` 刷新最终计划，然后 `apply`。apply 检查容器、挂载、追踪配置、数据库字段和源文件，任一变化即停止。
5. 恢复服务，核对健康、目录计数、保留资产的版本和默认图。默认版本改变时沿用正常队列生成预览。

## 恢复边界

apply 备份 catalog/jobs 双库，使用 SQLite 跨库事务同步提交合并映射与业务修改；重复应用同一计划不会重复合并。必须保留该次维护目录中的备份和 `applied.sqlite`，仅回放旧 ledger 不能恢复离线合并结果。

恢复前停服并另备份最新两个数据库，再同时恢复同次备份；不能只恢复一个库。恢复旧库会撤回备份后的数据库修改，原片保持原位。整库恢复到合并前状态后，重新预演并使用新作业目录。

验证入口：`python3 -m unittest discover -s scripts -p 'test_merge_tracked_folders.py'` 和 `cargo test --manifest-path server/Cargo.toml --test inspect_cli`。
