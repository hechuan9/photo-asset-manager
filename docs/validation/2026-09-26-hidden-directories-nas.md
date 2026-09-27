# 隐藏目录 NAS 部署验收

日期：2026-09-26（America/New_York）；NAS：`chuan-nas`，服务：`keeps-control-plane`。

## 升级保护

- 通过 `secrets-management` 的 `codex-secret` 访问 NAS，凭据未输出或写入工件。
- 线上 Compose 与本地一致；Cargo.lock 和 Dockerfile 未变化。
- 旧镜像保留为 `keeps-server:before-hidden-20260926`。
- 旧源码/Compose 备份：`/volume2/docker/keeps/before-hidden-source-20260926.tar.gz`，不包含 `.env`。
- 仅更新 `server/src`、Cargo 文件和 Dockerfile；原片不删除、移动或覆盖。
- 旧服务运行时先构建新镜像；成功后再停服，保留 210 秒正常停机窗口。

## 数据迁移

- 停服后通过 SQLite backup API 备份两个库，并分别通过 `PRAGMA integrity_check`：
  - `/volume2/myphoto/keeps/backups/before-hidden-20260926-control_plane.sqlite`
  - `/volume2/myphoto/keeps/backups/before-hidden-20260926-jobs.sqlite`
- `docker compose run --rm --no-deps control-plane migrate` 成功，schema 1 → 2；只新增 `catalog_hidden_directories`。
- 迁移后 `integrity_check=ok`；所有旧表行数与停服备份一致；`catalog_assets`、`catalog_files`、`catalog_paths` 逐行 `EXCEPT` 比对无差异。

| 表 | 迁移前后行数 |
| --- | ---: |
| catalog_assets | 117,344 |
| catalog_files | 293,446 |
| catalog_paths | 143,866 |
| ledger_events | 795,185 |
| derivative_objects | 117,328 |
| archive_receipts | 24,789 |
| ledger_sequence_counters | 1 |
| device_states | 1 |
| sync_conflicts | 0 |

## Live 验证

- 新镜像：`keeps-server:hidden-directories-20260926`，SHA `81af7a9a9fd77b62efe7da3193bea6a37a0f3e242188bde03ec84efd502038a7`；生产别名 `keeps-server:local`。
- `keeps-control-plane` 正常启动，40 秒时 Docker 状态 `healthy`；`/healthz` 返回 `{"status":"ok"}`。
- 不带认证访问资产 API 返回 HTTP 401；带认证的隐藏配置、资产和计数请求返回 HTTP 200。
- 使用现有真实父子目录临时标记隐藏，通过 `finally` 清理，最终配置回读 `{"paths":[]}`。
- 全库计数 117,344；隐藏父目录后 89,532；`showHidden=true` 恢复 117,344。
- 明确进入隐藏父目录返回 27,812；进入其子目录返回 178（继承隐藏，但明确进入可查看）。
- 将该子目录独立设为隐藏后，父目录查询减少至 27,634；明确进入子目录仍返回 178。
- 全库 `limit=1` 的分页 `total=89,532`，与隐藏后的计数一致；清理全部标记后资产计数恢复 117,344。
- 四个原片挂载均 `RW=false`：`/volume2/photo`、`/volume2/myphoto/未处理Raw`、`/volume2/myphoto/已处理Raw`、`/volume2/myphoto/和川专属`。仅服务数据 `/myphoto/keeps` 可写。
- 升级前的扫描任务 `34d27418-7f2c-4434-9144-d8d1f063280c` 在重启后恢复 `running`；同一次运行的 `skipped` 从 137 增至 342，说明后台任务持续推进。重启后计数从当前轮重新统计，不代表丢失既有索引。
- 本次完成的是服务端升级和隐藏功能验收；首次全库扫描尚未完成。

本地必要验证：Rust 43 项通过、1 项既有 Linux 媒体运行时测试跳过；Clippy（`-D warnings`）及格式检查通过。HTTP 回归另覆盖搜索、精选/回收站过滤、重启持久化、混合路径资产保守隐藏、非法路径和 schema 1 → 2 升级。

## 回退条件

Schema 2 不能由旧镜像直接读取。需要回退时先停止服务并备份升级后双库，恢复本次升级前的 `control_plane.sqlite` 与 `jobs.sqlite`，检查一致性后将旧镜像重新标记为 `keeps-server:local` 并启动；不能仅换回镜像而保留 schema 2。升级后扫描新增的索引需要重新扫描恢复，原片不受影响。
