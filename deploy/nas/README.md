# NAS 部署

NAS-first 部署把 Keeps 的服务端状态放在 `/myphoto/keeps` 下，但原片根目录必须放在 Keeps 外部。

推荐目录：

```text
/myphoto/
  keeps/
    db/
    ledger/
    previews/
    cache/
    ingest/
    exports/
    backups/
    logs/
    tmp/
  library/
```

约束：

- `/myphoto/keeps/previews` 保存 1200px `.heic` preview derivative。
- `/myphoto/keeps/db` 保存 Postgres 数据目录。
- `/myphoto/keeps/ledger` 用于后续 ledger snapshot、replay 校验和迁移工件。
- 原片、RAW、sidecar canonical 和 canonical export 永远不放在 `/myphoto/keeps` 下。
- `/myphoto/library` 第一阶段以只读方式挂给 control-plane，防止服务端误改原片。
- 不使用 `files` 作为 Keeps 的磁盘目录名。

启动：

```bash
cd deploy/nas
cp .env.example .env
docker compose up -d --build
```

最小检查：

```bash
curl http://localhost:2283/healthz
docker compose exec postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"
test ! -e /myphoto/keeps/files
```
