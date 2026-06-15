# Control Plane

```bash
cd control_plane
uv run pytest
```

NAS filesystem derivative backend 启动：

```bash
cd control_plane
export CONTROL_PLANE_DATABASE_URL='sqlite+pysqlite:////myphoto/keeps/db/control_plane.sqlite'
export DERIVATIVE_STORAGE_BACKEND=filesystem
export KEEPS_ROOT=/myphoto/keeps
export ORIGINAL_ROOT=/myphoto/library
export CONTROL_PLANE_PUBLIC_BASE_URL='http://localhost:2283'
uv run uvicorn control_plane.app:app --host 0.0.0.0 --port 2283
```

filesystem backend 只写 `/myphoto/keeps/previews`。`ORIGINAL_ROOT` 必须在 `KEEPS_ROOT` 外部；原片、RAW、sidecar canonical 和 canonical export 永远不放进 `/myphoto/keeps`。不使用 `files` 作为 Keeps 的磁盘目录名。

Derivative upload/download URLs 使用 control-plane 本地 upload/download URLs。`actorID == "server"` 和 trusted device IDs 只是第一版开发/测试授权 stub，不是生产认证边界。后续如果引入正式 migration，需要把 `CONTROL_PLANE_AUTO_CREATE_SCHEMA` 切回 `0` 并由迁移流程管理 schema。

构建 NAS runtime image：

```bash
docker buildx build \
  --platform linux/arm64 \
  -f control_plane/Dockerfile.nas \
  -t keeps-control-plane:local \
  .
```
