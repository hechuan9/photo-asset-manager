# Control Plane

```bash
cd control_plane
export CONTROL_PLANE_DATABASE_URL='sqlite+pysqlite:///./control_plane.sqlite'
export DERIVATIVE_BUCKET_NAME='local-derivatives-dev'
# 或者仅本地开发：
# export CONTROL_PLANE_ALLOW_SQLITE_DEV=1
# 需要禁用自动建表时：
# export CONTROL_PLANE_AUTO_CREATE_SCHEMA=0
uv run pytest
```

可选启动：

```bash
cd control_plane
export CONTROL_PLANE_DATABASE_URL='postgresql+psycopg://user:pass@host/dbname'
export DERIVATIVE_BUCKET_NAME='photo-asset-manager-dev-123456789012-derivatives'
uv run uvicorn control_plane.app:app --reload
```

Derivative upload/download URLs use S3 presigned URLs for `DERIVATIVE_BUCKET_NAME`; tests inject a fake presigner and do not call AWS. `actorID == "server"` 和 trusted device IDs 只是第一版开发/测试授权 stub，不是生产认证边界。当前 Terraform 首次部署会设置 `CONTROL_PLANE_AUTO_CREATE_SCHEMA=1`，确保 Aurora 首次冷启动即可建出 control-plane 所需表；后续如果引入正式 migration，再把该开关切回 `0`。

Lambda image 入口是 `control_plane.lambda_handler.handler`。在 AWS runtime 中，应用会从 `DATABASE_CONNECTION_SECRET_ARN` 读取 Aurora 连接 secret 并组装 `CONTROL_PLANE_DATABASE_URL` 等价连接串。

构建 Lambda image：

```bash
docker buildx build \
  --platform linux/arm64 \
  -f control_plane/Dockerfile \
  -t photo-asset-manager-control-plane:local \
  .
```
