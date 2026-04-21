# Photo Asset Manager

本仓库包含 macOS 照片资产管理器客户端，以及后续 AWS control plane / S3 衍生图层的基础设施定义。

## 目录

- `macos/`：SwiftPM macOS app、测试和本地打包脚本。
- `control_plane/`：Aurora authoritative event store 的 FastAPI/SQLAlchemy 后端与测试。
- `infra/terraform/`：AWS control plane 与 S3 衍生图层的 Terraform 入口。
- `feature.md`：产品和同步架构设计记录。

## macOS app

```bash
cd macos
swift test
swift build
./scripts/pre_merge_gate.sh
./scripts/package_app.sh
open .build/app/PhotoAssetManager.app
```

根目录保留兼容入口：

```bash
./scripts/pre_merge_gate.sh
./scripts/package_app.sh
open macos/.build/app/PhotoAssetManager.app
```

## Infra

```bash
cd infra/terraform
terraform init
terraform fmt -check
terraform validate
```

当前 infra 目录已经定义了：

- Terraform remote state 的 S3 bucket 和 DynamoDB lock table bootstrap 资源。
- 默认 VPC 下的 Aurora PostgreSQL Serverless v2 cluster、DB subnet group 和基础 security group。
- Lambda VPC 直连 Aurora PostgreSQL 的 control-plane runtime skeleton，以及连接 Aurora secret 的最小 IAM 边界。
- 只用于 thumbnail / preview 的 derivative S3 bucket，启用 encryption、versioning、Block Public Access 和受控上传 CORS。
- Lambda 访问 AWS APIs 所需的 VPC endpoints：Secrets Manager interface endpoint 和 S3 gateway endpoint。
- API Gateway `$default` 路由默认使用 `AWS_IAM`，不是公开入口。
- `runtime_image_uri` 必须显式传入已经打包 control-plane 应用的 Lambda image；Terraform 不提供空 base image 默认值。

仍需手工完成：

- 先用本地 backend 初始化并 apply，拿到 state bucket / lock table 名称，再切换到 S3 backend。
- 部署实际 control-plane 代码或镜像。
- 数据库 migration、健康检查和 smoke gate。
- 第一次本地 `terraform apply` 会把生成的 Aurora 密码和 secret version 写入本地 `tfstate`；切 remote backend 后要确认 state 已迁移，再保护或清理本地 state 文件。
- 默认实现依赖默认 VPC 的可用子网；生产环境如果使用私网子网，请通过 `lambda_subnet_ids` 显式传入。
- Secrets Manager 通过 interface endpoint、S3 通过 gateway endpoint 访问，因此 Lambda 不需要公网出口来拿 secret 或访问 derivative bucket。

前提：Terraform `>= 1.9.0`，因为这套变量校验用了跨变量 validation 语法。

原片、RAW、sidecar canonical 不应进入 S3。

## Control plane

```bash
cd control_plane
export CONTROL_PLANE_DATABASE_URL='sqlite+pysqlite:///./control_plane.sqlite'
export DERIVATIVE_BUCKET_NAME='local-derivatives-dev'
# 或者仅本地开发：
# export CONTROL_PLANE_ALLOW_SQLITE_DEV=1
# 生产或迁移场景可显式禁用自动建表：
# export CONTROL_PLANE_AUTO_CREATE_SCHEMA=0
uv run pytest

docker buildx build \
  --platform linux/arm64 \
  -f control_plane/Dockerfile \
  -t photo-asset-manager-control-plane:local \
  .
```

第一版后端通过 control-plane API 访问 Aurora authoritative event store，不允许客户端直连数据库。`actorID == "server"` / trusted device 只是第一版开发和测试授权 stub，不是最终生产权限边界；生产环境应通过迁移管理 schema，并显式关闭自动建表。
