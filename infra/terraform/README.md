# Photo Asset Manager Infra

这里是 AWS control plane 和 S3 衍生图层的 Terraform 入口。

第一版 infra 边界：

- Terraform 已定义：
  - 当前仓默认提交单一 dev 环境的 S3 backend，state bucket / lock table 分别是 `photo-asset-manager-dev-665303623943-tfstate` 和 `photo-asset-manager-dev-665303623943-tfstate-lock`。
  - 远程 state 的 S3 bucket 和 DynamoDB lock table bootstrap 资源。
  - 默认 VPC 下的 Aurora PostgreSQL subnet group 和 security group 边界。
  - Aurora PostgreSQL Serverless v2 cluster + connection secret。
  - Lambda VPC 直连 PostgreSQL 的 runtime skeleton，以及最小 IAM / CloudWatch logs 边界。
  - 只用于 thumbnail/preview 的 derivative S3 bucket，开启 encryption、versioning、Block Public Access 和受控上传 CORS。
  - Lambda 访问 AWS APIs 所需的 VPC endpoints：Secrets Manager interface endpoint 和 S3 gateway endpoint。
  - API Gateway `$default` 路由默认使用 `AWS_IAM`，避免入口公开。
  - `runtime_image_uri` 必须显式传入已经打包 control-plane 应用的 Lambda image；Terraform 不提供空 base image 默认值。
  - 首次部署默认通过 `CONTROL_PLANE_AUTO_CREATE_SCHEMA=1` 让 Lambda 启动时自动创建 control-plane schema，避免空 Aurora 集群首次请求即失败。
- 仍需外部完成：
  - 多环境 backend 策略；当前仓只固定绑定单一 dev backend，不适合直接拿去做多账号/多环境复用。
  - 真正的 control-plane 代码镜像或函数实现。
  - 数据库 migration、健康检查与 smoke gate。
  - 如果后续要改成专用 VPC、私网出口或更细的 CORS 策略，需要重新调整 Terraform。

约束：

- 原片 canonical store 仍然是单一 NAS/server，不进入 S3。
- derivative bucket 只保存 thumbnail/preview，不保存原图。
- `terraform.tfstate` 只允许 backend 管理，不能提交；当前默认 backend 是远程 S3。
- 如果临时切回本地 backend，第一次 `terraform apply` 会把生成的 Aurora 密码和 secret version 写进本地 `tfstate`，因此完成迁移后要立即回到受控远程 state。
- 删除/重建注意：`deletion_protection=true` 会阻止直接 destroy，Aurora 需要先显式关闭保护才能销毁；`final_snapshot_identifier` 会在销毁时保留最终快照；Secrets Manager 的删除恢复窗口会让同名 secret 在短期内不能立刻重建。
- 默认实现可以回落到 default VPC；如果账号没有 default VPC，或生产环境使用现有私网子网，请显式传入 `vpc_id`、`lambda_subnet_ids`、`db_subnet_ids` 和 `route_table_ids`，不要默认假设 default subnet 就是最终部署拓扑。
- Secrets Manager 走 interface endpoint，S3 走 gateway endpoint，因此 Lambda 不需要公网出口来访问这两个 AWS API。

## 本地检查

```bash
terraform init
terraform fmt -check
terraform validate
terraform plan -var='runtime_image_uri=<account>.dkr.ecr.<region>.amazonaws.com/photo-asset-manager-control-plane:<tag>'
```

前提：Terraform `>= 1.9.0`，因为这里用了跨变量 validation 约束。

当前默认 backend：

1. 仓库默认使用已提交的 `backend.tf`，直接指向当前唯一 dev state。
2. 初次 checkout 后执行 `terraform init` 即可接入远程 backend。
3. `runtime_image_uri` 仍需在 `plan/apply` 时显式传入。

如果后续需要多环境或多账号：

1. 移除或改写仓库里的 `backend.tf`。
2. 改回 `backend.tf.example` / `-backend-config` / 环境专属 backend 文件方案。
3. 重新 `terraform init -reconfigure`，不要让不同环境共享同一份 state。
