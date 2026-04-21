# Photo Asset Manager Infra

这里是 AWS control plane 和 S3 衍生图层的 Terraform 入口。

第一版 infra 边界：

- Terraform 已定义：
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
  - 先用本地 backend 初始化这套 Terraform，再切到 S3 backend。
  - 真正的 control-plane 代码镜像或函数实现。
  - 数据库 migration、健康检查与 smoke gate。
  - 如果后续要改成专用 VPC、私网出口或更细的 CORS 策略，需要重新调整 Terraform。

约束：

- 原片 canonical store 仍然是单一 NAS/server，不进入 S3。
- derivative bucket 只保存 thumbnail/preview，不保存原图。
- `terraform.tfstate` 只允许本地或远程 backend 管理，不能提交。
- 第一次本地 `terraform apply` 会把生成的 Aurora 密码和 secret version 写进本地 `tfstate`；切到远程 backend 后要用加密、权限受控的 state 存储，并确认 state 已迁移后再保护或清理本地文件。
- 删除/重建注意：`deletion_protection=true` 会阻止直接 destroy，Aurora 需要先显式关闭保护才能销毁；`final_snapshot_identifier` 会在销毁时保留最终快照；Secrets Manager 的删除恢复窗口会让同名 secret 在短期内不能立刻重建。
- 默认实现依赖默认 VPC 的可用子网和对应 route table；如果生产环境使用私网子网，请通过 `lambda_subnet_ids` 显式传入，不要默认假设 default subnet 就是最终部署拓扑。
- Secrets Manager 走 interface endpoint，S3 走 gateway endpoint，因此 Lambda 不需要公网出口来访问这两个 AWS API。

## 本地检查

```bash
terraform init
terraform fmt -check
terraform validate
terraform plan -var='runtime_image_uri=<account>.dkr.ecr.<region>.amazonaws.com/photo-asset-manager-control-plane:<tag>'
```

前提：Terraform `>= 1.9.0`，因为这里用了跨变量 validation 约束。

Bootstrap 推荐流程：

1. 先保留本地 backend，执行 `terraform init`、`terraform apply`。
2. 读取输出中的 `terraform_state_bucket_name` 和 `terraform_state_lock_table_name`。
3. 把 `backend.tf.example` 复制成 `backend.tf`，填入真实 bucket/table/key。
4. 再次 `terraform init` 切换到远程 backend。

当前仓库默认使用 AWS 托管远程状态的 bootstrap 资源，但 backend 本身不会自动替换自己，因此不能假装已经启用远程 state。
